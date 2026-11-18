"""Controllable local package origin used to demo/test the caching proxy.

Serves complete packages from PKG_DIR (a 32 MiB package is generated on
first boot). Control endpoints let you change content, add per-chunk delay,
cut the stream mid-body, and inspect how many times the proxy fetched.

Content swaps
-------------
``POST /control/content/{path}`` is safe under concurrency:

* Each request materialises its replacement in a *unique* temporary file in
  the destination directory, so concurrent swaps of the same path never share
  a file descriptor or buffer.
* Publishing is atomic per path: under that path's asyncio lock the finished
  temp file is ``os.replace``-d over the live package and the
  ``{sha256, size}`` record is updated in the same critical section.  Other
  paths use other locks and keep swapping in parallel.
* A failed request (generation or publish) deletes only its own temp file and
  leaves the last successfully published package and digest untouched.
* Downloads snapshot the published ``{sha256, size}`` and open the package
  file under the same per-path lock, then stream from that stable inode.
  Body, ``Content-Length`` and ``X-Package-Sha256`` therefore always describe
  one complete version, and ``/control/stats`` reports that same version.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

PKG_DIR = Path(os.environ.get("PKG_DIR", "./packages")).resolve()
DEFAULT_PACKAGES = {
    "pkg-32m.bin": 32 * 1024 * 1024,
    "pkg-1m.bin": 1 * 1024 * 1024,
}
REGEN_BLOCK = 1024 * 1024

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")

behavior = {
    "chunk_delay_ms": 0,
    "truncate_after_bytes": None,  # int | None
    "chunk_size": 256 * 1024,
}
hits = {"total": 0, "paths": {}}

# Current published version per path: key -> {"sha256": str, "size": int}.
# Replaced as a whole under the path's lock, so a reader never sees a
# half-updated version.
published: dict[str, dict] = {}

# One asyncio.Lock per package path.  They serialise publication/digest
# updates for that path only, never generation and never other paths.
_path_locks: dict[str, asyncio.Lock] = {}


class UpstreamAbort(Exception):
    """Raised mid-stream to simulate an upstream connection abort."""


def validate_key(raw: str) -> str:
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _lock_for(key: str) -> asyncio.Lock:
    """Return the per-path publish lock (event loop only, no await inside)."""
    lock = _path_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _path_locks[key] = lock
    return lock


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _generate(key: str, size: int) -> tuple[Path, str]:
    """Write ``size`` fresh random bytes to a private temp file.

    The temp file lives in the destination directory so publishing is a
    same-filesystem rename.  Its name is unique per request, which is what
    makes concurrent swaps of the same path safe.  On any failure the temp
    file is removed and the exception propagates: the published package is
    not touched.
    """
    final = PKG_DIR / key
    final.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=final.parent, prefix=".pkgswap-",
                                    suffix=".tmp")
    tmp = Path(tmp_name)
    h = hashlib.sha256()
    try:
        os.chmod(tmp, 0o644)
        with os.fdopen(fd, "wb") as f:
            fd = -1  # ownership moved to the file object
            left = size
            while left > 0:
                block = os.urandom(min(REGEN_BLOCK, left))
                f.write(block)
                h.update(block)
                left -= len(block)
            written = f.tell()
        if written != size:
            raise OSError(f"short write while generating package: {written}/{size}")
        return tmp, h.hexdigest()
    except BaseException:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        _unlink_quiet(tmp)
        raise


def _hash_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for block in iter(lambda: f.read(REGEN_BLOCK), b""):
            h.update(block)
    return h.hexdigest()


def _cleanup_temp_files() -> None:
    """Drop temp files left behind by a crashed generator.

    Current generators use ``.pkgswap-*.tmp``; older builds used
    ``<package>.tmp``, which is removed only when the real package exists so a
    legitimate package whose name merely ends in ``.tmp`` is preserved.
    """
    for p in PKG_DIR.rglob("*"):
        if not p.is_file():
            continue
        if p.name.startswith(".pkgswap-") and p.name.endswith(".tmp"):
            _unlink_quiet(p)
        elif p.name.endswith(".tmp") and p.with_name(p.name[: -len(".tmp")]).is_file():
            _unlink_quiet(p)


def _index_existing() -> None:
    """Hash every on-disk package so restarts keep serving matching digests."""
    _cleanup_temp_files()
    for p in PKG_DIR.rglob("*"):
        if not p.is_file():
            continue
        if p.name.startswith(".pkgswap-") and p.name.endswith(".tmp"):
            continue  # a generator crashed before publishing it
        key = p.relative_to(PKG_DIR).as_posix()
        try:
            size = p.stat().st_size
            sha = _hash_file(p)
        except OSError:
            continue
        published[key] = {"sha256": sha, "size": size}


async def publish(key: str, tmp: Path, sha: str, size: int) -> None:
    """Atomically make ``tmp`` the live package for ``key`` and record it.

    The rename and the version record update happen under the path's lock, so
    a concurrent download or ``/control/stats`` sees either the old or the new
    version -- never a body from one and a digest from the other.  Callers own
    ``tmp``: on failure it is left in place and the previous version stands.
    """
    async with _lock_for(key):
        os.replace(tmp, PKG_DIR / key)
        published[key] = {"sha256": sha, "size": size}


@asynccontextmanager
async def lifespan(app: FastAPI):
    PKG_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(_index_existing)
    for name, size in DEFAULT_PACKAGES.items():
        if not (PKG_DIR / name).is_file():
            tmp, sha = await asyncio.to_thread(_generate, name, size)
            await publish(name, tmp, sha, size)
    yield


app = FastAPI(title="pkg-origin", version="1.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}", summary="Download a complete package")
async def get_package(path: str):
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    # Count every fetch attempt against the origin, successful or not.
    hits["total"] += 1
    hits["paths"][key] = hits["paths"].get(key, 0) + 1

    beh = dict(behavior)  # snapshot for this request
    p = PKG_DIR / key

    # Snapshot one published version: the open fd pins the inode, so a swap
    # that lands while we stream cannot change the bytes, the length or the
    # advertised digest underneath us.
    async with _lock_for(key):
        try:
            f = open(p, "rb")
        except OSError:
            return PlainTextResponse("package not found", status_code=404)
        meta = published.get(key)
        size = os.fstat(f.fileno()).st_size
        sha = meta["sha256"] if meta is not None and meta["size"] == size else None

    async def body():
        sent = 0
        chunk_size = max(1, int(beh["chunk_size"]))
        delay = max(0, int(beh["chunk_delay_ms"])) / 1000.0
        trunc = beh["truncate_after_bytes"]
        try:
            while True:
                if delay:
                    await asyncio.sleep(delay)
                chunk = await asyncio.to_thread(f.read, chunk_size)
                if not chunk:
                    break
                if trunc is not None and sent + len(chunk) > trunc:
                    remain = max(0, int(trunc) - sent)
                    if remain:
                        sent += remain
                        yield chunk[:remain]
                    # Simulate upstream abort: die mid-body after a partial
                    # response so the client sees a truncated payload.
                    raise UpstreamAbort("simulated upstream abort")
                sent += len(chunk)
                yield chunk
        finally:
            f.close()

    headers = {"Content-Length": str(size)}
    if sha:
        headers["X-Package-Sha256"] = sha
    return StreamingResponse(body(), media_type="application/octet-stream", headers=headers)


class Behavior(BaseModel):
    chunk_delay_ms: int = Field(0, ge=0, description="sleep per streamed chunk")
    truncate_after_bytes: int | None = Field(
        None, ge=0, description="abort the stream after this many bytes"
    )
    chunk_size: int = Field(256 * 1024, ge=1, description="origin stream chunk size")


@app.post("/control/behavior")
async def set_behavior(b: Behavior):
    """Set delay / truncation for subsequent package downloads."""
    behavior.update(b.model_dump())
    return behavior


@app.get("/control/behavior")
async def get_behavior():
    return behavior


@app.post("/control/content/{path:path}", summary="Regenerate package content")
async def change_content(path: str, size: int | None = None):
    """Regenerate the package with fresh random content (new sha256).

    Concurrent calls for the same path each build their own temp file and
    publish atomically; every successful response returns the digest of the
    complete package that request generated.  A failed call changes nothing.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    final = PKG_DIR / key
    if size is None:
        async with _lock_for(key):
            meta = published.get(key)
            if meta is not None:
                size = meta["size"]
            elif final.is_file():
                size = final.stat().st_size
            else:
                size = 32 * 1024 * 1024
    if size <= 0:
        return PlainTextResponse("size must be positive", status_code=400)

    # Generation runs outside the publish lock: same-path swaps may build in
    # parallel and different paths never wait for each other.
    try:
        tmp, sha = await asyncio.to_thread(_generate, key, size)
    except Exception as exc:  # noqa: BLE001 - report any generator/IO failure
        raise HTTPException(
            status_code=500,
            detail=f"content generation failed: {type(exc).__name__}: {exc}",
        ) from exc

    try:
        await publish(key, tmp, sha, size)
    except Exception as exc:  # noqa: BLE001 - publish failed, keep previous version
        _unlink_quiet(tmp)
        raise HTTPException(
            status_code=500,
            detail=f"content publish failed: {type(exc).__name__}: {exc}",
        ) from exc

    return {"path": key, "size": size, "sha256": sha}


@app.get("/control/stats")
async def stats():
    """Origin fetch counters (what the proxy pulled) and current digests."""
    keys = set(published)
    try:
        for p in PKG_DIR.rglob("*"):
            if p.is_file() and not p.name.startswith(".pkgswap-"):
                keys.add(p.relative_to(PKG_DIR).as_posix())
    except OSError:
        pass

    packages = {}
    for key in sorted(keys):
        # Same lock as download/publish: report an entire version, never a
        # size from one generation and a digest from another.  Packages that
        # appeared on disk after boot (no published record) are hashed on
        # demand so stats never advertises a digest the body does not match.
        async with _lock_for(key):
            meta = published.get(key)
            if meta is not None:
                # Drop records whose package vanished from disk (e.g. an
                # operator removed it): stats must never describe a body that
                # /packages would answer with 404.
                try:
                    if not (PKG_DIR / key).is_file():
                        meta = None
                except OSError:
                    meta = None
                if meta is None:
                    published.pop(key, None)
            if meta is None:
                p = PKG_DIR / key
                try:
                    meta = {"sha256": await asyncio.to_thread(_hash_file, p),
                            "size": p.stat().st_size}
                except OSError:
                    meta = None
                if meta is not None:
                    published[key] = meta
            if meta is not None:
                packages[key] = {"size": meta["size"], "sha256": meta["sha256"]}
    return {
        "total_hits": hits["total"],
        "paths": dict(hits["paths"]),
        "packages": packages,
        "behavior": dict(behavior),
    }


@app.post("/control/reset")
async def reset():
    hits["total"] = 0
    hits["paths"] = {}
    return {"ok": True}
