"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
            },
        )
        r.raise_for_status()


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


# --------------------------------------------------------------------- #
# concurrent content swaps on the origin                                #
# --------------------------------------------------------------------- #

SWAP_SIZES = [300_000, 32 * MIB, MIB, 8 * MIB, 700_000, 20 * MIB, 100_000, 4 * MIB]


def swap_content(origin_url: str, path: str, size: int) -> tuple[int, dict]:
    with client(timeout_read=300) as c:
        r = c.post(f"{origin_url}/control/content/{path}", params={"size": size})
        return r.status_code, r.json() if r.status_code == 200 else {"text": r.text}


def assert_origin_consistent(origin_url: str, path: str) -> dict:
    """Body length and digest served by the origin must match its stats."""
    with client(timeout_read=300) as c:
        body = c.get(f"{origin_url}/packages/{path}").content
    stats = origin_stats(origin_url)["packages"][path]
    assert len(body) == stats["size"], f"body {len(body)} != stats {stats['size']}"
    assert sha256(body) == stats["sha256"]
    return stats


def test_concurrent_same_path_swaps_each_publish_complete_package(origin):
    path = "swap-same.bin"
    for round_no in range(3):
        with ThreadPoolExecutor(len(SWAP_SIZES)) as ex:
            results = list(ex.map(lambda s: swap_content(origin.url, path, s), SWAP_SIZES))

        assert all(code == 200 for code, _ in results), results
        digests = [body["sha256"] for _, body in results]
        assert len(set(digests)) == len(digests), f"round {round_no}: duplicate digests"
        for size, (_, body) in zip(SWAP_SIZES, results):
            assert body["size"] == size
            assert body["path"] == path

        # The body actually published must be one of the successful requests'
        # complete package, and stats must describe that same version.
        stats = assert_origin_consistent(origin.url, path)
        assert stats["sha256"] in set(digests), f"round {round_no}: unknown final version"
        assert stats["size"] in SWAP_SIZES

    temps = list(origin.dir.rglob("*.tmp"))
    assert temps == [], f"temp files left behind: {temps}"


def test_concurrent_swaps_on_different_paths_run_in_parallel(origin):
    paths = [f"par-{i}.bin" for i in range(4)]
    with ThreadPoolExecutor(len(paths)) as ex:
        results = list(ex.map(lambda p: swap_content(origin.url, p, 2 * MIB), paths))
    assert all(code == 200 for code, _ in results), results
    for p in paths:
        assert_origin_consistent(origin.url, p)


def test_per_path_locks_are_distinct_and_stable():
    import asyncio
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "origin_app_locks", ROOT / "origin" / "app.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    async def check():
        a1, a2 = mod._lock_for("a.bin"), mod._lock_for("a.bin")
        b = mod._lock_for("b.bin")
        assert a1 is a2, "same path must share one publish lock"
        assert a1 is not b, "different paths must not share a lock"
        assert not a1.locked() and not b.locked()

    asyncio.run(check())


def test_downloads_during_swaps_stay_coherent(origin):
    """Every GET is one complete version: body, Content-Length and digest."""
    path = "swap-live.bin"
    assert swap_content(origin.url, path, MIB)[0] == 200

    sizes = [300_000, 8 * MIB, 20 * MIB, MIB, 4 * MIB, 700_000]
    stop = threading.Event()
    problems: list[str] = []

    def reader():
        with client(timeout_read=300) as c:
            while not stop.is_set():
                r = c.get(f"{origin.url}/packages/{path}")
                if r.status_code != 200:
                    problems.append(f"status {r.status_code}: {r.text[:100]}")
                    return
                body = r.content
                advertised = r.headers.get("x-package-sha256")
                clen = r.headers.get("content-length")
                if clen is not None and int(clen) != len(body):
                    problems.append(f"Content-Length {clen} != body {len(body)}")
                if advertised and advertised != sha256(body):
                    problems.append("advertised digest != body digest")
                if len(body) not in sizes and len(body) != MIB:
                    problems.append(f"unexpected body size {len(body)}")

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        with ThreadPoolExecutor(len(sizes)) as ex:
            results = list(ex.map(lambda s: swap_content(origin.url, path, s), sizes))
        assert all(code == 200 for code, _ in results), results
        time.sleep(0.5)
    finally:
        stop.set()
        for t in threads:
            t.join(120)

    assert problems == [], problems[:5]

    # After the dust settles, body/length/digest and stats agree on one version.
    stats = assert_origin_consistent(origin.url, path)
    assert stats["sha256"] in {body["sha256"] for _, body in results}


def test_failed_swap_keeps_last_published_package(tmp_path, monkeypatch):
    """Endpoint-level: a swap that fails to publish must change nothing."""
    import importlib.util

    from fastapi.testclient import TestClient

    spec = importlib.util.spec_from_file_location(
        "origin_app_fail", ROOT / "origin" / "app.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "PKG_DIR", tmp_path)
    monkeypatch.setattr(mod, "DEFAULT_PACKAGES", {})

    path = "swap-fail.bin"
    with TestClient(mod.app) as c:
        r = c.post(f"/control/content/{path}", params={"size": 200_000})
        assert r.status_code == 200, r.text
        good = r.json()
        before_body = c.get(f"/packages/{path}").content
        assert len(before_body) == 200_000
        assert sha256(before_body) == good["sha256"]

        # Publish (atomic rename) now fails for every swap.
        def fail_replace(*_a, **_kw):
            raise OSError("injected rename failure")

        real_replace = mod.os.replace
        monkeypatch.setattr(mod.os, "replace", fail_replace)
        try:
            r = c.post(f"/control/content/{path}", params={"size": 700_000})
            assert r.status_code == 500, r.text
            assert "publish failed" in r.json()["detail"]
        finally:
            monkeypatch.setattr(mod.os, "replace", real_replace)

        # Last good version intact and still coherent with stats.
        stats = c.get("/control/stats").json()["packages"][path]
        assert stats == {"sha256": good["sha256"], "size": 200_000}
        body = c.get(f"/packages/{path}").content
        assert body == before_body
        assert list(tmp_path.glob("*.tmp")) == [], "failed swap leaked a temp file"

        # Generation failure (before publish) equally leaves everything alone.
        def fail_generate(*_a, **_kw):
            raise OSError("injected generation failure")

        real_generate = mod._generate
        monkeypatch.setattr(mod, "_generate", fail_generate)
        try:
            r = c.post(f"/control/content/{path}", params={"size": 300_000})
            assert r.status_code == 500, r.text
            assert "generation failed" in r.json()["detail"]
        finally:
            monkeypatch.setattr(mod, "_generate", real_generate)

        assert c.get("/control/stats").json()["packages"][path] == stats
        assert c.get(f"/packages/{path}").content == before_body

        # And a normal swap still works afterwards.
        r = c.post(f"/control/content/{path}", params={"size": 300_000})
        assert r.status_code == 200
        new = r.json()
        body = c.get(f"/packages/{path}").content
        assert len(body) == 300_000 and sha256(body) == new["sha256"]
        assert c.get("/control/stats").json()["packages"][path] == {
            "sha256": new["sha256"], "size": 300_000
        }


def test_proxy_serves_coherent_final_version_after_concurrent_swaps(proxy, origin):
    """Requirement end-to-end: after concurrent swaps, purge + download must
    return exactly the package that /control/stats reports."""
    path = PKG_SMALL
    with ThreadPoolExecutor(len(SWAP_SIZES)) as ex:
        swaps = list(ex.map(lambda s: swap_content(origin.url, path, s), SWAP_SIZES))
    assert all(code == 200 for code, _ in swaps), swaps
    final = origin_stats(origin.url)["packages"][path]
    assert final["size"] in SWAP_SIZES

    with client() as c:
        assert c.post(f"{proxy.url}/purge/{path}").json()["was_cached"] in (True, False)

    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, path), range(6)))

    # All concurrent downloads agree with each other and with the origin stats.
    assert all(b == bodies[0] for b in bodies)
    assert len(bodies[0]) == final["size"]
    assert sha256(bodies[0]) == final["sha256"]

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{path}")
    assert r.status_code == 200
    assert r.headers["content-length"] == str(final["size"])
    assert r.headers["x-package-sha256"] == final["sha256"]
    assert sha256(r.content) == final["sha256"]
    assert origin_stats(origin.url)["packages"][path] == final
    assert origin_stats(origin.url)["paths"][path] == 1  # one shared refill

    # A failed swap must not corrupt what the proxy serves next.
    with client() as c:
        assert c.post(f"{proxy.url}/purge/{path}").status_code == 200
    good_body = download_bytes(proxy.url, path)
    assert sha256(good_body) == final["sha256"]


def test_proxy_downloads_during_concurrent_swaps(proxy, origin):
    """Downloads racing swaps must each get one complete, self-consistent pack
    **or** a clean failure; nothing may mix a long tail into a short package."""
    path = "swap-proxy-live.bin"
    assert swap_content(origin.url, path, MIB)[0] == 200
    with client() as c:
        c.post(f"{proxy.url}/purge/{path}")

    sizes = [300_000, 8 * MIB, 20 * MIB, MIB, 4 * MIB, 700_000]
    problems: list[str] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                with client(timeout_read=300) as c:
                    c.post(f"{proxy.url}/purge/{path}")
                    r = c.get(f"{proxy.url}/packages/{path}")
            except httpx.HTTPError:
                # A swap landing between purge and GET may legitimately race the
                # origin; retry rather than judging a torn read.
                continue
            if r.status_code != 200:
                problems.append(f"status {r.status_code}: {r.text[:120]}")
                return
            body = r.content
            clen = r.headers.get("content-length")
            advertised = r.headers.get("x-package-sha256")
            if clen is not None and int(clen) != len(body):
                problems.append(f"Content-Length {clen} != body {len(body)}")
            if advertised and advertised != sha256(body):
                problems.append("digest header != body digest")
            if len(body) not in sizes and len(body) != MIB:
                problems.append(f"torn body: size {len(body)}")

    threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    try:
        with ThreadPoolExecutor(len(sizes)) as ex:
            swaps = list(ex.map(lambda s: swap_content(origin.url, path, s), sizes))
        assert all(code == 200 for code, _ in swaps), swaps
        time.sleep(0.3)
    finally:
        stop.set()
        for t in threads:
            t.join(120)

    assert problems == [], problems[:5]

    # Final state: purge, download, compare to stats.
    with client() as c:
        c.post(f"{proxy.url}/purge/{path}")
    body = download_bytes(proxy.url, path)
    final = origin_stats(origin.url)["packages"][path]
    assert len(body) == final["size"] and sha256(body) == final["sha256"]


def test_publish_failure_leaves_previous_version_record(tmp_path, monkeypatch):
    """If the atomic rename fails, the version record must not be updated."""
    import asyncio
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "origin_app_unit", ROOT / "origin" / "app.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "PKG_DIR", tmp_path)

    (tmp_path / "bad.bin").mkdir()  # rename over a non-empty dir fails
    (tmp_path / "bad.bin" / "child").write_bytes(b"x")
    tmp_file = tmp_path / ".pkgswap-unit.tmp"
    tmp_file.write_bytes(b"new content")
    mod.published["bad.bin"] = {"sha256": "old-digest", "size": 1}

    with pytest.raises(OSError):
        asyncio.run(mod.publish("bad.bin", tmp_file, "new-digest", 11))

    assert mod.published["bad.bin"] == {"sha256": "old-digest", "size": 1}
    assert tmp_file.exists()  # caller (endpoint) still owns cleanup
