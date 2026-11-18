# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
# 可用 ?size=字节数 指定新长度；同路径可并发调用，见下方“并发换包”
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。

### 并发换包（源站 `POST /control/content/{path}`）

多个构建工程师同时重建同一路径时，换包请求可能并发。源站保证：

* **各自生成、各自发布**：每个请求用唯一临时文件（同目录 `.pkgswap-*.tmp`）
  生成完整包；发布是该路径锁内的一次原子 `os.replace` 加版本记录更新。
  每个成功响应返回**自己生成的完整包**的 `size` 与 `sha256`，不会把两个
  包混在一起。
* **失败不影响上一次成功发布**：生成或发布失败返回 500，只清理自己的
  临时文件，已发布的包和 `/control/stats` 中的摘要/长度保持不变。
* **读到的永远是一个完整版本**：下载在路径锁内打开文件并快照
  `size`/`sha256`，之后流式读取固定 inode；因此正文、`Content-Length`、
  `X-Package-Sha256` 与 `/control/stats` 始终描述同一个完整版本。
* **不同路径互不阻塞**：锁按路径划分，只有同一路径的发布点串行；生成
  阶段完全并行。

代理侧无需改动：换包后仍需 `POST /purge/{path}` 才会重新回源；清除和流式
下载语义与之前一致（旧代下载继续读完，半包不入缓存）。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越；以及并发换包：同路径多尺寸并发换包的每个响应自洽、
换包期间下载的正文/长度/摘要一致、不同路径并行、生成与发布失败均不改写
上一次成功版本，且最终 `/packages` 与 `/control/stats` 一致。
`tests/run_server.py` 是测试专用启动器（本沙箱禁止
进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身不含测试钩子。
