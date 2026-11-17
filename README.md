# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

**请求被取消（客户端断开、上游取消慢查询）同样走完整收尾**：排队时取消
不占用连接；已送到数据库的语句会请求取消并等待其结束，确认事务结束、连接
空闲才归还，否则弃置补建。清理期间再次取消不会丢失或重复释放连接名额，
也不会把仍在执行的连接借给别的请求。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
docker compose exec api python -m tests.test_disconnect
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |
| 客户端已断开（上游取消） | 499 | `error.code=client_disconnected`；回执已无人接收，语句取消与连接收尾在后台完成 |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire_lease(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。排队
期间被取消时，信号量等待者被撤销并交还名额，**从未借出连接**。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 取消安全的执行与收尾（`app/executor.py`）

请求协程可能在任意 `await` 处被取消。若此时把借出的连接丢下不管，语句仍
在后端运行而名额再也不会归还——两次这样的取消就会占满两条连接，正是本次
故障的成因。因此：

1. 每次执行都在 executor 自有的 **worker 任务**中运行，与请求任务用
   `shield` 隔离；请求取消时由 worker（而非垂死的请求）完成收尾。
2. 收尾协议本身在**独立的 settler 任务**里执行并再次 `shield`，因此**收尾
   期间再次取消**也不会打断它：回滚、补建、空闲确认会跑完，名额恰好归还
   一次。`PoolLease` 的释放是**幂等**的（同步 claim + `finally` 释放）。
3. 异常收尾统一为：`PQcancel` → 等待语句任务真正结束 → 若无法结束则关闭
   连接（关闭即中止后端查询）并等任务收束 → `ROLLBACK` 并核对
   `transaction_status == IDLE` → 确认后才归还，否则弃置并补建。后端仍为
   `ACTIVE` 时拒绝复用，直接弃置。
4. 若收尾本身被取消到无法确认连接状态，则该连接**永不外借**：名额立即
   归还，连接在后台关闭，池自动补建容量。
5. 迟到完成的结果只做审计日志，回执固定为超时且不携带 `rows`。
6. HTTP 层额外监听 `http.disconnect`：ASGI 服务器不会因客户端断开而取消
   处理函数，不监听的话断开的调用仍会占着连接直到自身超时。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。补建在独立任务中
进行，即使触发补建的收尾任务被取消也会完成；若补建暂时失败，池临时缩水并
在下一次获取时懒建补回，不会死锁。补建与借出共用容量锁，物理后端数不会
超过 2。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # HTTP 端到端自测（46 项）
python -m tests.test_disconnect  # HTTP 客户端断开/上游取消（11 项）
python tests/test_protocol.py    # 超时/迟到结果/排队协议直测（11 项）
python tests/test_cancel.py      # 取消/二次取消/名额不变量直测（31 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_disconnect.py（HTTP 断开取消）、
                test_protocol.py（超时协议直测）、test_cancel.py（取消协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
