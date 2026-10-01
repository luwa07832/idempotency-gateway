# idempotency-gateway

把幂等键、请求指纹、首次响应快照和过期时间记录成可查询的服务，支持重复请求返回首次结果并识别指纹冲突。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `idempotency-gateway.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

## 幂等记录

一条记录把幂等键、请求指纹、首次响应快照、创建时间和过期时间作为同一整体保存；记录一经写入即不可变，重复提交不会更新快照，过期后重新提交会插入新记录，底层历史行不做物理删除。所有时间均为 RFC 3339 UTC 字符串；`response_snapshot` 接受任意 JSON 值并按首次提交的字节原样回放。

记录对象的字段与顺序固定为：`id`、`idempotency_key`、`status`（记录有效时恒为 `active`）、`request_fingerprint`、`response_snapshot`、`created_at`、`expires_at`。

### `POST /v1/idempotency/records`

提交幂等记录。请求体字段顺序不限：

```json
{
  "idempotency_key": "order-123",
  "request_fingerprint": "sha256:9c28...",
  "response_snapshot": {"status": "paid"},
  "expires_at": "2026-10-01T12:00:00Z"
}
```

- 首次提交：HTTP 200，返回 `{"record":{...}}`，`id` 形如 `rec_` 加 32 个小写十六进制字符。
- 幂等键相同且请求指纹一致（记录未过期）：HTTP 200，逐字节返回第一次保存的响应快照与同一记录标识，不产生第二条可见记录。
- 幂等键相同但请求指纹不同：HTTP 409，返回顶层 `error`，其中 `code` 为 `idempotency_fingerprint_conflict`，并带固定顺序的 `record_id` 与 `request_fingerprint`（已有记录的），原记录保持不变。
- 幂等键为空、请求指纹为空、`expires_at` 缺失或无法解析、或 `expires_at` 不严格晚于当前时刻（相等或更早）：HTTP 400，`code` 为 `invalid_idempotency_record`，不写入记录。`response_snapshot` 省略时按 `null` 存储，但必须是合法 JSON。

### `GET /v1/idempotency/records/:idempotency_key`

按键查询，成功返回 `{"record":{...}}`。键不存在或记录已过期时统一返回 HTTP 404，`code` 为 `not_found`，不返回历史响应快照，也不会自动创建占位记录。

### `GET /v1/idempotency/records`

列出未过期记录，按创建时间倒序（同时间以记录 `id` 倒序作为确定性兜底）。返回 `{"records":[...],"next_cursor":""}`，`next_cursor` 为空表示没有下一页。

| 查询参数 | 说明 |
|---|---|
| `key` | 仅返回该幂等键的记录，不会混入其他记录 |
| `status` | `active`（默认，等价于不传）；`expired` 恒返回空页，过期记录不参与正常结果；其他取值返回 `invalid_idempotency_record` |
| `expires_before` | RFC 3339 时间，仅返回严格早于该时间过期的记录 |
| `expires_after` | RFC 3339 时间，仅返回严格晚于该时间过期的记录 |
| `limit` | 每页条数，1–100，默认 50；非法值返回 `invalid_idempotency_record` |
| `cursor` | 上一页返回的不透明分页游标；游标缺失字段或无法解码时返回 HTTP 400，`code` 为 `invalid_cursor` |

游标为键集分页（编码了上一页末尾记录的创建时间与 `id`），翻页期间新插入的记录不会让更早的页重复或错位。

## 多实例部署

多个服务实例可以同时指向同一个 `DB_PATH` 文件。提交路径在单个 `BEGIN IMMEDIATE` 事务内完成"检查再插入"，SQLite 的文件锁会把跨实例的并发写入串行化：后到者等待先提交者（上限为 `busy_timeout` 5 秒），随后读到已提交的获胜记录并按回放或指纹冲突处理，因此同一幂等键在任何时刻只有一条未过期记录可见。等待超时或提交结果无法确认时提交入口返回 503 `storage_unavailable`；只读入口（查询、列表、健康检查）在 WAL 模式下不被写锁阻塞。过期后的并发重提同样只新增一条记录，历史行保留在底层表中。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。指纹冲突结果同样以顶层 `error` 呈现，并在其后固定附带 `record_id` 与 `request_fingerprint`。存储不可用时所有幂等入口返回 HTTP 503，`code` 为 `storage_unavailable`。
