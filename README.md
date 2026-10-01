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

## 记录模型

一条幂等记录在同一对象中保存幂等键、请求指纹、首次响应快照、创建时间和过期时间。记录一经保存不可修改：重复提交不会覆盖首次响应快照，指纹冲突的提交不会改动原记录。记录字段顺序固定如下：

```json
{
  "record_id": "rec_…",
  "status": "stored",
  "idempotency_key": "order-1",
  "request_fingerprint": "sha256:…",
  "response_snapshot": {"status": "ok"},
  "created_at": "2026-10-01T08:00:00Z",
  "expires_at": "2026-10-01T09:00:00Z"
}
```

- 时间字段统一使用 RFC 3339（纳秒精度时保留纳秒）并归一到 UTC。
- `status` 目前只有 `stored`；过期记录不作为正常结果返回。
- 每个幂等键同一时刻至多有一条有效（未过期）记录；过期后允许以新参数重新提交，旧行保留在底层存储中，不做物理删除。

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

### `POST /v1/idempotency/records`

提交一条幂等记录。请求体字段固定：

```json
{
  "idempotency_key": "order-1",
  "request_fingerprint": "sha256:…",
  "response_snapshot": {"status": "ok"},
  "expires_at": "2026-10-01T09:00:00Z"
}
```

- 首次提交成功返回 HTTP 201，体内为 `{"record": …}`。
- 幂等键未过期且请求指纹一致的重复提交返回 HTTP 200，体内结构与首次成功完全一致，原样返回首次保存的 `response_snapshot` 与同一个 `record_id`，不产生第二条可见记录。
- 幂等键未过期但请求指纹不一致返回 HTTP 409：

```json
{
  "error": {
    "code": "idempotency_fingerprint_conflict",
    "message": "a record for this idempotency key already exists with a different request fingerprint",
    "existing_record_id": "rec_…",
    "existing_request_fingerprint": "sha256:…"
  }
}
```

- 幂等键为空、请求指纹为空、`expires_at` 缺失/无法解析/不晚于创建时间时返回 HTTP 400，错误码 `invalid_idempotency_record`，且不写入任何记录。
- `response_snapshot` 可以是任意 JSON 值；缺省时按 JSON `null` 保存。

### `GET /v1/idempotency/records/{idempotency_key}`

按键查询唯一有效记录，返回 HTTP 200 与 `{"record": …}`。

- 键不存在或对应记录已过期时统一返回 HTTP 404，错误码 `not_found`，不暴露历史响应快照，也不创建占位记录。
- 查询结果只包含该键自己的记录，不会与其他记录混合。

### `GET /v1/idempotency/records`

按条件筛选有效记录。结果按创建时间倒序排列（创建时间相同时按 `record_id` 倒序），过期记录不参与结果。查询参数：

| 参数 | 说明 |
|---|---|
| `status` | 仅支持 `stored`；其他值返回 `invalid_idempotency_record` |
| `expires_after` | RFC 3339 时间，包含边界；仅返回过期时间不早于该值的记录 |
| `expires_before` | RFC 3339 时间，包含边界；仅返回过期时间不晚于该值的记录 |
| `limit` | 每页大小，1–100，默认 20；越界返回 `invalid_idempotency_record` |
| `cursor` | 上一页返回的 `next_cursor` |

响应结构固定：

```json
{
  "records": [ { "record_id": "…", "status": "stored", "…": "…" } ],
  "next_cursor": "v1.…"
}
```

- 没有更多记录时 `next_cursor` 为空字符串。
- `records` 始终是数组，没有匹配时为空数组。
- 游标无法解码时返回 HTTP 400，错误码 `invalid_cursor`，不回退为默认首页。

## 并发与过期

- 并发提交同一幂等键时，以先成功写入的记录为准；后续请求要么重放首次快照，要么收到指纹冲突。
- 过期判断以服务端当前时间为准。过期记录在按键查询与列表筛选中均不可见；底层历史数据的清理保持现状，本服务不主动物理删除。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含固定的 `code` 与 `message` 字符串字段（指纹冲突额外包含 `existing_record_id`、`existing_request_fingerprint`）；`message` 不包含 SQL、堆栈或文件路径。

本次新增错误码：

| 错误码 | HTTP 状态 | 含义 |
|---|---|---|
| `invalid_idempotency_record` | 400 | 提交参数或筛选参数不合法 |
| `idempotency_fingerprint_conflict` | 409 | 同键已存在且请求指纹不同 |
| `not_found` | 404 | 不存在有效记录 |
| `invalid_cursor` | 400 | 分页游标无效 |
