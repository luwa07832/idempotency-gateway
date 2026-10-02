# idempotency-gateway

把幂等键、请求指纹、首次响应快照和过期时间记录成可查询的服务，支持重复请求返回首次结果并识别指纹冲突。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

多个服务实例可以把 `DB_PATH` 指向同一个数据库文件组成并发部署。写入事务以
`BEGIN IMMEDIATE` 开始并配合 WAL 与 `busy_timeout`：SQLite 在同一时刻只向一个连接授予
RESERVED 锁，其他实例在锁上等待，因此跨进程的“先查后插”不会交错，实例内则由串行写锁
负责同样的顺序。并发启动时建表语句若遇到瞬时 `SQLITE_BUSY` 会短暂重试，不影响启动。

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

成功响应（首次提交与回放）除 JSON 响应体外还带两个响应头：

| 响应头 | 首次提交 | 相同指纹重复提交 |
|---|---|---|
| `Idempotency-Outcome` | `created` | `replayed` |
| `Idempotency-Record-ID` | 新记录的 `id` | 首次记录的同一 `id` |

指纹冲突（409）、校验失败（400）与存储不可用（503）的响应都不带这两个响应头；响应体本身的形态与字节在引入响应头前后保持不变，回放仍逐字节返回首次 `response_snapshot`。

### `POST /v1/idempotency/reservations`

创建执行占位（pending 占位），让结果尚未产生时同键同指纹的请求复用同一个占位而不是重复执行业务。请求体：

```json
{
  "idempotency_key": "order-123",
  "request_fingerprint": "sha256:9c28...",
  "expires_at": "2026-10-01T12:00:00Z"
}
```

- 首个请求：HTTP 201，直接返回占位对象（无外层信封），字段与顺序固定为 `id`、`idempotency_key`、`request_fingerprint`、`status`（恒为 `pending`）、`created_at`、`expires_at`；`id` 形如 `res_` 加 32 个小写十六进制字符，占位没有 `response_snapshot` 字段。
- 同键同指纹且占位未过期：HTTP 200，返回与首次完全相同的占位对象与 `id`，不创建第二个占位。
- 同键异指纹（占位仍 pending 且未过期）：HTTP 409，顶层 `error.code` 为 `idempotency_fingerprint_conflict`，并附已有占位的 `reservation_id` 与 `request_fingerprint`，原占位保持不变。
- `idempotency_key` 或 `request_fingerprint` 为空、`expires_at` 缺失、不是 RFC 3339 时间或不严格晚于创建时刻（相等也算）：HTTP 400，`code` 为 `invalid_idempotency_record`，不写入任何占位。
- 成功响应带 `Idempotency-Outcome` 头：首次为 `created`，复用为 `replayed`。409、400、503 不带该头。

### `POST /v1/idempotency/reservations/:reservation_id/results`

提交占位的首个执行结果。请求体为 `request_fingerprint` 与 `response_snapshot`（省略快照按 `null` 存储，但必须是合法 JSON，回放逐字节保留首次字节）：

- 首个结果：把占位提升为既有记录，HTTP 200 返回 `{"record":{...}}`，记录字段顺序、状态语义与 `POST /v1/idempotency/records` 完全一致；记录 `id` 为新的 `rec_` 标识，`created_at` 与 `expires_at` 沿用占位（不使用提交时刻），因此正常列表与过期历史都会自然包含这条记录。响应头 `Idempotency-Outcome` 为 `created`，并带 `Idempotency-Record-ID`。
- 重复结果提交：HTTP 200 逐字节回放首个记录值，`Idempotency-Outcome` 为 `replayed`，`Idempotency-Record-ID` 为首次记录的同一 `id`。
- 指纹与占位不符：HTTP 409，`code` 为 `idempotency_fingerprint_conflict`，并附 `reservation_id` 与占位的 `request_fingerprint`，占位不被提升、首个结果不受影响。
- `reservation_id` 不是 `res_` 加恰好 32 个小写十六进制字符，或请求体本身非法：HTTP 400，`code` 为 `invalid_idempotency_record`。
- 形态合法但不存在的占位：HTTP 404，`code` 为 `not_found`。
- 占位已到 `expires_at` 仍无结果：占位视为 expired，结果提交返回 HTTP 409，`code` 为 `idempotency_reservation_expired`，附 `reservation_id` 与 `request_fingerprint`；此后同键可以创建新的占位，旧占位不阻塞。
- 存储故障：HTTP 503，`code` 为 `storage_unavailable`；整个检查与提升在单事务内完成，失败不产生部分写入（不留下无快照记录或半个占位状态）。

### `GET /v1/idempotency/records/:idempotency_key`

按键查询，成功返回 `{"record":{...}}`。键不存在或记录已过期时统一返回 HTTP 404，`code` 为 `not_found`，不返回历史响应快照，也不会自动创建占位记录。

### `GET /v1/idempotency/records-by-id/:record_id`

按记录标识只读查询**任意一代**记录（包括已过期的历史行），不插入、不改写，也不改变其他入口的可见结果：

- 存在：HTTP 200，返回 `{"record":{...}}`，字段沿用记录对象的固定顺序，`response_snapshot` 保持首次提交的 JSON 字节；`expires_at` 严格晚于查询时刻时 `status` 为 `active`，否则（相等或更早）为 `expired`。
- `record_id` 形态不合法（不是 `rec_` 加恰好 32 个小写十六进制字符）：HTTP 400，顶层 `error.code` 为 `invalid_idempotency_record`，请求不访问存储。
- 形态合法但不存在：HTTP 404，顶层 `error.code` 为 `not_found`。
- 存储故障：HTTP 503，`code` 为 `storage_unavailable`。

### `GET /v1/idempotency/records`

列出未过期记录，按创建时间倒序（同时间以记录 `id` 倒序作为确定性兜底）。返回 `{"records":[...],"next_cursor":""}`，`next_cursor` 为空表示没有下一页。

| 查询参数 | 说明 |
|---|---|
| `key` | 仅返回该幂等键的记录，不会混入其他记录 |
| `request_fingerprint` | 仅返回请求指纹与该值逐字符完全相等的记录：不做大小写折叠、前缀匹配或其他规范化，空白也按原值匹配；省略或传空字符串等价于不过滤。过期记录不会因此泄露，可与其余参数组合使用 |
| `status` | `active`（默认，等价于不传）；`expired` 恒返回空页，过期记录不参与正常结果；其他取值返回 `invalid_idempotency_record` |
| `expires_before` | RFC 3339 时间，仅返回严格早于该时间过期的记录 |
| `expires_after` | RFC 3339 时间，仅返回严格晚于该时间过期的记录 |
| `limit` | 每页条数，1–100，默认 50；非法值返回 `invalid_idempotency_record` |
| `cursor` | 上一页返回的不透明分页游标；游标缺失字段或无法解码时返回 HTTP 400，`code` 为 `invalid_cursor` |

游标为键集分页（编码了上一页末尾记录的创建时间与 `id`），翻页期间新插入的记录不会让更早的页重复或错位。

### `GET /v1/idempotency/history`

过期记录审计查询，只读返回同一幂等键各代历史记录；不插入、不改写、不合并、不物理删除任何行，正常入口（提交、按键读取、列表）的响应与错误语义保持不变，也不会经它们泄露过期快照。只返回查询时刻已经过期（`expires_at` 早于或等于当前时刻）的行，记录字段与正常记录相同且顺序固定为 `id`、`idempotency_key`、`status`、`request_fingerprint`、`response_snapshot`、`created_at`、`expires_at`，其中 `status` 恒为 `expired`，`response_snapshot` 逐字节保留首次提交的 JSON。

| 查询参数 | 说明 |
|---|---|
| `key` | 必填，指定要审计的幂等键；为空或缺失返回 HTTP 400，`code` 为 `invalid_idempotency_record`。结果只含该键的各代记录 |
| `request_fingerprint` | 按原值逐字符精确匹配（不做大小写折叠、修剪或前缀匹配），空值不筛选；空白是真实筛选值 |
| `expires_before` | RFC 3339 时间，仅匹配 `expires_at` 严格早于该时间的行 |
| `expires_after` | RFC 3339 时间，仅匹配 `expires_at` 严格晚于该时间的行 |
| `limit` | 每页条数，只接受 1–100 的十进制整数，默认 50；其他取值返回 `invalid_idempotency_record` |
| `cursor` | 沿用与列表相同的不透明键集游标（上一页末条记录的 `created_at` 与 `id`） |

结果按 `created_at` 倒序、同刻按 `id` 倒序，响应信封为 `{"records":[...],"next_cursor":""}`，`next_cursor` 为空表示没有下一页。未知键、无匹配记录或筛选后为空都返回 HTTP 200、空 `records` 与空 `next_cursor`，不创建占位数据。游标无法解码、缺少字段、`id` 形态异常或不指向符合本查询条件的历史记录时，返回 HTTP 400，`code` 为 `invalid_cursor`；其他输入错误使用 `invalid_idempotency_record`。键集游标在翻页期间对新插入、新过期或并发变化保持稳定：已取得的页不会重复或错位。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。指纹冲突结果同样以顶层 `error` 呈现，并在其后固定附带 `record_id` 与 `request_fingerprint`。存储不可用时所有幂等入口返回 HTTP 503，`code` 为 `storage_unavailable`。

## 多实例并发语义

多个实例共享一个数据库文件并发提交同一 `idempotency_key` 时：

- 请求指纹相同：所有成功请求都返回 HTTP 200、相同的 `record.id`、逐字节一致的首次
  `response_snapshot` 与相同的时间字段，`Idempotency-Record-ID` 头也相同，且只有一个请求的
  `Idempotency-Outcome` 为 `created`，其余均为 `replayed`，最终只有一条未过期记录可见。
- 请求指纹不同：只有最先提交成功的请求返回 HTTP 200，其余返回 HTTP 409，顶层
  `error.code` 为 `idempotency_fingerprint_conflict`，并固定回传获胜记录的 `record_id`
  与 `request_fingerprint`；后到请求不会覆盖首次快照。
- 执行占位与结果：多实例并发为同一幂等键创建占位时，只有一个请求返回 HTTP 201 且
  `Idempotency-Outcome` 为 `created`，其余同指纹请求 HTTP 200 复用同一 `res_` 标识并标
  `replayed`，异指纹请求 HTTP 409；并发提交同一占位的首个结果时只有一个请求把占位提升为
  `rec_` 记录并标 `created`，其余全部回放该首次记录并标 `replayed`，最终只有一条占位与一条
  结果记录可见。占位过期后同键允许创建新占位，已提升的记录仍按既有过期规则进入历史。
- 记录过期后并发重提：只新增一条记录，获胜记录之后的请求回放它；新旧行都保留在底层历史
  表中，不删除、不改写、不合并，查询与列表始终只暴露未过期记录。
- 过期审计入口 `GET /v1/idempotency/history` 为只读查询：多实例共享 `DB_PATH` 时各实例都能
  看到同样的历史代际，但它不写入任何行，未过期记录的隔离、首次快照不可变与历史行不物理
  删除的语义都不受影响。
- 锁等待超过 `busy_timeout`、数据库无法读写或提交结果无法确认时，所有入口（含
  `records-by-id`）返回 HTTP 503，`code` 为 `storage_unavailable`，错误信息不泄露 SQL 细节。
