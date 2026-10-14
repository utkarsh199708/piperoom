# Contract change requests from the 2026-10-14 QA fix sweep

The fix nodes did not edit contract files (AGENTS.md §2). Each item below has a
local adapter in place today; the contract change removes the adapter.

## 1. `internal/actions/action.go` — `Validate` needs a context
```go
Validate(ctx context.Context, params json.RawMessage) error
```
Implementations call Connect/Registry (`connectorExists`) on `context.Background()`
with a 10 s timeout, so a client disconnect or shutdown cannot cancel them (rule 4).
The room service already holds a request ctx at proposal creation and before Execute.

## 2. `internal/clients/clients.go` — `ReplicationSlot`
```go
SlotType           string `json:"slot_type"`                     // logical | physical
InvalidationReason string `json:"invalidation_reason,omitempty"` // PG17
```
Today an empty `Plugin` means physical, and an unusable slot is reported as
`WALStatus: "lost"`.

## 3. `internal/clients/clients.go` — `SourceDB.Row` column types
Return column types (or add `RowTyped`) so `*_at` redaction does not depend on an
optional method found by type assertion.

## 4. `internal/clients/clients.go` — Kafka lag anomalies
Add `Start int64` and `Anomaly string` to `PartitionLag` (or `GroupLagReport` on
`KafkaAdmin`). Commits on deleted topics, past the log end, or below log start are
reset risks; the tool currently type-asserts for `GroupLagReport`.

## 5. `internal/clients/clients.go` — `SchemaVersion.SchemaType`
Add `SchemaType string` (AVRO | JSON | PROTOBUF). The diff tool type-asserts for
`SchemaTyped` and otherwise guesses from the schema text.

## 6. `internal/clients/clients.go` — partial Connect status
Document that `ConnectClient.Status` may return a partial status together with
`connect.ErrResponseTooLarge`, or add `Truncated bool` to `ConnectorStatus`.

## 7. `internal/clients/clients.go` — `Warehouse.Freshness` precision
Return `FreshnessInfo{TS time.Time; Coarse bool}` so a `date` column can be shown as
day-precision in the map instead of being read as end of day.

## 8. `internal/clients/clients.go` — `FindByKey` truncation
`KafkaAdmin.FindByKey` cannot say that `max_scan` cut a partition short, so a
"newest" match may not be the newest. Return `KeyScan{Refs, Truncated []int32}` (or
add a `Truncated` field). Today the tool type-asserts for `kafka.FindByKeyReport`.

## 9. `docs/api.md` — behaviour the code now has
- Signed alerts: replays are checked before the rate limit and never spend it; identical copy within the replay window → `202 {"ignored":true,"reason":"duplicate delivery"}`; `429 rate_limited` (60/min per source, `Retry-After`); which header checks happen before the body is read.
- Errors table: `413 too_large` for any JSON body over 1 MB; `400` for trailing data after the JSON object; comment/steer text ≤ 16 KiB.
- Approve/reject: 404 for an unknown proposal is checked before body validation; empty `layer` → 400.
- WebSocket: `after` past `last_seq` → HTTP 400 at upgrade; slow consumers closed with 1008 and a reason naming the resume seq.
- `agent_status` states: add `error`.
- IdP unavailable: `/auth/login` and `/auth/callback` redirect `302 /?login_error=idp_unavailable` (doc says 503).
- 409 list: `already_decided` for accepting an already-accepted hand-off.
- `POST /api/rooms` returns 201.
- Room events: UPDATE/DELETE on `room_events` is refused by the database (migration 00015) except inside a purge transaction.

## Not contract, but owner review needed
- `evals/scoring_test.go`, `evals/agreement_test.go` (C6): entries for the 9 new scenarios, required for `go test ./evals` to pass.
- `internal/wiring/topology_test.go` (A4), `cmd/server/main.go` (A5), `cmd/discover/integration_test.go` (C10), `web/src/hooks/useRoom.ts` (A6): small merge follow-ups by the integrator.
- Watcher (`internal/watch/dlq_signals.go`): a firing DLQ signal can now resolve while a connector config is unreadable — reverses an earlier design choice.
- Alert replay cache is per replica; across replicas a copy reaching another replica is accepted. A shared Postgres table with TTL would close it.
