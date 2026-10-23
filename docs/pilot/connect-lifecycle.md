# The Connect lifecycle calls and the Kafka version each needs

This page lists the Kafka Connect REST calls behind `connect_source_offsets` and the four
lifecycle actions: the method, the path, the Kafka release that adds the endpoint, what Connect
returns, when Piperoom refuses the call, and which tool or action uses it. Every claim cites a
file and line on `main` at `a45a1281`, or an Apache Kafka page. The credentials for these calls
are in [permissions.md](permissions.md#kafka-connect).

The two releases are fixed in code — `kip875ReadVersion = "3.5"` and `kip875ModifyVersion =
"3.6"` (`internal/clients/connect/lifecycle.go:112-114`) — and Apache Kafka states the same
split: 3.5.0 "only contains endpoints for listing offsets" ([3.5.0 announcement]), while 3.6.0
added the alter and reset endpoints ([3.6.0 announcement]); the KIP is [KIP-875].

**Status on main.** The four actions are declared in `internal/actions/connect_lifecycle.go:14-38`
and registered by `RegisterLifecycle` (`connect_lifecycle.go:62-76`), which wiring calls only with
write mode on (`connect_lifecycle.go:14-15`). Row Q5d, the wiring, has not landed
(`tasks/Q-board.md:298`), so on `main` that function is called only from tests. The read tool is
registered only when its client exposes `SourceOffsets` (`internal/tools/kafkatools/kafkatools.go:125-131`);
the room's read tools get a wrapper without it (`internal/wiring/kinds.go:120`;
`internal/wiring/readonly.go:30-44`).

## `GET /`

| | |
|---|---|
| Method, path | `GET /` (`internal/clients/connect/lifecycle.go:130-149`) |
| Kafka version | any Connect; documented in the 3.5 and 3.6 REST pages ([3.5 REST], [3.6 REST]) |
| Returns | `{"version","commit","kafka_cluster_id"}` (`lifecycle.go:75-83`): the worker's Kafka release, its git commit and the cluster id ([3.6 REST]) |
| Refused when | any non-2xx is an error; nothing here changes state |
| Used by | `connect_source_offsets` only, to name the running version when an endpoint is missing (`internal/tools/kafkatools/connect_offsets.go:312-322`) |

## `GET /connectors/{name}/offsets`

| | |
|---|---|
| Method, path | `GET /connectors/{name}/offsets` (`lifecycle.go:151-168`) |
| Kafka version | **3.5 or later**; the read half of KIP-875, shipped in 3.5 ([3.5.0 announcement]) |
| Returns | `{"offsets":[{"partition":{…},"offset":{…}}]}` (`lifecycle.go:160-167`); the maps are the connector's own, positions only |
| Refused when | 405, or 404 while `GET /connectors/{name}` still finds the connector → `ErrNotSupported` naming 3.5 (`lifecycle.go:317-349`); a missing connector stays `ErrNotFound` (`lifecycle.go:400-414`); a connector outside the room's pipeline is refused before any I/O (`connect_offsets.go:98-121`) |
| Used by | `connect_source_offsets`; `connect_reset_source_offsets` in `Validate` and `Preview` (`internal/actions/connect_stop_reset.go:309-321`) |

## `PUT /connectors/{name}/stop`

| | |
|---|---|
| Method, path | `PUT /connectors/{name}/stop` (`lifecycle.go:218-238`) |
| Kafka version | **3.5 or later**; the 3.5 page defines the call ([3.5 REST]), the 3.6 page still lists it ([3.6 REST]) |
| Returns | the client reads no body; Connect stops the connector and its tasks, deallocating their resources ([3.5 REST]); the stop is applied asynchronously (`connect_stop_reset.go:165-181`) |
| Refused when | missing route → `ErrNotSupported` naming 3.5 (`lifecycle.go:344-349`); a 409 naming a worker rebalance → `ErrRebalancing` (`lifecycle.go:386-398`); a connector outside the pipeline, or one that does not exist (`connect_stop_reset.go:84-101`); an already STOPPED connector is reported as "no change made", not sent (`connect_stop_reset.go:157-161`) |
| Used by | `connect_stop_connector`; a reset's proposals place this step before theirs (`connect_stop_reset.go:335-344`) |

## `DELETE /connectors/{name}/offsets`

| | |
|---|---|
| Method, path | `DELETE /connectors/{name}/offsets` (`lifecycle.go:240-265`) |
| Kafka version | **3.6 or later** (KIP-875 alter and reset; [3.6.0 announcement]). The 3.6 page adds: "The connector must exist and must be in the stopped state" ([3.6 REST]) |
| Returns | the client reads no body; every stored offset is cleared. A later GET shows each partition with a null offset, not a removed entry (`connect_stop_reset.go:459-470`) |
| Refused when | running or not STOPPED: 400 "Connectors must be in the STOPPED state…" or 409 → `ErrConnectorNotStopped` (`lifecycle.go:366-384`); missing route → `ErrNotSupported` naming 3.6 (`lifecycle.go:344-349`); sink connector (`connect_stop_reset.go:323-333`); more than one stored offset, because this action could not offer the rollback (`connect_stop_reset.go:34-38, 415-433`); a connector that stores nothing is not called at all (`connect_stop_reset.go:615-623`) |
| Used by | `connect_reset_source_offsets` with no `position` (`connect_stop_reset.go:615-624`) |

## `PATCH /connectors/{name}/offsets`

| | |
|---|---|
| Method, path | `PATCH /connectors/{name}/offsets` with `{"offsets":[…]}` (`lifecycle.go:267-315`) |
| Kafka version | **3.6 or later**; same stopped-state rule as above ([3.6 REST], [3.6.0 announcement]) |
| Returns | the client reads no body; the listed partitions get the new position. An `offset` of null resets that partition: "The 'offset' field may be null to reset the offset for a specific partition" ([3.6 REST]; `lifecycle.go:69-72`) |
| Refused when | the same not-stopped, missing-route and rebalance cases as the DELETE; no entries, or more than 1000, or a body over 1 MiB (`lifecycle.go:116-127, 272-304`); a position that does not match a stored partition, key or type → `ErrSourcePosition` (`connect_stop_reset.go:25-33, 676-740`) |
| Used by | `connect_reset_source_offsets` with a `position` (`connect_stop_reset.go:625-644`) |

## `POST /connectors`

| | |
|---|---|
| Method, path | `POST /connectors` with `{"name","config"}` (`lifecycle.go:170-193`) |
| Kafka version | no KIP-875 dependency; documented in both the 3.5 and 3.6 REST pages ([3.5 REST], [3.6 REST]) |
| Returns | the client reads no body and re-reads the status afterwards (`lifecycle.go:189`; `internal/actions/connect_create_delete.go:294-300`). Preview first asks Connect's validate endpoint, `PUT /connector-plugins/{class}/config/validate` (`connect_create_delete.go:235-258`) |
| Refused when | name already registered: 409 → `ErrConnectorExists` unless the 409 names a rebalance (`lifecycle.go:416-434`); name not in the pipeline's `connectors` (`connect_create_delete.go:123-131`); a credential not a `${secret:…}` reference, or a `${file:…}`/`${env:…}` reference (`connect_create_delete.go:20-37, 531-572`); a config `name` that differs from the parameter (`connect_create_delete.go:112-120`); a host the pipeline does not reach (`connect_create_delete.go:208-212`); a 400 from Connect, with the body redacted and capped (`lifecycle.go:433, 436-470`) |
| Used by | `connect_create_connector` (`connect_create_delete.go:260-301`) |

## `DELETE /connectors/{name}`

| | |
|---|---|
| Method, path | `DELETE /connectors/{name}` (`lifecycle.go:195-216`) |
| Kafka version | no KIP-875 dependency; documented in both the 3.5 and 3.6 REST pages ([3.5 REST], [3.6 REST]) |
| Returns | the client reads no body; Connect deletes the connector "halting all tasks and deleting its configuration" ([3.5 REST]). The stored offsets stay under the name (`lifecycle.go:59-61, 195-198`) |
| Refused when | a 404 → `ErrNotFound`; a 409 rebalance → `ErrRebalancing` (`lifecycle.go:209-214`); a stored config with a plaintext credential → `ErrPlaintextSecret`, because the only undo could not restore it (`connect_create_delete.go:29-37, 389-400`); a connector outside the pipeline (`connect_create_delete.go:381-387`). A timed-out delete that did remove it, or a 404, counts as done (`connect_create_delete.go:485-515`) |
| Used by | `connect_delete_connector` (`connect_create_delete.go:455-481`) |

## Why a pre-3.5 Connect answers `ErrNotSupported`

A worker that predates KIP-875 does not route these paths: it answers 405, or 404 on the offsets
path while `GET /connectors/{name}` still finds the connector (`lifecycle.go:317-341`). The client
turns that into `ErrNotSupported` naming the endpoint and the release that adds it ("connect: …
Kafka Connect 3.5 or later (KIP-875) adds this endpoint"; `lifecycle.go:344-349`;
`internal/clients/clients.go:39-40`). The tool adds the running version: "this Connect reports
Kafka <version>; reading connector offsets needs Kafka Connect 3.5 or later (KIP-875)"
(`connect_offsets.go:79-81, 312-322`). A 404 for a connector that does not exist stays
`ErrNotFound`, so a typo is never reported as an old Connect (`lifecycle.go:334-341`). The reset
action names 3.6 the same way (`connect_stop_reset.go:649-655`; `lifecycle.go:262, 312`), the stop action 3.5 (`lifecycle.go:235`).

## Offsets are positions only

Both maps come from the connector, not from the data: for Debezium Postgres the partition names
the slot/server and the offset holds the LSN and transaction id; for MySQL it holds the binlog file
and position. The offsets API returns no record data (`lifecycle.go:32-40`; `connect_offsets.go:40-45`).
`connect_source_offsets` keeps scalar values only, drops nested objects and arrays, redacts
credential-looking strings, and caps keys and values; the dropped count is reported
(`connect_offsets.go:123-132, 226-295`). So a room shows where a connector will resume, never a row
it copied.

## Create and delete: the re-registration ordering

Connect keeps the stored offsets under the name after a delete, so a later create with that name
resumes from them (`lifecycle.go:59-61, 195-198`; `connect_create_delete.go:436-445`). To
re-register, one proposal puts `connect_delete_connector` **first** and `connect_create_connector`
after it with `replace: true` (`connect_create_delete.go:72-74, 83-86`). `Validate` refuses a
connector that is present without `replace` (`connect_create_delete.go:161-164`), and `Execute`
re-checks: if the name is still registered, the create refuses rather than overwrite a registration
that may be someone else's (`connect_create_delete.go:274-283`). The create validates the config
again just before the POST (`connect_create_delete.go:284-288`), and the delete is refused when the
stored config holds a plaintext credential, since its only undo would have to carry that value
(`connect_create_delete.go:29-37, 389-400`). The reset pair is ordered the same way: stop first,
then reset with `stop_first: true`, and `Execute` re-checks that the connector really is STOPPED
(`connect_stop_reset.go:335-373`).

## What to allow

For the read side: `GET /` and `GET /connectors/{name}/offsets` on top of the three reads in
[permissions.md](permissions.md#kafka-connect). For the write side: the four calls above, on the
pipeline's connectors only; the client refuses any connector outside the room's pipeline before I/O
(`lifecycle.go:50-54`; `internal/clients/connect/fix.go:40-60`). Write mode off gives the actions a read-only client that refuses at `Execute` (`connect_stop_reset.go:151-156`; `internal/wiring/readonly.go:185`).

[3.5 REST]: https://kafka.apache.org/35/kafka-connect/user-guide/
[3.6 REST]: https://kafka.apache.org/36/kafka-connect/user-guide/
[3.5.0 announcement]: https://kafka.apache.org/blog/2023/06/15/apache-kafka-3.5.0-release-announcement/
[3.6.0 announcement]: https://kafka.apache.org/blog/2023/10/10/apache-kafka-3.6.0-release-announcement/
[KIP-875]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect
