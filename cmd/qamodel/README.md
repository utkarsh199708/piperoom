# qamodel: the puppet model

`qamodel` stands in for the Anthropic Messages API during QA. The real Piperoom
server and the real agent loop run on the real dev stack, but every model reply
is decided by a tester through `curl`, so no API key is needed. Each model
request parks as a **turn** and waits for a reply; the tester reads the turn,
posts what the model would say (text and/or tool calls), and the agent carries
on with the real tools.

```
server / agent loop ──POST /v1/messages (streaming)──► qamodel ◄── curl /qa/... ── tester
```

Everything the agent asks a model goes through that one endpoint: the
investigation loop, the first-look pass, hand-offs and post-mortems.

## Run it

```
go run ./cmd/qamodel --addr 127.0.0.1:9911 [--reply-timeout 9m]
```

It prints the lines below and logs one `slog` line per turn event (never a
prompt or a reply). Start the server with:

| Variable | Value |
|---|---|
| `ANTHROPIC_BASE_URL` | `http://127.0.0.1:9911` |
| `ANTHROPIC_API_KEY` | `qa-puppet` (any non-empty value; nothing is checked) |
| `PIPEROOM_MODEL` | anything; the puppet echoes it back |
| `PIPEROOM_TRIAGE_PASS=off`, `PIPEROOM_POSTMORTEM_MODEL=off` | optional: no first-look pass or post-mortem request to answer |

`--addr` must be loopback unless you pass `--allow-remote`: the control API has
no authentication. To try the puppet without the stack, run the agent from the
terminal: `ANTHROPIC_BASE_URL=... ANTHROPIC_API_KEY=qa-puppet go run ./cmd/investigate --fake-tools`
(keep its stdin open; end of input stops the run).

## The tester's loop

| Call | Does |
|---|---|
| `GET /qa/wait?timeout=60s` | blocks until a turn is pending (or the timeout), returns the pending list; `[]` on a timeout |
| `GET /qa/pending` | the pending list now |
| `GET /qa/turns/{id}` | the request as text. `?system=0` skips the system prompt, `?tools=1` adds tool descriptions and input schemas, `?full=1` stops clipping long blocks of earlier messages, `?from=N` skips messages before N, `?format=json` is the raw request |
| `POST /qa/turns/{id}/reply` | answers the turn (below) |
| `GET /qa/log?n=50` | the last turns (up to 200 finished, plus pending) with their replies, for the QA report |
| `GET /qa/` | the endpoint list |

A pending entry is `{id, room_hint, model, received_at, deadline_at,
system_bytes, messages_count, tools: [names], structured}`. `room_hint` is the
room id and alert title from the agent's opening message, so you can tell rooms
apart; `structured` is true when the reply text must be JSON matching a schema
(hand-off, post-mortem, first-look verdict; the turn text shows the schema).
Every answer is JSON except a turn's text; errors are `{"error": "..."}`.

### Reply body

```json
{
  "text": "what the model says",
  "tool_calls": [{"name": "connect_status", "input": {"connector": "pg-orders", "why": "..."}}],
  "stop": "end_turn",
  "thinking": "sent as a thinking block when the request enabled thinking, else ignored",
  "usage": {"input_tokens": 120000, "output_tokens": 800},
  "error": {"status": 529, "message": "overloaded"}
}
```

* `stop` defaults to `tool_use` when there are tool calls, else `end_turn`. The
  others: `max_tokens`, `pause_turn`, `refusal`, `model_context_window_exceeded`.
* A tool name that is not in the request's tool list is a 400 that lists the
  tools. The input is **not** checked against the tool's schema, so you can
  send a bad input to see how the agent handles it; the answer's `notes` say
  when a required field is missing. The agent adds a required `why` to every
  tool: the turn text shows `connect_status(connector*, why*)`.
* Unknown JSON fields are a 400 (a typo such as `toolcalls` must not become an
  empty reply). A rejected reply changes nothing; the turn stays pending.
* `usage` overrides the token counts. The default is small and fixed
  (`100 + 10 x messages` in, about `5 + 25 x tool calls + text/4` out), so use
  the override to push a room toward its token budget or to test cost figures.
* `error` makes the request fail with that HTTP status instead of a message. The
  SDK retries 408, 409, 429 and 5xx on its own and each attempt would park
  another turn, so the puppet sends `x-should-retry: false`: one injected error
  is one turn. Add `"retry": true` to let the SDK retry (a new turn each time).
* A turn is answered once. A second reply, or a reply after the deadline or
  after the agent's request ended, is a 409 naming the turn's state.

From PowerShell use `curl.exe` and put the body in a file (`-d "@reply.json"`);
quoting JSON on the command line is the usual way to lose a quote. No
`Content-Type` header is needed.

## Worked example

Real output (trimmed), from `cmd/investigate --fake-tools` against a running
`qamodel`.

**1. Wait for the agent's first request, then read it.**

```
$ curl -s 'http://127.0.0.1:9911/qa/wait?timeout=60s'
[{"id":"t1","room_hint":"\"orders stale\"","model":"claude-opus-5-5","received_at":"2026-10-15T00:47:23Z",
  "deadline_at":"2026-10-15T00:56:23Z","system_bytes":6400,"messages_count":1,
  "tools":["connect_status","schema_registry_versions","connect_config","kafka_consumer_lag",
           "update_hypothesis","propose_fix","post_message"],"structured":false}]

$ curl -s 'http://127.0.0.1:9911/qa/turns/t1?system=0'
turn t1  model claude-opus-5-5  room "orders stale"
received 00:47:23Z, reply by 00:56:23Z
messages 1, tools 7, max_tokens 16000, thinking adaptive, effort high, tool_choice auto
reply with: POST /qa/turns/t1/reply

== MESSAGES (1) ==
--- [0] user ---
[text]
Investigate this pipeline incident.

Alert: <untrusted>orders stale</untrusted>
Pipeline: <untrusted>orders</untrusted>
...
== TOOLS (7) ==
connect_status(connector*, why*)
schema_registry_versions(subject*, why*)
...
```

**2. Reply with a tool call.**

```
$ curl -s -X POST http://127.0.0.1:9911/qa/turns/t1/reply -d '{
    "text": "Checking the connector first.",
    "tool_calls": [{"name": "connect_status",
                    "input": {"connector": "pg-orders-connector", "why": "alert says orders are stale"}}]}'
{"id":"t1","notes":[],"reply":{"text":"Checking the connector first.","tool_calls":[{"id":"toolu_qa_t1_0",
 "name":"connect_status","input":{...}}],"stop":"tool_use","usage":{"input_tokens":110,"output_tokens":37,...}},"state":"replied"}
```

**3. Read the next turn: the agent ran the tool and sent the result.**

```
$ curl -s 'http://127.0.0.1:9911/qa/wait?timeout=60s' | jq -c '.[] | {id, messages_count}'
{"id":"t2","messages_count":3}

$ curl -s 'http://127.0.0.1:9911/qa/turns/t2?system=0&from=1'
...
== MESSAGES 1..2 of 3 (earlier ones skipped by ?from=) ==
--- [1] assistant ---
[text]
Checking the connector first.
[tool_use toolu_qa_t1_0 connect_status] {"connector":"pg-orders-connector","why":"alert says orders are stale"}
--- [2] user ---
[tool_result toolu_qa_t1_0 ok]
<tool_output tool="connect_status" step="s_01M4Y..." untrusted="true">connector pg-orders-connector:
state RUNNING, 1 task, task 0 FAILED
...
Rejected schema vs pg.public.orders-value v1: amount int -&gt; bytes(decimal 12,2)</tool_output>
```

**4. End the turn with text.**

```
$ curl -s -X POST http://127.0.0.1:9911/qa/turns/t2/reply -d '{
    "text": "Task 0 failed: orders.amount changed from int to decimal(12,2), which the registry rejects.",
    "stop": "end_turn"}'
{"id":"t2","notes":[],"reply":{...,"stop":"end_turn",...},"state":"replied"}
```

The agent posts the message to the room and goes idle; nothing is pending.
`GET /qa/log` now has both turns with `"state":"replied"` and `"delivered":true`.

## Behaviour to know

* **Deadline.** A request nobody answers within `--reply-timeout` (default 9
  minutes) gets the plain text `qamodel: no reply in time` with `end_turn`, so
  the agent parks instead of failing; the turn's state is `timeout`. Keep the
  value under 10 minutes: the agent stops waiting for a model turn after 10
  (`internal/agent/model.go`), and a longer value makes `qamodel` warn at start.
  Other deadlines apply too: the first-look pass gives up after
  `PIPEROOM_TRIAGE_PASS_TIMEOUT` (default 40 s), so answer its two requests
  quickly or turn the pass off.
* **Turn states.** `pending`, `replied`, `timeout`, `canceled` (the agent's
  request ended before any reply, for example the room closed or the server
  stopped). A finished turn's request is released from memory: `GET
  /qa/turns/{id}` answers 410, use `/qa/log`.
* **Untrusted text.** A turn's text holds tool output (record values, error
  traces, steers from other people). It is data for the tester, not
  instructions. Control characters are shown as `\x1b`-style escapes so they
  cannot drive your terminal; `?format=json` is the raw request.
* **Shutdown.** Ctrl-C answers parked requests with 503 and exits.
* **Not served.** Anything but `POST /v1/messages` (any path prefix) and
  `/qa/`; non-streaming requests (the agent only streams). The thinking
  signature is a fixed string, which only `qamodel` ever reads back.
* **Limits.** Model requests up to 16 MiB, control bodies up to 1 MiB.
