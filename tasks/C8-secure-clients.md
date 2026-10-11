# C8 — Secure connections: Kafka SASL/TLS, Schema Registry and Connect auth/TLS (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-11 23:50 IST):** implemented as **PR #9** (`claude/loving-feynman-pvc64c`), shared
> package at `internal/clients/netsec`. The "Contract" section below is **superseded** by the 18-field contract
> accepted on PR #9 (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_TLS_*`, `SCHEMA_REGISTRY_USERNAME/
> PASSWORD/TLS_*`, `CONNECT_USERNAME/PASSWORD/TLS_*`; empty = off), which the lead adds at merge. Client
> construction from config lives in A4's `internal/wiring` — no second "build clients from config" helper.


Read `AGENTS.md`, `docs/collab.md` and `tasks/A2-kafka-tools.md` first. Then read `internal/clients/{kafka,schemareg,connect}/*.go`,
`internal/config/config.go` and `cmd/investigate/wire.go`.
**While this task is open you own:** `internal/clients/kafka/**`, `internal/clients/schemareg/**` and `internal/clients/connect/**`.
A2 lends them to you and gets them back when your PR merges. You also own the **new** package `internal/clients/clientsec/**`,
one **new** `README.md` section called "Connecting to secure clusters", and the `go.mod`/`go.sum` lines for `kfake`.
Do not edit `cmd/investigate/**` (A4), `internal/tools/kafkatools/**`, `internal/actions/**` (A2) or any contract file.

You have no Docker, no dev stack, no Kafka or Confluent account and no API key. Write unit tests only, with `httptest` and `kfake`.

## Goal
Real customers run Confluent Cloud, Amazon MSK, or self-managed Kafka with TLS. The Kafka, Schema Registry and Connect clients must
be able to log in and encrypt their traffic. Settings come only from env vars through `internal/config`. Every new setting defaults
to "off", so the dev stack and every existing caller keep working with no change. Secrets never go into logs, errors or `String()` output.

## Contract (the lead adds this to `main` before you start)
`config.Config` gets these fields. Each one defaults to `""`, which means off:
`KafkaTLS` (`KAFKA_TLS`), `KafkaTLSCAFile` (`KAFKA_TLS_CA_FILE`), `KafkaTLSCertFile` (`KAFKA_TLS_CERT_FILE`), `KafkaTLSKeyFile` (`KAFKA_TLS_KEY_FILE`),
`KafkaSASLMechanism` (`KAFKA_SASL_MECHANISM`), `KafkaSASLUsername` (`KAFKA_SASL_USERNAME`), `KafkaSASLPassword` (`KAFKA_SASL_PASSWORD`),
`SchemaRegistryUsername/Password/CAFile/CertFile/KeyFile` (`SCHEMA_REGISTRY_USERNAME`, `_PASSWORD`, `_CA_FILE`, `_CERT_FILE`, `_KEY_FILE`),
`ConnectUsername/Password/CAFile/CertFile/KeyFile` (`CONNECT_USERNAME`, `_PASSWORD`, `_CA_FILE`, `_CERT_FILE`, `_KEY_FILE`).
`KafkaTLS` is a string because `Load()` cannot return an error. You check it yourself. If the contract on `main` is different from this, stop and ask in Slack.

## Work
1. **Do not break any existing signature.** Only add a variadic tail at the end:
   `kafka.New(brokers []string, sr *schemareg.Client, opts ...Option) (*Client, error)`,
   `schemareg.New(baseURL string, httpClient *http.Client, opts ...Option) *Client`,
   `connect.New(baseURL string, httpClient *http.Client, opts ...Option) *Client`.
   `cmd/investigate/wire.go`, `internal/tools/kafkatools/kafkatools_test.go` and every existing test must compile and pass untouched.
2. Changes in `kafka`. Check every symbol with `go doc` first. All of them are in franz-go v1.22.1, which is already in `go.mod`.
   - `type Option func(*options)`. `WithTLS(tc *tls.Config) Option` maps to `kgo.DialTLSConfig(tc)`, which clones `tc` on each dial and sets
     ServerName from the broker host (Confluent Cloud needs SNI). `WithSASL(m sasl.Mechanism) Option` maps to `kgo.SASL(m)`.
     The `sasl` package is `github.com/twmb/franz-go/pkg/sasl`.
   - `SASLMechanism(name, user, pass string) (sasl.Mechanism, error)`. The name match ignores case.
     `PLAIN` gives `plain.Auth{User: user, Pass: pass}.AsMechanism()` (`.../pkg/sasl/plain`).
     `SCRAM-SHA-256` and `SCRAM-SHA-512` give `scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()` and `.AsSha512Mechanism()` (`.../pkg/sasl/scram`).
     An empty user or pass is an error. `AWS_MSK_IAM`, `OAUTHBEARER`, `GSSAPI` and any other name return a "not supported" error.
   - Keep the base options on `Client`: `kgo.SeedBrokers` plus TLS plus SASL. Use them in **all three** `kgo.NewClient` calls:
     `New`, the scan client in `FindByKey` and the client in `recordTimestamps`. Today the two short-lived clients pass only `SeedBrokers`,
     so they would connect without auth. Build each call with `slices.Concat(c.base, []kgo.Opt{kgo.ConsumePartitions(...)})`.
     Never `append` onto the shared slice.
   - `New` returns an error if the mechanism's `Name()` is `"PLAIN"` and TLS is off, because PLAIN sends the password in clear text.
     SCRAM without TLS is allowed.
3. Changes in `schemareg` and `connect`:
   - Add `type Option func(*Client)` and `WithBasicAuth(user, pass string) Option`. In `do`, call `req.SetBasicAuth` only when credentials are set.
   - TLS comes from the `*http.Client` that the caller passes in (step 4). These two packages get no TLS code.
   - Add `String() string` on `*Client`. It returns the base URL only, never the credentials.
4. New package `internal/clients/clientsec`. It is the only package that imports `internal/config`.
   - `type TLSFiles struct{ CAFile, CertFile, KeyFile string }` and `TLSConfig(f TLSFiles) (*tls.Config, error)`:
     - Always set `MinVersion: tls.VersionTLS12`. Never set `InsecureSkipVerify`.
     - If `CAFile` is set, `RootCAs` is a new `x509.NewCertPool()` filled with `AppendCertsFromPEM`. This **replaces** the system roots, the same way a Kafka truststore does.
       If `AppendCertsFromPEM` returns false, return an error that names the path.
     - If `CAFile` is empty, `RootCAs` stays nil, so the system roots are used. That is enough for Confluent Cloud (Let's Encrypt roots) and MSK (Amazon Trust Services).
     - `CertFile` and `KeyFile` must be set together or not at all. Load them with `tls.LoadX509KeyPair`.
   - `HTTPClient(tc *tls.Config) *http.Client` returns a client with a 10 s timeout. Its transport is
     `http.DefaultTransport.(*http.Transport).Clone()` (check the type assertion) with `TLSClientConfig = tc`.
     Build one client per service, because Schema Registry and Connect may trust different CAs. Today `wire.go` shares one client.
   - Add `type Clients struct{ Connect *connect.Client; Registry *schemareg.Client; Kafka *kafka.Client }`, `(*Clients).Close()`,
     and `NewClients(cfg config.Config) (*Clients, error)`, which is a drop-in replacement for `newA2Clients` in `wire.go`.
     It checks the config first. It returns an error that names the env vars, never their values, in any of these cases:
     - `KAFKA_TLS` is not empty and `strconv.ParseBool` cannot parse it.
     - A `KAFKA_TLS_*_FILE` is set but `KAFKA_TLS` is not true.
     - A mechanism is set without both username and password, or a username or password is set without a mechanism.
     - The mechanism is `PLAIN` and `KAFKA_TLS` is not true.
     - A Schema Registry or Connect username, password or file is set while its URL is not `https://`. Basic auth must never go over clear text.
     - Only one of username and password is set.
     - A URL contains user info (`(*url.URL).User != nil`). The error must not repeat the URL.
5. Secrets: no `slog` line, error, or `String()` output may contain a password, an API secret or key file contents.
   A log line may show only the mechanism name and whether TLS is on.
6. Add a `README.md` section called "Connecting to secure clusters", placed just before "## How the team works". It has:
   - an env var table;
   - **Confluent Cloud**: `KAFKA_BROKERS=<bootstrap server>:9092`, `KAFKA_TLS=true`, `KAFKA_SASL_MECHANISM=PLAIN`, username and password set to the cluster
     API key and secret, `SCHEMA_REGISTRY_URL=https://<endpoint>` with the *separate* Schema Registry API key and secret, and no CA file;
   - **MSK**: `SCRAM-SHA-512` (the only SCRAM mechanism MSK supports) on port 9096, with `KAFKA_TLS=true`;
   - **self-managed** TLS/mTLS with CA, cert and key files;
   - a note that the binaries pick these settings up only after they switch to `clientsec.NewClients` (a follow-up for A4/A5).

**Out of scope** (list these under "Not done" in the PR):
- MSK IAM. franz-go has `pkg/sasl/aws` (`AWS_MSK_IAM`) with no AWS SDK import, but it takes static keys only. Rotating role credentials need `aws-sdk-go-v2`.
- `OAUTHBEARER`.
- Confluent Cloud fully managed connectors. They use a different API, `api.confluent.cloud/connect/v1/environments/{env}/clusters/{cluster}/connectors`.
  `CONNECT_URL` is for self-managed Connect workers that use `BasicAuthSecurityRestExtension`.
- Reloading certificates without a restart, per-pipeline credentials (finding F7), and editing `wire.go`.

## Tests
- `clientsec` table tests:
  - `TLSConfig`: CA file is OK, missing (`errors.Is(err, fs.ErrNotExist)`), or holds no PEM; cert without key; key without cert; a key that does not match the cert.
  - `NewClients`: every validation rule above, plus the happy path. Each error case asserts `!strings.Contains(err.Error(), password)`.
- HTTPS end to end:
  - Start `httptest.NewTLSServer`. Write the PEM of `srv.Certificate()` to a CA file. The handler checks `r.BasicAuth()`.
  - Through `NewClients`, `Registry.Subjects` and `Connect.ListConnectors` succeed.
  - Without the CA file, the call fails with `errors.As(err, new(*tls.CertificateVerificationError))`.
  - mTLS: use `httptest.NewUnstartedServer`, set `srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}`,
    then call `StartTLS()`. A client with cert files works; one without them fails. Do not use `NewTestServer`, because its in-memory network bypasses your transport.
- `internal/clients/clientsec/clientsectest`: `New(t testing.TB)` generates an ECDSA P-256 CA, a server cert (IP SAN `127.0.0.1`, `ExtKeyUsageServerAuth`)
  and a client cert (`ExtKeyUsageClientAuth`). It uses `x509.CreateCertificate` and writes PEM files to `t.TempDir()`.
  It returns the file paths plus a server `*tls.Config`.
- `kafka` with `kfake`, a test-only dependency:
  - Run `go get github.com/twmb/franz-go/pkg/kfake@v0.0.0-20261007040850-d3792b34935a`.
    It has no tagged releases. Its go.mod needs franz-go v1.21.7, kadm v1.18.0 and kmsg v1.14.0, which our versions already meet,
    but it raises `golang.org/x/crypto` to v0.51.0. List this in the PR.
  - First run `go doc` on `kfake.NewCluster`, `EnableSASL`, `Superuser`, `TLS`, `NumBrokers`, `SeedTopics`, `Cluster.ListenAddrs` and `Cluster.Close`.
  - Each test cluster: `kfake.NewCluster(kfake.NumBrokers(1), kfake.EnableSASL(), kfake.Superuser(mech, "piperoom", pw), kfake.TLS(serverTLS), kfake.SeedTopics(1, "orders"))`.
    `Superuser` alone does not turn SASL on, so always pass `EnableSASL()` too.
  - For PLAIN+TLS, SCRAM-SHA-256 and SCRAM-SHA-512, `Topics` returns `orders`.
  - A wrong password and a missing SASL option both fail before a 5 s ctx deadline. The error does not contain the password.
  - TLS only: a client with the right CA works, and a client with no TLS fails.
  - PLAIN without TLS makes `New` return an error.
  - Short-lived clients get the options: produce one `&kgo.Record{Topic: "orders", Key: []byte("42")}` with `ProduceSync(...).FirstErr()`.
    Then `TopicOffsets` returns a non-zero `LastTimestamp`, which proves `recordTimestamps` logged in.
  - `SASLMechanism` table: names, case, empty credentials, unsupported names, and `Name()` values.
- `schemareg` and `connect`: the `Authorization` header is present when auth is set and absent when it is not. A 401 error has no password in it.
  `fmt.Sprint(c)` has no password in it.

## Done when
- `gofmt -l .` prints nothing, and `go vet ./...`, `go build ./...` and `go test ./...` all pass without Docker. All existing tests still pass.
- `git diff main -- cmd internal/config internal/tools internal/actions` is empty.
- The new options and `NewClients` are tested as listed above.
- The README section is in the PR.
- The PR is about 400–800 changed lines, not counting `go.sum`.
- The PR lists `kfake` and the version bumps under "New dependencies".
- The PR has a "Follow-up for A4/A5" section with the exact replacement code:
  `cs, err := clientsec.NewClients(cfg)` → `&a2Clients{connect: cs.Connect, kafka: cs.Kafka, registry: cs.Registry}, cs.Close`.
