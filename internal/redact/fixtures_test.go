package redact

import (
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

// The secrets in this file are generated at test time from a fixed seed.
// None of them is real; the templates only mimic where real ones show up in
// connector configs, error traces, client logs and record values.

const (
	alnum  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b64    = alnum + "+/"
	urlb64 = alnum + "-_"
	upnum  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	hexd   = "0123456789abcdef"
	pwset  = alnum + "!#%^*-_~"
)

type gen struct{ r *rand.Rand }

func newGen(seed uint64) *gen { return &gen{rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))} }

func (g *gen) str(set string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = set[g.r.IntN(len(set))]
	}
	return string(b)
}

// password returns a human-style password: always mixed classes, 10-20 long.
func (g *gen) password() string {
	return g.str("ABCDEFGHJKLMNPQRSTUVWXYZ", 1) + g.str("abcdefghijkmnpqrstuvwxyz", 3) + g.str("23456789", 2) + g.str(pwset, 4+g.r.IntN(11))
}

func (g *gen) jwt() string {
	enc := base64.RawURLEncoding
	h := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT","kid":"` + g.str(hexd, 8) + `"}`))
	p := enc.EncodeToString([]byte(`{"sub":"svc-` + g.str(hexd, 6) + `","exp":` + fmt.Sprint(1760000000+g.r.IntN(1e6)) + `}`))
	return h + "." + p + "." + g.str(urlb64, 43)
}

func (g *gen) pem(kind string) string {
	var b strings.Builder
	b.WriteString("-----BEGIN " + kind + "PRIVATE KEY-----\n")
	for range 4 {
		b.WriteString(g.str(b64, 64) + "\n")
	}
	b.WriteString(g.str(b64, 20) + "==\n-----END " + kind + "PRIVATE KEY-----")
	return b.String()
}

// secretGen makes one value for a {placeholder} in a template.
var secretGen = map[string]func(g *gen) string{
	"pw":     (*gen).password,
	"awsid":  func(g *gen) string { return "AKIA" + g.str(upnum, 16) },
	"awssts": func(g *gen) string { return "ASIA" + g.str(upnum, 16) },
	"awssec": func(g *gen) string {
		return g.str("ABCDEFGHIJ", 2) + g.str("abcdefghij", 2) + g.str("0123456789", 2) + g.str(b64, 34)
	},
	"jwt":   (*gen).jwt,
	"skant": func(g *gen) string { return "sk-ant-api03-" + g.str(urlb64, 93) + "AA" },
	"sk":    func(g *gen) string { return "sk-proj-" + g.str(urlb64, 48) },
	"xoxb": func(g *gen) string {
		return "xoxb-" + g.str("0123456789", 12) + "-" + g.str("0123456789", 13) + "-" + g.str(alnum, 24)
	},
	"ghp":   func(g *gen) string { return "ghp_" + g.str(alnum, 36) },
	"ghpat": func(g *gen) string { return "github_pat_" + g.str(alnum, 22) + "_" + g.str(alnum, 59) },
	"gcp":   func(g *gen) string { return "AIza" + g.str(urlb64, 35) },
	"ya29":  func(g *gen) string { return "ya29." + g.str(urlb64, 120) },
	"azsec": func(g *gen) string { return g.str(alnum, 3) + "8Q~" + g.str(alnum, 34) },
	"sas":   func(g *gen) string { return g.str(alnum, 30) + "%3D" },
	"sf": func(g *gen) string {
		return "ver:1-hint:" + g.str("0123456789", 8) + "-ETMsDgAAA" + g.str(b64, 80) + "="
	},
	"tok":     func(g *gen) string { return g.str(alnum, 8) + g.str("0123456789", 2) + g.str(alnum, 30) },
	"b64key":  func(g *gen) string { return g.str(b64, 2) + "Aa1" + g.str(b64, 81) + "==" },
	"pemrsa":  func(g *gen) string { return g.pem("RSA ") },
	"pem":     func(g *gen) string { return g.pem("") },
	"pemenc":  func(g *gen) string { return g.pem("ENCRYPTED ") },
	"local":   func(g *gen) string { return g.str("abcdefghijklmnop", 5) + "." + g.str("abcdefghijklmnop", 6) },
	"glpat":   func(g *gen) string { return "glpat-" + g.str(urlb64, 20) },
	"stripe":  func(g *gen) string { return "sk_live_" + g.str(alnum, 24) },
	"basic":   func(g *gen) string { return base64.StdEncoding.EncodeToString([]byte("svc_cdc:" + g.password())) },
	"cookie":  func(g *gen) string { return g.str(alnum, 32) },
	"sfmastr": func(g *gen) string { return "ver:1-hint:" + g.str("0123456789", 6) + "-" + g.str(b64, 60) },
}

// secretTemplates are realistic lines that each carry one or more secrets.
var secretTemplates = []string{
	// Postgres and JDBC DSNs.
	`dial postgres://debezium:{pw}@pg-primary.internal:5432/orders?sslmode=require: connection refused`,
	`source dsn=postgresql://piperoom:{pw}@localhost:5432/orders`,
	`failed to connect to host=10.0.4.12 user=app password={pw} dbname=orders: server error (FATAL: password authentication failed for user "app" (SQLSTATE 28P01))`,
	`database.url: jdbc:postgresql://pg-primary:5432/orders?user=debezium&password={pw}&ssl=true`,
	`warehouse dsn: snowflake://svc_loader:{pw}@xy12345.eu-west-1/ANALYTICS/PUBLIC?warehouse=LOAD_WH`,
	`mysql://root:{pw}@mysql-legacy:3306/inventory`,
	`redis://:{pw}@cache-0.cache:6379/0`,
	`amqp://guest:{pw}@rabbit:5672/vhost`,
	`mongodb+srv://reader:{pw}@cluster0.ab1cd.mongodb.net/orders?retryWrites=true`,
	`PGPASSWORD={pw} psql -h pg-primary -U debezium orders -c 'select 1'`,
	`DATABASE_PASSWORD="{pw}"`,
	`  database.password: {pw}`,
	`"database.password": "{pw}",`,
	`connection.password={pw}`,
	`Connection string: Host=pg;Port=5432;Username=etl;Password={pw};Database=orders`,
	`env PG_PASS={pw} SNOWFLAKE_PASSWORD={pw}`,
	`{"host":"pg-primary","port":5432,"user":"debezium","password":"{pw}","dbname":"orders"}`,
	`password: '{pw}'`,
	`db_passwd={pw}`,
	`ssl.keystore.password={pw}`,
	`ssl.key.password={pw}`,
	`ssl.truststore.password={pw}`,
	`clientSecret: {tok}`,
	// Kafka SASL / JAAS.
	`sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="cdc" password="{pw}";`,
	`sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username='svc-orders' password='{pw}';`,
	`producer.override.sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="{awsid}" password="{awssec}";`,
	`KAFKA_SASL_PASSWORD={pw}`,
	`sasl.password = {pw}`,
	`"sasl.password":"{pw}"`,
	`schema.registry.basic.auth.user.info=sr-user:{pw}`,
	`basic.auth.user.info: "{awsid}:{tok}"`,
	`schema.registry.url=https://sr-reader:{pw}@schema-registry.prod:8081`,
	// AWS.
	`aws_access_key_id = {awsid}`,
	`aws_secret_access_key = {awssec}`,
	`AWS_SECRET_ACCESS_KEY={awssec}`,
	`aws_session_token = {b64key}`,
	`Credentials: AccessKeyId={awssts} SecretAccessKey={awssec} SessionToken={b64key}`,
	`{awsid},{awssec}`,
	`s3 put failed: InvalidAccessKeyId: The AWS Access Key Id you provided ({awsid}) does not exist in our records.`,
	`"AccessKeyId": "{awssts}", "SecretAccessKey": "{awssec}"`,
	`export AWS_ACCESS_KEY_ID={awsid} AWS_SECRET_ACCESS_KEY={awssec}`,
	`s3.credentials.provider.secret = {awssec}`,
	// GCP / Azure.
	`gcs: googleapi: Error 403: key={gcp} is not valid`,
	`GOOGLE_API_KEY={gcp}`,
	`Authorization: Bearer {ya29}`,
	`"private_key_id": "3f1c0d9e8b7a6f5e4d3c2b1a09f8e7d6c5b4a392","private_key": "{pemescaped}"`,
	`DefaultEndpointsProtocol=https;AccountName=cdcarchive;AccountKey={b64key};EndpointSuffix=core.windows.net`,
	`https://cdcarchive.blob.core.windows.net/raw/orders.avro?sv=2022-11-02&ss=b&srt=o&sp=r&se=2026-12-01T00:00:00Z&sig={sas}`,
	`AZURE_CLIENT_SECRET={azsec}`,
	`azure client secret rotated: {azsec}`,
	`SharedAccessSignature=sv=2022-11-02&sig={sas}`,
	// Tokens and headers.
	`Authorization: Bearer {jwt}`,
	`authorization: Basic {basic}`,
	`Proxy-Authorization: Basic {basic}`,
	`curl -H "Authorization: Bearer {tok}" https://connect:8083/connectors`,
	`Authorization: Snowflake Token="{sf}"`,
	`snowflake session token {sf} expired`,
	`login-response masterToken={sfmastr} validityInSeconds=14400`,
	`"token": "{sf}"`,
	`id_token={jwt}`,
	`got jwt {jwt} from idp`,
	`X-Api-Key: {tok}`,
	`api_key={tok}`,
	`"apiKey": "{tok}"`,
	`ANTHROPIC_API_KEY={skant}`,
	`using key {skant} for model calls`,
	`OPENAI_API_KEY={sk}`,
	`slack post failed with token {xoxb}: not_in_channel`,
	`SLACK_BOT_TOKEN={xoxb}`,
	`git clone https://x-access-token:{ghp}@github.com/acme/pipelines.git`,
	`GH_TOKEN={ghp}`,
	`token {ghpat} lacks scope repo`,
	`gitlab token {glpat} used by ci`,
	`STRIPE_KEY={stripe}`,
	`access_token={tok}&token_type=Bearer&expires_in=3600`,
	`"refresh_token": "{tok}"`,
	`Set-Cookie: session={cookie}; Path=/; HttpOnly`,
	`Cookie: JSESSIONID={cookie}`,
	`sessionId={cookie}`,
	`auth_token: {tok}`,
	`client_secret={tok}`,
	`secret: {pw}`,
	`webhook secret={tok}`,
	`signing_key = {b64key}`,
	`encryption.key={b64key}`,
	`"credentials": "{b64key}"`,
	// Private keys.
	`{pemrsa}`,
	`snowflake private key: {pem}`,
	`{pemenc}`,
	// E-mails (personal data in record values and audit lines).
	`order 101 customer_email={local}@example.com status=paid`,
	`{"id":102,"email":"{local}@acme.io","amount":"12.50"}`,
	`approved by {local}@company.co.uk at 2026-10-11T09:44:00Z`,
	// Several on one line.
	`config dump: user=debezium password={pw} token={tok} url=postgres://debezium:{pw}@pg:5432/orders`,
	`env: AWS_ACCESS_KEY_ID={awsid} AWS_SECRET_ACCESS_KEY={awssec} ANTHROPIC_API_KEY={skant}`,
	`level=ERROR msg="snowflake login failed" err="390144 (08004): JWT token is invalid" jwt={jwt}`,
	`record value: {"id":103,"note":"pls reset pw","password":"{pw}","api_key":"{tok}"}`,
}

// evidenceLines carry no secret and must come out byte for byte unchanged.
var evidenceLines = []string{
	`slot debezium_orders: active=false retained=48.6 GiB wal_status=extended restart_lsn=1A/3F0021C8`,
	`confirmed_flush_lsn=1A/3F0021C8 pg_current_wal_lsn=1A/9B4410F0`,
	`ERROR: must be owner of table orders (SQLSTATE 42501)`,
	`FATAL: password authentication failed for user "debezium" (SQLSTATE 28P01)`,
	`org.postgresql.util.PSQLException: FATAL: password authentication failed for user "debezium"`,
	`Caused by: org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler`,
	`Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema; error code: 409`,
	`org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [pg.public.orders]`,
	`org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`,
	`at io.debezium.connector.postgresql.PostgresConnectorTask.start(PostgresConnectorTask.java:142)`,
	`devconsumer: key at pg.public.orders/1@25377154: not confluent wire format`,
	`pg.public.orders/0: committed 34, end 34, lag 0`,
	`partition 1: 0–31 (31 records), newest 15 min ago`,
	`pg.public.orders-value: versions [1 2 3], latest id 7, compatibility BACKWARD (global)`,
	`schema id 42 fingerprint 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08`,
	`commit 4f3c2a9e1b7d4e2a9c1e2f6b8d0a3e5c12345678 deployed`,
	`newest=2026-10-11T09:40:58.402113Z (stale for 14m)`,
	`2026-10-11T09:41:07.118204Z ALTER TABLE public.orders | ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2) | piperoom`,
	`pg-orders-connector (source): RUNNING on 172.18.0.6:8083.`,
	`Task 0: FAILED: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :`,
	`password_encryption = scram-sha-256`,
	`database.password: ***`,
	`"database.password": "********"`,
	`ssl.keystore.type=PKCS12`,
	`ssl.keystore.location=/etc/kafka/secrets/kafka.keystore.p12`,
	`sasl.mechanism=SCRAM-SHA-512`,
	`security.protocol=SASL_SSL`,
	`auth.method=scram-sha-256`,
	`basic.auth.credentials.source=USER_INFO`,
	`token_type: Bearer`,
	`max_tokens: 16000`,
	`session.timeout.ms=45000`,
	`key.converter=io.confluent.connect.avro.AvroConverter`,
	`value.converter.schema.registry.url=http://schema-registry:8081`,
	`key: 101`,
	`Key: {"id":101}`,
	`trace_record key=102 stopped at: kafka-connect pg-orders-connector`,
	`clusterId: MkU3OEVBNTcwNTJENDM2Qk leader epoch 12`,
	`consumer-order-sync-1-4f3c2a9e-1b7d-4e2a-9c1e-2f6b8d0a3e5c joined generation 17`,
	`Bearer token expired at 2026-10-11T09:41:07Z; refreshing`,
	`Authorization failed: user debezium lacks REPLICATION`,
	`secret_name: kafka-connect-creds`,
	`secretRef: pg-credentials`,
	`credentials file /var/run/secrets/aws/token not found`,
	`password: ${file:/opt/secrets/pg.properties:password}`,
	`database.password: ${env:PG_PASSWORD}`,
	`connector.client.config.override.policy=All`,
	`transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState`,
	`publication.autocreate.mode=filtered`,
	`Unable to update filtered publication dbz_publication for "public"."orders"`,
	`wal_status=lost safe_wal_size=-1 slot debezium_orders`,
	`log start offset 120394 > committed 118220 on pg.public.orders/2 (records deleted by retention)`,
	`retention.ms=21600000`,
	`analytics.orders rows differ: 1183402`,
	`order-sync: Stable, 1 members, total lag 0`,
	`LSN{1A/3F0021C8} processed, txId=884213`,
	`kafka-connect pg-orders-connector: stopped_here tasks_failed=1`,
	`worker id connect-1:8083 rebalance generation 33`,
	`io.debezium.DebeziumException: Error while processing event at offset {transaction_id=null, lsn_proc=4412901, lsn=4412901, txId=884213, ts_usec=1760175667118204}`,
	`Snowflake: 390114 (08001): Authentication token has expired. The user must authenticate again.`,
	`snowflake: 002003 (02000): SQL compilation error: Object 'ANALYTICS.PUBLIC.ORDERS' does not exist or not authorized.`,
	`HTTP 401 Unauthorized from http://connect:8083/connectors/pg-orders-connector/status`,
	`auth: ok`,
	`tokens used: in=12034 out=844`,
	`aws region eu-west-1 bucket cdc-archive-prod`,
	`Value.amount: int → bytes(decimal 12,2)`,
	`- Value.amount: int`,
	`git@github.com:acme/pipelines.git`,
	`subject pg.public.orders-value version 3 id 1042`,
	`2026-10-11 09:41:09.270 UTC [4412] debezium@orders LOG:  starting logical decoding for slot "debezium_orders"`,
}

// pemEscaped is a PEM block as it sits inside a JSON string.
func pemEscaped(g *gen) string { return strings.ReplaceAll(g.pem(""), "\n", `\n`) }

type fixture struct {
	text    string
	secrets []string
}

// buildFixtures expands every template several times with fresh secrets.
func buildFixtures(seed uint64, rounds int) []fixture {
	g := newGen(seed)
	var out []fixture
	for range rounds {
		for _, tpl := range secretTemplates {
			var secrets []string
			text := tpl
			for {
				i := strings.Index(text, "{")
				if i < 0 {
					break
				}
				j := strings.Index(text[i:], "}")
				if j < 0 {
					break
				}
				name := text[i+1 : i+j]
				var v string
				switch {
				case name == "pemescaped":
					v = pemEscaped(g)
				case secretGen[name] != nil:
					v = secretGen[name](g)
				default:
					// JSON braces in the template: skip past them.
					text = text[:i] + "\x00" + text[i+1:]
					continue
				}
				secrets = append(secrets, v)
				text = text[:i] + v + text[i+j+1:]
			}
			out = append(out, fixture{text: strings.ReplaceAll(text, "\x00", "{"), secrets: secrets})
		}
	}
	return out
}

// leaked reports whether redacted output still holds any part of secret
// beyond what a mask may keep (first 4 and last 2 characters).
func leaked(out, secret string) bool {
	if strings.Contains(out, secret) {
		return true
	}
	// Any 8-character window from the inside of the secret is a leak.
	if len(secret) >= 14 {
		inner := secret[4 : len(secret)-2]
		for i := 0; i+8 <= len(inner); i += 4 {
			if strings.Contains(out, inner[i:i+8]) {
				return true
			}
		}
	}
	return false
}

func TestFixturesRecallAndPrecision(t *testing.T) {
	fx := buildFixtures(42, 3)
	lines := 0
	for _, f := range fx {
		lines += strings.Count(f.text, "\n") + 1
	}
	lines += len(evidenceLines)
	if lines < 150 {
		t.Fatalf("fixture set has %d lines, want >= 150", lines)
	}

	var secrets, caught int
	for _, f := range fx {
		out, st := Text(f.text)
		for _, s := range f.secrets {
			secrets++
			if leaked(out, s) {
				t.Errorf("missed secret %q\n  in:  %s\n  out: %s", s, f.text, out)
				continue
			}
			caught++
		}
		if st.Total == 0 {
			t.Errorf("no mask reported for %q", f.text)
		}
	}

	// Precision: a mask in an evidence line is a false positive. Masks in
	// secret lines all hit a secret (checked by the per-template tests).
	var fp int
	for _, l := range evidenceLines {
		out, st := Text(l)
		if out != l || st.Total != 0 {
			fp += max(st.Total, 1)
			t.Errorf("evidence line changed:\n  in:  %s\n  out: %s", l, out)
		}
	}
	var masks int
	for _, f := range fx {
		_, st := Text(f.text)
		masks += st.Total
	}
	recall := float64(caught) / float64(secrets)
	precision := float64(masks) / float64(masks+fp)
	t.Logf("fixture lines=%d secrets=%d caught=%d recall=%.4f masks=%d evidence_fp=%d precision=%.4f",
		lines, secrets, caught, recall, masks, fp, precision)
	if caught != secrets {
		t.Errorf("recall %.4f: %d of %d secrets missed", recall, secrets-caught, secrets)
	}
}

// TestNoExtraMasks checks precision inside secret lines: every mask must
// cover a generated secret, so the number of masks never exceeds the number
// of secrets (one secret may be covered by one mask only).
func TestNoExtraMasks(t *testing.T) {
	for _, f := range buildFixtures(7, 1) {
		out, st := Text(f.text)
		if st.Total > len(f.secrets) {
			t.Errorf("%d masks for %d secrets:\n  in:  %s\n  out: %s", st.Total, len(f.secrets), f.text, out)
		}
	}
}
