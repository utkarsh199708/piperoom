package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Credential-shaped test values. They are AWS's and the vendors' documented
// example shapes, never real keys.
const (
	fakeAWSKeyID     = "AKIAIOSFODNN7EXAMPLE"
	fakeAWSSecret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	fakeCCloudKey    = "ABCD1234EFGH5678"
	fakeCCloudSecret = "aB3dE5fG7hI9jK1lM3nO5pQ7rS9tU1vW3xY5zA7bC9dE1fG3hI5jK7lM9nO1pQ3r"
)

func TestScrubberTextRows(t *testing.T) {
	s := Scrubber{Names: map[string]string{"acme-plugins": "piperoom-bucket", "acme-prod": "project-0"}}
	cases := []struct {
		name, in, want string
	}{
		{"account in an ARN and its region", "arn:aws:kafkaconnect:eu-west-1:123456789012:connector/c/abc", "arn:aws:kafkaconnect:us-east-1:000000000000:connector/c/abc"},
		{"ARN without a region", "arn:aws:iam::123456789012:role/r", "arn:aws:iam::000000000000:role/r"},
		{"bare account id", `"Account":"123456789012"`, `"Account":"000000000000"`},
		{"13 digits are not an account", "1700000000000", "1700000000000"},
		{"cloud hostname", "b-1.demo.abc123.c2.kafka.eu-west-1.amazonaws.com:9098", "scrubbed.amazonaws.com:9098"},
		{"confluent hostname", "pkc-abc12.us-west-2.aws.confluent.cloud:9092", "scrubbed.confluent.cloud:9092"},
		{"the Confluent API host stays", "https://api.confluent.cloud/connect/v1", "https://api.confluent.cloud/connect/v1"},
		{"aiven hostname", "connect-acme-prod.aivencloud.com:443", "scrubbed.aivencloud.com:443"},
		{"confluent ids", "/environments/env-a1b2c/clusters/lkc-x9y8z sa-q1w2e", "/environments/env-00000/clusters/lkc-00000 sa-00000"},
		{"listed names", "s3://acme-plugins/debezium.zip in acme-prod", "s3://piperoom-bucket/debezium.zip in project-0"},
		{"URL userinfo", "postgres://app:hunter2@db:5432/orders", "postgres://REDACTED@db:5432/orders"},
		{"AWS service principal stays", `"Service":"kafkaconnect.amazonaws.com"`, `"Service":"kafkaconnect.amazonaws.com"`},
		{"AWS regional endpoint stays", "kafkaconnect.eu-west-1.amazonaws.com", "kafkaconnect.eu-west-1.amazonaws.com"},
		{"S3 virtual host is scrubbed", "acme.s3.eu-west-1.amazonaws.com", "scrubbed.amazonaws.com"},
		{"plain text is unchanged", "connector is RUNNING", "connector is RUNNING"},
		{"AWS key id", "key " + fakeAWSKeyID, "key REDACTED"},
		{"AWS secret shape", "s " + fakeAWSSecret + " e", "s REDACTED e"},
		{"Confluent key id", `"id":"` + fakeCCloudKey + `"`, `"id":"REDACTED"`},
		{"Confluent secret shape", fakeCCloudSecret, "REDACTED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.Text(tc.in); got != tc.want {
				t.Errorf("Text(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestScrubberBodyRows(t *testing.T) {
	s := Scrubber{}
	cases := []struct {
		name, ctype, in string
		gone            []string
		keep            []string
	}{
		{"secret key", "application/json", `{"spec":{"secret":"s3cr3t-value"}}`, []string{"s3cr3t-value"}, []string{`"secret":"REDACTED"`}},
		{"LooksSecret key", "application/json", `{"database.password":"pw-1"}`, []string{"pw-1"}, []string{`"database.password":"REDACTED"`}},
		{"service_uri", "application/json", `{"service":{"service_uri":"https://avnadmin:pw@x.aivencloud.com:443"}}`, []string{"avnadmin", "pw@"}, []string{`"service_uri":"REDACTED"`}},
		{"users[].password", "application/json", `{"users":[{"username":"avnadmin","password":"pw-2"}]}`, []string{"pw-2"}, []string{`"username":"avnadmin"`}},
		{"connection_info object", "application/json", `{"connection_info":{"kafka":["a:b@c"]}}`, []string{"a:b@c"}, []string{`"connection_info":"REDACTED"`}},
		{"access_key and access_cert", "application/json", `{"access_key":"-----BEGIN","access_cert":"-----BEGIN C"}`, []string{"BEGIN"}, nil},
		{"Connect name/value form", "application/json", `{"configs":[{"value":{"name":"database.password","value":"pw-3"}}]}`, []string{"pw-3"}, []string{`"name":"database.password"`}},
		{"ConnectorConfiguration", "application/x-amz-json-1.1", `{"connectorConfiguration":{"database.password":"pw-4","database.hostname":"db"}}`, []string{"pw-4"}, []string{`"database.hostname":"db"`}},
		{"form body", "application/x-www-form-urlencoded", "Action=GetRole&RoleName=r&Password=pw-5", []string{"pw-5"}, []string{"Action=GetRole"}},
		{"text body", "text/plain", "connect to https://u:p@h/", []string{"u:p@"}, []string{"REDACTED@h"}},
		{"keys are sorted", "application/json", `{"b":1,"a":2}`, nil, []string{`{"a":2,"b":1}`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s.Body(tc.ctype, []byte(tc.in))
			for _, g := range tc.gone {
				if strings.Contains(got, g) {
					t.Errorf("Body kept %q: %s", g, got)
				}
			}
			for _, k := range tc.keep {
				if !strings.Contains(got, k) {
					t.Errorf("Body lost %q: %s", k, got)
				}
			}
		})
	}
}

func TestScrubberHeaders(t *testing.T) {
	s := Scrubber{}
	h := http.Header{}
	h.Set("Authorization", "AWS4-HMAC-SHA256 Credential=x")
	h.Set("X-Amz-Date", "20261030T000000Z")
	h.Set("X-Amz-Security-Token", "tok")
	h.Set("X-Amzn-RequestId", "id")
	h.Set("Set-Cookie", "a=b")
	h.Set("Retry-After", "2")
	h.Set("Www-Authenticate", "Basic realm=x")
	got := s.ResponseHeaders(h)
	for _, k := range []string{"Authorization", "X-Amz-Date", "X-Amz-Security-Token", "X-Amzn-Requestid", "Set-Cookie"} {
		if _, ok := got[k]; ok {
			t.Errorf("header %s kept", k)
		}
	}
	if got["Retry-After"] != "2" {
		t.Errorf("Retry-After = %q, want 2", got["Retry-After"])
	}
	if got["Www-Authenticate"] != Redacted {
		t.Errorf("an auth header was not REDACTED: %q", got["Www-Authenticate"])
	}
	req := http.Header{}
	req.Set("Authorization", "aivenv1 tok")
	req.Set("Content-Type", "application/json")
	rh := s.RequestHeaders(req)
	if _, ok := rh["Authorization"]; ok || rh["Content-Type"] != "application/json" {
		t.Errorf("RequestHeaders = %v", rh)
	}
}

// record runs one exchange against a server answering body and returns the
// recorded cassette and what the caller received.
func record(t *testing.T, provider, path, reqBody, respBody string) (*Cassette, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "session=abc")
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	rec := NewRecorder(srv.Client().Transport, Scrubber{}, provider, t.Name(), "test server", time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
	c := &http.Client{Transport: rec, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+path, strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "aivenv1 the-token-value")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	return rec.Cassette(), string(got)
}

// Design §5: each provider's create-key, create-user and describe cassette
// has a dedicated test asserting the response was rewritten.
func TestRecorderScrubsProviderShapes(t *testing.T) {
	cases := []struct {
		name, provider, path, reqBody, respBody string
		secrets                                 []string
	}{
		{"confluent IAM v2 api-key spec.secret", ProviderConfluent, "/iam/v2/api-keys", `{"spec":{"owner":{"id":"sa-abc123"}}}`,
			`{"id":"` + fakeCCloudKey + `","spec":{"secret":"` + fakeCCloudSecret + `","owner":{"id":"sa-abc123"}}}`,
			[]string{fakeCCloudSecret}},
		{"aiven service_uri and users[]", ProviderAiven, "/v1/project/acme/service/kc", `{}`,
			`{"service":{"service_uri":"https://avnadmin:AVNS_pw1@kc-acme.aivencloud.com:443","users":[{"username":"avnadmin","password":"AVNS_pw1"}],"connection_info":{"kafka_connect_uri":"https://avnadmin:AVNS_pw1@kc-acme.aivencloud.com"}}}`,
			[]string{"AVNS_pw1"}},
		{"aiven create-user", ProviderAiven, "/v1/project/acme/service/kc/user", `{"username":"piperoom-ro"}`,
			`{"user":{"username":"piperoom-ro","password":"AVNS_pw2","access_cert":"-----BEGIN CERTIFICATE-----x","access_key":"-----BEGIN PRIVATE KEY-----y"}}`,
			[]string{"AVNS_pw2", "BEGIN"}},
		{"DescribeConnector ConnectorConfiguration", ProviderAWS, "/v1/connectors/arn", ``,
			`{"connectorArn":"arn:aws:kafkaconnect:eu-west-1:123456789012:connector/c/1","connectorConfiguration":{"database.password":"pw-msk","database.user":"debezium"}}`,
			[]string{"pw-msk", "123456789012"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, got := record(t, tc.provider, tc.path, tc.reqBody, tc.respBody)
			if got != tc.respBody {
				t.Errorf("the caller got a scrubbed body; want the real response")
			}
			raw, _ := json.Marshal(c)
			for _, s := range tc.secrets {
				if strings.Contains(string(raw), s) {
					t.Errorf("cassette kept %q: %s", s, raw)
				}
			}
			for _, h := range []string{"aivenv1", "the-token-value", "session=abc"} {
				if strings.Contains(string(raw), h) {
					t.Errorf("cassette kept header text %q", h)
				}
			}
			if f := Check(c); len(f) > 0 {
				t.Errorf("a scrubbed cassette fails Check: %v", f)
			}
			path := filepath.Join(t.TempDir(), "c.json")
			if err := Save(c, path); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if _, err := Load(path); err != nil {
				t.Errorf("Load of a scrubbed cassette: %v", err)
			}
		})
	}
}

func cassetteWith(resp Response, req Request) *Cassette {
	if req.Method == "" {
		req.Method = http.MethodGet
	}
	if req.Path == "" {
		req.Path = "/v1/x"
	}
	return &Cassette{Provider: ProviderTest, Name: "t", RecordedAt: time.Now(), Interactions: []Interaction{{Request: req, Response: resp}}}
}

// The board's U13a "Done when": a cassette with a credential-looking string
// fails the test that loads it.
func TestLoadRefusesCredentialShapes(t *testing.T) {
	cases := []struct {
		name string
		c    *Cassette
	}{
		{"AWS access key id", cassetteWith(Response{Status: 200, Body: `{"note":"` + fakeAWSKeyID + `"}`}, Request{})},
		{"AWS secret key", cassetteWith(Response{Status: 200, Body: `{"note":"` + fakeAWSSecret + `"}`}, Request{})},
		{"Confluent API key", cassetteWith(Response{Status: 200, Body: `{"id":"` + fakeCCloudKey + `"}`}, Request{})},
		{"Confluent secret", cassetteWith(Response{Status: 200, Body: `{"note":"` + fakeCCloudSecret + `"}`}, Request{})},
		{"Aiven token header text", cassetteWith(Response{Status: 200, Body: `{"note":"aivenv1 abc"}`}, Request{})},
		{"URL userinfo", cassetteWith(Response{Status: 200, Body: `{"uri":"https://avnadmin:pw@host:443"}`}, Request{})},
		{"secret key not REDACTED", cassetteWith(Response{Status: 200, Body: `{"spec":{"secret":"x"}}`}, Request{})},
		{"password in a request body", cassetteWith(Response{Status: 200, Body: `{}`}, Request{Method: "POST", Body: `{"config":{"database.password":"pw"}}`})},
		{"Connect name/value password", cassetteWith(Response{Status: 200, Body: `{"configs":[{"value":{"name":"database.password","value":"pw"}}]}`}, Request{})},
		{"Authorization header", cassetteWith(Response{Status: 200, Body: `{}`}, Request{Headers: map[string]string{"Authorization": "Basic eDp5"}})},
		{"SigV4 header", cassetteWith(Response{Status: 200, Body: `{}`}, Request{Headers: map[string]string{"X-Amz-Security-Token": "x"}})},
		{"password in a query", cassetteWith(Response{Status: 200, Body: `{}`}, Request{Query: "password=hunter2xyz"})},
		{"bearer token", cassetteWith(Response{Status: 200, Body: `{"h":"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcdefghij"}`}, Request{})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.c)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "bad.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); !errors.Is(err, ErrCredential) {
				t.Fatalf("Load = %v, want ErrCredential", err)
			}
			if err := Save(tc.c, filepath.Join(t.TempDir(), "out.json")); !errors.Is(err, ErrCredential) {
				t.Errorf("Save = %v, want ErrCredential", err)
			}
			ftb := &fatalTB{TB: t}
			if c := MustLoad(ftb, path); c != nil || !ftb.failed {
				t.Errorf("MustLoad did not fail the test that loads it")
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("the error quotes the value: %v", err)
			}
		})
	}
}

// fatalTB records Fatalf instead of stopping the goroutine.
type fatalTB struct {
	testing.TB
	failed bool
}

func (f *fatalTB) Fatalf(string, ...any) { f.failed = true }
func (f *fatalTB) Helper()               {}

// Realistic clean shapes must pass the scan: a false positive would make
// every real cassette unloadable.
func TestCheckAcceptsCleanShapes(t *testing.T) {
	bodies := []string{
		`{"connectorArn":"arn:aws:kafkaconnect:us-east-1:000000000000:connector/it_orders/0a1b2c3d-4e5f-6789-abcd-ef0123456789-2","connectorState":"RUNNING","currentVersion":"K3AEGXETSR30VB","connectorConfiguration":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.password":"REDACTED","topic.prefix":"orders"}}`,
		`{"customPluginArn":"arn:aws:kafkaconnect:us-east-1:000000000000:custom-plugin/debezium-pg/9f8e7d6c-5b4a-3210-fedc-ba9876543210-3","name":"debezium-pg","revision":1,"customPluginState":"CREATING"}`,
		`{"propertiesFileContent":"a2V5LmNvbnZlcnRlcj1vcmcuYXBhY2hlLmthZmthLmNvbm5lY3QuanNvbi5Kc29uQ29udmVydGVyCnZhbHVlLmNvbnZlcnRlcj1vcmcuYXBhY2hlLmthZmthLmNvbm5lY3QuanNvbi5Kc29uQ29udmVydGVyCg=="}`,
		`<GetCallerIdentityResult><Arn>arn:aws:sts::000000000000:assumed-role/PiperoomSetup/session</Arn><UserId>AROAEXAMPLEROLEIDXX:session</UserId><Account>000000000000</Account></GetCallerIdentityResult>`,
		`{"name":"it_orders","connector":{"state":"RUNNING","worker_id":"it_orders"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"it_orders"}],"type":"source"}`,
		`{"error_count":1,"configs":[{"value":{"name":"database.hostname","value":"db.example.com","errors":["Could not connect"]}},{"value":{"name":"database.password","value":"REDACTED","errors":[]}}]}`,
		`{"bootstrapBrokerStringSaslIam":"scrubbed.amazonaws.com:9098,scrubbed.amazonaws.com:9098"}`,
	}
	for i, b := range bodies {
		c := cassetteWith(Response{Status: 200, Body: b}, Request{})
		if f := Check(c); len(f) > 0 {
			t.Errorf("body %d: unexpected findings %v", i, f)
		}
	}
}

func TestReplayerSequenceAndMatching(t *testing.T) {
	c := &Cassette{Provider: ProviderTest, Name: "seq", Interactions: []Interaction{
		{Request: Request{Method: "GET", Path: "/v1/connectors/arn:aws:kafkaconnect:us-east-1:000000000000:connector/c/1"}, Response: Response{Status: 200, Body: `{"connectorState":"CREATING"}`}},
		{Request: Request{Method: "GET", Path: "/v1/connectors/arn:aws:kafkaconnect:us-east-1:000000000000:connector/c/1"}, Response: Response{Status: 200, Body: `{"connectorState":"RUNNING"}`}},
		{Request: Request{Method: "POST", Path: "/v1/connectors", Body: `{"config":{"database.password":"REDACTED"},"name":"c"}`}, Response: Response{Status: 200, Body: `{"ok":true}`}},
		{Request: Request{Method: "GET", Path: "/v1/list", Query: "a=1&b=2"}, Response: Response{Status: 200, Body: `{}`}},
		{Request: Request{Method: "GET", Path: "/v1/cut"}, Response: Response{Status: 200, Body: `{"conn`, Cut: true}},
	}}
	r := NewReplayer(c, Scrubber{})
	hc := r.Client()
	get := func(path string) (int, string, error) {
		resp, err := hc.Get("https://kafkaconnect.eu-west-1.amazonaws.com" + path)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), err
	}
	// The SDK escapes the ARN; the live account id is scrubbed before the match.
	arn := "/v1/connectors/arn%3Aaws%3Akafkaconnect%3Aeu-west-1%3A123456789012%3Aconnector%2Fc%2F1"
	if _, b, err := get(arn); err != nil || !strings.Contains(b, "CREATING") {
		t.Fatalf("first poll = %q, %v", b, err)
	}
	if _, b, err := get(arn); err != nil || !strings.Contains(b, "RUNNING") {
		t.Fatalf("second poll = %q, %v", b, err)
	}
	if _, _, err := get(arn); !errors.Is(err, ErrUnmatched) {
		t.Fatalf("third poll = %v, want ErrUnmatched", err)
	}
	resp, err := hc.Post("https://kafkaconnect.eu-west-1.amazonaws.com/v1/connectors", "application/json",
		strings.NewReader(`{"name":"c","config":{"database.password":"the-live-password"}}`))
	if err != nil {
		t.Fatalf("POST with a live password did not match the scrubbed body: %v", err)
	}
	resp.Body.Close()
	if _, _, err := get("/v1/list?b=2&a=1"); err != nil {
		t.Errorf("query in another order did not match: %v", err)
	}
	if _, _, err := get("/v1/cut"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("cut body = %v, want io.ErrUnexpectedEOF", err)
	}
	if u := r.Unmatched(); len(u) != 1 || !strings.Contains(u[0], "GET /v1/connectors/") {
		t.Errorf("Unmatched = %v", u)
	}
	if r.Remaining() != 0 {
		t.Errorf("Remaining = %d, want 0", r.Remaining())
	}
}

func TestReplayerBodyHash(t *testing.T) {
	body := `{"a":"b"}`
	c := cassetteWith(Response{Status: 200, Body: `{}`}, Request{Method: "POST", Path: "/x", BodySHA256: bodyHash(body)})
	r := NewReplayer(c, Scrubber{})
	resp, err := r.Client().Post("http://h/x", "application/json", strings.NewReader(`{ "a" : "b" }`))
	if err != nil {
		t.Fatalf("a normalised body did not match its hash: %v", err)
	}
	resp.Body.Close()
	r2 := NewReplayer(c, Scrubber{})
	if _, err := r2.Client().Post("http://h/x", "application/json", strings.NewReader(`{"a":"c"}`)); !errors.Is(err, ErrUnmatched) {
		t.Errorf("another body = %v, want ErrUnmatched", err)
	}
}

func TestStale(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	fresh := &Cassette{RecordedAt: now.Add(-24 * time.Hour)}
	old := &Cassette{RecordedAt: now.Add(-181 * 24 * time.Hour)}
	if fresh.Stale(now) || !old.Stale(now) {
		t.Errorf("Stale: fresh %v old %v", fresh.Stale(now), old.Stale(now))
	}
}

func TestParseRefusesMalformed(t *testing.T) {
	for i, raw := range []string{`{`, `{"interactions":[]}`, `{"interactions":[{"request":{"method":"GET"},"response":{"status":200}}]}`,
		`{"interactions":[{"request":{"method":"GET","path":"/"},"response":{"status":0}}]}`} {
		if _, err := Parse([]byte(raw), fmt.Sprint(i)); !errors.Is(err, ErrCassette) {
			t.Errorf("case %d: %v, want ErrCassette", i, err)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json")); !errors.Is(err, ErrCassette) {
		t.Errorf("missing file: %v", err)
	}
}
