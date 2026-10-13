package redact

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTextTable(t *testing.T) {
	// Synthetic values, built here so no scanner mistakes them for real ones.
	ant := "sk-ant-api03-" + strings.Repeat("Ab3x", 12) + "9Q"
	akia := "AKIA" + "Z7QW3RTY5UIOPLKJ"
	tests := []struct {
		name, in, want string
		kind           Kind
	}{
		{"anthropic key", "key " + ant + " end", "key sk-a…[redacted 64]…9Q end", KindAPIKey},
		{"aws key id", "id=" + akia, "id=AKIA…[redacted 24]…KJ", KindAWSKeyID},
		{"dsn password", "postgres://app:Pa55-word!@db:5432/orders", "postgres://app:[redacted 16]@db:5432/orders", KindURLPass},
		{"libpq password", "host=db user=app password=Pa55word dbname=orders", "host=db user=app password=[redacted 8] dbname=orders", KindKeyValue},
		{"jaas", `username="svc" password="Pa55word";`, `username="svc" password="[redacted 8]";`, KindKeyValue},
		{"json password", `{"password":"Pa55word"}`, `{"password":"[redacted 8]"}`, KindKeyValue},
		{"yaml secret", "client_secret: Zx81Lk29Qp0Wm3Nb7Vc5", "client_secret: Zx81…[redacted 24]…c5", KindKeyValue},
		{"bearer header", "Authorization: Bearer Zx81Lk29Qp0Wm3Nb7Vc5", "Authorization: Bearer Zx81…[redacted 24]…c5", KindAuthHeader},
		{"snowflake header", `Authorization: Snowflake Token="ver:1-hint:1234-ETMsDgAAAZx81Lk29Qp0Wm3"`, `Authorization: Snowflake Token="ver:…[redacted 48]…m3"`, KindAuthHeader},
		{"pem", "-----BEGIN PRIVATE KEY-----\n" + strings.Repeat("QUJD", 16) + "\n-----END PRIVATE KEY-----", "[redacted private key 128]", KindPrivateKey},
		{"pem truncated", "key: -----BEGIN RSA PRIVATE KEY-----\n" + strings.Repeat("QUJD", 16), "key: [redacted private key 128]", KindPrivateKey},
		{"email", "owner priya.s@example.com", "owner [redacted 8]@example.com", KindEmail},
		{"weak key low entropy kept", "token_type=bearer auth=md5", "token_type=bearer auth=md5", ""},
		{"masked placeholder kept", "database.password: ***", "database.password: ***", ""},
		{"meta key kept", "password_encryption=scram-sha-256", "password_encryption=scram-sha-256", ""},
		{"ip kept by default", "worker 172.18.0.6:8083", "worker 172.18.0.6:8083", ""},
		{"record key kept", "trace_record key=101", "trace_record key=101", ""},
		{"empty", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, st := Text(tc.in)
			if got != tc.want {
				t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
			}
			if tc.kind == "" && st.Total != 0 {
				t.Errorf("stats = %+v, want none", st)
			}
			if tc.kind != "" && st.ByKind[tc.kind] == 0 {
				t.Errorf("stats = %+v, want kind %s", st, tc.kind)
			}
		})
	}
}

func TestOptionsIPs(t *testing.T) {
	r := New(Options{IPs: true})
	got, st := r.Text("broker 10.0.4.12:9092 and fe80::1ff:fe23:4567:890a down")
	if strings.Contains(got, "10.0.4.12") || strings.Contains(got, "fe80::1ff") || st.ByKind[KindIP] != 2 {
		t.Errorf("IPs not masked: %q %+v", got, st)
	}
	// Times and LSNs are not addresses.
	in := "at 09:41:07 lsn 1A/3F0021C8"
	if got, _ := r.Text(in); got != in {
		t.Errorf("Text(%q) = %q", in, got)
	}
	r = New(Options{})
	in = "mail priya@example.com"
	if got, _ := r.Text(in); got != in {
		t.Errorf("emails off: Text(%q) = %q", in, got)
	}
}

func TestJSON(t *testing.T) {
	in := map[string]any{
		"name":   "pg-orders-connector",
		"config": map[string]any{"database.password": "Pa55word", "database.user": "debezium", "slot.name": "debezium_orders"},
		"tasks":  []any{map[string]any{"id": json.Number("0"), "trace": "dial postgres://a:Pa55word@db/orders"}},
		"n":      float64(3),
	}
	got := JSON(in).(map[string]any)
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "Pa55word") {
		t.Fatalf("password survived: %s", b)
	}
	for _, keep := range []string{"pg-orders-connector", "debezium_orders", `"database.user":"debezium"`, `"id":0`, `"n":3`} {
		if !strings.Contains(string(b), keep) {
			t.Errorf("lost %s: %s", keep, b)
		}
	}
	// Input untouched.
	if in["config"].(map[string]any)["database.password"] != "Pa55word" {
		t.Error("JSON modified its input")
	}
}

type connStatus struct {
	Name   string `json:"name"`
	Trace  string `json:"trace"`
	Offset int64  `json:"offset"`
}

func TestJSONStructs(t *testing.T) {
	clean := connStatus{Name: "pg-orders-connector", Trace: "ok", Offset: 25377154}
	if got := JSON(clean); got != any(clean) {
		t.Errorf("clean struct replaced: %#v", got)
	}
	dirty := connStatus{Name: "c", Trace: "password=Pa55word", Offset: 9007199254740993}
	b, _ := json.Marshal(JSON(dirty))
	if strings.Contains(string(b), "Pa55word") || !strings.Contains(string(b), "9007199254740993") {
		t.Errorf("got %s", b)
	}
	if got := JSON(nil); got != nil {
		t.Errorf("JSON(nil) = %v", got)
	}
	if got := JSON(make(chan int)); got == nil {
		t.Error("unmarshalable value dropped")
	}
	ss := JSON([]string{"ok", "token=Zx81Lk29Qp0Wm3Nb7Vc5"}).([]string)
	if ss[0] != "ok" || strings.Contains(ss[1], "Lk29Qp0Wm3") {
		t.Errorf("got %q", ss)
	}
	ms := JSON(map[string]string{"api_key": "Zx81Lk29Qp0Wm3Nb7Vc5"}).(map[string]string)
	if strings.Contains(ms["api_key"], "Lk29Qp0Wm3") {
		t.Errorf("got %q", ms)
	}
}

func TestIdempotentOnFixtures(t *testing.T) {
	for _, f := range buildFixtures(3, 1) {
		once, _ := Text(f.text)
		twice, st := Text(once)
		if twice != once || st.Total != 0 {
			t.Errorf("not idempotent:\n  once:  %s\n  twice: %s", once, twice)
		}
	}
}

// eightKB builds an ~8 KB tool summary: fixture text with a few secrets.
func eightKB() string {
	var b strings.Builder
	fx := buildFixtures(1, 1)
	for i := 0; b.Len() < 8<<10; i++ {
		if i%10 == 0 {
			b.WriteString(fx[i%len(fx)].text)
		} else {
			b.WriteString(evidenceLines[i%len(evidenceLines)])
		}
		b.WriteByte('\n')
	}
	return b.String()[:8<<10]
}

func BenchmarkText(b *testing.B) {
	s := eightKB()
	b.SetBytes(int64(len(s)))
	b.ReportAllocs()
	for b.Loop() {
		Text(s)
	}
}

func BenchmarkTextClean(b *testing.B) {
	var sb strings.Builder
	for i := 0; sb.Len() < 8<<10; i++ {
		sb.WriteString(evidenceLines[i%len(evidenceLines)] + "\n")
	}
	s := sb.String()
	b.SetBytes(int64(len(s)))
	for b.Loop() {
		Text(s)
	}
}

func FuzzText(f *testing.F) {
	for _, fx := range buildFixtures(5, 1) {
		f.Add(fx.text)
	}
	for _, l := range evidenceLines {
		f.Add(l)
	}
	f.Add("password=\"\\\"x\" token='' postgres://:@ sk-ant-" + strings.Repeat("a1", 15))
	f.Fuzz(func(t *testing.T, s string) {
		once, _ := Text(s)
		twice, _ := Text(once)
		if twice != once {
			t.Fatalf("not idempotent:\n in:    %q\n once:  %q\n twice: %q", s, once, twice)
		}
		if utf8.ValidString(s) && !utf8.ValidString(once) {
			t.Fatalf("produced invalid UTF-8 from %q: %q", s, once)
		}
	})
}
