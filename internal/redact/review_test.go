package redact

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Cases from the round-1 review of PR #96.

const pw = "Ab9xYz12"

// taskRow is shaped like the connect_status tool's typed task rows.
type taskRow struct {
	ID     int    `json:"id"`
	State  string `json:"state"`
	Worker string `json:"worker_id"`
	Error  string `json:"error,omitempty"`
}

func TestJSONTypedNested(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: dial postgres://debezium:" + pw + "@pg:5432/orders"
	cases := map[string]any{
		"typed slice in map": map[string]any{
			"name":  "pg-orders-connector",
			"tasks": []taskRow{{ID: 0, State: "FAILED", Worker: "172.18.0.6:8083", Error: trace}},
		},
		"struct": struct {
			Tasks []taskRow `json:"tasks"`
		}{Tasks: []taskRow{{ID: 0, State: "FAILED", Error: trace}}},
		"slice of maps":   []map[string]any{{"error": trace}},
		"raw json in map": map[string]any{"body": json.RawMessage(`{"trace":"password=\"` + pw + `\""}`)},
		"map of string":   map[string]string{"trace": trace},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			out, st := JSONStats(v)
			b, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), pw) || st.Total == 0 {
				t.Errorf("password survived (stats %+v): %s", st, b)
			}
			if strings.Contains(name, "typed") && !strings.Contains(string(b), "pg-orders-connector") {
				t.Errorf("evidence lost: %s", b)
			}
		})
	}
}

func TestJSONStrongKeyMasksEverythingBelow(t *testing.T) {
	cases := []string{
		`{"password":12345678}`,
		`{"passwords":["` + pw + `","Zz9-other"]}`,
		`{"credentials":{"value":"` + pw + `","n":12345678}}`,
		`{"db":{"password":true}}`,
	}
	for _, c := range cases {
		var v any
		d := json.NewDecoder(strings.NewReader(c))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(JSON(v))
		for _, leak := range []string{pw, "12345678", "Zz9-other", "true"} {
			if strings.Contains(string(b), leak) {
				t.Errorf("%s -> %s keeps %q", c, b, leak)
			}
		}
		// Idempotent on the generic tree.
		var again any
		_ = json.Unmarshal(b, &again)
		b2, _ := json.Marshal(JSON(again))
		if string(b2) != string(b) {
			t.Errorf("not idempotent: %s -> %s", b, b2)
		}
	}
}

func TestEscapedQuotes(t *testing.T) {
	cases := []string{
		`password=\"` + pw + `\"`,
		fmt.Sprintf("connect: %q", `login failed: password="`+pw+`"`),
		`{"sasl.jaas.config":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"cdc\" password=\"` + pw + `\";"}`,
		`{\"user\":\"cdc\",\"password\":\"` + pw + `\"}`,
	}
	for _, c := range cases {
		out, st := Text(c)
		if strings.Contains(out, pw) || st.Total != 1 {
			t.Errorf("Text(%s) = %s (stats %+v)", c, out, st)
		}
		if strings.Contains(c, "PlainLoginModule") && !strings.Contains(out, "PlainLoginModule") {
			t.Errorf("login module lost: %s", out)
		}
	}
	// A one-character value is never a mask.
	for _, c := range []string{`password=\`, `password="x"`, `token: \`} {
		if out, st := Text(c); st.Total != 0 || out != c {
			t.Errorf("Text(%q) = %q (%+v)", c, out, st)
		}
	}
}

func TestBasicAuthKeepsNoChars(t *testing.T) {
	in := "Authorization: Basic c3ZjX2NkYzpBYjl4WXoxMlNlY3JldA=="
	out, _ := Text(in)
	if out != "Authorization: Basic [redacted 32]" {
		t.Errorf("got %q", out)
	}
	in = "Authorization: Bearer Zx81Lk29Qp0Wm3Nb7Vc5"
	if out, _ := Text(in); !strings.Contains(out, "Zx81…") {
		t.Errorf("bearer lost its head: %q", out)
	}
}

func TestSlackWebhook(t *testing.T) {
	sec := "Xk29aB7qLm3PzR8tYv1WcD5e"
	for _, in := range []string{
		"posting to https://hooks.slack.com/services/T0ABC1234/B0DEF5678/" + sec + " failed: 404",
		"SLACK_WEBHOOK_URL=https://hooks.slack.com/triggers/T0ABC1234/1234567890/" + sec,
	} {
		out, st := Text(in)
		if strings.Contains(out, sec) || st.Total == 0 {
			t.Errorf("Text(%q) = %q", in, out)
		}
	}
}

func TestAWSShapeAfterKeyWord(t *testing.T) {
	// A 40-character record key is evidence, not an AWS secret.
	k := "Ab12Cd34Ef56Gh78Ij90Kl12Mn34Op56Qr78St90"
	for _, in := range []string{"record key " + k + " not found", "message_id=" + k} {
		if out, _ := Text(in); out != in {
			t.Errorf("Text(%q) = %q", in, out)
		}
	}
	if out, _ := Text("creds AKIAZ7QW3RTY5UIOPLKJ " + k); strings.Contains(out, k) {
		t.Errorf("bare AWS secret kept: %q", out)
	}
}

func TestPEMLinear(t *testing.T) {
	// The round-1 regexp was quadratic on repeated BEGIN lines: 8 KB took
	// 85 ms and 64 KB took 7 s.
	for _, unit := range []string{
		"-----BEGIN PRIVATE KEY-----\n",
		"-----BEGIN PRIVATE KEY-----",
		"-----BEGIN RSA PRIVATE KEY----- abc ",
		"-----BEGIN PRIVATE KEY-----\n-----END CERTIFICATE-----\n",
	} {
		s := strings.Repeat(unit, MaxScan/len(unit))
		start := time.Now()
		out, _ := Text(s)
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Errorf("%q x %d: %v", unit, MaxScan/len(unit), d)
		}
		if strings.Contains(out, "BEGIN") {
			t.Errorf("unmasked BEGIN left for unit %q", unit)
		}
	}
	// A key, then evidence: the evidence stays.
	key := "-----BEGIN EC PRIVATE KEY-----\n" + strings.Repeat("QUJDRA==", 8) + "\n-----END EC PRIVATE KEY-----"
	out, _ := Text(key + "\nslot debezium_orders active=false")
	if out != "[redacted private key 128]\nslot debezium_orders active=false" {
		t.Errorf("got %q", out)
	}
	// Truncated key followed by prose.
	out, _ = Text("-----BEGIN PRIVATE KEY-----\n" + strings.Repeat("QUJD", 8) + "\nslot debezium_orders")
	if out != "[redacted private key 64]\nslot debezium_orders" {
		t.Errorf("got %q", out)
	}
}

func TestScanCap(t *testing.T) {
	long := strings.Repeat("slot debezium_orders lag 0\n", (MaxScan/27)+100) + "password=" + pw
	out, st := Text(long)
	if strings.Contains(out, pw) || st.ByKind[KindOverflow] != 1 {
		t.Fatalf("tail not masked: ...%s (%+v)", out[len(out)-80:], st)
	}
	if !strings.HasPrefix(out, "slot debezium_orders lag 0\n") || !strings.HasSuffix(out, overflowSuffix) {
		t.Fatalf("bad shape: ...%s", out[len(out)-80:])
	}
	if again, _ := Text(out); again != out {
		t.Fatal("cap output not idempotent")
	}
}

func BenchmarkTextRepeatedBegin(b *testing.B) {
	for _, n := range []int{8 << 10, 64 << 10} {
		s := strings.Repeat("-----BEGIN PRIVATE KEY-----\n", n/28)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			for b.Loop() {
				Text(s)
			}
		})
	}
}
