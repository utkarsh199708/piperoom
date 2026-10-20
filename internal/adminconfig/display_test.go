package adminconfig

import (
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/owners"
	"piperoom/internal/wiring"
)

// The key table covers exactly owners.ConnectionKeys, each process-wide
// variable is the key in upper case (as in internal/config/config.go), and
// every kind has its required key.
func TestKeysMatchOwners(t *testing.T) {
	var got []string
	for k := range keys {
		got = append(got, k)
	}
	sort.Strings(got)
	if want := owners.ConnectionKeys(); !slices.Equal(got, want) {
		t.Fatalf("keys = %v\nwant owners.ConnectionKeys() = %v", got, want)
	}
	for k, info := range keys {
		if info.env != strings.ToUpper(k) {
			t.Errorf("%s: env %s, want %s", k, info.env, strings.ToUpper(k))
		}
	}
	for _, kind := range connprobe.Kinds {
		rk, ok := requiredKey[kind]
		if !ok || keys[rk].kind != kind {
			t.Errorf("kind %s: required key %q is not one of its keys", kind, rk)
		}
	}
}

// Each key fills the same config.Config field here as in wiring (connFields):
// wiring.PipelineConfig resolves a one-key pipeline and only that field holds
// the value.
func TestKeysFillWiringFields(t *testing.T) {
	literal := map[string]string{
		"warehouse_kind":          "snowflake",
		"kafka_security_protocol": "SASL_SSL",
		"kafka_sasl_mechanism":    "PLAIN",
		"kafka_tls_server_name":   "broker.example.com",
	}
	for _, k := range owners.ConnectionKeys() {
		env := "ADMINCONFIG_TEST_" + strings.ToUpper(k)
		v := "value-of-" + k
		if lv, ok := literal[k]; ok {
			v = lv
		}
		t.Setenv(env, v)
		c, err := wiring.PipelineConfig(owners.Pipeline{ID: "p", Connections: owners.Connections{k: {Env: env}}})
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		if got := *keys[k].field(&c); got != v {
			t.Errorf("%s: our field holds %q, want %q (wiring fills another field)", k, got, v)
		}
		*keys[k].field(&c) = ""
		if !reflect.DeepEqual(c, config.Config{}) {
			t.Errorf("%s: wiring filled a field other than ours: %+v", k, c)
		}
	}
}

func TestDisplayURL(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"http://localhost:8083", "http://localhost:8083", true},
		{"https://user:pw@connect.example.com:8443/base?token=t#frag", "https://connect.example.com:8443/base", true},
		{"https://user@sr.example.com", "https://sr.example.com", true},
		{"http://u:p%40ss@[::1]:8081/x", "http://[::1]:8081/x", true},
		{"http://schema_registry:8081", "http://schema_registry:8081", true},
		{"  http://10.0.0.5:8083/  ", "http://10.0.0.5:8083/", true},
		{"localhost:8083", "", false},          // opaque: no host
		{"not a url", "", false},               // no scheme or host
		{"http://u:1234/x@host:80", "", false}, // '@' left in the path
		{"http://u:12?x@host", "", false},      // '@' left in the query
		{"http://host:80/a@b", "", false},      // '@' in the path
		{"http://ho st:80", "", false},         // bad host
		{"http://host:port", "", false},        // bad port
		{"postgres://u:pw@db:5432/x", "postgres://db:5432/x", true},
		{"", "", false},
	} {
		got, ok := displayURL(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("displayURL(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestDisplayBrokers(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"localhost:29092", "localhost:29092", true},
		{"b-1.prod:9096, b-2.prod:9096", "b-1.prod:9096,b-2.prod:9096", true},
		{"user:pw@b-1:9092,user:pw@b-2:9092", "b-1:9092,b-2:9092", true},
		{"SASL_SSL://b-1:9092", "b-1:9092", true},
		{"[::1]:9092", "[::1]:9092", true},
		{"user:12,34@host:9092", "", false},  // a comma in the password
		{"b-1:9092,u:p@b-2:9092", "", false}, // user info on only some entries
		{"b-1", "", false},                   // no port
		{"b-1:9092/x", "", false},
		{"S3CRET", "", false},
		{"", "", false},
		{" , ", "", false},
	} {
		got, ok := displayBrokers(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("displayBrokers(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestDisplayValue(t *testing.T) {
	for _, c := range []struct {
		key, in, want string
		secret        bool
	}{
		{"kafka_security_protocol", "SASL_SSL", "SASL_SSL", false},
		{"warehouse_kind", "postgres", "postgres", false},
		{"kafka_tls_server_name", "broker.example.com", "broker.example.com", false},
		{"kafka_tls_server_name", "postgres://u:pw@db/x", "", true}, // not a plain token
		{"kafka_tls_ca_file", "/etc/ssl/ca.pem", "/etc/ssl/ca.pem", false},
		{"connect_tls_key_file", `C:\certs\key.pem`, `C:\certs\key.pem`, false},
		{"connect_url", "http://u:pw@c:8083", "http://c:8083", false},
		{"connect_url", "garbage", "", true},
		{"kafka_brokers", "u:pw@b:9092", "b:9092", false},
		{"source_db_url", "postgres://u:pw@db/x", "", true}, // a credential is never displayed
		{"kafka_sasl_password", "pw", "", true},
	} {
		got, secret := displayValue(c.key, c.in)
		if got != c.want || secret != c.secret {
			t.Errorf("displayValue(%s, %q) = %q, %v; want %q, %v", c.key, c.in, got, secret, c.want, c.secret)
		}
	}
}

func TestIsSecretKey(t *testing.T) {
	want := map[string]bool{
		"source_db_url": true, "warehouse_db_url": true,
		"kafka_sasl_username": true, "kafka_sasl_password": true,
		"connect_username": true, "connect_password": true,
		"schema_registry_username": true, "schema_registry_password": true,
	}
	for _, k := range owners.ConnectionKeys() {
		if got := isSecretKey(k); got != want[k] {
			t.Errorf("isSecretKey(%s) = %v, want %v", k, got, want[k])
		}
	}
}

func TestRefFrom(t *testing.T) {
	for _, c := range []struct {
		r    owners.ConnRef
		want string
	}{
		{owners.ConnRef{Env: "ORDERS_DB_URL"}, "env ORDERS_DB_URL"},
		{owners.ConnRef{File: "/run/secrets/dsn"}, "file /run/secrets/dsn"},
		{owners.ConnRef{Value: "SASL_SSL"}, "value"},
	} {
		if got := refFrom(c.r); got != c.want {
			t.Errorf("refFrom(%+v) = %q, want %q", c.r, got, c.want)
		}
	}
}
