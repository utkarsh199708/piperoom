package secrets

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
)

func TestExtractKey(t *testing.T) {
	tests := []struct {
		name, data, key, want string
		err                   bool
	}{
		{name: "no key", data: "plain", key: "", want: "plain"},
		{name: "json string", data: `{"username":"u","password":"p w"}`, key: "password", want: "p w"},
		{name: "json number", data: `{"port":5432}`, key: "port", want: "5432"},
		{name: "json bool", data: ` {"tls":true}`, key: "tls", want: "true"},
		{name: "json missing", data: `{"a":"b"}`, key: "c", err: true},
		{name: "kv lines", data: "# c\nexport USER=u\nPASSWORD=\"p=1\"\n\n", key: "PASSWORD", want: "p=1"},
		{name: "kv single quotes", data: "K='v'", key: "K", want: "v"},
		{name: "plain secret", data: "hunter2", key: "k", err: true},
		{name: "bad json", data: `{"a":`, key: "a", err: true},
		{name: "mixed lines", data: "a=b\nnot a pair", key: "a", err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractKey(netsec.Secret(tt.data), tt.key)
			if tt.err {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("err = %v, want ErrKeyNotFound", err)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Fatal("error quotes the data")
				}
				return
			}
			if err != nil || got.Reveal() != tt.want {
				t.Fatalf("got %q, %v; want %q", got.Reveal(), err, tt.want)
			}
		})
	}
}

func TestKeysOf(t *testing.T) {
	if got := KeysOf(`{"b":1,"a":"x"}`); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("json keys = %v", got)
	}
	// A base64 password must not turn into a "key name" in the picker.
	for _, plain := range []string{"dGVzdA==", "a=b", "hunter2", ""} {
		if got := KeysOf(netsec.Secret(plain)); len(got) != 0 || got == nil {
			t.Errorf("KeysOf(%q) = %v, want []", plain, got)
		}
	}
}
