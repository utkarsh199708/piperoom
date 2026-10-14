package integrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

func testKey(b byte) netsec.Secret {
	return netsec.Secret(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)))
}

func TestKeyringRoundTrip(t *testing.T) {
	kr, err := NewKeyring(testKey(1), "")
	if err != nil {
		t.Fatal(err)
	}
	aad := SealAAD("default", "int_1", spec.CategoryConnect)
	in := map[string]netsec.Secret{"password": "canary-pass-123", "ca_cert": "-----BEGIN CERTIFICATE-----"}
	sealed, keyID, err := kr.Seal(aad, in)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("canary-pass-123")) {
		t.Fatal("sealed blob contains the plaintext")
	}
	sum := sha256.Sum256(bytes.Repeat([]byte{1}, 32))
	if want := hex.EncodeToString(sum[:])[:8]; keyID != want || kr.CurrentKeyID() != want {
		t.Fatalf("key id = %q, want %q", keyID, want)
	}
	out, err := kr.Open(aad, sealed, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if out["password"] != "canary-pass-123" || out["ca_cert"] != in["ca_cert"] || len(out) != 2 {
		t.Fatalf("round trip lost values: %v", out)
	}
	// Two seals of the same input differ (random nonce).
	again, _, _ := kr.Seal(aad, in)
	if bytes.Equal(again, sealed) {
		t.Fatal("nonce reused")
	}
}

func TestKeyringOpenFailures(t *testing.T) {
	kr, _ := NewKeyring(testKey(1), "")
	other, _ := NewKeyring(testKey(2), "")
	aad := SealAAD("default", "int_1", spec.CategoryConnect)
	sealed, keyID, err := kr.Seal(aad, map[string]netsec.Secret{"password": "pw-secret"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		kr     *Keyring
		aad    []byte
		sealed []byte
	}{
		{"wrong key", other, aad, sealed},
		{"other row id", kr, SealAAD("default", "int_2", spec.CategoryConnect), sealed},
		{"other workspace", kr, SealAAD("acme", "int_1", spec.CategoryConnect), sealed},
		{"other category", kr, SealAAD("default", "int_1", spec.CategoryKafka), sealed},
		{"truncated", kr, aad, sealed[:5]},
		{"tampered", kr, aad, append(append([]byte{}, sealed[:len(sealed)-1]...), sealed[len(sealed)-1]^1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.kr.Open(tc.aad, tc.sealed, keyID)
			if !errors.Is(err, ErrOpen) {
				t.Fatalf("err = %v, want ErrOpen", err)
			}
			if strings.Contains(fmt.Sprint(err), "pw-secret") {
				t.Fatal("error leaks the secret")
			}
		})
	}
}

func TestKeyringRotation(t *testing.T) {
	old, _ := NewKeyring(testKey(1), "")
	aad := SealAAD("default", "int_1", spec.CategoryKafka)
	sealed, oldID, _ := old.Seal(aad, map[string]netsec.Secret{"password": "pw"})

	rotated, err := NewKeyring(testKey(2), testKey(1))
	if err != nil {
		t.Fatal(err)
	}
	got, err := rotated.Open(aad, sealed, oldID)
	if err != nil || got["password"] != "pw" {
		t.Fatalf("previous key did not open: %v %v", got, err)
	}
	// An unknown key id tries both keys.
	if _, err := rotated.Open(aad, sealed, ""); err != nil {
		t.Fatalf("open with unknown key id: %v", err)
	}
	// New seals use the current key.
	_, newID, _ := rotated.Seal(aad, map[string]netsec.Secret{"password": "pw"})
	if newID == oldID {
		t.Fatal("rotation kept the old key id")
	}
	// Once the previous key is dropped, old blobs stop opening.
	only, _ := NewKeyring(testKey(2), "")
	if _, err := only.Open(aad, sealed, oldID); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want ErrOpen", err)
	}
}

func TestNewKeyringBadKeys(t *testing.T) {
	short := netsec.Secret(base64.StdEncoding.EncodeToString([]byte("short")))
	for name, tc := range map[string][2]netsec.Secret{
		"not base64":           {"%%%not-base64%%%", ""},
		"16 bytes":             {netsec.Secret(base64.StdEncoding.EncodeToString(make([]byte, 16))), ""},
		"short":                {short, ""},
		"bad previous":         {testKey(1), short},
		"previous, no current": {"", testKey(1)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewKeyring(tc[0], tc[1])
			if !errors.Is(err, ErrBadKey) {
				t.Fatalf("err = %v, want ErrBadKey", err)
			}
			if strings.Contains(err.Error(), "%%%not") {
				t.Fatal("error quotes the key")
			}
		})
	}
	kr, err := NewKeyring("", "")
	if kr != nil || err != nil {
		t.Fatalf("no key = %v, %v; want nil, nil", kr, err)
	}
}

func TestSealEmpty(t *testing.T) {
	kr, _ := NewKeyring(testKey(1), "")
	sealed, id, err := kr.Seal([]byte("x"), nil)
	if sealed != nil || id != "" || err != nil {
		t.Fatalf("empty seal = %v %q %v", sealed, id, err)
	}
	m, err := kr.Open([]byte("x"), nil, "")
	if err != nil || len(m) != 0 {
		t.Fatalf("open nil = %v %v", m, err)
	}
}
