package schemareg

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
)

const secureTestSecret = "cc-api-secret"

// newAuthTLSServer is an https Schema Registry that wants basic auth
// api-key/secureTestSecret; it returns the server and a PEM file of its CA.
func newAuthTLSServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "api-key" || p != secureTestSecret {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error_code":401,"message":"Unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", mediaType)
		w.Write([]byte(`["orders-value"]`))
	}))
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return srv, ca
}

func TestNewSecure(t *testing.T) {
	srv, ca := newAuthTLSServer(t)

	c, err := NewSecure(srv.URL, netsec.HTTP{Username: "api-key", Password: secureTestSecret, TLS: netsec.TLS{CAFile: ca}})
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := c.Subjects(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 1 || subjects[0] != "orders-value" {
		t.Errorf("subjects = %v", subjects)
	}

	// Wrong secret: a typed 401, and the secret is not in the error.
	c, err = NewSecure(srv.URL, netsec.HTTP{Username: "api-key", Password: "wrong-" + secureTestSecret, TLS: netsec.TLS{CAFile: ca}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Subjects(context.Background())
	var serr *Error
	if !errors.As(err, &serr) || serr.Code != 401 {
		t.Fatalf("err = %v, want schema registry 401", err)
	}
	if strings.Contains(err.Error(), secureTestSecret) {
		t.Errorf("error leaks the secret: %v", err)
	}

	// No private CA: the server certificate is not trusted.
	c, err = NewSecure(srv.URL, netsec.HTTP{Username: "api-key", Password: secureTestSecret})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subjects(context.Background()); err == nil {
		t.Fatal("untrusted server accepted")
	}
}

func TestNewSecureRefusesClearTextPassword(t *testing.T) {
	_, err := NewSecure("http://localhost:8081", netsec.HTTP{Username: "api-key", Password: secureTestSecret})
	if !errors.Is(err, netsec.ErrInvalid) {
		t.Fatalf("err = %v, want netsec.ErrInvalid", err)
	}
	if !strings.HasPrefix(err.Error(), "schemareg: ") {
		t.Errorf("error not wrapped with package prefix: %v", err)
	}
}

func TestNewSecureZeroIsDevDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected Authorization header")
		}
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, err := NewSecure(srv.URL, netsec.HTTP{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subjects(context.Background()); err != nil {
		t.Fatal(err)
	}
}
