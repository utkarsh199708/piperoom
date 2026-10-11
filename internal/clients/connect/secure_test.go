package connect

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

	"piperoom/internal/clients/kafka/netsec"
)

const secureTestSecret = "connect-secret"

func TestNewSecure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "piperoom" || p != secureTestSecret {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error_code":401,"message":"Unauthorized"}`))
			return
		}
		w.Write([]byte(`["pg-orders-connector"]`))
	}))
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := NewSecure(srv.URL, netsec.HTTP{Username: "piperoom", Password: secureTestSecret, TLS: netsec.TLS{CAFile: ca}})
	if err != nil {
		t.Fatal(err)
	}
	names, err := c.ListConnectors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "pg-orders-connector" {
		t.Errorf("connectors = %v", names)
	}

	c, err = NewSecure(srv.URL, netsec.HTTP{Username: "piperoom", Password: "wrong-" + secureTestSecret, TLS: netsec.TLS{CAFile: ca}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListConnectors(context.Background())
	if err == nil || !strings.Contains(err.Error(), "status 401") {
		t.Fatalf("err = %v, want status 401", err)
	}
	if strings.Contains(err.Error(), secureTestSecret) {
		t.Errorf("error leaks the secret: %v", err)
	}
}

func TestNewSecureRefusesClearTextPassword(t *testing.T) {
	_, err := NewSecure("http://localhost:8083", netsec.HTTP{Username: "piperoom", Password: secureTestSecret})
	if !errors.Is(err, netsec.ErrInvalid) {
		t.Fatalf("err = %v, want netsec.ErrInvalid", err)
	}
	if !strings.HasPrefix(err.Error(), "connect: ") {
		t.Errorf("error not wrapped with package prefix: %v", err)
	}
}
