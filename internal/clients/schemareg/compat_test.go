package schemareg

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/clients"
)

func compatServer(t *testing.T, status int, body string, gotPath, gotBody, gotCT *string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*gotPath, *gotBody, *gotCT = r.Method+" "+r.URL.RequestURI(), string(b), r.Header.Get("Content-Type")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, nil)
}

func TestCheckCompatibilityVerbose(t *testing.T) {
	var path, body, ct string
	c := compatServer(t, 200, `{"is_compatible":false,"messages":["amount: type changed"]}`, &path, &body, &ct)
	ok, msgs, err := c.CheckCompatibility(context.Background(), "a/b-value", `{"type":"string"}`)
	if err != nil || ok || len(msgs) != 1 || msgs[0] != "amount: type changed" {
		t.Fatalf("got %v %v %v", ok, msgs, err)
	}
	if path != "POST /compatibility/subjects/a%2Fb-value/versions/latest?verbose=true" || ct != mediaType ||
		body != `{"schema":"{\"type\":\"string\"}"}` {
		t.Fatalf("request: %q %q %q", path, ct, body)
	}
}

func TestCheckCompatibilityNoMessages(t *testing.T) {
	var p, b, ct string
	c := compatServer(t, 200, `{"is_compatible":false}`, &p, &b, &ct)
	ok, msgs, err := c.CheckCompatibility(context.Background(), "s", "{}")
	if err != nil || ok || msgs != nil {
		t.Fatalf("got %v %v %v", ok, msgs, err)
	}
	c = compatServer(t, 200, `{"is_compatible":true}`, &p, &b, &ct)
	if ok, _, err := c.CheckCompatibility(context.Background(), "s", "{}"); err != nil || !ok {
		t.Fatalf("compatible: %v %v", ok, err)
	}
}

func TestCheckCompatibilityErrors(t *testing.T) {
	var p, b, ct string
	c := compatServer(t, 404, `{"error_code":40401,"message":"Subject not found"}`, &p, &b, &ct)
	if _, _, err := c.CheckCompatibility(context.Background(), "s", "{}"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	c = compatServer(t, 422, `{"error_code":42201,"message":"Invalid schema"}`, &p, &b, &ct)
	_, _, err := c.CheckCompatibility(context.Background(), "s", "{")
	var se *Error
	if !errors.As(err, &se) || se.Code != 42201 {
		t.Fatalf("want *Error 42201, got %v", err)
	}
}
