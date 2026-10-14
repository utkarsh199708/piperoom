package connect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

const validateBody = `{"name":"X","error_count":2,"groups":[],"configs":[
 {"definition":{"name":"a"},"value":{"name":"a","value":"s3cret-A","recommended_values":[],"errors":[],"visible":true}},
 {"definition":{"name":"decimal.handling.mode"},"value":{"name":"decimal.handling.mode","value":"s3cret-B","errors":["Invalid value"],"visible":true}},
 {"definition":{"name":"topic.prefix"},"value":{"name":"topic.prefix","value":null,"errors":["Missing"],"visible":true}}]}`

func validateClient(t *testing.T, status int, body string, got *string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*got = r.Method + " " + r.URL.EscapedPath() + " " + r.Header.Get("Content-Type") + " " + string(b)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, nil)
}

func TestValidateConfigErrors(t *testing.T) {
	var got string
	c := validateClient(t, 200, validateBody, &got)
	res, err := c.ValidateConfig(context.Background(), "io.debezium.Conn", map[string]string{"decimal.handling.mode": "bad"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ErrorCount != 2 || len(res.Errors) != 2 || res.Errors["decimal.handling.mode"][0] != "Invalid value" || res.Errors["topic.prefix"][0] != "Missing" {
		t.Fatalf("result: %+v", res)
	}
	if !strings.HasPrefix(got, "PUT /connector-plugins/io.debezium.Conn/config/validate application/json ") ||
		!strings.Contains(got, `"decimal.handling.mode":"bad"`) {
		t.Fatalf("request: %s", got)
	}
	var none string
	res, err = validateClient(t, 200, `{"error_count":0,"configs":[]}`, &none).ValidateConfig(context.Background(), "c", nil)
	if err != nil || res.ErrorCount != 0 || len(res.Errors) != 0 {
		t.Fatalf("clean: %+v %v", res, err)
	}
}

func TestValidateConfigNeverLogsValues(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	var got string
	res, err := validateClient(t, 200, validateBody, &got).ValidateConfig(context.Background(), "c", map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "s3cret") {
		t.Fatalf("log holds a config value: %s", buf.String())
	}
	for k, msgs := range res.Errors {
		for _, m := range msgs {
			if strings.Contains(m, "s3cret") || strings.Contains(k, "s3cret") {
				t.Fatalf("result holds a config value: %+v", res)
			}
		}
	}
}

func TestValidateConfig405And404(t *testing.T) {
	var got string
	_, err := validateClient(t, 405, "nope", &got).ValidateConfig(context.Background(), "c", nil)
	var se *statusError
	if !errors.As(err, &se) || se.Code != 405 {
		t.Fatalf("want statusError 405, got %v", err)
	}
	_, err = validateClient(t, 404, "", &got).ValidateConfig(context.Background(), "c", nil)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := New("http://x", nil).ValidateConfig(context.Background(), "", nil); err == nil {
		t.Fatal("empty class must fail")
	}
}
