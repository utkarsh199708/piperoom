package schemareg

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func readFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	return string(b)
}

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return New(srv.URL, srv.Client())
}

func TestSubjects(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "subjects.json")))
	})
	got, err := c.Subjects(context.Background())
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	if len(got) != 2 || got[0] != "pg.public.orders-key" {
		t.Fatalf("got %v", got)
	}
}

func TestVersions(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "versions.json")))
	})
	got, err := c.Versions(context.Background(), "pg.public.orders-value")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestSchemaVersionAndLatest(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/1") {
			w.Write([]byte(readFile(t, "schema_v1.json")))
			return
		}
		w.Write([]byte(readFile(t, "schema_latest.json")))
	})
	v1, err := c.Schema(context.Background(), "pg.public.orders-value", 1)
	if err != nil {
		t.Fatalf("Schema v1: %v", err)
	}
	if v1.Version != 1 || v1.ID != 2 || v1.Subject != "pg.public.orders-value" || !strings.HasPrefix(v1.Schema, `{"type":"record"`) {
		t.Fatalf("v1 %+v", v1)
	}
	latest, err := c.Schema(context.Background(), "pg.public.orders-value", -1)
	if err != nil {
		t.Fatalf("Schema latest: %v", err)
	}
	if latest.Version != 2 || latest.ID != 3 {
		t.Fatalf("latest %+v", latest)
	}
}

func TestSchemaByID(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/3" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(readFile(t, "schema_latest.json")))
	})
	s, err := c.SchemaByID(context.Background(), 3)
	if err != nil {
		t.Fatalf("SchemaByID: %v", err)
	}
	if !strings.HasPrefix(s, `{"type":"record"`) {
		t.Fatalf("got %q", s)
	}
}

func TestCompatibilitySubjectLevel(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "config_subject.json")))
	})
	level, subjectLevel, err := c.Compatibility(context.Background(), "pg.public.orders-value")
	if err != nil {
		t.Fatalf("Compatibility: %v", err)
	}
	if level != "BACKWARD" || !subjectLevel {
		t.Fatalf("got level=%q subjectLevel=%v", level, subjectLevel)
	}
}

func TestCompatibilityFallsBackToGlobal(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/config" {
			w.Write([]byte(readFile(t, "config_global.json")))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(readFile(t, "error_40408.json")))
	})
	level, subjectLevel, err := c.Compatibility(context.Background(), "no-subject")
	if err != nil {
		t.Fatalf("Compatibility: %v", err)
	}
	if level != "BACKWARD" || subjectLevel {
		t.Fatalf("got level=%q subjectLevel=%v", level, subjectLevel)
	}
}

func TestSetCompatibility(t *testing.T) {
	var method, path, body string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"compatibilityLevel":"FULL"}`))
	})
	if err := c.SetCompatibility(context.Background(), "pg.public.orders-value", "FULL"); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}
	if method != http.MethodPut || path != "/config/pg.public.orders-value" {
		t.Fatalf("method=%s path=%s", method, path)
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(body), &m); err != nil || m["compatibility"] != "FULL" {
		t.Fatalf("body %q", body)
	}
}

func TestRegister(t *testing.T) {
	var body string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.Write([]byte(`{"id":99}`))
	})
	id, err := c.Register(context.Background(), "pg.public.orders-value", `{"type":"record","name":"X","fields":[]}`)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if id != 99 {
		t.Fatalf("id %d", id)
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(body), &m); err != nil || !strings.HasPrefix(m["schema"], `{"type":"record"`) {
		t.Fatalf("body %q", body)
	}
}

func TestNotFoundWrapsErrNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":40401,"message":"Subject 'nope' not found"}`))
	})
	_, err := c.Schema(context.Background(), "nope", 1)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	var serr *Error
	if !errors.As(err, &serr) || serr.Code != 40401 {
		t.Fatalf("want typed Error 40401, got %v", err)
	}
}

func TestIncompatibilityTypedError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(readFile(t, "error_409.json")))
	})
	_, err := c.Register(context.Background(), "pg.public.orders-value", "{}")
	var serr *Error
	if !errors.As(err, &serr) || serr.Code != 409 {
		t.Fatalf("want typed Error 409, got %v", err)
	}
}

func TestNotFoundNonJSONBody(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("plain text 404"))
	})
	_, err := c.Schema(context.Background(), "nope", 1)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestInternalErrorHTML(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("<html>boom</html>"))
	})
	_, err := c.Subjects(context.Background())
	if err == nil {
		t.Fatal("want error for 500")
	}
	var serr *Error
	if errors.As(err, &serr) {
		t.Fatalf("HTML 500 must not parse as typed Error: %v", err)
	}
}

func TestTargetDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	c := New(srv.URL, srv.Client())
	srv.Close() // connection refused from here on
	if _, err := c.Subjects(context.Background()); err == nil {
		t.Fatal("want error for closed server")
	}
}

func TestOversizedBody(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.Copy(w, strings.NewReader(strings.Repeat("x", 5<<20))) // 5 MB > 4 MB cap
	})
	_, err := c.Subjects(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("want decode error for oversized body, got %v", err)
	}
}

func recordCtx() (context.Context, *tools.Recorder) {
	rec := tools.NewRecorder()
	return tools.WithRecorder(context.Background(), rec), rec
}

func TestRegistrySchemaRecordsPath(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", mediaType)
		w.Write([]byte(`{"subject":"pg.public.orders-value","version":3,"id":7,"schema":"{}"}`))
	})
	ctx, rec := recordCtx()
	if _, err := c.Schema(ctx, "pg.public.orders-value", -1); err != nil {
		t.Fatalf("Latest: %v", err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("got %d calls, want 1: %+v", len(calls), calls)
	}
	if calls[0].Kind != tools.CallRegistry || calls[0].Text != "GET /subjects/pg.public.orders-value/versions/latest" || calls[0].Target != c.baseURL {
		t.Fatalf("call = %+v", calls[0])
	}
}

func TestRegistryErrorStillRecords(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "<html>boom</html>", http.StatusInternalServerError)
	})
	ctx, rec := recordCtx()
	if _, err := c.Subjects(ctx); err == nil {
		t.Fatal("want error")
	}
	if calls := rec.Calls(); len(calls) != 1 || calls[0].Text != "GET /subjects" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestRegisterNeverRecordsSchema(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"id":1}`)) })
	ctx, rec := recordCtx()
	if _, err := c.Register(ctx, "s-value", `{"type":"string","doc":"SECRET-SCHEMA"}`); err != nil {
		t.Fatalf("Register: %v", err)
	}
	calls := rec.Calls()
	if len(calls) != 1 || calls[0].Text != "POST /subjects/s-value/versions" {
		t.Fatalf("calls = %+v", calls)
	}
	if b, _ := json.Marshal(calls); strings.Contains(string(b), "SECRET-SCHEMA") {
		t.Fatalf("schema leaked: %s", b)
	}
}
