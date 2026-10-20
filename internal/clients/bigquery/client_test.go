package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/option"

	"piperoom/internal/clients"
)

// fakeBQ is a minimal BigQuery REST endpoint for the jobs.query path, so the
// real client library (clientRunner) is exercised without a network. Each
// request body is kept for assertions; answer picks the response.
type fakeBQ struct {
	mu     sync.Mutex
	bodies []map[string]any
	answer func(sql string) (int, string)
}

func (f *fakeBQ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/projects/acme-data/queries") {
		http.Error(w, `{"error":{"code":404,"message":"unexpected `+r.URL.Path+`"}}`, http.StatusNotFound)
		return
	}
	sql, _ := body["query"].(string)
	code, resp := f.answer(sql)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, resp)
}

func queryResponse(fields, rows string) string {
	return `{"kind":"bigquery#queryResponse","jobComplete":true,` +
		`"jobReference":{"projectId":"acme-data","jobId":"j1","location":"US"},` +
		`"schema":{"fields":[` + fields + `]},"rows":[` + rows + `],"totalRows":"1"}`
}

func newTestClient(t *testing.T, f *fakeBQ) *DB {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	db, err := New(context.Background(), "bigquery://acme-data/us?max_bytes_billed=1000",
		option.WithEndpoint(srv.URL), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestClientRunnerAgainstFakeEndpoint(t *testing.T) {
	f := &fakeBQ{answer: func(sql string) (int, string) {
		switch {
		case sql == "SELECT 1":
			return 200, queryResponse(`{"name":"f0_","type":"INTEGER"}`, `{"f":[{"v":"1"}]}`)
		case strings.Contains(sql, "`missing`"):
			return 404, `{"error":{"code":404,"message":"Not found: Table acme-data:analytics.missing was not found in location US","errors":[{"reason":"notFound","message":"Not found: Table acme-data:analytics.missing"}]}}`
		case strings.Contains(sql, "`secret`"):
			return 403, `{"error":{"code":403,"message":"Access Denied: Table acme-data:analytics.secret","errors":[{"reason":"accessDenied","message":"Access Denied"}]}}`
		case strings.HasPrefix(sql, "SELECT MAX(`loaded_at`)"):
			return 200, queryResponse(`{"name":"newest","type":"TIMESTAMP"}`, `{"f":[{"v":"1760774400000000"}]}`)
		}
		return 400, `{"error":{"code":400,"message":"unexpected query"}}`
	}}
	db := newTestClient(t, f)
	ctx := context.Background()

	got, err := db.Freshness(ctx, "analytics.orders", "loaded_at")
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	if want := time.Unix(1760774400, 0).UTC(); !got.Equal(want) {
		t.Fatalf("Freshness = %v, want %v", got, want)
	}

	if _, err := db.Freshness(ctx, "analytics.missing", "loaded_at"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v", err)
	}
	if _, err := db.Freshness(ctx, "analytics.secret", "loaded_at"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("denied: %v", err)
	}

	// The request carries the location, the bytes cap and the label.
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) < 2 {
		t.Fatalf("bodies = %v", f.bodies)
	}
	b := f.bodies[1]
	if b["location"] != "us" || b["maximumBytesBilled"] != "1000" {
		t.Fatalf("request body = %v", b)
	}
	if labels, _ := b["labels"].(map[string]any); labels["app"] != "piperoom" {
		t.Fatalf("labels = %v", b["labels"])
	}
}

func TestClientRunnerParametersSent(t *testing.T) {
	f := &fakeBQ{answer: func(sql string) (int, string) {
		if sql == "SELECT 1" {
			return 200, queryResponse(`{"name":"f0_","type":"INTEGER"}`, `{"f":[{"v":"1"}]}`)
		}
		return 200, queryResponse(`{"name":"id","type":"STRING"},{"name":"updated_at","type":"TIMESTAMP"}`,
			`{"f":[{"v":"o-1"},{"v":"1760774400000000"}]}`)
	}}
	db := newTestClient(t, f)
	row, found, err := db.Row(context.Background(), "analytics.orders", "id", "o-1' OR 1=1 --")
	if err != nil || !found {
		t.Fatalf("Row: found=%v err=%v", found, err)
	}
	if row["id"] != "o-1" || row["updated_at"] != "2025-10-18T08:00:00Z" {
		t.Fatalf("row = %v", row)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.bodies[len(f.bodies)-1]
	if q, _ := b["query"].(string); strings.Contains(q, "OR 1=1") {
		t.Fatalf("key interpolated into SQL: %q", q)
	}
	params, _ := b["queryParameters"].([]any)
	if len(params) != 1 {
		t.Fatalf("queryParameters = %v", b["queryParameters"])
	}
	p, _ := params[0].(map[string]any)
	pv, _ := p["parameterValue"].(map[string]any)
	if p["name"] != "key" || pv["value"] != "o-1' OR 1=1 --" {
		t.Fatalf("parameter = %v", p)
	}
}

func TestNewPingFailure(t *testing.T) {
	f := &fakeBQ{answer: func(string) (int, string) {
		return 403, `{"error":{"code":403,"message":"Access Denied: Project acme-data: User does not have bigquery.jobs.create permission","errors":[{"reason":"accessDenied"}]}}`
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	_, err := New(context.Background(), "bigquery://acme-data/us",
		option.WithEndpoint(srv.URL), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("New err = %v", err)
	}
}

func TestNewTargetDown(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens any more
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := New(ctx, "bigquery://acme-data/us",
		option.WithEndpoint(url), option.WithoutAuthentication()); err == nil {
		t.Fatal("New succeeded against a closed endpoint")
	}
}

func TestNewBadDSN(t *testing.T) {
	if _, err := New(context.Background(), "bigquery://user:pw@acme-data/us"); err == nil || strings.Contains(err.Error(), "pw") {
		t.Fatalf("err = %v", err)
	}
}
