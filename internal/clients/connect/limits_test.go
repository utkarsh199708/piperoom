package connect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// hugeStatus is a status body whose second task's trace pushes it past 4 MiB.
func hugeStatus() string {
	return `{"name":"big","connector":{"state":"RUNNING","worker_id":"w1:8083"},"tasks":[` +
		`{"id":0,"state":"FAILED","worker_id":"w1:8083","trace":"org.apache.kafka.connect.errors.ConnectException: small"},` +
		`{"id":1,"state":"FAILED","worker_id":"w2:8083","trace":"org.apache.kafka.connect.errors.ConnectException: huge` +
		strings.Repeat(`\n\tat x.Y.z(Y.java:1)`, 300_000) + `"},` +
		`{"id":2,"state":"RUNNING","worker_id":"w3:8083"}],"type":"source"}`
}

func TestStatusOverCapReturnsPartial(t *testing.T) {
	body := hugeStatus()
	if len(body) <= maxResponseBytes {
		t.Fatalf("fixture is only %d bytes", len(body))
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, body)
	})
	st, err := c.Status(context.Background(), "big")
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("err = %v, want ErrResponseTooLarge", err)
	}
	if strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("error should say why, got %v", err)
	}
	if st.Name != "big" || st.State != "RUNNING" || st.WorkerID != "w1:8083" {
		t.Fatalf("partial connector status lost: %+v", st)
	}
	if len(st.Tasks) != 2 {
		t.Fatalf("tasks = %d, want the 2 read before the cut", len(st.Tasks))
	}
	if st.Tasks[0].Trace != "org.apache.kafka.connect.errors.ConnectException: small" {
		t.Fatalf("task 0 trace = %q", st.Tasks[0].Trace)
	}
	if st.Tasks[1].ID != 1 || st.Tasks[1].State != "FAILED" || !strings.HasSuffix(st.Tasks[1].Trace, TruncatedTrace) ||
		!strings.HasPrefix(st.Tasks[1].Trace, "org.apache.kafka.connect.errors.ConnectException: huge"+TruncatedTrace) {
		t.Fatalf("cut task = %+v", st.Tasks[1])
	}
}

func TestStatusUnderCapIsComplete(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"name":"n","connector":{"state":"RUNNING","worker_id":"w"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"w","extra":{"a":[1,2]}}],"type":"sink","other":1}`)
	})
	st, err := c.Status(context.Background(), "n")
	if err != nil {
		t.Fatal(err)
	}
	if st.Type != "sink" || len(st.Tasks) != 1 || st.Tasks[0].State != "RUNNING" {
		t.Fatalf("status = %+v", st)
	}
}

func TestStatusMalformed(t *testing.T) {
	for _, body := range []string{`[]`, `{"name":1}`, `{"tasks":{}}`, `{"name":"n"`} {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
		if _, err := c.Status(context.Background(), "n"); err == nil || errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("body %q: err = %v, want a decode error", body, err)
		}
	}
}

func TestListOverCapSaysTooLarge(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `["`+strings.Repeat("x", 5<<20)+`"]`)
	})
	_, err := c.ListConnectors(context.Background())
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("err = %v, want ErrResponseTooLarge", err)
	}
}

func TestBodyExactlyAtCapIsRead(t *testing.T) {
	body := `["` + strings.Repeat("x", maxResponseBytes-4) + `"]`
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
	names, err := c.ListConnectors(context.Background())
	if err != nil || len(names) != 1 {
		t.Fatalf("names %d, err %v", len(names), err)
	}
}

func TestErrorBodyIsShortSnippet(t *testing.T) {
	html := "<html><body>" + strings.Repeat("<p>proxy error</p>", 400) + "</body></html>"
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, html)
	})
	_, err := c.ListConnectors(context.Background())
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("err = %v", err)
	}
	if n := len(err.Error()); n > maxErrorSnippet+100 {
		t.Fatalf("error is %d bytes: %s", n, err)
	}
	if !strings.HasSuffix(err.Error(), "…") {
		t.Fatalf("cut snippet not marked: %s", err)
	}
}
