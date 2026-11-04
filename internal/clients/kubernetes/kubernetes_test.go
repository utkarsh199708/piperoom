package kubernetes

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/tools"
)

// newTest starts a server with h and a client that reads the token "tok".
func newTest(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{BaseURL: srv.URL, Token: "tok", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const connectorJSON = `{
 "kind":"KafkaConnector","apiVersion":"kafka.strimzi.io/v1beta2",
 "metadata":{"name":"pg-orders","namespace":"kafka","generation":3,
   "labels":{"strimzi.io/cluster":"my-connect"},
   "annotations":{"strimzi.io/pause-reconciliation":"true",
     "kubectl.kubernetes.io/last-applied-configuration":"{\"spec\":{\"config\":{\"database.password\":\"hunter2\"}}}"}},
 "spec":{"class":"io.debezium.connector.postgresql.PostgresConnector","tasksMax":1,"state":"running",
   "config":{"database.password":"hunter2"}},
 "status":{"observedGeneration":2,"conditions":[{"type":"Ready","status":"False","reason":"ConnectRestException","message":"boom"}]}
}`

func TestCustomResourceFallsBackToV1beta2AndDropsSecrets(t *testing.T) {
	var paths []string
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Method != http.MethodGet {
			t.Errorf("method %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("auth header %q", got)
		}
		if strings.Contains(r.URL.Path, "/v1/") {
			http.Error(w, "404 page not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(connectorJSON))
	})
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	r, err := c.CustomResource(ctx, KindKafkaConnector, "kafka", "pg-orders")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/apis/kafka.strimzi.io/v1/namespaces/kafka/kafkaconnectors/pg-orders",
		"/apis/kafka.strimzi.io/v1beta2/namespaces/kafka/kafkaconnectors/pg-orders",
	}
	if strings.Join(paths, " ") != strings.Join(want, " ") {
		t.Fatalf("paths %v, want %v", paths, want)
	}
	if r.Metadata.Generation != 3 || r.Metadata.Labels["strimzi.io/cluster"] != "my-connect" {
		t.Fatalf("meta %+v", r.Metadata)
	}
	if _, ok := r.Metadata.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Fatal("last-applied annotation kept")
	}
	if r.Metadata.Annotations["strimzi.io/pause-reconciliation"] != "true" {
		t.Fatal("strimzi annotation dropped")
	}
	if _, ok := r.Spec["config"]; ok {
		t.Fatal("spec.config kept")
	}
	if string(r.Spec["state"]) != `"running"` {
		t.Fatalf("spec %v", r.Spec)
	}
	if len(rec.Calls()) != 2 || rec.Calls()[0].Kind != tools.CallHTTP || strings.Contains(rec.Calls()[0].Text, "tok") {
		t.Fatalf("provenance %+v", rec.Calls())
	}
	// The answering version is remembered: one request next time.
	paths = nil
	if _, err := c.CustomResource(ctx, KindKafkaConnector, "kafka", "pg-orders"); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || !strings.Contains(paths[0], "/v1beta2/") {
		t.Fatalf("second read paths %v", paths)
	}
}

func TestObjectNotFoundDoesNotFallBack(t *testing.T) {
	var n atomic.Int32
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"kind":"Status","status":"Failure","reason":"NotFound","details":{"name":"x","kind":"kafkaconnectors"}}`))
	})
	_, err := c.CustomResource(context.Background(), KindKafkaConnector, "kafka", "x")
	if !errors.Is(err, ErrNotFound) || errors.Is(err, errNoVersion) {
		t.Fatalf("err = %v", err)
	}
	if n.Load() != 1 {
		t.Fatalf("%d requests, want 1 (no fallback on an object 404)", n.Load())
	}
}

func TestNoVersionServed(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "404 page not found", http.StatusNotFound)
	})
	_, err := c.CustomResource(context.Background(), KindKafkaConnect, "kafka", "c")
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "not served") {
		t.Fatalf("err = %v", err)
	}
}

func TestStatusCodes(t *testing.T) {
	for _, tt := range []struct {
		code int
		want error
	}{
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusInternalServerError, ErrAPI},
		{http.StatusFound, ErrAPI},
	} {
		c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
			if tt.code == http.StatusFound {
				w.Header().Set("Location", "http://127.0.0.1:1/steal")
			}
			w.WriteHeader(tt.code)
			w.Write([]byte(`{"kind":"Status","reason":"InternalError","message":"secret-ish detail"}`))
		})
		_, err := c.Deployment(context.Background(), "op", "strimzi-cluster-operator")
		if !errors.Is(err, tt.want) {
			t.Errorf("%d: err = %v, want %v", tt.code, err, tt.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret-ish") {
			t.Errorf("%d: error echoes the body: %v", tt.code, err)
		}
	}
}

func TestOversizedBody(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"items":[`))
		w.Write([]byte(strings.Repeat(" ", MaxResponseBytes)))
		w.Write([]byte(`]}`))
	})
	if _, err := c.Pods(context.Background(), "kafka", map[string]string{"strimzi.io/cluster": "c"}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

func TestTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(block); srv.Close() })
	c, err := New(Config{BaseURL: srv.URL, Token: "tok", Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := c.EventsFor(context.Background(), "kafka", "Pod", "p-0"); err == nil {
		t.Fatal("want a timeout error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the client timeout did not apply")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c2, _ := New(Config{BaseURL: srv.URL, Token: "tok", Timeout: time.Minute})
	if _, err := c2.EventsFor(ctx, "kafka", "Pod", "p-0"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx deadline: err = %v", err)
	}
}

func TestBadNamesSendNothing(t *testing.T) {
	var n atomic.Int32
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) { n.Add(1) })
	ctx := context.Background()
	checks := []error{
		func() error { _, err := c.CustomResource(ctx, KindKafkaConnector, "kafka", "../secrets/x"); return err }(),
		func() error { _, err := c.CustomResource(ctx, KindKafkaConnector, "Kafka", "x"); return err }(),
		func() error { _, err := c.CustomResource(ctx, "Secret", "kafka", "x"); return err }(),
		func() error { _, err := c.EventsFor(ctx, "kafka", "Pod", "a,involvedObject.kind=Secret"); return err }(),
		func() error { _, err := c.EventsFor(ctx, "kafka", "Pod,x", "a"); return err }(),
		func() error { _, err := c.Pods(ctx, "kafka", map[string]string{"app": "a,b=c"}); return err }(),
		func() error { _, err := c.Pods(ctx, "kafka", nil); return err }(),
		func() error { _, err := c.Deployment(ctx, "kube system", "x"); return err }(),
		func() error { _, err := c.Deployment(ctx, "op", strings.Repeat("a", 254)); return err }(),
	}
	for i, err := range checks {
		if !errors.Is(err, ErrBadName) {
			t.Errorf("check %d: err = %v, want ErrBadName", i, err)
		}
	}
	if n.Load() != 0 {
		t.Fatalf("%d requests sent for bad names", n.Load())
	}
}

func TestEventsQueryAndOrder(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/kafka/events" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("fieldSelector"); got != "involvedObject.name=my-connect,involvedObject.kind=KafkaConnect" {
			t.Errorf("fieldSelector %q", got)
		}
		if r.URL.Query().Get("limit") != "50" {
			t.Errorf("limit %q", r.URL.Query().Get("limit"))
		}
		w.Write([]byte(`{"items":[
		 {"type":"Normal","reason":"Old","message":"a","count":1,"lastTimestamp":"2026-11-01T00:00:00Z","source":{"component":"strimzi"}},
		 {"type":"Warning","reason":"New","message":"b","count":4,"lastTimestamp":"2026-11-02T00:00:00Z"}]}`))
	})
	ev, err := c.EventsFor(context.Background(), "kafka", "KafkaConnect", "my-connect")
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 2 || ev[0].Reason != "New" || ev[1].Source != "strimzi" {
		t.Fatalf("events %+v", ev)
	}
}

func TestPodsDecodeStatusOnly(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("labelSelector"); got != "strimzi.io/cluster=my-connect,strimzi.io/kind=KafkaConnect" {
			t.Errorf("labelSelector %q", got)
		}
		w.Write([]byte(`{"items":[{"metadata":{"name":"my-connect-connect-0"},
		 "spec":{"containers":[{"env":[{"name":"PASSWORD","value":"hunter2"}]}]},
		 "status":{"phase":"Running","containerStatuses":[{"name":"connect","ready":false,"restartCount":7,
		   "state":{"waiting":{"reason":"CrashLoopBackOff"}},
		   "lastState":{"terminated":{"reason":"OOMKilled","exitCode":137,"finishedAt":"2026-11-02T00:00:00Z"}}}]}}]}`))
	})
	pods, err := c.Pods(context.Background(), "kafka", map[string]string{"strimzi.io/kind": "KafkaConnect", "strimzi.io/cluster": "my-connect"})
	if err != nil {
		t.Fatal(err)
	}
	cs := pods[0].Containers[0]
	if cs.State != "waiting" || cs.Reason != "CrashLoopBackOff" || cs.LastTerminationReason != "OOMKilled" || *cs.LastTerminationCode != 137 || cs.RestartCount != 7 {
		t.Fatalf("container %+v", cs)
	}
}

func TestDeployment(t *testing.T) {
	c := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/apps/v1/namespaces/op/deployments/strimzi-cluster-operator" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"metadata":{"name":"strimzi-cluster-operator","generation":2},
		 "spec":{"replicas":1,"selector":{"matchLabels":{"name":"strimzi-cluster-operator"}},"template":{"spec":{"containers":[{"env":[{"name":"X","value":"y"}]}]}}},
		 "status":{"observedGeneration":2,"replicas":1,"readyReplicas":0,"availableReplicas":0,"unavailableReplicas":1}}`))
	})
	d, err := c.Deployment(context.Background(), "op", "strimzi-cluster-operator")
	if err != nil {
		t.Fatal(err)
	}
	if d.DesiredReplicas != 1 || d.ReadyReplicas != 0 || d.Selector["name"] != "strimzi-cluster-operator" {
		t.Fatalf("deployment %+v", d)
	}
}

func TestTokenFileReadPerRequest(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.Write([]byte(`{"items":[]}`))
	}))
	t.Cleanup(srv.Close)
	c, err := New(Config{BaseURL: srv.URL, TokenFile: file})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.EventsFor(ctx, "kafka", "Pod", "p"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EventsFor(ctx, "kafka", "Pod", "p"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(seen, "|") != "Bearer first|Bearer second" {
		t.Fatalf("auth headers %v", seen)
	}
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EventsFor(ctx, "kafka", "Pod", "p"); !errors.Is(err, ErrConfig) {
		t.Fatalf("empty token file: err = %v", err)
	}
}

func TestNewRefusesBadSettings(t *testing.T) {
	for _, tt := range []Config{
		{BaseURL: "http://kube.example.com", Token: "t"},
		{BaseURL: "https://user:pw@kube.example.com", Token: "t"},
		{BaseURL: "https://kube.example.com/api", Token: "t"},
		{BaseURL: "https://kube.example.com?x=1", Token: "t"},
		{BaseURL: "ftp://kube.example.com", Token: "t"},
		{BaseURL: "https://kube.example.com"},
		{BaseURL: "https://kube.example.com", Token: "t", TokenFile: "/x"},
		{BaseURL: "https://kube.example.com", Token: "t", CAFile: "/does/not/exist.pem"},
	} {
		if _, err := New(tt); err == nil {
			t.Errorf("New(%q) accepted", tt.BaseURL)
		} else if strings.Contains(err.Error(), "pw") {
			t.Errorf("error echoes user info: %v", err)
		}
	}
	if _, err := New(Config{BaseURL: "https://kube.example.com:6443", Token: "t"}); err != nil {
		t.Fatal(err)
	}
}
