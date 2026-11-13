package k8stools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeKube serves objects from maps keyed "kind/ns/name"; err, when set,
// fails every call.
type fakeKube struct {
	resources map[string]kubernetes.Resource
	events    map[string][]kubernetes.Event
	pods      map[string][]kubernetes.Pod // by "ns/selector"
	deps      map[string]kubernetes.Deployment
	err       error
	calls     []string
}

func (f *fakeKube) CustomResource(_ context.Context, kind, ns, name string) (kubernetes.Resource, error) {
	f.calls = append(f.calls, "cr "+kind+"/"+ns+"/"+name)
	if f.err != nil {
		return kubernetes.Resource{}, f.err
	}
	r, ok := f.resources[kind+"/"+ns+"/"+name]
	if !ok {
		return kubernetes.Resource{}, kubernetes.ErrNotFound
	}
	return r, nil
}

func (f *fakeKube) EventsFor(_ context.Context, ns, kind, name string) ([]kubernetes.Event, error) {
	f.calls = append(f.calls, "events "+kind+"/"+ns+"/"+name)
	if f.err != nil {
		return nil, f.err
	}
	return f.events[kind+"/"+ns+"/"+name], nil
}

func (f *fakeKube) Pods(_ context.Context, ns string, sel map[string]string) ([]kubernetes.Pod, error) {
	key := ns + "/" + sel[clusterLabel] + sel["name"]
	f.calls = append(f.calls, "pods "+key)
	if f.err != nil {
		return nil, f.err
	}
	return f.pods[key], nil
}

func (f *fakeKube) Deployment(_ context.Context, ns, name string) (kubernetes.Deployment, error) {
	f.calls = append(f.calls, "deploy "+ns+"/"+name)
	if f.err != nil {
		return kubernetes.Deployment{}, f.err
	}
	d, ok := f.deps[ns+"/"+name]
	if !ok {
		return kubernetes.Deployment{}, kubernetes.ErrNotFound
	}
	return d, nil
}

func testOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders"}},
		{ID: "other", TopicPrefix: "ot", SourceTables: []string{"public.x"}, Connectors: []string{"other-conn"}},
	}}
}

func roomCtx() context.Context { return tools.WithPipelineID(context.Background(), "orders") }

func spec(kv ...string) map[string]json.RawMessage {
	m := map[string]json.RawMessage{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = json.RawMessage(kv[i+1])
	}
	return m
}

// healthy is a ready connector on a ready, managing KafkaConnect.
func healthy() *fakeKube {
	return &fakeKube{
		resources: map[string]kubernetes.Resource{
			"KafkaConnector/kafka/pg-orders": {
				Metadata: kubernetes.ObjectMeta{Name: "pg-orders", Generation: 2, Labels: map[string]string{clusterLabel: "my-connect"}},
				Spec:     spec("state", `"running"`),
				Status: json.RawMessage(`{"observedGeneration":2,"conditions":[{"type":"Ready","status":"True"}],
				  "connectorStatus":{"connector":{"state":"RUNNING","worker_id":"10.0.0.1:8083"},"tasks":[{"id":0,"state":"RUNNING"}]}}`),
			},
			"KafkaConnector/kafka/other-conn": {
				Metadata: kubernetes.ObjectMeta{Name: "other-conn", Labels: map[string]string{clusterLabel: "other-connect"}},
			},
			"KafkaConnect/kafka/my-connect": {
				Metadata: kubernetes.ObjectMeta{Name: "my-connect", Generation: 1, Annotations: map[string]string{connectorResourcesAnnotation: "true"}},
				Spec:     spec("replicas", `2`),
				Status:   json.RawMessage(`{"observedGeneration":1,"url":"http://my-connect-connect-api.kafka.svc:8083","conditions":[{"type":"Ready","status":"True"}]}`),
			},
		},
		pods: map[string][]kubernetes.Pod{
			"kafka/my-connect": {
				{Name: "my-connect-connect-0", Phase: "Running", Containers: []kubernetes.ContainerStatus{{Name: "c", Ready: true, State: "running"}}},
				{Name: "my-connect-connect-1", Phase: "Running", Containers: []kubernetes.ContainerStatus{{Name: "c", Ready: true, State: "running"}}},
			},
			"strimzi/strimzi-cluster-operator": {
				{Name: "strimzi-cluster-operator-abc", Phase: "Running", Containers: []kubernetes.ContainerStatus{{Name: "op", Ready: true, State: "running"}}},
			},
		},
		deps: map[string]kubernetes.Deployment{
			"strimzi/strimzi-cluster-operator": {Name: "strimzi-cluster-operator", DesiredReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1,
				Selector: map[string]string{"name": "strimzi-cluster-operator"}},
		},
		events: map[string][]kubernetes.Event{},
	}
}

func registry(t *testing.T, f *fakeKube) *tools.Registry {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Kube: f, Owners: testOwners(), Settings: Settings{Namespace: "kafka", OperatorNamespace: "strimzi"}}); err != nil {
		t.Fatal(err)
	}
	return reg
}

func run(t *testing.T, reg *tools.Registry, ctx context.Context, name, in string) (tools.Result, error) {
	t.Helper()
	tl, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	if tl.Kind() != tools.KindRead {
		t.Fatalf("%s is not KindRead", name)
	}
	res, err := tl.Run(ctx, json.RawMessage(in))
	if err == nil {
		if len(res.Summary) > tools.MaxSummaryBytes {
			t.Fatalf("%s summary over the cap", name)
		}
		if _, err := json.Marshal(res.Data); err != nil {
			t.Fatalf("%s data does not marshal: %v", name, err)
		}
	}
	return res, err
}

func TestRegisterNames(t *testing.T) {
	reg := registry(t, healthy())
	var got []string
	for _, tl := range reg.All() {
		got = append(got, tl.Name())
		s := tl.InputSchema()
		if s["type"] != "object" || s["additionalProperties"] != false || s["required"] == nil {
			t.Errorf("%s: schema %v", tl.Name(), s)
		}
	}
	if strings.Join(got, ",") != strings.Join(Names, ",") {
		t.Fatalf("registered %v", got)
	}
	if err := Register(tools.NewReadOnlyRegistry(), Deps{Kube: healthy(), Owners: testOwners(), Settings: Settings{Namespace: "Bad NS"}}); err == nil {
		t.Fatal("bad namespace accepted")
	}
	if err := Register(tools.NewReadOnlyRegistry(), Deps{Owners: testOwners(), Settings: Settings{Namespace: "kafka"}}); err == nil {
		t.Fatal("nil Kube accepted")
	}
}

func TestConnectorOutOfScopeSendsNothing(t *testing.T) {
	f := healthy()
	reg := registry(t, f)
	_, err := run(t, reg, roomCtx(), ConnectorStatusName, `{"connector":"other-conn"}`)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err = %v", err)
	}
	if _, err := run(t, reg, context.Background(), ConnectorStatusName, `{"connector":"pg-orders"}`); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: err = %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("calls before the scope check: %v", f.calls)
	}
}

func TestConnectorHealthy(t *testing.T) {
	res, err := run(t, registry(t, healthy()), roomCtx(), ConnectorStatusName, `{"connector":"pg-orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "Ready=True") || !strings.Contains(res.Summary, "No problem") {
		t.Fatalf("summary:\n%s", res.Summary)
	}
}

func TestConnectorNotReadySurfacesReasonAndTrace(t *testing.T) {
	f := healthy()
	r := f.resources["KafkaConnector/kafka/pg-orders"]
	r.Metadata.Generation = 4
	r.Metadata.Annotations = map[string]string{pauseAnnotation: "true"}
	r.Status = json.RawMessage(`{"observedGeneration":3,
	  "conditions":[{"type":"Ready","status":"False","reason":"ConnectRestException","message":"GET /connectors/pg-orders/topics returned 500 password=hunter2"}],
	  "connectorStatus":{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"FAILED",
	    "trace":"org.apache.kafka.connect.errors.ConnectException: An exception occurred\n\tat io.debezium.Foo.bar(Foo.java:1)\nCaused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"debezium\" password=hunter2"}]}}`)
	f.resources["KafkaConnector/kafka/pg-orders"] = r
	res, err := run(t, registry(t, f), roomCtx(), ConnectorStatusName, `{"connector":"pg-orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Ready=False (ConnectRestException)",
		"pause-reconciliation=true",
		"generation 4 not reconciled (observedGeneration 3)",
		"task 0 FAILED",
		"PSQLException",
		"untrusted",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary misses %q:\n%s", want, res.Summary)
		}
	}
	if strings.Contains(res.Summary, "hunter2") {
		t.Fatalf("secret in summary:\n%s", res.Summary)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(string(b), "hunter2") {
		t.Fatalf("secret in data: %s", b)
	}
	st := res.Data.(ConnectorStatus)
	if st.Reconciled || !st.ReconciliationPaused || st.Tasks[0].State != "FAILED" {
		t.Fatalf("data %+v", st)
	}
}

func TestConnectorStateAndOwningConnect(t *testing.T) {
	f := healthy()
	r := f.resources["KafkaConnector/kafka/pg-orders"]
	r.Spec = spec("state", `"stopped"`)
	f.resources["KafkaConnector/kafka/pg-orders"] = r
	c := f.resources["KafkaConnect/kafka/my-connect"]
	c.Metadata.Annotations = nil
	c.Status = json.RawMessage(`{"conditions":[{"type":"Ready","status":"False","reason":"TimeoutException","message":"Exceeded timeout of 300000ms"}]}`)
	f.resources["KafkaConnect/kafka/my-connect"] = c
	res, err := run(t, registry(t, f), roomCtx(), ConnectorStatusName, `{"connector":"pg-orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"spec.state=stopped", "lacks strimzi.io/use-connector-resources", "KafkaConnect my-connect Ready=False (TimeoutException)"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary misses %q:\n%s", want, res.Summary)
		}
	}
}

func TestConnectorNotFoundAndErrors(t *testing.T) {
	f := healthy()
	delete(f.resources, "KafkaConnector/kafka/pg-orders")
	res, err := run(t, registry(t, f), roomCtx(), ConnectorStatusName, `{"connector":"pg-orders"}`)
	if err != nil || !strings.Contains(res.Summary, "not found") {
		t.Fatalf("not found: %v %q", err, res.Summary)
	}
	f.err = kubernetes.ErrForbidden
	if _, err := run(t, registry(t, f), roomCtx(), ConnectorStatusName, `{"connector":"pg-orders"}`); !errors.Is(err, kubernetes.ErrForbidden) || !strings.Contains(err.Error(), "strimzi-target.md") {
		t.Fatalf("forbidden: err = %v", err)
	}
	for _, in := range []string{``, `{}`, `{"connector":"pg-orders","x":1}`, `[1]`} {
		if _, err := run(t, registry(t, healthy()), roomCtx(), ConnectorStatusName, in); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("input %q: err = %v", in, err)
		}
	}
}

func TestConnectorBadNameUnscoped(t *testing.T) {
	f := healthy()
	ctx := tools.WithUnscoped(context.Background())
	if _, err := run(t, registry(t, f), ctx, ConnectorStatusName, `{"connector":"../x"}`); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("calls %v", f.calls)
	}
}

func TestConnectOOMKilledPod(t *testing.T) {
	f := healthy()
	code := int32(137)
	f.pods["kafka/my-connect"][1] = kubernetes.Pod{Name: "my-connect-connect-1", Phase: "Running", Containers: []kubernetes.ContainerStatus{{
		Name: "c", Ready: false, RestartCount: 6, State: "waiting", Reason: "CrashLoopBackOff",
		LastTerminationReason: "OOMKilled", LastTerminationCode: &code, LastTerminationAt: "2026-11-04T08:00:00Z"}}}
	res, err := run(t, registry(t, f), roomCtx(), ConnectStatusName, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"KafkaConnect kafka/my-connect: Ready=True; 1/2 pods ready", "restarts=6", "OOMKilled (exit 137)", "CrashLoopBackOff", "1 of 2 desired Connect pods ready", "memory limit"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary misses %q:\n%s", want, res.Summary)
		}
	}
	st := res.Data.(ConnectStatus)
	if st.ReadyPods != 1 || st.URL == "" {
		t.Fatalf("data %+v", st)
	}
}

func TestConnectScope(t *testing.T) {
	f := healthy()
	reg := registry(t, f)
	if _, err := run(t, reg, roomCtx(), ConnectStatusName, `{"connect":"other-connect"}`); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline's cluster: err = %v", err)
	}
	if _, err := run(t, reg, roomCtx(), ConnectStatusName, `{"connect":"my-connect"}`); err != nil {
		t.Fatalf("own cluster: %v", err)
	}
	if _, err := run(t, reg, roomCtx(), ConnectStatusName, `{"connect":"Bad_Name"}`); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("bad name: err = %v", err)
	}
	delete(f.resources, "KafkaConnector/kafka/pg-orders")
	res, err := run(t, reg, roomCtx(), ConnectStatusName, ``)
	if err != nil || !strings.Contains(res.Summary, "No KafkaConnector") {
		t.Fatalf("no connectors: %v %q", err, res.Summary)
	}
}

func TestOperatorStatus(t *testing.T) {
	f := healthy()
	res, err := run(t, registry(t, f), roomCtx(), OperatorStatusName, `{}`)
	if err != nil || !strings.Contains(res.Summary, "1 desired, 1 ready, 1 available") || !strings.Contains(res.Summary, "is available") {
		t.Fatalf("healthy: %v\n%s", err, res.Summary)
	}
	code := int32(137)
	f.deps["strimzi/strimzi-cluster-operator"] = kubernetes.Deployment{Name: "strimzi-cluster-operator", DesiredReplicas: 1, UnavailableReplicas: 1,
		Selector:   map[string]string{"name": "strimzi-cluster-operator"},
		Conditions: []kubernetes.Condition{{Type: "Available", Status: "False", Reason: "MinimumReplicasUnavailable"}}}
	f.pods["strimzi/strimzi-cluster-operator"][0].Containers[0] = kubernetes.ContainerStatus{Name: "op", RestartCount: 3, State: "waiting", Reason: "CrashLoopBackOff", LastTerminationReason: "OOMKilled", LastTerminationCode: &code}
	res, err = run(t, registry(t, f), roomCtx(), OperatorStatusName, ``)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"0 of 1 replicas available", "Available=False (MinimumReplicasUnavailable)", "restarted 3 times, last: OOMKilled (exit 137)"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary misses %q:\n%s", want, res.Summary)
		}
	}
	delete(f.deps, "strimzi/strimzi-cluster-operator")
	res, err = run(t, registry(t, f), roomCtx(), OperatorStatusName, `{}`)
	if err != nil || !strings.Contains(res.Summary, "not found") {
		t.Fatalf("missing: %v %q", err, res.Summary)
	}
	if _, err := run(t, registry(t, f), context.Background(), OperatorStatusName, `{}`); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: err = %v", err)
	}
}

func TestEventsRedactedAndCapped(t *testing.T) {
	f := healthy()
	var evs []kubernetes.Event
	for i := 0; i < 30; i++ {
		evs = append(evs, kubernetes.Event{Type: "Warning", Reason: "ReconciliationFailed", Count: int32(i + 1),
			LastTimestamp: "2026-11-04T08:00:00Z", Message: "Failed: sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"hunter2\";"})
	}
	f.events["KafkaConnector/kafka/pg-orders"] = evs
	res, err := run(t, registry(t, f), roomCtx(), ObjectEventsName, `{"kind":"KafkaConnector","name":"pg-orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	o := res.Data.(ObjectEvents)
	if len(o.Events) != maxEvents || o.Total != 30 {
		t.Fatalf("events %d of %d", len(o.Events), o.Total)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(res.Summary, "hunter2") || strings.Contains(string(b), "hunter2") {
		t.Fatalf("secret leaked:\n%s", res.Summary)
	}
	if !strings.Contains(res.Summary, "ReconciliationFailed") || !strings.Contains(res.Summary, "untrusted") {
		t.Fatalf("summary:\n%s", res.Summary)
	}
}

func TestEventsScope(t *testing.T) {
	f := healthy()
	f.events["Pod/kafka/my-connect-connect-0"] = []kubernetes.Event{{Type: "Warning", Reason: "BackOff", Message: "Back-off restarting failed container"}}
	f.events["Pod/strimzi/strimzi-cluster-operator-abc"] = []kubernetes.Event{{Type: "Warning", Reason: "OOMKilling", Message: "Memory cgroup out of memory"}}
	reg := registry(t, f)
	ctx := roomCtx()
	for _, tc := range []struct {
		in   string
		want error
		has  string
	}{
		{`{"kind":"KafkaConnector","name":"other-conn"}`, tools.ErrOutOfScope, ""},
		{`{"kind":"KafkaConnect","name":"other-connect"}`, tools.ErrOutOfScope, ""},
		{`{"kind":"Pod","name":"other-connect-connect-0"}`, tools.ErrOutOfScope, ""},
		{`{"kind":"Secret","name":"x"}`, tools.ErrBadInput, ""},
		{`{"kind":"Pod","name":"Bad/Name"}`, tools.ErrBadInput, ""},
		{`{"kind":"Pod","name":"my-connect-connect-0"}`, nil, "BackOff"},
		{`{"kind":"Pod","name":"strimzi-cluster-operator-abc"}`, nil, "strimzi/strimzi-cluster-operator-abc"},
		{`{"kind":"KafkaConnect","name":"my-connect"}`, nil, "No events"},
	} {
		res, err := run(t, reg, ctx, ObjectEventsName, tc.in)
		if tc.want != nil {
			if !errors.Is(err, tc.want) {
				t.Errorf("%s: err = %v, want %v", tc.in, err, tc.want)
			}
			continue
		}
		if err != nil || !strings.Contains(res.Summary, tc.has) {
			t.Errorf("%s: err = %v, summary %q", tc.in, err, res.Summary)
		}
	}
}
