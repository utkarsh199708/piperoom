package nettools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func run(t *testing.T, d Deps, ctx context.Context, input string) (tools.Result, error) {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("net_probe")
	if !ok {
		t.Fatal("net_probe not registered")
	}
	return tool.Run(ctx, json.RawMessage(input))
}

func TestRegisterReadOnly(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("net_probe")
	if !ok {
		t.Fatal("not registered")
	}
	if tool.Kind() != tools.KindRead {
		t.Fatalf("kind = %q", tool.Kind())
	}
	schema := tool.InputSchema()
	if schema["additionalProperties"] != false {
		t.Fatalf("schema = %+v", schema)
	}
	props, _ := schema["properties"].(map[string]any)
	if len(props) != 1 {
		t.Fatalf("properties = %+v", props)
	}
	if _, ok := props["target"]; !ok {
		t.Fatalf("properties = %+v", props)
	}
	if req, _ := schema["required"].([]string); len(req) != 1 || req[0] != "target" {
		t.Fatalf("required = %+v", schema["required"])
	}
	if desc := tool.Description(); !strings.Contains(desc, "never an address") {
		t.Fatalf("description does not state the label rule: %q", desc)
	}
}

func TestToolRequiresRoomScope(t *testing.T) {
	_, err := run(t, Deps{}, context.Background(), `{"target":"connect"}`)
	if !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("err = %v, want ErrNoScope", err)
	}
}

func TestToolRejectsAddressInInput(t *testing.T) {
	ctx := scoped(owners.Pipeline{ID: "orders"})
	for _, input := range []string{
		`{"target":"source","address":"127.0.0.1:1"}`,
		`{"target":""}`,
		`{"target":"source database!"}`,
		`{"target":"source"} {"target":"source"}`,
		`not json`,
	} {
		if _, err := run(t, Deps{Config: config.Config{}}, ctx, input); !errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("input %s: err = %v, want ErrBadInput", input, err)
		}
	}
}

func TestToolUnknownLabel(t *testing.T) {
	cfg := config.Config{ConnectURL: "http://127.0.0.1:8083"}
	ctx := scoped(owners.Pipeline{ID: "orders"})
	_, err := run(t, Deps{Config: cfg}, ctx, `{"target":"source"}`)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err = %v, want ErrOutOfScope", err)
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Fatalf("err %q does not name the labels the room has", err)
	}
	// A pipeline with no configured target at all is out of scope too.
	_, err = run(t, Deps{Config: config.Config{}}, ctx, `{"target":"connect"}`)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err = %v, want ErrOutOfScope", err)
	}
}

func TestToolSourceLoopback(t *testing.T) {
	addr := listenLoopback(t)
	cfg := config.Config{SourceDBURL: "postgres://srcuser:srcpass@" + addr + "/orders?sslmode=disable"}
	res, err := run(t, Deps{Config: cfg}, scoped(owners.Pipeline{ID: "orders"}), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v (summary %s)", err, res.Summary)
	}
	if !strings.Contains(res.Summary, "tcp ok") || !strings.Contains(res.Summary, addr) {
		t.Fatalf("summary = %q", res.Summary)
	}
	if strings.Contains(res.Summary, "srcpass") || strings.Contains(res.Summary, "srcuser") {
		t.Fatalf("summary carries DSN credentials: %q", res.Summary)
	}
	var data probeData
	b, _ := json.Marshal(res.Data)
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatal(err)
	}
	if data.Target != LabelSource || len(data.Endpoints) != 1 || data.Endpoints[0] != addr {
		t.Fatalf("data = %+v", data)
	}
	if strings.Contains(string(b), "srcpass") {
		t.Fatalf("data carries the DSN: %s", b)
	}
}

// The pipeline in the context decides which host is probed: one tool built over
// empty settings serves rooms whose provisional pipelines name their own hosts.
func TestToolPipelineScope(t *testing.T) {
	a := listenLoopback(t)
	b := listenLoopback(t)
	d := Deps{StepTimeout: time.Second}
	pa := owners.Pipeline{ID: "a", Connections: owners.Connections{
		"source_db_url": {Value: "postgres://u:p@" + a + "/db"},
	}}
	pb := owners.Pipeline{ID: "b", Connections: owners.Connections{
		"source_db_url": {Value: "postgres://u:p@" + b + "/db"},
	}}
	resA, err := run(t, d, scoped(pa), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("pipeline a: %v", err)
	}
	resB, err := run(t, d, scoped(pb), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("pipeline b: %v", err)
	}
	if !strings.Contains(resA.Summary, a) || strings.Contains(resA.Summary, b) {
		t.Fatalf("pipeline a summary = %q", resA.Summary)
	}
	if !strings.Contains(resB.Summary, b) || strings.Contains(resB.Summary, a) {
		t.Fatalf("pipeline b summary = %q", resB.Summary)
	}
}

func hostPortOfAddr(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return host, n
}

func TestToolKafkaAdvertisedListeners(t *testing.T) {
	bootstrap := listenLoopback(t)
	bHost, bPort := hostPortOfAddr(t, bootstrap)
	advertised := listenLoopback(t)
	aLiveHost, aLivePort := hostPortOfAddr(t, advertised)
	dead := deadLoopbackAddr(t)
	dHost, dPort := hostPortOfAddr(t, dead)
	cfg := config.Config{KafkaBrokers: bootstrap}
	ctx := scoped(owners.Pipeline{ID: "orders"})

	// Every advertised listener is a configured address: no mismatch.
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{{NodeID: 1, Host: bHost, Port: int32(bPort)}}}}
	res, err := run(t, Deps{Config: cfg, Kafka: kk}, ctx, `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "listeners ok") {
		t.Fatalf("summary = %q", res.Summary)
	}

	// A broker that advertises a reachable address outside the configured list
	// is a warning, and its listener is probed.
	kk = &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: bHost, Port: int32(bPort)},
		{NodeID: 2, Host: aLiveHost, Port: int32(aLivePort)},
	}}}
	res, err = run(t, Deps{Config: cfg, Kafka: kk}, ctx, `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "listeners warn") || !strings.Contains(res.Summary, advertised) {
		t.Fatalf("summary = %q", res.Summary)
	}

	// A broker that advertises an address that is not reachable here fails,
	// and the failing step is the TCP probe of that advertised address.
	kk = &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: bHost, Port: int32(bPort)},
		{NodeID: 2, Host: dHost, Port: int32(dPort)},
	}}}
	res, err = run(t, Deps{Config: cfg, Kafka: kk}, ctx, `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "listeners fail") || !strings.Contains(res.Summary, dead) {
		t.Fatalf("summary = %q", res.Summary)
	}
	var data probeData
	b, _ := json.Marshal(res.Data)
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatal(err)
	}
	var advertisedTCP bool
	for _, c := range data.Checks {
		if c.ID == "tcp" && c.Addr == dead && c.Status == StatusFail {
			advertisedTCP = true
		}
	}
	if !advertisedTCP {
		t.Fatalf("checks = %+v", data.Checks)
	}
}

// A broker that advertises a name that does not resolve from here (a
// Kubernetes headless-service name) fails the listeners check like a refused
// port, with the dns step of the advertised address as the failing step.
func TestToolKafkaAdvertisedListenerUnresolvable(t *testing.T) {
	bootstrap := listenLoopback(t)
	bHost, bPort := hostPortOfAddr(t, bootstrap)
	const internalName = "kafka-2.kafka-headless.kafka.svc.cluster.local"
	loc := &fakeLocation{lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
		if host == internalName {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		return ProcessLocation{}.Lookup(ctx, host)
	}}
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: bHost, Port: int32(bPort)},
		{NodeID: 2, Host: internalName, Port: 9094},
	}}}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: bootstrap}, Kafka: kk, Location: loc}, scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, want := range []string{"2 of ", "listeners fail: node 2 advertises " + internalName + ":9094", "not reachable from here", "dns " + internalName + ":9094 fail"} {
		if !strings.Contains(res.Summary, want) {
			t.Fatalf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
}

func TestToolKafkaListenersSkippedWhenBootstrapDown(t *testing.T) {
	dead := deadLoopbackAddr(t)
	kk := &fakeKafka{}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: dead}, Kafka: kk},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if kk.calls != 0 {
		t.Fatalf("metadata read %d times after the bootstrap brokers failed", kk.calls)
	}
	if !strings.Contains(res.Summary, "listeners skip") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestToolConnectView(t *testing.T) {
	addr := listenLoopback(t)
	host, port := hostPortOfAddr(t, addr)
	running := map[string]string{
		"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": host,
		"database.port":     strconv.Itoa(port),
		"database.user":     "debezium",
		"database.password": "dbpass",
	}
	var gotClass string
	var gotCfg map[string]string
	fc := &fakeConnect{
		configFn: func(_ context.Context, name string) (map[string]string, error) {
			if name != "pg-orders" {
				return nil, errFakeWrite
			}
			out := make(map[string]string, len(running))
			for k, v := range running {
				out[k] = v
			}
			return out, nil
		},
		validateFn: func(_ context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
			gotClass, gotCfg = class, cfg
			return connect.ValidateResult{}, nil
		},
	}
	cfg := config.Config{SourceDBURL: "postgres://debezium:dbpass@" + addr + "/orders"}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders"}}
	res, err := run(t, Deps{Config: cfg, Connect: fc}, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "connect_view ok") {
		t.Fatalf("summary = %q", res.Summary)
	}
	if gotClass != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Fatalf("class = %q", gotClass)
	}
	// Connect validates the config it already holds, credentials included; the
	// model supplied none.
	if gotCfg["database.password"] != "dbpass" || gotCfg["database.hostname"] != host {
		t.Fatalf("validated config = %+v", gotCfg)
	}

	// Connect cannot reach the database: the host key's message is the finding,
	// redacted and clipped.
	fc.validateFn = func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
		return connect.ValidateResult{ErrorCount: 1, Errors: map[string][]string{
			"database.hostname": {"Unable to connect to jdbc:postgresql://debezium:dbpass@" + addr + "/orders"},
		}}, nil
	}
	res, err = run(t, Deps{Config: cfg, Connect: fc}, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "connect_view fail") || !strings.Contains(res.Summary, "database.hostname") {
		t.Fatalf("summary = %q", res.Summary)
	}
	if strings.Contains(res.Summary, "dbpass") {
		t.Fatalf("summary leaks a credential: %q", res.Summary)
	}

	// Errors elsewhere in the config are a warning: they say nothing about the
	// network.
	fc.validateFn = func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
		return connect.ValidateResult{ErrorCount: 1, Errors: map[string][]string{
			"table.include.list": {"Table public.missing does not exist"},
		}}, nil
	}
	res, err = run(t, Deps{Config: cfg, Connect: fc}, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "connect_view warn") || !strings.Contains(res.Summary, "table.include.list") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

// A Connect client without the validator leaves the check out instead of
// failing the probe.
func TestToolConnectViewWithoutValidator(t *testing.T) {
	addr := listenLoopback(t)
	fc := &plainConnect{}
	res, err := run(t, Deps{Config: config.Config{ConnectURL: "http://" + addr}, Connect: fc},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"connect"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "tcp ok") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

// plainConnect is a Connect client without ValidateConfig.
type plainConnect struct{ clients.ConnectClient }

func TestToolKafkaTLSNoClientCert(t *testing.T) {
	now := time.Now()
	addr, _, caFile, clientCerts := tlsServer(t, certOpts{
		dns: []string{"localhost"}, ips: []string{"127.0.0.1"},
		notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	host, port := hostPortOfAddr(t, addr)
	cfg := config.Config{
		KafkaBrokers:          addr,
		KafkaSecurityProtocol: "SSL",
		KafkaTLSCAFile:        caFile,
	}
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{{NodeID: 1, Host: host, Port: int32(port)}}}}
	res, err := run(t, Deps{Config: cfg, Kafka: kk}, scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "tls ok") || !strings.Contains(res.Summary, "chain verified") {
		t.Fatalf("summary = %q", res.Summary)
	}
	if n := clientCerts.Load(); n != 0 {
		t.Fatalf("the probe presented %d client certificate(s)", n)
	}
}

func TestToolOverallDeadline(t *testing.T) {
	loc := &fakeLocation{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(scoped(owners.Pipeline{ID: "orders"}), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	res, err := run(t, Deps{Config: config.Config{ConnectURL: "http://10.0.0.1:8083"}, Location: loc}, ctx, `{"target":"connect"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the tool took %v on a 120ms budget", took)
	}
	if !strings.Contains(res.Summary, "within the step's time") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestResultCaps(t *testing.T) {
	tool := &netProbe{}
	checks := make([]Check, 0, 60)
	for i := 0; i < 60; i++ {
		checks = append(checks, Check{ID: "tcp", Addr: fmt.Sprintf("h%d:1", i), Status: StatusOK, Detail: "TCP connected"})
	}
	res := tool.result(probeData{Target: LabelKafka, Kind: KindKafka, Endpoints: []string{"h:1"}, Checks: checks})
	data := res.Data.(probeData)
	if len(data.Checks) != maxChecks || !res.Truncated {
		t.Fatalf("checks = %d truncated = %v", len(data.Checks), res.Truncated)
	}
	if !strings.Contains(res.Summary, "more checks not shown") {
		t.Fatalf("summary = %q", res.Summary)
	}
	if len(res.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary is %d bytes", len(res.Summary))
	}
}

func TestHostKeyAndMessages(t *testing.T) {
	for key, want := range map[string]bool{
		"database.hostname":   true,
		"database.port":       true,
		"schema.registry.url": true,
		"database.password":   false,
		"slot.name":           false,
		"key.converter":       false,
		"database.history.kafka.bootstrap.servers": true,
	} {
		if got := hostKey(key); got != want {
			t.Errorf("hostKey(%q) = %v, want %v", key, got, want)
		}
	}
	hostProblems, other := hostMessages(map[string][]string{
		"database.hostname":  {"Connection refused (password=dbpass)"},
		"table.include.list": {"Table missing"},
	})
	if len(hostProblems) != 1 || !strings.Contains(hostProblems[0], "database.hostname") {
		t.Fatalf("hostProblems = %+v", hostProblems)
	}
	if strings.Contains(hostProblems[0], "dbpass") {
		t.Fatalf("message not redacted: %q", hostProblems[0])
	}
	if len(other) != 1 || other[0] != "table.include.list" {
		t.Fatalf("other = %+v", other)
	}
}
