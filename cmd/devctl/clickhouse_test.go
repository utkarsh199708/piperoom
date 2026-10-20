package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestClickHouseInitStartsProfileAndCreatesOrdersSink(t *testing.T) {
	var composeCalls int
	var gotSQL string
	st := &stack{
		composeFn: func(_ context.Context, _ map[string]string, args ...string) error {
			composeCalls++
			want := []string{"--profile", "clickhouse", "up", "-d", "--wait", "clickhouse"}
			if strings.Join(args, " ") != strings.Join(want, " ") {
				t.Fatalf("compose args = %v, want %v", args, want)
			}
			return nil
		},
		clickhouseExecFn: func(_ context.Context, query string) error {
			gotSQL = query
			return nil
		},
	}

	if err := st.clickhouseInit(context.Background()); err != nil {
		t.Fatalf("clickhouseInit: %v", err)
	}
	if composeCalls != 1 {
		t.Fatalf("compose calls = %d, want 1", composeCalls)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS analytics.orders",
		"ENGINE = ReplacingMergeTree(version, is_deleted)",
		"amount Int64,",
		"toDecimal64(tupleElement(if(op = 'd', \"before\", \"after\"), 'amount'), 2) AS amount",
		"toUInt8(op = 'd') AS is_deleted",
		"CREATE TABLE IF NOT EXISTS analytics.orders_kafka",
		"kafka_topic_list = 'pg.public.orders'",
		"kafka_format = 'AvroConfluent'",
		"format_avro_schema_registry_url = 'http://schema-registry:8081'",
		"CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv",
		"TO analytics.orders",
	} {
		if !strings.Contains(gotSQL, want) {
			t.Errorf("ClickHouse init SQL missing %q", want)
		}
	}
}

// clickhouseImage is the exact 25.8 LTS release and its multi-arch index
// digest, so a re-pushed tag cannot change what the dev stack runs.
const clickhouseImage = "clickhouse/clickhouse-server:25.8.33.6@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77"

func TestClickHouseInitSQLDoesNotScaleIntoDecimal(t *testing.T) {
	// The Kafka engine must not declare amount as Decimal: an Avro int read
	// into Decimal is stored unscaled (4969 becomes 49.69).
	kafkaTable := clickhouseInitSQL[strings.Index(clickhouseInitSQL, "CREATE TABLE IF NOT EXISTS analytics.orders_kafka"):strings.Index(clickhouseInitSQL, "ENGINE = Kafka")]
	if strings.Contains(kafkaTable, "Decimal") {
		t.Fatalf("orders_kafka declares a Decimal column; read amount as Int64 and convert in the view:\n%s", kafkaTable)
	}
	if strings.Contains(clickhouseInitSQL, "kafka_handle_error_mode") {
		t.Fatal("init SQL sets kafka_handle_error_mode; the sink must stop on the schema change, not skip rows")
	}
	if strings.Contains(clickhouseInitSQL, "WHERE op != 'd'") {
		t.Fatal("the view drops deletes; it must keep them as is_deleted rows")
	}
}

func TestClickHouseInitErrors(t *testing.T) {
	notReady := fmt.Errorf("%w (clickhouse-client exit 210): exit status 210", errClickHouseNotReady)
	sqlErr := errors.New("exit status 62")
	tests := []struct {
		name        string
		composeErr  error
		execErrs    []error // one per call; the last repeats
		wantCalls   int     // exact; -1 means "more than one"
		wantErrIs   error
		wantContain string
	}{
		{"compose fails", errors.New("devctl: docker up: exit status 1"), nil, 0, nil, "docker up"},
		{"exec fails with a SQL error", nil, []error{sqlErr}, 1, sqlErr, "initialize ClickHouse orders tables"},
		{"retry recovers", nil, []error{notReady, notReady, nil}, 3, nil, ""},
		{"retry gives up", nil, []error{notReady}, -1, errClickHouseNotReady, "still not accepting connections"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			st := &stack{
				composeFn: func(context.Context, map[string]string, ...string) error { return tt.composeErr },
				clickhouseExecFn: func(context.Context, string) error {
					calls++
					if len(tt.execErrs) == 0 {
						return nil
					}
					return tt.execErrs[min(calls, len(tt.execErrs))-1]
				},
				clickhouseRetryEvery: time.Millisecond,
				clickhouseRetryFor:   50 * time.Millisecond,
			}
			err := st.clickhouseInit(context.Background())
			switch {
			case tt.wantContain == "" && err != nil:
				t.Fatalf("clickhouseInit: %v", err)
			case tt.wantContain != "" && (err == nil || !strings.Contains(err.Error(), tt.wantContain)):
				t.Fatalf("clickhouseInit = %v, want error containing %q", err, tt.wantContain)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Fatalf("clickhouseInit = %v, want errors.Is %v", err, tt.wantErrIs)
			}
			if tt.wantCalls >= 0 && calls != tt.wantCalls {
				t.Fatalf("exec calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantCalls < 0 && calls < 2 {
				t.Fatalf("exec calls = %d, want retries", calls)
			}
		})
	}
}

func TestRetryClickHouseStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryClickHouse(ctx, time.Hour, time.Hour, func(context.Context) error {
		calls++
		cancel()
		return errClickHouseNotReady
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errClickHouseNotReady) || calls != 1 {
		t.Fatalf("retryClickHouse = %v after %d calls, want context.Canceled after 1", err, calls)
	}
}

// TestClickHouseExitHelper is not a test: TestClassifyClickHouseExit runs the
// test binary as a child that exits with the code in the env var. A real
// *exec.ExitError cannot be built any other way.
func TestClickHouseExitHelper(t *testing.T) {
	code := os.Getenv("DEVCTL_CH_EXIT_HELPER")
	if code == "" {
		return // a plain run; a skip would trip CI's fail-on-skips check
	}
	n, _ := strconv.Atoi(code)
	os.Exit(n)
}

func TestClassifyClickHouseExit(t *testing.T) {
	if classifyClickHouseExit(nil) != nil {
		t.Fatal("nil error must stay nil")
	}
	for _, tt := range []struct {
		code      int
		wantRetry bool
	}{{210, true}, {209, true}, {32, true}, {62, false}, {1, false}} {
		cmd := osexec.Command(os.Args[0], "-test.run=^TestClickHouseExitHelper$")
		cmd.Env = append(os.Environ(), "DEVCTL_CH_EXIT_HELPER="+strconv.Itoa(tt.code))
		err := classifyClickHouseExit(cmd.Run())
		if err == nil {
			t.Fatalf("exit %d: classifyClickHouseExit = nil, want an error", tt.code)
		}
		if got := errors.Is(err, errClickHouseNotReady); got != tt.wantRetry {
			t.Errorf("exit %d: retryable = %v, want %v (%v)", tt.code, got, tt.wantRetry, err)
		}
	}
	if err := classifyClickHouseExit(errors.New("docker: not found")); errors.Is(err, errClickHouseNotReady) {
		t.Errorf("a start failure must not be retried: %v", err)
	}
}

func TestRunCommandClickHouseUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"start"}, {"init", "extra"}, {"--owner", "me"}} {
		err := runCommand(context.Background(), &stack{}, "clickhouse", args)
		if err == nil || !strings.Contains(err.Error(), "usage: devctl clickhouse init") {
			t.Errorf("clickhouse %v = %v, want the usage error", args, err)
		}
	}
	if err := runCommand(context.Background(), &stack{}, "clickhouse", []string{"--owner"}); err == nil {
		t.Error("clickhouse --owner without a value = nil, want an error")
	}
}

func TestClickHouseComposeProfileAndReaderGrants(t *testing.T) {
	st := repoStack(t)
	raw, err := os.ReadFile(filepath.Join(st.root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Profiles    []string          `yaml:"profiles"`
			Image       string            `yaml:"image"`
			Ports       []string          `yaml:"ports"`
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	svc, ok := doc.Services["clickhouse"]
	if !ok {
		t.Fatal("clickhouse service missing")
	}
	if strings.Join(svc.Profiles, ",") != "clickhouse" {
		t.Fatalf("profiles = %v, want [clickhouse]", svc.Profiles)
	}
	if svc.Image != clickhouseImage {
		t.Fatalf("image = %q, want a pinned ClickHouse server release tag", svc.Image)
	}
	if len(svc.Ports) != 1 || svc.Ports[0] != "127.0.0.1:9001:9000" {
		t.Fatalf("ports = %v, want native port bound to host loopback", svc.Ports)
	}
	if svc.Environment["CLICKHOUSE_DB"] != "analytics" ||
		svc.Environment["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] != "1" {
		t.Fatalf("ClickHouse environment does not initialize analytics and access management: %v", svc.Environment)
	}

	initSQL, err := os.ReadFile(filepath.Join(st.root, "dev", "clickhouse", "init", "01-reader.sql"))
	if err != nil {
		t.Fatal(err)
	}
	normalized := strings.ToLower(string(initSQL))
	for _, want := range []string{
		"create settings profile if not exists piperoom_reader",
		"settings readonly = 2",
		"create user if not exists piperoom_reader",
		"grant select on analytics.* to piperoom_reader",
		"grant select on system.query_log to piperoom_reader",
	} {
		if !strings.Contains(normalized, want) {
			t.Errorf("reader initialization SQL missing %q", want)
		}
	}
	if strings.Contains(normalized, "readonly = 1") {
		t.Fatal("reader initialization must use readonly=2, not readonly=1")
	}
	if got := strings.Count(normalized, "grant "); got != 2 {
		t.Fatalf("reader has %d grants, want exactly the two SELECT grants", got)
	}
}
