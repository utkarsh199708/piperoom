package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Every write method on the test fakes must refuse unless the test opts in
// with allowWrites. Each case runs twice: with the flag off, the write is
// refused and its hook never runs; with it on, the hook runs.
func TestFakeWritesRefusedByDefault(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		// run performs the write and reports whether its hook was reached.
		run func(allow bool) (reached bool, err error)
	}{
		{"fakeConnect.RestartTask", func(allow bool) (bool, error) {
			hit := false
			f := &fakeConnect{allowWrites: allow, restartTFn: func(context.Context, string, int) error { hit = true; return nil }}
			err := f.RestartTask(ctx, "pg-orders-connector", 0)
			return hit, err
		}},
		{"fakeConnect.RestartConnector", func(allow bool) (bool, error) {
			hit := false
			f := &fakeConnect{allowWrites: allow, restartCFn: func(context.Context, string) error { hit = true; return nil }}
			err := f.RestartConnector(ctx, "pg-orders-connector")
			return hit, err
		}},
		{"fakeRegistry.SetCompatibility", func(allow bool) (bool, error) {
			hit := false
			f := &fakeRegistry{allowWrites: allow, setFn: func(context.Context, string, string) error { hit = true; return nil }}
			err := f.SetCompatibility(ctx, "pg.public.orders-value", "FULL")
			return hit, err
		}},
		{"fakeRegistry.Register", func(allow bool) (bool, error) {
			hit := false
			f := &fakeRegistry{allowWrites: allow, registerFn: func(context.Context, string, string) (int, error) { hit = true; return 1, nil }}
			_, err := f.Register(ctx, "pg.public.orders-value", "{}")
			return hit, err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reached, err := tc.run(false)
			if reached || !errors.Is(err, errWriteRefused) {
				t.Fatalf("default: hook reached=%v err=%v, want refused with errWriteRefused and no hook call", reached, err)
			}
			reached, err = tc.run(true)
			if !reached || err != nil {
				t.Fatalf("allowWrites: hook reached=%v err=%v, want hook called and no error", reached, err)
			}
		})
	}
}

// Every registered read-only tool runs against fakes whose read hooks answer
// and whose write hooks fail the test if reached. Inputs are sent twice: an
// empty object, and one carrying every field the tools read, so the calls get
// past input checks. The point is that no tool reaches a write.
func TestReadOnlyToolsNeverReachAWrite(t *testing.T) {
	ctx := tools.WithUnscoped(context.Background())
	fc := &fakeConnect{
		listFn:   func(context.Context) ([]string, error) { return nil, nil },
		statusFn: func(context.Context, string) (clients.ConnectorStatus, error) { return clients.ConnectorStatus{}, nil },
		configFn: func(context.Context, string) (map[string]string, error) { return map[string]string{}, nil },
		restartTFn: func(context.Context, string, int) error {
			t.Error("RestartTask hook reached by a read-only tool")
			return nil
		},
		restartCFn: func(context.Context, string) error {
			t.Error("RestartConnector hook reached by a read-only tool")
			return nil
		},
	}
	fr := &fakeRegistry{
		subjectsFn: func(context.Context) ([]string, error) { return nil, nil },
		versionsFn: func(context.Context, string) ([]int, error) { return nil, nil },
		schemaFn:   func(context.Context, string, int) (clients.SchemaVersion, error) { return clients.SchemaVersion{}, nil },
		compatFn:   func(context.Context, string) (string, bool, error) { return "BACKWARD", true, nil },
		setFn: func(context.Context, string, string) error {
			t.Error("SetCompatibility hook reached by a read-only tool")
			return nil
		},
		registerFn: func(context.Context, string, string) (int, error) {
			t.Error("Register hook reached by a read-only tool")
			return 0, nil
		},
	}
	fk := &fakeKafka{
		topicsFn:  func(context.Context) ([]string, error) { return nil, nil },
		offsetsFn: func(context.Context, string) ([]clients.PartitionOffsets, error) { return nil, nil },
		groupsFn:  func(context.Context) ([]string, error) { return nil, nil },
		lagFn:     func(context.Context, string) (clients.GroupLag, error) { return clients.GroupLag{}, nil },
		findFn: func(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
			return nil, nil
		},
	}
	guardWrites(t, fc, fr)
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc, Registry: fr, Kafka: fk, Owners: scopeOwners()}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	inputs := []string{
		`{}`,
		`{"connector":"pg-orders-connector","task":0,"subject":"pg.public.orders-value","topic":"pg.public.orders","group":"orders-sink","key":"42","limit":5,"max_scan":10}`,
	}
	for _, tool := range reg.All() {
		for _, in := range inputs {
			// Errors are expected for some inputs; only a write is a failure here.
			_, _ = tool.Run(ctx, json.RawMessage(in))
		}
	}
}

// The fakes must still satisfy the contract interfaces; a write method that
// stopped matching them would fail to compile here.
var (
	_ clients.ConnectClient        = (*fakeConnect)(nil)
	_ clients.SchemaRegistryClient = (*fakeRegistry)(nil)
)
