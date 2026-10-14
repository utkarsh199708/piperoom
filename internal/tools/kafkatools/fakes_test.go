package kafkatools

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
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

// The fakes must still satisfy the contract interfaces; a write method that
// stopped matching them would fail to compile here.
var (
	_ clients.ConnectClient        = (*fakeConnect)(nil)
	_ clients.SchemaRegistryClient = (*fakeRegistry)(nil)
)
