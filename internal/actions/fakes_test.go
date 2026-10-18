package actions

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
)

// Every write method on the test fakes must refuse unless the test opts in
// with allowWrites. Each case runs twice: with the flag off, the write returns
// errWriteRefused and changes nothing; with it on, the write takes effect.
func TestFakeWritesRefusedByDefault(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		// run performs the write and reports whether it took effect.
		run func(allow bool) (applied bool, err error)
	}{
		{"fakeConnect.RestartTask", func(allow bool) (bool, error) {
			f := &fakeConnect{allowWrites: allow}
			err := f.RestartTask(ctx, "pg-orders-connector", 2)
			return f.restarted == "pg-orders-connector" && f.restartTask == 2, err
		}},
		{"fakeConnect.RestartConnector", func(allow bool) (bool, error) {
			f := &fakeConnect{allowWrites: allow}
			err := f.RestartConnector(ctx, "pg-orders-connector")
			return f.restarted == "pg-orders-connector", err
		}},
		{"fakeConnect.Resume", func(allow bool) (bool, error) {
			f := &fakeConnect{allowWrites: allow}
			err := f.Resume(ctx, "pg-orders-connector")
			return f.resumed == "pg-orders-connector", err
		}},
		{"fakeRegistry.SetCompatibility", func(allow bool) (bool, error) {
			f := &fakeRegistry{allowWrites: allow}
			err := f.SetCompatibility(ctx, "pg.public.orders-value", "FULL")
			return f.compatLevel == "FULL", err
		}},
		{"fakeRegistry.Register", func(allow bool) (bool, error) {
			f := &fakeRegistry{allowWrites: allow, regID: 7}
			id, err := f.Register(ctx, "pg.public.orders-value", "{}")
			return id == 7, err
		}},
		{"scriptedConnect.RestartTask", func(allow bool) (bool, error) {
			f := &scriptedConnect{allowWrites: allow}
			err := f.RestartTask(ctx, "pg-orders-connector", 0)
			return err == nil, err
		}},
		{"scriptedConnect.RestartConnector", func(allow bool) (bool, error) {
			f := &scriptedConnect{allowWrites: allow}
			err := f.RestartConnector(ctx, "pg-orders-connector")
			return err == nil, err
		}},
		{"noResumeConnect.RestartTask", func(allow bool) (bool, error) {
			f := &noResumeConnect{allowWrites: allow}
			err := f.RestartTask(ctx, "pg-orders-connector", 0)
			return err == nil, err
		}},
		{"noResumeConnect.RestartConnector", func(allow bool) (bool, error) {
			f := &noResumeConnect{allowWrites: allow}
			err := f.RestartConnector(ctx, "pg-orders-connector")
			return err == nil, err
		}},
		{"configConnect.UpdateConfig", func(allow bool) (bool, error) {
			f := newConfigConnect()
			f.allowWrites = allow
			err := f.UpdateConfig(ctx, "pg-orders-connector", "decimal.handling.mode", "double")
			return f.cfg["decimal.handling.mode"] == "double", err
		}},
		{"configConnect.UpdateConnectorConfigKeys", func(allow bool) (bool, error) {
			f := newConfigConnect()
			f.allowWrites = allow
			err := f.UpdateConnectorConfigKeys(ctx, "pg-orders-connector", map[string]string{"decimal.handling.mode": "string", "tombstones.on.delete": "false"})
			return f.cfg["decimal.handling.mode"] == "string" && f.cfg["tombstones.on.delete"] == "false", err
		}},
		{"pauseConnect.PauseConnector", func(allow bool) (bool, error) {
			f := &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: allow}}
			err := f.PauseConnector(ctx, "pg-orders-connector")
			return f.paused == "pg-orders-connector", err
		}},
		{"inheritedUntilSet.SetCompatibility", func(allow bool) (bool, error) {
			f := &inheritedUntilSet{fakeRegistry: &fakeRegistry{allowWrites: allow}}
			err := f.SetCompatibility(ctx, "pg.public.orders-value", "FULL")
			return f.set && f.compatLevel == "FULL", err
		}},
		{"compatSetFails.SetCompatibility", func(allow bool) (bool, error) {
			// The fake's own error stands in for the registry failure; with
			// writes off, the refusal must come back instead of that error.
			boom := errors.New("registry 500")
			f := &compatSetFails{fakeRegistry: &fakeRegistry{allowWrites: allow}, err: boom}
			err := f.SetCompatibility(ctx, "pg.public.orders-value", "FULL")
			return errors.Is(err, boom), err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			applied, err := tc.run(false)
			if applied || !errors.Is(err, errWriteRefused) {
				t.Fatalf("default: applied=%v err=%v, want refused with errWriteRefused and no state change", applied, err)
			}
			applied, err = tc.run(true)
			if !applied {
				t.Fatalf("allowWrites: write did not take effect (err=%v)", err)
			}
		})
	}
}

// The fakes must keep satisfying the contract interface; a write method that
// drifted from it would fail to compile here.
var _ clients.ConnectClient = (*fakeConnect)(nil)
