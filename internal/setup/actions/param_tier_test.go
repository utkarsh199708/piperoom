package setupactions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/room"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/host"
	"piperoom/internal/tools"
)

const (
	outsidePOSIX   = "/etc/kafka/connect-distributed.properties"
	outsideWindows = `C:\kafka\config\connect-standalone.properties`
)

func propsParams(file string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"file": file, "entries": map[string]string{"group.id": "piperoom-connect"}})
	return raw
}

// propertiesAdapter is the properties_write action of EnvironmentSpecs over f.
func propertiesAdapter(t *testing.T, f *fakeRunner) *Adapter {
	t.Helper()
	for _, spec := range EnvironmentSpecs() {
		if spec.Name == "properties_write" {
			a, err := New(spec, Deps{Runner: f, RoomID: "r_setup"})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return a
		}
	}
	t.Fatal("no properties_write spec")
	return nil
}

// TestPropertiesWriteTierByParams is the server's half of the U8a ruling: a
// file outside the project directory is T2 (raised further by the class,
// plan.TierOf), a project-root file stays T1, and the Compose target's own
// files stay T1.
func TestPropertiesWriteTierByParams(t *testing.T) {
	a := propertiesAdapter(t, &fakeRunner{})
	cases := []struct {
		file, class, want string
	}{
		{".env", protocol.ClassLocal, plan.TierT1},
		{"worker.properties", protocol.ClassLocal, plan.TierT1},
		{"connect-distributed.properties", protocol.ClassLocal, plan.TierT1},
		{"connect-standalone.properties", protocol.ClassLocal, plan.TierT1},
		{"connect-distributed.properties", protocol.ClassShared, plan.TierT2},
		{"connect-distributed.properties", protocol.ClassProduction, plan.TierT3},
		{outsidePOSIX, protocol.ClassLocal, plan.TierT2},
		{outsideWindows, protocol.ClassLocal, plan.TierT2},
		{outsidePOSIX, protocol.ClassShared, plan.TierT2},
		// plan.TierOf still raises the param tier to the class.
		{outsidePOSIX, protocol.ClassProduction, plan.TierT3},
	}
	for _, c := range cases {
		got, err := a.Tier(propsParams(c.file), c.class)
		if err != nil || got != c.want {
			t.Errorf("Tier(%s, %s) = %q, %v; want %q", c.file, c.class, got, err, c.want)
		}
	}
	// An unknown class is still an error, with or without a param tier.
	if _, err := a.Tier(propsParams(outsidePOSIX), "galactic"); !errors.Is(err, plan.ErrClass) {
		t.Errorf("unknown class: %v, want plan.ErrClass", err)
	}
}

// TestPropertiesWriteFileRule mirrors host's validator: an absolute path is
// accepted only with a worker file's base name and plain components.
func TestPropertiesWriteFileRule(t *testing.T) {
	a := propertiesAdapter(t, &fakeRunner{})
	for _, ok := range []string{
		".env", "worker.properties", "connect-distributed.properties", "connect-standalone.properties",
		outsidePOSIX, outsideWindows, "/opt/kafka/config/connect-standalone.properties",
		"C:/kafka/config/connect-distributed.properties",
	} {
		if err := a.Validate(context.Background(), propsParams(ok)); err != nil {
			t.Errorf("Validate(%s): %v", ok, err)
		}
	}
	for _, bad := range []string{
		"",                                      // empty
		"config/connect-distributed.properties", // relative, not a root name
		"/etc/kafka/server.properties",          // another base name
		"/etc/kafka/.env",                       // a Compose file outside
		"/connect-distributed.properties",       // no directory
		"/etc/../kafka/connect-distributed.properties",
		"/etc/./kafka/connect-distributed.properties",
		"/etc//kafka/connect-distributed.properties",
		"/etc/kafka/connect-distributed.properties/",
		"/etc/my kafka/connect-distributed.properties",  // a space in a POSIX path
		`\\server\share\connect-distributed.properties`, // UNC
		`\kafka\connect-distributed.properties`,         // rooted, no drive
		`C:kafka\connect-distributed.properties`,        // drive-relative
		`C:\kafka\..\connect-distributed.properties`,
	} {
		err := a.Validate(context.Background(), propsParams(bad))
		if !errors.Is(err, ErrBadInput) {
			t.Errorf("Validate(%q) = %v, want ErrBadInput", bad, err)
		}
		if _, err := a.Tier(propsParams(bad), protocol.ClassLocal); !errors.Is(err, ErrBadInput) {
			t.Errorf("Tier(%q) = %v, want ErrBadInput", bad, err)
		}
	}
}

// TestPropertiesWriteTierMatchesTheRunner: the server's tier and the runner's
// floor (ops.Registry.FloorFor over host's TierFor) agree on every file, so a
// step the room routes is never refused below its floor and a T1 route never
// covers a T2 step.
func TestPropertiesWriteTierMatchesTheRunner(t *testing.T) {
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	if _, err := host.Register(reg, host.Options{TierRouted: true}); err != nil {
		t.Fatalf("host.Register: %v", err)
	}
	a := propertiesAdapter(t, &fakeRunner{})
	for _, file := range []string{"connect-distributed.properties", "connect-standalone.properties", outsidePOSIX, outsideWindows} {
		params, _ := json.Marshal(host.PropertiesParams{File: file, Entries: map[string]string{"group.id": "g"}})
		floor, ok, err := reg.FloorFor("properties_write", params)
		if err != nil || !ok {
			t.Fatalf("FloorFor(%s) = %q, %v, %v", file, floor, ok, err)
		}
		got, err := a.Tier(params, protocol.ClassLocal)
		if err != nil || got != floor {
			t.Errorf("%s: server tier %q (%v), runner floor %q", file, got, err, floor)
		}
	}
}

// propertiesContext is a setup room's action context for one properties_write
// step routed at tier.
func propertiesContext(params json.RawMessage, tier string) context.Context {
	ctx := tools.WithPipeline(context.Background(), testPipeline())
	ctx = room.WithRoomID(ctx, "r_setup")
	return room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_props", StepIndex: 0, ParamsHash: "hash_props", Tier: tier,
		Class: protocol.ClassLocal, Action: "properties_write", Params: params,
	})
}

// TestPropertiesWriteOutsideRefusesAT1Step: a step the room routed at T1 for a
// file outside the project directory is refused before anything is sent; at
// T2 it goes out at T2; a project-root file at T1 goes out at T1.
func TestPropertiesWriteOutsideRefusesAT1Step(t *testing.T) {
	f := &fakeRunner{}
	a := propertiesAdapter(t, f)
	outside := propsParams(outsidePOSIX)
	if _, err := a.Execute(propertiesContext(outside, plan.TierT1), outside); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("T1 outside: %v, want ErrStepMismatch", err)
	}
	if n := len(f.recorded()); n != 0 {
		t.Fatalf("a T1 outside step sent %d requests, want 0", n)
	}
	if _, err := a.Execute(propertiesContext(outside, plan.TierT2), outside); err != nil {
		t.Fatalf("T2 outside: %v", err)
	}
	if got := f.recorded()[0].req.Tier; got != plan.TierT2 {
		t.Fatalf("T2 outside sent at %q", got)
	}
	// A step the room left untiered is raised to the param tier.
	if _, err := a.Execute(propertiesContext(outside, ""), outside); err != nil {
		t.Fatalf("untiered outside: %v", err)
	}
	if got := f.recorded()[1].req.Tier; got != plan.TierT2 {
		t.Fatalf("untiered outside sent at %q, want T2", got)
	}
	root := propsParams("connect-distributed.properties")
	if _, err := a.Execute(propertiesContext(root, plan.TierT1), root); err != nil {
		t.Fatalf("T1 root: %v", err)
	}
	if got := f.recorded()[2].req.Tier; got != plan.TierT1 {
		t.Fatalf("T1 root sent at %q", got)
	}
}

// TestTierWithoutTierForIsUnchanged: a spec without TierFor answers exactly
// plan.TierOf of its floor, and the guard table still forbids its pairs.
func TestTierWithoutTierForIsUnchanged(t *testing.T) {
	reg := actions.NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	for _, spec := range EnvironmentSpecs() {
		if spec.Name != "properties_write" && spec.TierFor != nil {
			t.Errorf("%s has a TierFor; only properties_write's tier depends on its params", spec.Name)
		}
	}
	fw, _ := reg.Get("file_write")
	for _, class := range []string{protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction} {
		want, _ := plan.TierOf(plan.TierT1, class)
		got, err := fw.(actions.Tiered).Tier(json.RawMessage(`{"path":"/etc/x","content":"y"}`), class)
		if err != nil || got != want {
			t.Errorf("file_write at %s = %q, %v; want %q", class, got, err, want)
		}
	}
	if _, forbidden := plan.Forbidden(plan.GuardRequest{Op: "file_write", Class: protocol.ClassProduction}); !forbidden {
		t.Error("the guard table no longer forbids file_write on production")
	}
}

// TestFloorForRefusesAnUnusableTierFor: a TierFor that errors or answers a
// non-tier refuses the params; it never falls back to the spec's floor.
func TestFloorForRefusesAnUnusableTierFor(t *testing.T) {
	for name, tf := range map[string]func(json.RawMessage) (string, error){
		"error":    func(json.RawMessage) (string, error) { return "", errors.New("no") },
		"non-tier": func(json.RawMessage) (string, error) { return "T7", nil },
	} {
		spec := testSpec()
		spec.Tier = plan.TierT1
		spec.TierFor = tf
		f := &fakeRunner{}
		a, err := New(spec, Deps{Runner: f, RoomID: "r_setup"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Tier(grantParams, protocol.ClassLocal); !errors.Is(err, ErrBadInput) {
			t.Errorf("%s: Tier = %v, want ErrBadInput", name, err)
		}
		if _, err := a.Execute(testContext(t, grantParams), grantParams); !errors.Is(err, ErrBadInput) {
			t.Errorf("%s: Execute = %v, want ErrBadInput", name, err)
		}
		if n := len(f.recorded()); n != 0 {
			t.Errorf("%s: %d requests sent", name, n)
		}
	}
}
