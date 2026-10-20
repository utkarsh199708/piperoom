package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room/demo"
	"piperoom/internal/store/memstore"
	"piperoom/internal/wiring"
)

// scriptedCfg is a config that satisfies config.ParseFakeAgent.
func scriptedCfg() config.Config {
	return config.Config{
		FakeAgent: config.FakeAgentScripted,
		AuthMode:  config.DefaultAuthMode,
		DevAck:    config.DevAckValue,
	}
}

func TestFakeAgentRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*config.Config)
		wantErr string
	}{
		{"oidc", func(c *config.Config) { c.AuthMode = "oidc" }, "PIPEROOM_AUTH_MODE"},
		{"no ack", func(c *config.Config) { c.DevAck = "" }, "PIPEROOM_DEV_ACK"},
		{"write mode on", func(c *config.Config) { c.WriteMode = "on" }, "PIPEROOM_WRITE_MODE"},
		{"slack token", func(c *config.Config) { c.SlackBotToken = "xoxb-test" }, "SLACK_BOT_TOKEN"},
		{"typo", func(c *config.Config) { c.FakeAgent = "Scripted" }, "want"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := scriptedCfg()
			tc.edit(&cfg)
			on, err := fakeAgentSetup(cfg, false, false, discardLog())
			if on {
				t.Fatal("fakeAgentSetup = true, want false")
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestFakeAgentUnsetUsesRealAgent(t *testing.T) {
	log, buf := testLog()
	on, err := fakeAgentSetup(config.Config{}, false, false, log)
	if err != nil || on {
		t.Fatalf("fakeAgentSetup(unset) = %v, %v; want false, nil", on, err)
	}
	if strings.Contains(buf.String(), "fake scripted agent") {
		t.Fatalf("unset must not warn:\n%s", buf.String())
	}

	// Unset means the real path: wireAgent builds the model endpoint.
	endpointBuilt := false
	_, err = wireAgent(context.Background(),
		func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
			t.Fatal("the real path must build the endpoint before the stacks")
			return nil, nil
		},
		func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error) {
			endpointBuilt = true
			return nil, errors.New("model endpoint")
		},
		wireAgentInput{cfg: config.Config{}, log: discardLog()},
	)
	if !endpointBuilt {
		t.Fatal("wireAgent did not build the model endpoint for the real path")
	}
	if err == nil || !strings.Contains(err.Error(), "model endpoint") {
		t.Fatalf("wireAgent err = %v, want the endpoint error to stop the start", err)
	}
}

func TestFakeAgentIgnoredUnderDemo(t *testing.T) {
	for _, tc := range []struct {
		name             string
		demo, quickstart bool
	}{
		{"demo", true, false},
		{"quickstart", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A value that would refuse outside --demo/--quickstart (oidc,
			// write mode on, a Slack token) is ignored: ParseFakeAgent is not
			// consulted, so no error and no warning.
			for _, v := range []string{config.FakeAgentScripted, "garbage"} {
				cfg := config.Config{FakeAgent: v, AuthMode: "oidc", WriteMode: "on", SlackBotToken: "xoxb-test"}
				log, buf := testLog()
				on, err := fakeAgentSetup(cfg, tc.demo, tc.quickstart, log)
				if err != nil || on {
					t.Fatalf("FakeAgent=%q: fakeAgentSetup = %v, %v; want false, nil (ignored)", v, on, err)
				}
				if strings.Contains(buf.String(), "fake scripted agent") {
					t.Fatalf("FakeAgent=%q: must not warn when ignored:\n%s", v, buf.String())
				}
			}
		})
	}
}

func TestFakeAgentWarns(t *testing.T) {
	log, buf := testLog()
	on, err := fakeAgentSetup(scriptedCfg(), false, false, log)
	if err != nil || !on {
		t.Fatalf("fakeAgentSetup = %v, %v; want true, nil", on, err)
	}
	if !strings.Contains(buf.String(), "level=WARN") ||
		!strings.Contains(buf.String(), "fake scripted agent: diagnoses are not real (PIPEROOM_FAKE_AGENT=scripted)") {
		t.Fatalf("missing the one start-up warning:\n%s", buf.String())
	}
}

func TestFakeAgentScriptedWiring(t *testing.T) {
	cfg := scriptedCfg()
	log, _ := testLog()
	on, err := fakeAgentSetup(cfg, false, false, log)
	if err != nil || !on {
		t.Fatalf("fakeAgentSetup = %v, %v; want true, nil", on, err)
	}

	endpointBuilt := false
	openStacks := func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
		return wiring.OpenStacks(ctx, cfg, awsCfg, own, opts)
	}
	newEndpoint := func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error) {
		endpointBuilt = true
		return nil, errors.New("must not build a model endpoint for the scripted agent")
	}

	w, err := wireAgent(context.Background(), openStacks, newEndpoint, wireAgentInput{
		cfg:        cfg,
		log:        discardLog(),
		own:        &owners.Owners{},
		st:         memstore.New(),
		spendStore: memstore.New(),
		spend:      spendSettings{},
		usage:      &usageRelay{log: discardLog()},
		hook:       &roomClosedHook{},
		fakeAgent:  true,
		pmMode:     config.PostmortemModelAuto,
		writeOn:    false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if endpointBuilt {
		t.Fatal("the scripted agent must not build a model endpoint")
	}
	if w.stacks == nil || w.stackFor == nil || w.topo == nil {
		t.Fatal("the real stacks must stay wired: stacks, stackFor and topo must all be set")
	}
	if w.triage != nil || w.triageTimeout != 0 {
		t.Fatalf("the scripted agent must have no triage: %+v", w)
	}
	if w.writePM == nil {
		t.Fatal("the scripted agent must wire the scripted post-mortem")
	}
	if funcPtr(w.run) != funcPtr(demo.Run) ||
		funcPtr(w.write) != funcPtr(demo.WriteHandoff) ||
		funcPtr(w.writePM) != funcPtr(demo.WritePostmortem) {
		t.Fatal("the scripted agent must use demo.Run/WriteHandoff/WritePostmortem")
	}
}

// funcPtr returns the code pointer of a package-level function, so a test can
// check that the wired agent paths are exactly the demo's.
func funcPtr(f any) uintptr {
	return reflect.ValueOf(f).Pointer()
}

// TestFakeAgentHelper is not a test: the refusal test below re-runs this test
// binary with PIPEROOM_TEST_FAKE_AGENT=1 so run() executes in real mode (no
// --demo, no --quickstart) in its own process, without clashing with the test
// flags. The bad value stops the start before anything connects, so no stack
// or database is needed.
func TestFakeAgentHelper(t *testing.T) {
	if os.Getenv("PIPEROOM_TEST_FAKE_AGENT") != "1" {
		t.Skip("helper for subprocess tests")
	}
	os.Args = []string{"server"}
	if err := run(); err != nil {
		os.Stdout.WriteString("RUN ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

// A typo in PIPEROOM_FAKE_AGENT refuses the whole start before it connects to
// anything, naming the variable.
func TestStartRefusesBadFakeAgent(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestFakeAgentHelper$")
	cmd.Env = append(os.Environ(),
		"PIPEROOM_TEST_FAKE_AGENT=1",
		"PIPEROOM_FAKE_AGENT=Scripted",
		"PIPEROOM_HTTP_ADDR=127.0.0.1:0",
		"PIPEROOM_WRITE_MODE=",
		"PIPEROOM_AUTH_MODE=",
		"PIPEROOM_DEV_ACK=",
	)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "PIPEROOM_FAKE_AGENT") {
		t.Fatalf("PIPEROOM_FAKE_AGENT=Scripted: err %v, want a start-up refusal naming the variable\n%s", err, out)
	}
	if strings.Contains(string(out), "server: listening") {
		t.Fatalf("the refused start listened:\n%s", out)
	}
}
