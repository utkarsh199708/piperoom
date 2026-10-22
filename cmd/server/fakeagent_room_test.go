package main

// Regression tests for KAN-383: PIPEROOM_FAKE_AGENT=scripted on a real server.
// The wiring itself is unit-tested in fakeagent_test.go; here the exact
// environment a demo exports is read through config.Load, and the scripted
// agent then runs a room through the real room service, store and HTTP API.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
)

// fakeAgentEnv is the exact environment KAN-144 exports for the scripted agent
// on the real stack: dev login acknowledged, write mode off, no Slack token.
// PIPEROOM_WATCH_MODE=rooms is part of the demo but not of the agent switch.
func fakeAgentEnv() map[string]string {
	return map[string]string{
		"PIPEROOM_FAKE_AGENT": config.FakeAgentScripted,
		"PIPEROOM_AUTH_MODE":  config.DefaultAuthMode,
		"PIPEROOM_DEV_ACK":    config.DevAckValue,
	}
}

// clearFakeAgentEnv unsets every variable fakeAgentSetup and ParseFakeAgent
// read, so an ambient value in the developer's shell or CI cannot change the
// result of the test.
func clearFakeAgentEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"PIPEROOM_FAKE_AGENT",
		"PIPEROOM_AUTH_MODE",
		"PIPEROOM_DEV_ACK",
		"PIPEROOM_WRITE_MODE",
		"SLACK_BOT_TOKEN",
	} {
		t.Setenv(k, "")
	}
}

// TestFakeAgentEnvSelection pins the selection at the environment boundary:
// config.Load reads PIPEROOM_FAKE_AGENT and fakeAgentSetup turns the scripted
// agent on only for the full dev condition set. Every other value or
// combination refuses the start with the config error naming the variable,
// instead of silently running the real agent.
func TestFakeAgentEnvSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantOn  bool
		wantErr string // "" = no error, as config.ParseFakeAgent writes it
	}{
		{name: "unset"},
		{name: "scripted with the dev conditions", env: fakeAgentEnv(), wantOn: true},
		{name: "scripted, auth mode any case", env: func() map[string]string {
			m := fakeAgentEnv()
			m["PIPEROOM_AUTH_MODE"] = "DEV"
			return m
		}(), wantOn: true},
		{name: "oidc", env: func() map[string]string {
			m := fakeAgentEnv()
			m["PIPEROOM_AUTH_MODE"] = "oidc"
			return m
		}(), wantErr: "PIPEROOM_AUTH_MODE"},
		{name: "no dev ack", env: func() map[string]string {
			m := fakeAgentEnv()
			delete(m, "PIPEROOM_DEV_ACK")
			return m
		}(), wantErr: "PIPEROOM_DEV_ACK"},
		{name: "write mode on", env: func() map[string]string {
			m := fakeAgentEnv()
			m["PIPEROOM_WRITE_MODE"] = "on"
			return m
		}(), wantErr: "PIPEROOM_WRITE_MODE"},
		{name: "slack token set", env: func() map[string]string {
			m := fakeAgentEnv()
			m["SLACK_BOT_TOKEN"] = "xoxb-test"
			return m
		}(), wantErr: "SLACK_BOT_TOKEN"},
		{name: "typo", env: map[string]string{
			"PIPEROOM_FAKE_AGENT": "Scripted",
			"PIPEROOM_AUTH_MODE":  config.DefaultAuthMode,
			"PIPEROOM_DEV_ACK":    config.DevAckValue,
		}, wantErr: "PIPEROOM_FAKE_AGENT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearFakeAgentEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			log, buf := testLog()
			on, err := fakeAgentSetup(config.Load(), false, false, log)
			if tc.wantErr != "" {
				if on {
					t.Fatal("fakeAgentSetup = true, want the start refused")
				}
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				if strings.Contains(buf.String(), "fake scripted agent") {
					t.Fatalf("a refused start must not warn about a scripted agent:\n%s", buf.String())
				}
				return
			}
			if err != nil || on != tc.wantOn {
				t.Fatalf("fakeAgentSetup = %v, %v; want %v, nil", on, err, tc.wantOn)
			}
			if warned := strings.Contains(buf.String(), "fake scripted agent: diagnoses are not real"); warned != tc.wantOn {
				t.Fatalf("warning logged = %v, want %v:\n%s", warned, tc.wantOn, buf.String())
			}
		})
	}
}

// TestFakeAgentScriptedRunsThroughHTTP is the end-to-end regression: with the
// demo environment the server wires the scripted agent the way runWith does
// outside --demo (no model endpoint, the real stacks kept), opens a room
// through the HTTP alert webhook and serves the scripted investigation back
// over the room API on a real event log (in memory here; the same room.Service
// runs on Postgres). The story is demo.Run's, so the event log can only come
// from the scripted runner.
func TestFakeAgentScriptedRunsThroughHTTP(t *testing.T) {
	clearFakeAgentEnv(t)
	for k, v := range fakeAgentEnv() {
		t.Setenv(k, v)
	}
	cfg := config.Load()
	log, logs := testLog()
	on, err := fakeAgentSetup(cfg, false, false, log)
	if err != nil || !on {
		t.Fatalf("fakeAgentSetup = %v, %v; want the scripted agent on", on, err)
	}
	if !strings.Contains(logs.String(), "fake scripted agent: diagnoses are not real (PIPEROOM_FAKE_AGENT=scripted)") {
		t.Fatalf("missing the one start-up warning:\n%s", logs.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpointBuilt, stacksOpened := false, false
	openStacks := func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
		stacksOpened = true
		// No pipeline: an empty client set, so the test needs no stack.
		opts.Only = []string{}
		return wiring.OpenStacks(ctx, cfg, awsCfg, own, opts)
	}
	newEndpoint := func(context.Context, config.Config, *slog.Logger, *aws.Config) (*agent.Endpoint, error) {
		endpointBuilt = true
		return nil, errors.New("must not build a model endpoint for the scripted agent")
	}

	st := memstore.New()
	usage := &usageRelay{log: discardLog()}
	w, err := wireAgent(ctx, openStacks, newEndpoint, wireAgentInput{
		cfg:        cfg,
		log:        discardLog(),
		own:        &owners.Owners{},
		st:         st,
		spendStore: st,
		spend:      spendSettings{},
		usage:      usage,
		hook:       &roomClosedHook{},
		fakeAgent:  on,
		pmMode:     config.PostmortemModelAuto,
		writeOn:    false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.stacks.Close()
	if endpointBuilt {
		t.Fatal("the scripted agent must not build a model endpoint")
	}
	if !stacksOpened {
		t.Fatal("the scripted agent must keep the real stack wiring (openStacks was not called)")
	}

	// The same composition as runWith outside --demo: no process-wide actions
	// (the per-pipeline sets serve them), the scripted model paths, triage off.
	actionCatalogue := func() []actions.Action {
		var out []actions.Action
		for _, s := range w.stacks.All() {
			out = append(out, s.Actions.All()...)
		}
		return out
	}
	authSettings, err := authSetup(cfg, false, false, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	svc := newRoomService(room.Deps{
		Store:             st,
		Tools:             tools.NewRegistry(),
		StackFor:          w.stackFor,
		ActionCatalogue:   actionCatalogue,
		Topology:          w.topo,
		Users:             room.DefaultUsers(),
		Owners:            &owners.Owners{},
		Run:               w.run,
		WriteHandoff:      w.write,
		WritePostmortem:   w.writePM,
		Triage:            w.triage,
		TriagePassTimeout: w.triageTimeout,
		Options:           agentOptions(cfg, config.Limits{}),
		Logger:            discardLog(),
		WriteModeOff:      true,
	}, authSettings, usage)
	api := httpapi.New(svc, st, discardLog())
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})

	// The alert opens the room through the same entry point as production.
	body := strings.NewReader(`{"title":"orders lag","severity":"sev2","pipeline_id":"orders"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/alerts/webhook", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User", "arjun")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("alert webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("alert webhook: want 201, got %d", resp.StatusCode)
	}
	var snap struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil || snap.ID == "" {
		t.Fatalf("alert webhook room id = %q (err %v)", snap.ID, err)
	}

	// The first scripted tool result arrives after a few of demo.Run's pauses;
	// poll the room API until the demo's connect_status step is in the log.
	got, err := waitForEvent(ctx, srv.URL, snap.ID, 15*time.Second, func(e events.Event) bool {
		if e.Type != events.AgentToolResult {
			return false
		}
		p, err := events.Decode[events.AgentToolResultPayload](e)
		return err == nil && p.Tool == "connect_status" && strings.Contains(p.Summary, "pg-orders-connector")
	})
	if err != nil {
		t.Fatal(err)
	}
	// It is the demo's step, not a real tool call: step s_1 and the fixture
	// summary, emitted as the agent actor.
	p, err := events.Decode[events.AgentToolResultPayload](got)
	if err != nil {
		t.Fatal(err)
	}
	if p.StepID != "s_1" || p.IsError || got.Actor.Kind != events.ActorAgent {
		t.Fatalf("tool result %+v (actor %+v), want the scripted s_1 from the agent", p, got.Actor)
	}
}

// waitForEvent polls GET /api/rooms/{id}/events until match accepts an event,
// and returns it. It fails with the room log when the deadline passes, so a
// regression says what the scripted agent did instead.
func waitForEvent(ctx context.Context, base, roomID string, d time.Duration, match func(events.Event) bool) (events.Event, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(d)
	var seen []string
	url := base + "/api/rooms/" + roomID + "/events?after=0&limit=500"
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return events.Event{}, err
		}
		req.Header.Set("X-User", "arjun")
		resp, err := client.Do(req)
		if err != nil {
			return events.Event{}, fmt.Errorf("get room events: %w", err)
		}
		var body struct {
			Events []events.Event `json:"events"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil {
			return events.Event{}, fmt.Errorf("decode room events (HTTP %d): %w", resp.StatusCode, err)
		}
		seen = seen[:0]
		for _, e := range body.Events {
			if match(e) {
				return e, nil
			}
			seen = append(seen, string(e.Type))
		}
		select {
		case <-ctx.Done():
			return events.Event{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return events.Event{}, fmt.Errorf("no matching event within %s; room log holds %v", d, seen)
}
