package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/wiring"
)

// A standalone or quickstart server whose model does not build starts in the
// no model state: the agent paths answer errNoModel until the picker installs
// an endpoint, and then use it.
func TestWireLiveNoModelThenPick(t *testing.T) {
	log, logs := testLog()
	openStacks := func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error) {
		opts.Only = []string{}
		return wiring.OpenStacks(ctx, cfg, awsCfg, own, opts)
	}
	fail := true
	newEndpoint := func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error) {
		if fail {
			return nil, errors.New("agent: PIPEROOM_MODEL_PROVIDER=openai with flavor groq needs an API key: set GROQ_API_KEY")
		}
		return wiring.NewAgentEndpoint(ctx, cfg, log, nil)
	}
	live := &liveModel{}
	hook := &roomClosedHook{}
	w, err := wireAgent(context.Background(), openStacks, newEndpoint, wireAgentInput{
		cfg: config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}, log: log, own: &owners.Owners{},
		st: memstore.New(), spendStore: memstore.New(), usage: &usageRelay{log: discardLog()}, hook: hook,
		pmMode: config.PostmortemModelAuto, live: live,
	})
	if err != nil {
		t.Fatalf("a model that does not build must not stop a quickstart start: %v", err)
	}
	defer w.stacks.Close()
	if live.ready() || !strings.Contains(logs.String(), "no usable model") {
		t.Fatalf("ready = %v, log:\n%s", live.ready(), logs.String())
	}
	if err := w.run(context.Background(), agent.Alert{}, agent.Deps{}, agent.Options{}); !errors.Is(err, errNoModel) {
		t.Fatalf("run = %v, want errNoModel", err)
	}
	if _, err := w.write(context.Background(), nil, agent.Options{}); !errors.Is(err, errNoModel) {
		t.Fatalf("write = %v", err)
	}
	if _, err := w.triage(context.Background(), agent.Alert{}, agent.Deps{}, agent.Options{}); !errors.Is(err, errNoModel) {
		t.Fatalf("triage = %v", err)
	}
	hook.onClosed(context.Background(), "r1", "p", "") // no endpoint: nothing to forget, no panic
	fail = false
	commit, _, err := live.build(context.Background(), config.Config{ModelProvider: "anthropic", Model: "claude-sonnet-5"})
	if err != nil {
		t.Fatal(err)
	}
	if live.ready() {
		t.Fatal("build installed before commit")
	}
	commit()
	if c := live.cur.Load(); c == nil || c.model != "claude-sonnet-5" {
		t.Fatalf("installed = %+v", c)
	}
}

func TestModelsOption(t *testing.T) {
	users := room.DefaultUsers()
	if o, err := modelsOption(serveMode{demo: true}, config.Config{}, nil, nil, "", users); o != nil || err != nil {
		t.Fatalf("--demo = %v, %v; want no model routes", o != nil, err)
	}
	o, err := modelsOption(serveMode{}, config.Config{}, nil, nil, "", users)
	if err != nil || o == nil {
		t.Fatal(err)
	}
	tm := teamModels{cfg: config.Config{Model: "claude-opus-5-5"}}
	if tm.Editable() || tm.SoloUser() != "" || tm.Panel().Mode != httpapi.ModelModeTeam {
		t.Fatalf("team panel = %+v", tm.Panel())
	}
}

// The board's done-when: a quickstart server with no usable model starts and
// serves the picker data, and its PUT keeps the Origin rule.
func TestQuickstartNoModelServesPicker(t *testing.T) {
	ownersPath := writeOwnersFile(t, "user:"+quickstartOwner)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := quickstartCmd(ctx, t, "^TestQuickstartHelper$", ownersPath, tmp,
		"PIPEROOM_MODEL_PROVIDER=openai", "PIPEROOM_MODEL_FLAVOR=groq", "PIPEROOM_MODEL=openai/gpt-oss-120b",
		"GROQ_API_KEY=", config.EnvModelAPIKey+"=", config.EnvModelAPIKeyFile+"=")
	base, logs, stop := startQuickstart(t, cmd)
	defer stop()
	base = strings.TrimSuffix(base, "/")
	client := &http.Client{Timeout: 5 * time.Second}
	do := func(method, origin, body string) (int, []byte) {
		req, err := http.NewRequestWithContext(ctx, method, base+"/api/models", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-User", quickstartOwner)
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s /api/models: %v\n%s", method, err, logs.String())
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, b
	}
	code, body := do(http.MethodGet, "", "")
	var p httpapi.ModelPanel
	if code != http.StatusOK || json.Unmarshal(body, &p) != nil {
		t.Fatalf("GET = %d %s\n%s", code, body, logs.String())
	}
	if p.Mode != httpapi.ModelModeQuickstart || p.State != httpapi.ModelStateNoModel || p.Supported == nil ||
		len(p.Roles) == 0 || p.Roles[0].Provider != "openai" || p.Roles[0].KeySet {
		t.Fatalf("panel = %s", body)
	}
	if code, body := do(http.MethodPut, "http://evil.example", `{"provider":"openai","model":"x","key":"k"}`); code != http.StatusForbidden || !strings.Contains(string(body), `"origin"`) {
		t.Fatalf("foreign PUT = %d %s", code, body)
	}
	if !strings.Contains(logs.String(), "no usable model") {
		t.Fatalf("the log lacks the no model note:\n%s", logs.String())
	}
}

// An endpoint the picker builds lives on the server's context, not the PUT's:
// Vertex keeps the build context for later token refreshes. The request
// bounds the build only; a replaced endpoint's context ends once its last
// in-flight call returns; the server's end ends the current one.
func TestLiveEndpointOutlivesTheRequest(t *testing.T) {
	srvCtx, stopSrv := context.WithCancel(context.Background())
	defer stopSrv()
	var kept []context.Context
	var cancelDuringBuild context.CancelFunc
	newEndpoint := func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error) {
		kept = append(kept, ctx)
		if cancelDuringBuild != nil {
			cancelDuringBuild()
		}
		return wiring.NewAgentEndpoint(ctx, cfg, log, nil)
	}
	live := &liveModel{}
	in := wireAgentInput{cfg: config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}, log: discardLog(),
		spendStore: memstore.New(), usage: &usageRelay{log: discardLog()}, hook: &roomClosedHook{}, live: live}
	if _, err := wireLive(srvCtx, live, in, nil, newEndpoint); err != nil {
		t.Fatal(err)
	}
	start := kept[0]

	build := func() context.Context {
		t.Helper()
		reqCtx, endReq := context.WithCancel(context.Background())
		commit, _, err := live.build(reqCtx, config.Config{ModelProvider: "anthropic", Model: "claude-sonnet-5"})
		if err != nil {
			t.Fatal(err)
		}
		commit()
		endReq() // the PUT's response is sent
		return kept[len(kept)-1]
	}

	// A run is in flight on the start endpoint while the picker swaps it.
	inflight := live.acquire()
	first := build()
	if first.Err() != nil {
		t.Fatal("the endpoint's context ended with the request")
	}
	if start.Err() != nil {
		t.Fatal("the replaced endpoint's context ended under an in-flight run")
	}
	inflight.release()
	if start.Err() == nil {
		t.Fatal("the replaced endpoint's context outlived its last call")
	}

	// No call in flight: the replaced endpoint's context ends at once.
	second := build()
	if first.Err() == nil || second.Err() != nil {
		t.Fatalf("first %v, second %v", first.Err(), second.Err())
	}

	// A request that ends during the build fails it, and its context ends.
	reqCtx, endReq := context.WithCancel(context.Background())
	cancelDuringBuild = endReq
	if _, _, err := live.build(reqCtx, config.Config{ModelProvider: "anthropic", Model: "claude-sonnet-5"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("build = %v, want context.Canceled", err)
	}
	if kept[len(kept)-1].Err() == nil {
		t.Fatal("a failed build's context stayed alive")
	}
	if live.cur.Load().model != "claude-sonnet-5" || second.Err() != nil {
		t.Fatal("a failed build changed the current endpoint")
	}

	stopSrv()
	if second.Err() == nil {
		t.Fatal("the server's end did not end the current endpoint's context")
	}
}
