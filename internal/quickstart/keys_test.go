package quickstart

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/httpapi"
)

var pickerKeyVars = []string{config.EnvModelAPIKey, config.EnvModelAPIKeyFile, "ANTHROPIC_API_KEY"}

// V18 MP1: on the process environment (Env nil) the picker's keys stay in
// its overlay: the build reads them through getenv, os.Getenv never sees
// them, after the start or after a PUT.
func TestPickerKeysNeverEnterTheProcessEnvironment(t *testing.T) {
	for _, v := range pickerKeyVars {
		t.Setenv(v, "")
		if err := os.Unsetenv(v); err != nil {
			t.Fatal(err)
		}
	}
	a, _, _ := hostCounter(t)
	var seen []string
	m := NewModels(ModelsOptions{
		Mode: httpapi.ModelModeStandalone, ProjectDir: t.TempDir(), Ready: true, SoloUser: "solo",
		Config: config.Config{ModelProvider: "openai", ModelFlavor: "vllm", ModelBaseURL: a.URL + "/v1", Model: "qwen3:32b", ModelUnverified: "on"},
		Stored: ModelEnv{KeyVar: config.EnvModelAPIKey, Key: testKey},
		Build: func(_ context.Context, _ config.Config, getenv func(string) string) (func(), func(), error) {
			seen = append(seen, getenv(config.EnvModelAPIKey)+"|"+getenv("ANTHROPIC_API_KEY"))
			return func() {}, func() {}, nil
		},
	})
	check := func(when string) {
		t.Helper()
		for _, v := range pickerKeyVars {
			if _, ok := os.LookupEnv(v); ok {
				t.Fatalf("%s: %s is in the process environment", when, v)
			}
		}
	}
	if p := m.Panel(); !p.Roles[0].KeySet {
		t.Fatalf("the stored key is not seen by the panel: %+v", p.Roles[0])
	}
	check("after the start")
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "qwen3:32b", Key: ptr("sk-put-1")}); err != nil {
		t.Fatal(err)
	}
	check("after a vllm PUT")
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "anthropic", Model: "claude-opus-5-5", Key: ptr("sk-ant-put-2")}); err != nil {
		t.Fatal(err)
	}
	check("after an anthropic PUT")
	// The second build: the vllm key is gone (no role reads it) and the
	// Anthropic key is the PUT's.
	if want := []string{"sk-put-1|", "|sk-ant-put-2"}; strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("builds read %q, want %q", seen, want)
	}
}

// A failed build leaves the overlay as it was (the restore).
func TestKeysFailedBuildKeepsOverlay(t *testing.T) {
	a, _, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	before := p.m.keys
	p.buildErr = fmt.Errorf("agent: refused")
	if _, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "anthropic", Model: "claude-opus-5-5", Key: ptr("sk-ant-x")}); err == nil {
		t.Fatal("Apply built")
	}
	if keyGet(p.m, config.EnvModelAPIKey) != testKey || keyGet(p.m, "ANTHROPIC_API_KEY") != "" || before.Getenv(config.EnvModelAPIKey) != testKey {
		t.Fatal("a failed build changed the keys")
	}
}

// The overlay never prints a key under fmt or slog.
func TestKeysRedacted(t *testing.T) {
	k := NewKeys(&mapEnv{m: map[string]string{}}, ModelEnv{KeyVar: "ANTHROPIC_API_KEY", Key: "sk-ant-secret-5555"})
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("keys", "k", k)
	for _, s := range []string{fmt.Sprint(k), fmt.Sprintf("%+v", k), fmt.Sprintf("%#v", k), fmt.Sprintf("%v", &k), buf.String()} {
		if strings.Contains(s, "sk-ant-secret") {
			t.Fatalf("leaks: %s", s)
		}
	}
}
