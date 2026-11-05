package main

import (
	"bytes"
	"strings"
	"testing"

	"piperoom/internal/config"
)

// notAKey is a fake value; tests check it never appears in an error.
const notAKey = "sk-test-NOT-A-REAL-KEY-7f3a"

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func noFiles(string) bool { return false }

func TestCheckModelKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.Config
		env  map[string]string
		want string // "" = no error; else a substring of the error
	}{
		{"anthropic default, no key", config.Config{}, nil, "ANTHROPIC_API_KEY is not set (use -check"},
		{"anthropic default, key", config.Config{}, map[string]string{"ANTHROPIC_API_KEY": notAKey}, ""},
		{"anthropic, the model key does not count", config.Config{ModelProvider: "anthropic"}, map[string]string{config.EnvModelAPIKey: notAKey}, "ANTHROPIC_API_KEY"},
		{"bedrock: its own chain", config.Config{ModelProvider: "bedrock", AWSRegion: "us-east-1"}, nil, ""},
		{"vertex: its own chain", config.Config{ModelProvider: "vertex", VertexProject: "p", VertexRegion: "us-east5"}, nil, ""},

		{"openai, no key", config.Config{ModelProvider: "openai", Model: "gpt-4.1"}, nil, "OPENAI_API_KEY"},
		{"openai, standard key", config.Config{ModelProvider: "openai", Model: "gpt-4.1"}, map[string]string{"OPENAI_API_KEY": notAKey}, ""},
		{"openai, main key", config.Config{ModelProvider: "openai", Model: "gpt-4.1"}, map[string]string{config.EnvModelAPIKey: notAKey}, ""},
		{"openai, main key file", config.Config{ModelProvider: "openai", Model: "gpt-4.1"}, map[string]string{config.EnvModelAPIKeyFile: "/run/secrets/k"}, ""},
		{"openai, the anthropic key does not count", config.Config{ModelProvider: "openai", Model: "gpt-4.1"}, map[string]string{"ANTHROPIC_API_KEY": notAKey}, "OPENAI_API_KEY"},
		{"deepseek, no key", config.Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat", ModelAckCNEndpoint: "yes"}, map[string]string{"OPENAI_API_KEY": notAKey}, "DEEPSEEK_API_KEY"},
		{"deepseek, key", config.Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat", ModelAckCNEndpoint: "yes"}, map[string]string{"DEEPSEEK_API_KEY": notAKey}, ""},
		{"azure, no key", config.Config{ModelProvider: "openai", ModelFlavor: "azure", Model: "prod-gpt", ModelBaseURL: "https://x.openai.azure.com/openai/v1"}, nil, "AZURE_OPENAI_API_KEY"},
		{"groq, no key", config.Config{ModelProvider: "openai", ModelFlavor: "groq", Model: "openai/gpt-oss-120b"}, nil, "GROQ_API_KEY"},
		{"ollama: none", config.Config{ModelProvider: "openai", ModelFlavor: "ollama", Model: "qwen3:32b"}, nil, ""},
		{"vllm: optional", config.Config{ModelProvider: "openai", ModelFlavor: "vllm", Model: "Qwen/Qwen3-32B", ModelBaseURL: "https://vllm.internal.example/v1"}, nil, ""},

		{"gemini, no key", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro"}, nil, "GEMINI_API_KEY"},
		{"gemini, key", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro"}, map[string]string{"GEMINI_API_KEY": notAKey}, ""},
		{"gemini on vertex, no project", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro", GeminiBackend: "vertex"}, nil, "VERTEX_PROJECT_ID"},
		{"gemini on vertex", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro", GeminiBackend: "vertex", VertexProject: "p", VertexRegion: "global"}, nil, ""},

		{"converse, no region", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0"}, map[string]string{"AWS_ACCESS_KEY_ID": "AKIDTEST"}, "AWS_REGION"},
		{"converse, no credentials", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}, nil, "AWS_BEARER_TOKEN_BEDROCK"},
		{"converse, access key", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}, map[string]string{"AWS_ACCESS_KEY_ID": "AKIDTEST"}, ""},
		{"converse, bearer token", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}, map[string]string{"AWS_BEARER_TOKEN_BEDROCK": notAKey}, ""},
		{"converse, shared file", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}, map[string]string{"AWS_SHARED_CREDENTIALS_FILE": "/creds"}, ""},

		// A role on another provider needs its own key, never the main one's.
		{"summary on openai under anthropic", config.Config{SummaryProvider: "openai", SummaryModel: "gpt-4.1"},
			map[string]string{"ANTHROPIC_API_KEY": notAKey, config.EnvModelAPIKey: notAKey}, "PIPEROOM_SUMMARY_PROVIDER=openai with flavor openai: OPENAI_API_KEY is not set"},
		{"judge on anthropic under gemini", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro", JudgeProvider: "anthropic", JudgeModel: "claude-opus-5-5"},
			map[string]string{"GEMINI_API_KEY": notAKey}, "PIPEROOM_JUDGE_PROVIDER=anthropic: ANTHROPIC_API_KEY"},
		{"judge on anthropic, auth token", config.Config{ModelProvider: "gemini", Model: "gemini-2.5-pro", JudgeProvider: "anthropic", JudgeModel: "claude-opus-5-5"},
			map[string]string{"GEMINI_API_KEY": notAKey, "ANTHROPIC_AUTH_TOKEN": notAKey}, ""},
		{"first look off is not checked", config.Config{TriagePass: "off", TriagePassProvider: "gemini", TriagePassModel: "gemini-2.5-flash"},
			map[string]string{"ANTHROPIC_API_KEY": notAKey}, ""},
		{"first look on is checked", config.Config{TriagePassProvider: "gemini", TriagePassModel: "gemini-2.5-flash"},
			map[string]string{"ANTHROPIC_API_KEY": notAKey}, "PIPEROOM_TRIAGE_PASS_PROVIDER=gemini: GEMINI_API_KEY is not set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkModelKeys(tc.cfg, envOf(tc.env), func(p string) bool { return p == "/creds" })
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("err = %v, want %q", err, tc.want)
			case err != nil && strings.Contains(err.Error(), notAKey):
				t.Fatalf("the error carries a key value: %v", err)
			}
		})
	}
}

// The check runs before the first scenario: a run with a missing provider
// key prints no run line, writes no report and names the variable.
func TestRunRefusesMissingProviderKeyBeforeAnyScenario(t *testing.T) {
	for _, tc := range []struct {
		f    flags
		env  map[string]string
		want string
	}{
		{flags{provider: "openai", model: "gpt-4.1"}, nil, "OPENAI_API_KEY"},
		{flags{provider: "openai", flavor: "mistral", model: "mistral-large-latest"}, map[string]string{"OPENAI_API_KEY": notAKey}, "MISTRAL_API_KEY"},
		{flags{provider: "gemini", model: "gemini-2.5-pro"}, nil, "GEMINI_API_KEY"},
		{flags{provider: "bedrock-converse", model: "amazon.nova-pro-v1:0"}, map[string]string{"AWS_REGION": "us-east-1"}, "AWS_BEARER_TOKEN_BEDROCK"},
		{flags{provider: "anthropic"}, nil, "ANTHROPIC_API_KEY is not set"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			for _, v := range []string{
				"PIPEROOM_MODEL_PROVIDER", "PIPEROOM_MODEL_FLAVOR", "PIPEROOM_MODEL", config.EnvModelAPIKey, config.EnvModelAPIKeyFile,
				"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY", "MISTRAL_API_KEY", "GEMINI_API_KEY", "PIPEROOM_GEMINI_BACKEND",
				"AWS_REGION", "AWS_BEARER_TOKEN_BEDROCK", "AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE",
				"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
				"PIPEROOM_SUMMARY_PROVIDER", "PIPEROOM_JUDGE_PROVIDER", "PIPEROOM_TRIAGE_PASS_PROVIDER",
			} {
				t.Setenv(v, "")
			}
			// No shared AWS files from the machine running the test.
			t.Setenv("HOME", t.TempDir())
			t.Setenv("USERPROFILE", t.TempDir())
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")
			t.Setenv("AWS_CONFIG_FILE", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			out := t.TempDir() + "/report.json"
			f := tc.f
			f.scenarios, f.runs, f.out = "../../scenarios", 1, out
			var stdout, stderr bytes.Buffer
			code, err := run(f, &stdout, &stderr)
			if code != 1 || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("code %d err %v, want %q", code, err, tc.want)
			}
			if strings.Contains(err.Error(), notAKey) {
				t.Fatalf("the error carries a key value: %v", err)
			}
			if strings.Contains(stderr.String(), " #1: ") || stdout.Len() > 0 {
				t.Fatalf("work ran before the key check: stdout %q stderr %q", stdout.String(), stderr.String())
			}
			if fileExists(out) {
				t.Fatal("a report was written")
			}
		})
	}
}
