package config

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"
)

// v0Vars are every variable V0 added; the tests clear them so the host
// environment cannot leak in.
var v0Vars = []string{
	"PIPEROOM_MODEL_FLAVOR", "PIPEROOM_MODEL_BASE_URL", "PIPEROOM_MODEL_CONTEXT_TOKENS", "PIPEROOM_MODEL_CAPS",
	"PIPEROOM_MODEL_UNVERIFIED", "PIPEROOM_MODEL_MAX_CONCURRENCY", "PIPEROOM_MODEL_TPM", "PIPEROOM_MODEL_PROXY",
	"PIPEROOM_GEMINI_BACKEND", "PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP", "PIPEROOM_MODEL_ACK_CN_ENDPOINT",
	"PIPEROOM_MODEL_PRICING_STRICT", "PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS", "PIPEROOM_MODEL_PROMPT_VARIANT",
	"PIPEROOM_POSTMORTEM_MODE", "PIPEROOM_TRIAGE_PASS_PROVIDER", "PIPEROOM_SUMMARY_PROVIDER", "PIPEROOM_SUMMARY_MODEL",
	"PIPEROOM_JUDGE_PROVIDER", "PIPEROOM_JUDGE_MODEL", "PIPEROOM_TRIAGE_PASS_BASE_URL", "PIPEROOM_SUMMARY_BASE_URL",
	"PIPEROOM_JUDGE_BASE_URL",
}

func clearModelEnv(t *testing.T) {
	t.Helper()
	for _, k := range append([]string{
		"PIPEROOM_MODEL", "PIPEROOM_MODEL_PROVIDER", "PIPEROOM_TRIAGE_MODEL", "PIPEROOM_TRIAGE_PASS_MODEL",
		"PIPEROOM_POSTMORTEM_MODEL", "PIPEROOM_PIPELINE_DAILY_COST_USD",
	}, v0Vars...) {
		t.Setenv(k, "")
	}
}

func TestParseBaseURL(t *testing.T) {
	for _, tc := range []struct {
		in          string
		private     bool
		ok          bool
		privateHTTP bool
		host        string
		why         string // part of the error
	}{
		{in: "https://api.openai.com/v1", ok: true, host: "api.openai.com"},
		{in: "HTTPS://Res.OpenAI.Azure.com/openai/v1/", ok: true, host: "res.openai.azure.com"},
		{in: "https://10.0.0.5:8443/v1", ok: true, host: "10.0.0.5"},
		{in: "http://localhost:11434/v1", ok: true, host: "localhost"},
		{in: "http://127.0.0.1:8000/v1", ok: true, host: "127.0.0.1"},
		{in: "http://127.9.9.9/v1", ok: true, host: "127.9.9.9"},
		{in: "http://[::1]:8000/v1", ok: true, host: "::1"},
		{in: "http://10.1.2.3:8000/v1", why: "plain http"},
		{in: "http://vllm.svc:8000/v1", why: "plain http"},
		{in: "http://api.openai.com/v1", why: "plain http"},
		{in: "http://10.1.2.3:8000/v1", private: true, ok: true, privateHTTP: true, host: "10.1.2.3"},
		{in: "http://172.16.0.9/v1", private: true, ok: true, privateHTTP: true},
		{in: "http://192.168.1.10/v1", private: true, ok: true, privateHTTP: true},
		{in: "http://[fd12:3456::1]/v1", private: true, ok: true, privateHTTP: true},
		{in: "http://vllm.svc:8000/v1", private: true, ok: true, privateHTTP: true},
		{in: "http://localhost:11434/v1", private: true, ok: true},
		{in: "http://8.8.8.8/v1", private: true, why: "plain http only to loopback"},
		{in: "http://100.64.0.1/v1", private: true, why: "plain http only to loopback"},
		{in: "https://user:pw@api.openai.com/v1", why: "user info"},
		{in: "https://key@api.openai.com/v1", why: "user info"},
		{in: "https://api.openai.com/v1?api-version=1", why: "query"},
		{in: "https://api.openai.com/v1?", why: "query"},
		{in: "https://api.openai.com/v1#frag", why: "fragment"},
		{in: "ftp://api.openai.com/v1", why: "want an https"},
		{in: "api.openai.com/v1", why: "want an https"},
		{in: "https:///v1", why: "want an https"},
		{in: "", why: "empty"},
		{in: "https://api.openai.com:99999/v1", why: "port"},
		// The metadata deny list: always, whatever the scheme or the knob.
		{in: "http://169.254.169.254/latest", why: "metadata"},
		{in: "http://169.254.169.254/latest", private: true, why: "metadata"},
		{in: "https://169.254.169.254/", private: true, why: "metadata"},
		{in: "https://169.254.170.2/v2", why: "metadata"},
		{in: "https://[fd00:ec2::254]/", private: true, why: "metadata"},
		{in: "http://[fd00:ec2::254]/", private: true, why: "metadata"},
		{in: "https://[::ffff:169.254.169.254]/", private: true, why: "metadata"},
		{in: "https://[64:ff9b::a9fe:a9fe]/", why: "metadata"},
		{in: "https://[fe80::1]/", private: true, why: "metadata"},
		{in: "https://100.100.100.200/", private: true, why: "metadata"},
		{in: "https://metadata.google.internal/computeMetadata/v1", private: true, why: "metadata host"},
		{in: "https://METADATA.google.internal./", why: "metadata host"},
		{in: "http://metadata/", private: true, why: "metadata host"},
		{in: "https://0.0.0.0/", private: true, why: "metadata"},
		{in: "https://[::]/", why: "metadata"},
		{in: "https://224.0.0.1/", why: "metadata"},
		{in: "https://255.255.255.255/", why: "metadata"},
		{in: "https://2852039166/", private: true, why: "dotted decimal"},
		{in: "https://0xA9FEA9FE/", private: true, why: "dotted decimal"},
		{in: "https://0251.0376.0251.0376/", private: true, why: "dotted decimal"},
		{in: "https://169.254.43518/", why: "dotted decimal"},
	} {
		b, err := ParseBaseURL("PIPEROOM_MODEL_BASE_URL", tc.in, tc.private)
		if tc.ok {
			if err != nil {
				t.Errorf("%q (private %v): %v", tc.in, tc.private, err)
				continue
			}
			if b.PrivateHTTP != tc.privateHTTP || (tc.host != "" && b.Host != tc.host) {
				t.Errorf("%q: got %+v host %q", tc.in, b, b.Host)
			}
			if b.URL.RawQuery != "" || b.URL.Fragment != "" || b.URL.User != nil {
				t.Errorf("%q: URL kept more than scheme, host, path: %v", tc.in, b.URL)
			}
			continue
		}
		if err == nil {
			t.Errorf("%q (private %v): accepted %+v, want refused (%s)", tc.in, tc.private, b, tc.why)
			continue
		}
		if !strings.Contains(err.Error(), "PIPEROOM_MODEL_BASE_URL") || !strings.Contains(err.Error(), tc.why) {
			t.Errorf("%q: err = %v, want the var and %q", tc.in, err, tc.why)
		}
		if strings.Contains(tc.in, "pw@") && strings.Contains(err.Error(), "pw") {
			t.Errorf("%q: the error quotes the value: %v", tc.in, err)
		}
	}
}

func TestParseBaseURLKeepsPath(t *testing.T) {
	b, err := ParseBaseURL("X", "https://res.openai.azure.com/openai/v1", false)
	if err != nil || b.String() != "https://res.openai.azure.com/openai/v1" {
		t.Fatalf("got %q, %v", b.String(), err)
	}
	if got := b.Redacted(); got != "https://res.openai.azure.com" {
		t.Fatalf("Redacted = %q", got)
	}
}

func TestModelAddrLists(t *testing.T) {
	for _, s := range []string{"169.254.169.254", "169.254.170.2", "fd00:ec2::254", "100.100.100.200", "fe80::1", "::ffff:169.254.169.254", "0.0.0.0", "::", "ff02::1", "255.255.255.255", "0.1.2.3",
		"::169.254.169.254", "64:ff9b::a9fe:a9fe", "64:ff9b:1::a9fe:a9fe", "2002:a9fe:a9fe::1"} {
		a := netip.MustParseAddr(s)
		if !ModelAddrDenied(a) || ModelPrivateHTTPAddr(a) {
			t.Errorf("%s: denied %v, private-http %v; want denied", s, ModelAddrDenied(a), ModelPrivateHTTPAddr(a))
		}
	}
	for _, s := range []string{"127.0.0.1", "::1", "10.0.0.1", "172.31.255.255", "192.168.0.1", "fd12::1", "::ffff:10.0.0.1"} {
		if a := netip.MustParseAddr(s); !ModelPrivateHTTPAddr(a) {
			t.Errorf("%s: want private-http allowed", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "172.32.0.1", "100.64.0.1", "2001:db8::1", "2002:808:808::1", "64:ff9b:1::808:808"} {
		if a := netip.MustParseAddr(s); ModelPrivateHTTPAddr(a) || ModelAddrDenied(a) {
			t.Errorf("%s: want public (not denied, not private-http)", s)
		}
	}
}

// TestParseModelRolesClaudeUnchanged: a Claude deployment that sets nothing
// new parses to today's values, and Load no longer fills the first-look model.
func TestParseModelRolesClaudeUnchanged(t *testing.T) {
	clearModelEnv(t)
	c := Load()
	if c.TriagePassModel != "" || c.PostmortemModel != "" {
		t.Fatalf("Load filled TriagePassModel=%q PostmortemModel=%q; the defaults moved to the parsers", c.TriagePassModel, c.PostmortemModel)
	}
	r, err := ParseModelRoles(c)
	if err != nil {
		t.Fatal(err)
	}
	main := ModelRole{Provider: "anthropic", Model: "claude-opus-5-5"}
	want := ModelRoles{Main: main, FirstLook: ModelRole{Provider: "anthropic", Model: "claude-haiku-5-5"}, Summary: main, Judge: main}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("roles = %+v\nwant    %+v", r, want)
	}
	if tp, err := ParseTriagePass(c); err != nil || tp.Model != "claude-haiku-5-5" {
		t.Errorf("ParseTriagePass model = %q, %v; want today's claude-haiku-5-5", tp.Model, err)
	}
	if m, err := ParsePostmortemModel(c); err != nil || m != PostmortemModelAuto {
		t.Errorf("ParsePostmortemModel = %q, %v", m, err)
	}
	k, err := ParseModelKnobs(c)
	if err != nil {
		t.Fatal(err)
	}
	if k.PricingStrict != PricingStrictNonClaude || k.NeutraliseControlTokens != NeutraliseAll || k.PostmortemMode != PostmortemModelAuto ||
		k.GeminiBackend != GeminiBackendAPI || k.AllowPrivateHTTP || k.Unverified || k.Proxy || k.AckCNEndpoint || len(k.Warnings) != 0 {
		t.Errorf("knob defaults = %+v", k)
	}

	for _, p := range []string{"bedrock", "vertex", "VERTEX"} {
		t.Setenv("PIPEROOM_MODEL_PROVIDER", p)
		t.Setenv("PIPEROOM_MODEL", "in.anthropic.claude-opus-5-5")
		t.Setenv("PIPEROOM_TRIAGE_MODEL", "in.anthropic.claude-haiku-5-5")
		r, err := ParseModelRoles(Load())
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		lp := strings.ToLower(p)
		if r.Main != (ModelRole{Provider: lp, Model: "in.anthropic.claude-opus-5-5"}) || r.Triage != "in.anthropic.claude-haiku-5-5" ||
			r.FirstLook.Model != "claude-haiku-5-5" || r.Summary != r.Main || r.Judge != r.Main {
			t.Errorf("%s: roles = %+v", p, r)
		}
	}
}

func TestParseModelRoles(t *testing.T) {
	type tc struct {
		name string
		c    Config
		want func(ModelRoles) bool
		err  string
	}
	for _, tt := range []tc{
		{name: "openai defaults", c: Config{ModelProvider: "openai", Model: "gpt-5"}, want: func(r ModelRoles) bool {
			m := ModelRole{Provider: "openai", Model: "gpt-5", Flavor: "openai"}
			return r.Main == m && r.FirstLook == m && r.Summary == m && r.Judge == m
		}},
		{name: "first look keeps its model on a new provider", c: Config{ModelProvider: "gemini", Model: "gemini-3-pro", TriagePassModel: "gemini-3-flash"}, want: func(r ModelRoles) bool {
			return r.FirstLook.Model == "gemini-3-flash" && r.FirstLook.Provider == "gemini" && r.Summary.Model == "gemini-3-pro"
		}},
		{name: "first look on claude from a new main", c: Config{ModelProvider: "openai", Model: "gpt-5", TriagePassProvider: "anthropic"}, want: func(r ModelRoles) bool {
			return r.FirstLook == ModelRole{Provider: "anthropic", Model: "claude-haiku-5-5"}
		}},
		{name: "ollama flavor default base", c: Config{ModelProvider: "openai", ModelFlavor: "Ollama", Model: "qwen3"}, want: func(r ModelRoles) bool {
			return r.Main.Flavor == "ollama" && r.Main.BaseURL == nil && FlavorDefaultBaseURL("ollama") == "http://localhost:11434/v1"
		}},
		{name: "role on another provider with its base", c: Config{Model: "claude-opus-5-5", JudgeProvider: "openai", JudgeModel: "gpt-5", JudgeBaseURL: "https://example.test/v1"}, want: func(r ModelRoles) bool {
			return r.Main.Provider == "anthropic" && r.Judge.Provider == "openai" && r.Judge.Flavor == "openai" && r.Judge.BaseURL != nil && r.Judge.BaseURL.Host == "example.test"
		}},
		{name: "same-provider role shares main endpoint", c: Config{ModelProvider: "openai", ModelFlavor: "azure", ModelBaseURL: "https://r.openai.azure.com/openai/v1", Model: "dep1", SummaryModel: "dep2"}, want: func(r ModelRoles) bool {
			return r.Summary.Model == "dep2" && r.Summary.BaseURL == r.Main.BaseURL && r.Summary.Flavor == "azure"
		}},
		{name: "unknown provider", c: Config{ModelProvider: "cohere"}, err: "PIPEROOM_MODEL_PROVIDER"},
		{name: "unknown role provider", c: Config{SummaryProvider: "x"}, err: "PIPEROOM_SUMMARY_PROVIDER"},
		{name: "unknown flavor", c: Config{ModelProvider: "openai", ModelFlavor: "bard"}, err: "PIPEROOM_MODEL_FLAVOR"},
		{name: "flavor on claude", c: Config{ModelFlavor: "groq"}, err: "PIPEROOM_MODEL_FLAVOR is for provider openai only"},
		{name: "azure needs base", c: Config{ModelProvider: "openai", ModelFlavor: "azure", Model: "d"}, err: "needs a base URL"},
		{name: "generic needs base", c: Config{ModelProvider: "openai", ModelFlavor: "generic", Model: "d"}, err: "needs a base URL"},
		{name: "base url on claude", c: Config{ModelBaseURL: "https://api.anthropic.com"}, err: "PIPEROOM_MODEL_BASE_URL is for providers openai and gemini only"},
		{name: "role base same provider", c: Config{ModelProvider: "openai", Model: "m", SummaryBaseURL: "https://x.test/v1"}, err: "PIPEROOM_SUMMARY_BASE_URL is only for a role whose provider differs"},
		{name: "role other provider needs model", c: Config{Model: "claude-opus-5-5", SummaryProvider: "gemini"}, err: "PIPEROOM_SUMMARY_MODEL is required"},
		{name: "first look new provider needs model", c: Config{Model: "claude-opus-5-5", TriagePassProvider: "openai"}, err: "PIPEROOM_TRIAGE_PASS_MODEL is required"},
		{name: "model with space", c: Config{Model: "claude opus"}, err: "PIPEROOM_MODEL="},
		{name: "bad main base", c: Config{ModelProvider: "openai", Model: "m", ModelBaseURL: "http://10.0.0.1/v1"}, err: "plain http"},
		{name: "private http knob", c: Config{ModelProvider: "openai", ModelFlavor: "vllm", Model: "m", ModelBaseURL: "http://10.0.0.1/v1", ModelAllowPrivateHTTP: "on"}, want: func(r ModelRoles) bool {
			return r.Main.BaseURL != nil && r.Main.BaseURL.PrivateHTTP
		}},
		{name: "private http knob bad", c: Config{ModelAllowPrivateHTTP: "yes"}, err: "PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP"},
	} {
		r, err := ParseModelRoles(tt.c)
		if tt.err != "" {
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("%s: err = %v, want %q", tt.name, err, tt.err)
			}
			continue
		}
		if err != nil || !tt.want(r) {
			t.Errorf("%s: %+v, %v", tt.name, r, err)
		}
	}
}

func TestParseModelKnobs(t *testing.T) {
	for _, tt := range []struct {
		name string
		c    Config
		err  string
		want func(ModelKnobs) bool
	}{
		{name: "pricing off with cost cap refuses", c: Config{ModelPricingStrict: "off", PipelineDailyCostUSD: "25", ModelPrices: "p.json"}, err: "PIPEROOM_MODEL_PRICING_STRICT=off with PIPEROOM_PIPELINE_DAILY_COST_USD"},
		{name: "pricing off without cap", c: Config{ModelPricingStrict: "OFF"}, want: func(k ModelKnobs) bool { return k.PricingStrict == PricingStrictOff }},
		{name: "pricing off with cap 0", c: Config{ModelPricingStrict: "off", PipelineDailyCostUSD: "0"}, want: func(k ModelKnobs) bool { return k.PricingStrict == PricingStrictOff }},
		{name: "pricing all", c: Config{ModelPricingStrict: "all", PipelineDailyCostUSD: "25"}, want: func(k ModelKnobs) bool { return k.PricingStrict == PricingStrictAll }},
		{name: "pricing bad", c: Config{ModelPricingStrict: "some"}, err: "PIPEROOM_MODEL_PRICING_STRICT"},
		{name: "neutralise non-claude", c: Config{ModelNeutraliseControlTokens: "non-claude"}, want: func(k ModelKnobs) bool { return k.NeutraliseControlTokens == NeutraliseNonClaude }},
		{name: "neutralise all", c: Config{ModelNeutraliseControlTokens: "ALL"}, want: func(k ModelKnobs) bool { return k.NeutraliseControlTokens == NeutraliseAll }},
		{name: "neutralise off refused", c: Config{ModelNeutraliseControlTokens: "off"}, err: "there is no off"},
		{name: "neutralise none refused", c: Config{ModelNeutraliseControlTokens: "none"}, err: "PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS"},
		{name: "prompt variant refused", c: Config{ModelPromptVariant: "openai-v1"}, err: "reserved, not implemented"},
		{name: "prompt variant blank refused", c: Config{ModelPromptVariant: " "}, err: "reserved, not implemented"},
		{name: "deepseek flavor needs ack", c: Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat"}, err: "PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes"},
		{name: "deepseek flavor with ack", c: Config{ModelProvider: "openai", ModelFlavor: "deepseek", Model: "deepseek-chat", ModelAckCNEndpoint: "yes"}, want: func(k ModelKnobs) bool { return k.AckCNEndpoint }},
		{name: "dashscope china needs ack", c: Config{ModelProvider: "openai", ModelFlavor: "qwen", Model: "qwen3", ModelBaseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1"}, err: "People's Republic of China"},
		{name: "dashscope intl is not china", c: Config{ModelProvider: "openai", ModelFlavor: "qwen", Model: "qwen3"}, want: func(k ModelKnobs) bool { return !k.AckCNEndpoint }},
		{name: "deepseek on a role needs ack", c: Config{JudgeProvider: "openai", JudgeModel: "deepseek-chat", JudgeBaseURL: "https://api.deepseek.com/v1"}, err: "PIPEROOM_JUDGE"},
		{name: ".cn host needs ack", c: Config{ModelProvider: "gemini", Model: "g", ModelBaseURL: "https://llm.example.cn/v1"}, err: "PIPEROOM_MODEL_ACK_CN_ENDPOINT"},
		{name: "ack bad value", c: Config{ModelAckCNEndpoint: "true"}, err: "PIPEROOM_MODEL_ACK_CN_ENDPOINT"},
		{name: "postmortem mode", c: Config{PostmortemMode: "Off"}, want: func(k ModelKnobs) bool { return k.PostmortemMode == PostmortemModelOff && len(k.Warnings) == 0 }},
		{name: "postmortem alias warns", c: Config{PostmortemModel: "off"}, want: func(k ModelKnobs) bool {
			return k.PostmortemMode == PostmortemModelOff && len(k.Warnings) == 1 && strings.Contains(k.Warnings[0], "PIPEROOM_POSTMORTEM_MODEL is deprecated")
		}},
		{name: "postmortem both same", c: Config{PostmortemMode: "auto", PostmortemModel: "AUTO"}, want: func(k ModelKnobs) bool { return k.PostmortemMode == PostmortemModelAuto && len(k.Warnings) == 1 }},
		{name: "postmortem both differ", c: Config{PostmortemMode: "off", PostmortemModel: "auto"}, err: "disagree"},
		{name: "postmortem mode bad", c: Config{PostmortemMode: "always"}, err: "PIPEROOM_POSTMORTEM_MODE="},
		{name: "caps", c: Config{ModelCaps: "reasoning, parallel_tools,reasoning"}, want: func(k ModelKnobs) bool { return reflect.DeepEqual(k.Caps, []string{"reasoning", "parallel_tools"}) }},
		{name: "caps cannot lower", c: Config{ModelCaps: "no_validation"}, err: "PIPEROOM_MODEL_CAPS"},
		{name: "context tokens", c: Config{ModelContextTokens: "131072"}, want: func(k ModelKnobs) bool { return k.ContextTokens == 131072 }},
		{name: "context tokens zero", c: Config{ModelContextTokens: "0"}, err: "PIPEROOM_MODEL_CONTEXT_TOKENS"},
		{name: "context tokens huge", c: Config{ModelContextTokens: "1000000000"}, err: "PIPEROOM_MODEL_CONTEXT_TOKENS"},
		{name: "concurrency", c: Config{ModelMaxConcurrency: "4", ModelTPM: "200000"}, want: func(k ModelKnobs) bool { return k.MaxConcurrency == 4 && k.TPM == 200000 }},
		{name: "concurrency bad", c: Config{ModelMaxConcurrency: "-1"}, err: "PIPEROOM_MODEL_MAX_CONCURRENCY"},
		{name: "tpm bad", c: Config{ModelTPM: "1k"}, err: "PIPEROOM_MODEL_TPM"},
		{name: "unverified proxy", c: Config{ModelUnverified: "on", ModelProxy: "ON"}, want: func(k ModelKnobs) bool { return k.Unverified && k.Proxy }},
		{name: "unverified bad", c: Config{ModelUnverified: "yes"}, err: "PIPEROOM_MODEL_UNVERIFIED"},
		{name: "proxy bad", c: Config{ModelProxy: "1"}, err: "PIPEROOM_MODEL_PROXY"},
		{name: "gemini vertex", c: Config{GeminiBackend: "Vertex"}, want: func(k ModelKnobs) bool { return k.GeminiBackend == GeminiBackendVertex }},
		{name: "gemini bad", c: Config{GeminiBackend: "aistudio"}, err: "PIPEROOM_GEMINI_BACKEND"},
		{name: "roles error surfaces", c: Config{ModelProvider: "nope"}, err: "PIPEROOM_MODEL_PROVIDER"},
	} {
		k, err := ParseModelKnobs(tt.c)
		if tt.err != "" {
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("%s: err = %v, want %q", tt.name, err, tt.err)
			}
			continue
		}
		if err != nil || !tt.want(k) {
			t.Errorf("%s: %+v, %v", tt.name, k, err)
		}
	}
}

// TestNoKnobReachesTheDenyList sets every knob to its most permissive value
// and checks that a metadata endpoint is still refused for every role.
func TestNoKnobReachesTheDenyList(t *testing.T) {
	permissive := Config{
		ModelAllowPrivateHTTP: "on", ModelUnverified: "on", ModelProxy: "on", ModelAckCNEndpoint: "yes",
		ModelPricingStrict: "off", ModelNeutraliseControlTokens: "non-claude", ModelCaps: "parallel_tools,structured_output,reasoning",
		GeminiBackend: "vertex",
	}
	for _, target := range []string{
		"http://169.254.169.254/latest/meta-data", "https://169.254.169.254/", "http://[fd00:ec2::254]/",
		"http://metadata.google.internal/", "http://100.100.100.200/", "http://[::ffff:169.254.169.254]/",
		"http://2852039166/",
	} {
		for _, set := range []func(*Config){
			func(c *Config) {
				c.ModelProvider, c.Model, c.ModelFlavor, c.ModelBaseURL = "openai", "m", "vllm", target
			},
			func(c *Config) { c.ModelProvider, c.Model, c.ModelBaseURL = "gemini", "m", target },
			func(c *Config) { c.TriagePassProvider, c.TriagePassModel, c.TriagePassBaseURL = "openai", "m", target },
			func(c *Config) { c.SummaryProvider, c.SummaryModel, c.SummaryBaseURL = "gemini", "m", target },
			func(c *Config) { c.JudgeProvider, c.JudgeModel, c.JudgeBaseURL = "openai", "m", target },
		} {
			c := permissive
			set(&c)
			if _, err := ParseModelKnobs(c); err == nil {
				t.Errorf("%s accepted with every knob permissive (%+v)", target, c)
			}
		}
	}
}

// TestV0EnvNames pins every new variable name to its field, and that no V0
// name collides with a variable Load already read before V0.
func TestV0EnvNames(t *testing.T) {
	clearModelEnv(t)
	for i, k := range v0Vars {
		t.Setenv(k, "v"+string(rune('a'+i)))
	}
	c := Load()
	got := []string{
		c.ModelFlavor, c.ModelBaseURL, c.ModelContextTokens, c.ModelCaps, c.ModelUnverified, c.ModelMaxConcurrency,
		c.ModelTPM, c.ModelProxy, c.GeminiBackend, c.ModelAllowPrivateHTTP, c.ModelAckCNEndpoint, c.ModelPricingStrict,
		c.ModelNeutraliseControlTokens, c.ModelPromptVariant, c.PostmortemMode, c.TriagePassProvider, c.SummaryProvider,
		c.SummaryModel, c.JudgeProvider, c.JudgeModel, c.TriagePassBaseURL, c.SummaryBaseURL, c.JudgeBaseURL,
	}
	for i, k := range v0Vars {
		if want := "v" + string(rune('a'+i)); got[i] != want {
			t.Errorf("%s: field = %q, want %q", k, got[i], want)
		}
	}
	// Every other field keeps its pre-V0 value: no V0 name is read elsewhere.
	clearModelEnv(t)
	base := Load()
	for _, k := range v0Vars {
		t.Setenv(k, "zz-collision")
	}
	after := Load()
	v := reflect.ValueOf(after)
	bv := reflect.ValueOf(base)
	newFields := map[string]bool{}
	for _, f := range []string{"ModelFlavor", "ModelBaseURL", "ModelContextTokens", "ModelCaps", "ModelUnverified", "ModelMaxConcurrency",
		"ModelTPM", "ModelProxy", "GeminiBackend", "ModelAllowPrivateHTTP", "ModelAckCNEndpoint", "ModelPricingStrict",
		"ModelNeutraliseControlTokens", "ModelPromptVariant", "PostmortemMode", "TriagePassProvider", "SummaryProvider",
		"SummaryModel", "JudgeProvider", "JudgeModel", "TriagePassBaseURL", "SummaryBaseURL", "JudgeBaseURL"} {
		newFields[f] = true
	}
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		if newFields[name] {
			continue
		}
		if !reflect.DeepEqual(v.Field(i).Interface(), bv.Field(i).Interface()) {
			t.Errorf("field %s changed when only V0 variables were set: a name collides", name)
		}
	}
}

// TestParseTriagePassAgreesWithModelRoles: the live first-look parser and
// ParseModelRoles give the same first-look model, or both refuse, for every
// first-look provider over a Claude and an openai main provider.
func TestParseTriagePassAgreesWithModelRoles(t *testing.T) {
	for _, main := range []string{"anthropic", "openai"} {
		for _, fl := range []string{"", "anthropic", "bedrock", "vertex", "openai", "gemini", "bedrock-converse"} {
			c := Config{ModelProvider: main, Model: "m-main", TriagePassProvider: fl}
			if main == "anthropic" {
				c.Model = "claude-opus-5-5"
			}
			tp, tpErr := ParseTriagePass(c)
			r, rErr := ParseModelRoles(c)
			if rErr != nil && strings.Contains(rErr.Error(), "TRIAGE_PASS_MODEL is required") {
				if tpErr == nil {
					t.Errorf("main %s, first look %q: ParseModelRoles refuses (%v), ParseTriagePass gives %q", main, fl, rErr, tp.Model)
				}
				continue
			}
			if rErr != nil {
				continue // refused for another reason (a role setting this test does not set)
			}
			if tpErr != nil || tp.Model != r.FirstLook.Model {
				t.Errorf("main %s, first look %q: ParseTriagePass = %q, %v; ParseModelRoles = %q", main, fl, tp.Model, tpErr, r.FirstLook.Model)
			}
		}
	}
}
