package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"piperoom/internal/agent/llm/converse"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/config"
)

// errNoAnthropicKey is today's message for the default provider, unchanged.
var errNoAnthropicKey = errors.New("ANTHROPIC_API_KEY is not set (use -check to validate scenarios without it)")

// checkModelKeys refuses a live run whose model roles lack their credentials,
// before any scenario runs (V11). It checks presence only: no key is read
// into memory, logged or put in an error, and an error names the variable to
// set. The rules are the agent's (internal/agent/roles.go): the main role may
// take PIPEROOM_MODEL_API_KEY or PIPEROOM_MODEL_API_KEY_FILE, another role
// only its provider's standard variable, and a role on the main provider
// shares the main client.
//
//   - anthropic: ANTHROPIC_API_KEY (another role: or ANTHROPIC_AUTH_TOKEN);
//   - bedrock, vertex: none here, as before (their own credential chains);
//   - openai: the flavor's variable (openai.KeyEnv: OPENAI_API_KEY,
//     AZURE_OPENAI_API_KEY, DEEPSEEK_API_KEY, ...); none for ollama, vllm
//     and generic, whose key is optional;
//   - gemini: GEMINI_API_KEY, or with PIPEROOM_GEMINI_BACKEND=vertex
//     VERTEX_PROJECT_ID and VERTEX_REGION (the Google credentials themselves
//     are found when the client is built);
//   - bedrock-converse: AWS_REGION and AWS credentials the default chain can
//     find without a network call (awsCredentialVars or a shared file), or
//     AWS_BEARER_TOKEN_BEDROCK.
//
// exists reports whether a file exists (the AWS shared files).
func checkModelKeys(cfg config.Config, getenv func(string) string, exists func(string) bool) error {
	mainProvider := normProvider(cfg.ModelProvider)
	// The default provider's check comes first and reads exactly as before.
	if (mainProvider == "" || mainProvider == config.ModelProviderAnthropic) && getenv("ANTHROPIC_API_KEY") == "" {
		return errNoAnthropicKey
	}
	knobs, err := config.ParseModelKnobs(cfg)
	if err != nil {
		// The Endpoint refuses the same settings at start, before any work.
		return err
	}
	k := keyCheck{cfg: cfg, knobs: knobs, getenv: getenv, exists: exists}
	if err := k.role("PIPEROOM_MODEL", knobs.Roles.Main, true); err != nil {
		return err
	}
	type role struct {
		env string
		r   config.ModelRole
		on  bool
	}
	firstLook := true
	if tp, err := config.ParseTriagePass(cfg); err == nil {
		firstLook = tp.On
	}
	for _, r := range []role{
		{"PIPEROOM_TRIAGE_PASS", knobs.Roles.FirstLook, firstLook},
		{"PIPEROOM_SUMMARY", knobs.Roles.Summary, true},
		{"PIPEROOM_JUDGE", knobs.Roles.Judge, true},
	} {
		if !r.on || r.r.Provider == knobs.Roles.Main.Provider {
			continue
		}
		if err := k.role(r.env, r.r, false); err != nil {
			return err
		}
	}
	return nil
}

// keyCheck holds what the role checks read.
type keyCheck struct {
	cfg    config.Config
	knobs  config.ModelKnobs
	getenv func(string) string
	exists func(string) bool
}

// set reports whether variable v is set to something other than blanks.
func (k keyCheck) set(v string) bool { return strings.TrimSpace(k.getenv(v)) != "" }

// mainKeySet reports whether the main role's own key variables are set.
func (k keyCheck) mainKeySet() bool {
	return k.set(config.EnvModelAPIKey) || k.set(config.EnvModelAPIKeyFile)
}

// role checks the credentials of one role; env is the variable that names
// its model, main is true for the main role.
func (k keyCheck) role(env string, r config.ModelRole, main bool) error {
	pv := env + "_PROVIDER"
	if main {
		pv = "PIPEROOM_MODEL_PROVIDER"
	}
	need := func(what string) error {
		return fmt.Errorf("%s=%s: %s is not set (use -check to validate scenarios without it)", pv, r.Provider, what)
	}
	needOr := func(what, other string) error {
		return fmt.Errorf("%s=%s: %s is not set (or set %s; use -check to validate scenarios without it)", pv, r.Provider, what, other)
	}
	withMain := func(std string) string {
		if main {
			return config.EnvModelAPIKey + ", " + config.EnvModelAPIKeyFile + " or " + std
		}
		return std
	}
	switch r.Provider {
	case config.ModelProviderAnthropic:
		if main {
			if !k.set("ANTHROPIC_API_KEY") {
				return errNoAnthropicKey
			}
			return nil
		}
		if !k.set("ANTHROPIC_API_KEY") && !k.set("ANTHROPIC_AUTH_TOKEN") {
			return need("ANTHROPIC_API_KEY")
		}
	case config.ModelProviderOpenAI:
		flavor := r.Flavor
		if flavor == "" {
			flavor = openai.FlavorOpenAI
		}
		std := openai.KeyEnv(flavor)
		if std == "" || std == config.EnvModelAPIKey {
			return nil // ollama: no key; vllm, generic: optional
		}
		if (main && k.mainKeySet()) || k.set(std) {
			return nil
		}
		return fmt.Errorf("%s=openai with flavor %s: %s is not set (use -check to validate scenarios without it)", pv, flavor, withMain(std))
	case config.ModelProviderGemini:
		if k.knobs.GeminiBackend == config.GeminiBackendVertex {
			if strings.TrimSpace(k.cfg.VertexProject) == "" || strings.TrimSpace(k.cfg.VertexRegion) == "" {
				return fmt.Errorf("%s=gemini with PIPEROOM_GEMINI_BACKEND=vertex: VERTEX_PROJECT_ID and VERTEX_REGION must both be set", pv)
			}
			return nil
		}
		if (main && k.mainKeySet()) || k.set("GEMINI_API_KEY") {
			return nil
		}
		return needOr(withMain("GEMINI_API_KEY"), "PIPEROOM_GEMINI_BACKEND=vertex")
	case config.ModelProviderBedrockConverse:
		if strings.TrimSpace(k.cfg.AWSRegion) == "" {
			return need("AWS_REGION")
		}
		if k.set(converse.BearerTokenEnv) || k.awsCredentials() {
			return nil
		}
		return need("AWS credentials (" + strings.Join(awsCredentialVars, ", ") + ", a shared credentials file) or " + converse.BearerTokenEnv)
	}
	return nil
}

// awsCredentialVars are the variables that point the AWS default credential
// chain at a source it can use without probing the network.
var awsCredentialVars = []string{
	"AWS_ACCESS_KEY_ID",
	"AWS_PROFILE",
	"AWS_WEB_IDENTITY_TOKEN_FILE",
	"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	"AWS_CONTAINER_CREDENTIALS_FULL_URI",
}

// awsCredentials reports whether the default chain has a source it can find
// without a network call: a variable above, or a shared credentials or
// config file. An EC2 instance role alone is not seen (finding it needs the
// metadata service); see evals/README.md.
func (k keyCheck) awsCredentials() bool {
	for _, v := range awsCredentialVars {
		if k.set(v) {
			return true
		}
	}
	files := []string{k.getenv("AWS_SHARED_CREDENTIALS_FILE"), k.getenv("AWS_CONFIG_FILE")}
	if home, err := os.UserHomeDir(); err == nil {
		files = append(files, filepath.Join(home, ".aws", "credentials"), filepath.Join(home, ".aws", "config"))
	}
	for _, f := range files {
		if f = strings.TrimSpace(f); f != "" && k.exists(f) {
			return true
		}
	}
	return false
}

// fileExists reports whether path names an existing file.
func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}
