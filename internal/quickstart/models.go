package quickstart

// The model picker of a standalone or quickstart server (V7, PUT
// /api/models; docs/api.md and docs/design/model-providers.md section 11).

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
)

// BuildFunc builds the model endpoint for cfg (its start checks included)
// without installing it. Exactly one of commit (install it) and discard
// (release it unused) is called after a nil error. ctx bounds the build only:
// the endpoint must outlive it. Nothing is installed on error. getenv answers
// the model key variables (the picker's Keys over the process environment):
// the endpoint reads every key through it (wiring.WithGetenv), never through
// os.Getenv, and may keep it for its whole life.
type BuildFunc func(ctx context.Context, cfg config.Config, getenv func(string) string) (commit, discard func(), err error)

// ModelsOptions configures NewModels.
type ModelsOptions struct {
	// Mode is httpapi.ModelModeStandalone or httpapi.ModelModeQuickstart.
	Mode string
	// ProjectDir holds .piperoom/model.env; "" is the working directory.
	ProjectDir string
	// Config is the effective configuration at start (model.env applied).
	Config config.Config
	// Stored is the model.env read at start (the zero value when none).
	Stored ModelEnv
	// Ready reports that the endpoint built at start: false is the no
	// model state.
	Ready bool
	// SoloUser may change the model; "" lets no one.
	SoloUser string
	Build    BuildFunc
	// Env is the real process environment, read only; nil = os.LookupEnv.
	// The picker's keys are laid over it (NewKeys(Env, Stored) at start).
	Env Env
	// Panel builds the panel; Mode, State, Config and Getenv (the keys)
	// are filled in.
	Panel httpapi.PanelInput
	Now   func() time.Time
}

// Models implements httpapi.Models for a standalone or quickstart server.
type Models struct {
	o ModelsOptions

	// applyMu serialises Apply, which holds it through the build and its
	// probe (up to the route's 90 s); mu is held only to read or commit the
	// state below, so a GET never waits behind a PUT.
	applyMu sync.Mutex

	mu     sync.Mutex
	cfg    config.Config
	stored ModelEnv
	ready  bool
	// keys is the overlay the current endpoint was built on: model.env's key
	// at start, then each committed PUT's. A PUT builds on a copy and keeps
	// it only once it is committed.
	keys Keys
}

// NewModels returns the picker over o.
func NewModels(o ModelsOptions) *Models {
	if o.Now == nil {
		o.Now = time.Now
	}
	// cmd/server built the start endpoint on the same overlay.
	return &Models{o: o, cfg: o.Config, stored: o.Stored, ready: o.Ready, keys: NewKeys(o.Env, o.Stored)}
}

// Editable is always true: a team server uses a read-only httpapi.Models.
func (m *Models) Editable() bool { return true }

// SoloUser is the user who may change the model.
func (m *Models) SoloUser() string { return m.o.SoloUser }

// Config is the effective configuration now.
func (m *Models) Config() config.Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

// Panel is the current panel.
func (m *Models) Panel() httpapi.ModelPanel {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.panelLocked()
}

func (m *Models) panelLocked() httpapi.ModelPanel {
	in := m.o.Panel
	in.Mode, in.Config, in.Getenv = m.o.Mode, m.cfg, m.keys.Getenv
	in.State = httpapi.ModelStateReady
	if !m.ready {
		in.State = httpapi.ModelStateNoModel
	}
	if in.Now.IsZero() {
		in.Now = m.o.Now()
	}
	p := httpapi.BuildModelPanel(in)
	// A main role without its key is no usable model either: the Anthropic
	// client builds without one and fails only on the first request.
	if len(p.Roles) == 0 || !p.Roles[0].KeySet {
		p.State = httpapi.ModelStateNoModel
	}
	return p
}

func validationErr(msg string) *room.Error {
	return &room.Error{Status: http.StatusBadRequest, Code: "validation", Msg: msg}
}

// errKeyRequired is docs/api.md's 400 key_required.
var errKeyRequired = &room.Error{Status: http.StatusBadRequest, Code: "key_required",
	Msg: "the stored key is bound to the provider, flavor and base URL it was entered with: send key with this change"}

// Apply validates c, writes model.env, rebuilds the endpoint and returns the
// panel. Requests are applied one at a time (applyMu). The order keeps the
// key where it belongs: every role's key binding is checked before anything
// is built, so a change of host without a key sends nothing anywhere.
func (m *Models) Apply(ctx context.Context, c httpapi.ModelChoice) (httpapi.ModelPanel, error) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	// Only Apply changes cfg and stored, and applyMu keeps it to one at a
	// time, so this snapshot stays current until the commit below.
	m.mu.Lock()
	cur, stored, keys := m.cfg, m.stored, m.keys
	m.mu.Unlock()
	cand, err := candidate(cur, c)
	if err != nil {
		return httpapi.ModelPanel{}, err
	}
	knobs, err := config.ParseModelKnobs(cand)
	if err != nil {
		return httpapi.ModelPanel{}, validationErr(fieldMessage(err.Error(), c))
	}
	if err := vertexBaseURL(knobs); err != nil {
		return httpapi.ModelPanel{}, err
	}
	old, oerr := config.ParseModelRoles(cur)
	rebind := oerr != nil || binding(old.Main) != binding(knobs.Roles.Main)
	if rebind && c.Key == nil {
		return httpapi.ModelPanel{}, errKeyRequired
	}
	// An empty key clears the picker's key, and the adapter then falls back
	// to the provider's standard variable (OPENAI_API_KEY, GROQ_API_KEY,
	// GEMINI_API_KEY ...). That variable is only ever sent to the provider's
	// default endpoint: with a base URL the picker set, an empty key is
	// refused whether or not the binding changed, because an earlier PUT with
	// a throwaway key can have bound that base URL already.
	if c.Key != nil && strings.TrimSpace(*c.Key) == "" && stdKeyVar(knobs.Roles.Main, knobs) != "" && knobs.Roles.Main.BaseURL != nil {
		return httpapi.ModelPanel{}, errKeyRequired
	}
	if err := m.roleEnvKey(knobs); err != nil {
		return httpapi.ModelPanel{}, err
	}
	if err := m.gate(knobs); err != nil {
		return httpapi.ModelPanel{}, err
	}
	stored.Vars = modelEnvOf(cand)
	var keyVar string
	if c.Key != nil {
		key := strings.TrimSpace(*c.Key)
		if keyVar, err = keyVarOf(knobs.Roles.Main, knobs, key); err != nil {
			return httpapi.ModelPanel{}, err
		}
		if !safeValue(key) || len(key) > 8<<10 {
			return httpapi.ModelPanel{}, validationErr("key: holds a control character or is longer than 8 KiB")
		}
		stored.KeyVar, stored.Key = keyVar, netsec.Secret(key)
		if key == "" {
			stored.KeyVar = ""
		}
	}
	// The candidate's keys are a copy: a failed build or write leaves the
	// current keys as they were, which is the restore.
	next := nextKeys(keys, c.Key, keyVar, usedKeyVars(knobs))
	commit, discard, err := m.o.Build(ctx, cand, next.Getenv)
	if err != nil {
		return httpapi.ModelPanel{}, validationErr("model: the endpoint did not start: " + scrub(err.Error(), c))
	}
	if err := WriteModelEnv(m.o.ProjectDir, stored); err != nil {
		discard()
		return httpapi.ModelPanel{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	commit()
	m.cfg, m.stored, m.ready, m.keys = cand, stored, true, next
	return m.panelLocked(), nil
}

// candidate is the configuration c asks for: the model variables replaced,
// every other setting kept. A role c leaves out falls back to the main one.
func candidate(cur config.Config, c httpapi.ModelChoice) (config.Config, error) {
	fields := []struct{ name, v string }{{"provider", c.Provider}, {"flavor", c.Flavor}, {"base_url", c.BaseURL}, {"model", c.Model}}
	var roles httpapi.ModelRoleChoices
	if c.Roles != nil {
		roles = *c.Roles
	}
	for _, r := range []struct {
		name string
		rc   *httpapi.ModelRoleChoice
	}{{"first_look", roles.FirstLook}, {"summary", roles.Summary}, {"judge", roles.Judge}} {
		if r.rc != nil {
			fields = append(fields, struct{ name, v string }{"roles." + r.name + ".provider", r.rc.Provider},
				struct{ name, v string }{"roles." + r.name + ".model", r.rc.Model},
				struct{ name, v string }{"roles." + r.name + ".base_url", r.rc.BaseURL})
		}
	}
	for _, f := range fields {
		if !safeValue(f.v) || len(f.v) > 2048 {
			return config.Config{}, validationErr(f.name + ": holds a control character, surrounding space or is too long")
		}
	}
	if c.Provider == "" || c.Model == "" {
		return config.Config{}, validationErr("provider and model are required")
	}
	cand := cur
	cand.ModelProvider, cand.ModelFlavor, cand.ModelBaseURL, cand.Model = c.Provider, c.Flavor, c.BaseURL, c.Model
	set := func(rc *httpapi.ModelRoleChoice, p, mo, b *string) {
		*p, *mo, *b = "", "", ""
		if rc != nil {
			*p, *mo, *b = rc.Provider, rc.Model, rc.BaseURL
		}
	}
	set(roles.FirstLook, &cand.TriagePassProvider, &cand.TriagePassModel, &cand.TriagePassBaseURL)
	set(roles.Summary, &cand.SummaryProvider, &cand.SummaryModel, &cand.SummaryBaseURL)
	set(roles.Judge, &cand.JudgeProvider, &cand.JudgeModel, &cand.JudgeBaseURL)
	return cand, nil
}

// gate refuses a model on a new provider that is not on the supported list
// (or is past its sunset) unless PIPEROOM_MODEL_UNVERIFIED=on.
func (m *Models) gate(k config.ModelKnobs) error {
	if k.Unverified {
		return nil
	}
	for _, r := range []struct {
		field string
		role  config.ModelRole
	}{{"model", k.Roles.Main}, {"roles.first_look.model", k.Roles.FirstLook}, {"roles.summary.model", k.Roles.Summary}, {"roles.judge.model", k.Roles.Judge}} {
		if claudeProvider(r.role.Provider) {
			continue
		}
		g := llm.LookupGate(m.o.Panel.Supported, r.role.Provider, r.role.Flavor, r.role.Model, m.o.Now())
		if g.Status == llm.GateUnverified {
			return validationErr(r.field + ": " + r.role.Model + " is not on the supported list for " + r.role.Provider +
				" (or is past its sunset); set PIPEROOM_MODEL_UNVERIFIED=on to run it anyway")
		}
	}
	return nil
}

func claudeProvider(p string) bool {
	return p == config.ModelProviderAnthropic || p == config.ModelProviderBedrock || p == config.ModelProviderVertex
}

// binding is what a stored key is bound to: the provider, the flavor and the
// whole normalised base URL (docs/api.md).
func binding(r config.ModelRole) string {
	b := ""
	if r.BaseURL != nil {
		b = r.BaseURL.String()
	}
	return r.Provider + "\x00" + r.Flavor + "\x00" + b
}

// stdKeyVar is the standard variable an adapter falls back to for role r's
// key when the picker's own key is not set (openai.LoadKey, the agent's
// loadKey): "" when there is none or it is the picker's own
// PIPEROOM_MODEL_API_KEY (vllm, generic), which the binding already covers.
// anthropic is left out: its endpoint cannot be changed here.
func stdKeyVar(r config.ModelRole, k config.ModelKnobs) string {
	switch r.Provider {
	case config.ModelProviderOpenAI:
		if v := openai.KeyEnv(r.Flavor); v != config.EnvModelAPIKey {
			return v
		}
	case config.ModelProviderGemini:
		if k.GeminiBackend != config.GeminiBackendVertex {
			return "GEMINI_API_KEY"
		}
	}
	return ""
}

// roleEnvKey refuses a single-shot role on another provider than the main
// one that has a base URL while its key comes from the environment: such a
// role reads only its standard variable (never the picker's key), so the
// picker has no key to bind to that endpoint, and the operator's key would
// follow a host typed in the web. A standard variable goes only to the
// provider's default endpoint, so this holds whether or not the role's
// endpoint changed. The message names the field and the variable, never the
// value.
func (m *Models) roleEnvKey(k config.ModelKnobs) error {
	for _, r := range []struct {
		field string
		role  config.ModelRole
	}{
		{"roles.first_look", k.Roles.FirstLook},
		{"roles.summary", k.Roles.Summary},
		{"roles.judge", k.Roles.Judge},
	} {
		if r.role.Provider == k.Roles.Main.Provider || r.role.BaseURL == nil {
			// It shares the main endpoint and its key (checked above), or
			// its key goes to the provider's default endpoint.
			continue
		}
		v := stdKeyVar(r.role, k)
		if v == "" {
			continue
		}
		// The operator's environment, past the picker's keys: the picker
		// never sets a standard variable.
		if val, ok := lookupEnv(m.o.Env, v); !ok || strings.TrimSpace(val) == "" {
			continue
		}
		return validationErr(r.field + ".base_url: provider " + r.role.Provider + " would send " + v +
			" from the environment to this endpoint; the picker cannot bind that key: change this role in the environment instead")
	}
	return nil
}

// vertexBaseURL refuses a base URL on any gemini role when the backend is
// Vertex: the adapter signs every request with Google application default
// credentials, and the token would go to whatever host was typed here.
// Every base URL in a candidate comes from the PUT (candidate replaces them
// all).
func vertexBaseURL(k config.ModelKnobs) error {
	if k.GeminiBackend != config.GeminiBackendVertex {
		return nil
	}
	for _, r := range []struct {
		field string
		role  config.ModelRole
	}{
		{"base_url", k.Roles.Main},
		{"roles.first_look.base_url", k.Roles.FirstLook},
		{"roles.summary.base_url", k.Roles.Summary},
		{"roles.judge.base_url", k.Roles.Judge},
	} {
		if r.role.Provider == config.ModelProviderGemini && r.role.BaseURL != nil {
			return validationErr(r.field + ": gemini on vertex signs with Google credentials; set it in the environment")
		}
	}
	return nil
}

// usedKeyVars are the key variables some role of k reads.
func usedKeyVars(k config.ModelKnobs) map[string]bool {
	used := map[string]bool{}
	main := k.Roles.Main.Provider
	for _, r := range []config.ModelRole{k.Roles.Main, k.Roles.FirstLook, k.Roles.Summary, k.Roles.Judge} {
		switch r.Provider {
		case config.ModelProviderAnthropic:
			used["ANTHROPIC_API_KEY"] = true
		case config.ModelProviderOpenAI, config.ModelProviderGemini:
			if r.Provider == config.ModelProviderGemini && k.GeminiBackend == config.GeminiBackendVertex {
				continue
			}
			if r.Provider == main {
				used[config.EnvModelAPIKey], used[config.EnvModelAPIKeyFile] = true, true
			}
			if v := stdKeyVar(r, k); v != "" {
				used[v] = true
			}
		}
	}
	return used
}

// keyVarOf is the variable the main role's key goes in. A provider that
// signs with cloud credentials takes no key here.
func keyVarOf(r config.ModelRole, k config.ModelKnobs, key string) (string, error) {
	switch {
	case r.Provider == config.ModelProviderAnthropic:
		return "ANTHROPIC_API_KEY", nil
	case r.Provider == config.ModelProviderOpenAI,
		r.Provider == config.ModelProviderGemini && k.GeminiBackend != config.GeminiBackendVertex:
		return config.EnvModelAPIKey, nil
	case key != "":
		return "", validationErr("key: provider " + r.Provider + " signs with cloud credentials, not a key: send an empty key")
	}
	return "", nil
}

// nextKeys is the overlay for a PUT, as a copy of cur. A nil key keeps the
// stored one; an empty key removes the main key variables (the operator's
// too, as seen by the adapters), so no old key reaches a target that takes
// none. A key the picker set that no role of the new target reads (used) is
// removed too, so a key entered for one provider does not stay behind after
// a switch; an operator's own variable is never touched.
func nextKeys(cur Keys, key *string, keyVar string, used map[string]bool) Keys {
	return cur.with(func(vars map[string]keyEntry) {
		unset := func(n string) { vars[n] = keyEntry{unset: true} }
		if key != nil {
			k := strings.TrimSpace(*key)
			switch {
			case k == "" || keyVar == "":
				unset(config.EnvModelAPIKey)
				unset(config.EnvModelAPIKeyFile)
			case keyVar == config.EnvModelAPIKey:
				unset(config.EnvModelAPIKeyFile)
				vars[keyVar] = keyEntry{value: netsec.Secret(k)}
			default:
				vars[keyVar] = keyEntry{value: netsec.Secret(k)}
			}
		}
		for n, e := range vars {
			if !e.unset && !used[n] {
				unset(n)
			}
		}
	})
}

// envFields maps a variable named in a config error to the ModelChoice field.
var envFields = strings.NewReplacer(
	"PIPEROOM_MODEL_PROVIDER", "provider",
	"PIPEROOM_MODEL_FLAVOR", "flavor",
	"PIPEROOM_MODEL_BASE_URL", "base_url",
	"PIPEROOM_TRIAGE_PASS_PROVIDER", "roles.first_look.provider",
	"PIPEROOM_TRIAGE_PASS_MODEL", "roles.first_look.model",
	"PIPEROOM_TRIAGE_PASS_BASE_URL", "roles.first_look.base_url",
	"PIPEROOM_TRIAGE_PASS", "roles.first_look",
	"PIPEROOM_SUMMARY_PROVIDER", "roles.summary.provider",
	"PIPEROOM_SUMMARY_MODEL", "roles.summary.model",
	"PIPEROOM_SUMMARY_BASE_URL", "roles.summary.base_url",
	"PIPEROOM_SUMMARY", "roles.summary",
	"PIPEROOM_JUDGE_PROVIDER", "roles.judge.provider",
	"PIPEROOM_JUDGE_MODEL", "roles.judge.model",
	"PIPEROOM_JUDGE_BASE_URL", "roles.judge.base_url",
	"PIPEROOM_JUDGE", "roles.judge",
	"PIPEROOM_MODEL=", "model=",
)

// fieldMessage names the ModelChoice field in a config error and scrubs it.
func fieldMessage(msg string, c httpapi.ModelChoice) string {
	return scrub(envFields.Replace(strings.TrimPrefix(msg, "config: ")), c)
}

// scrub removes the key and every base URL of c from msg: an adapter error
// may quote the endpoint, and its path can carry an account id.
func scrub(msg string, c httpapi.ModelChoice) string {
	secrets := []string{c.BaseURL}
	if c.Key != nil {
		secrets = append(secrets, strings.TrimSpace(*c.Key))
	}
	if c.Roles != nil {
		for _, r := range []*httpapi.ModelRoleChoice{c.Roles.FirstLook, c.Roles.Summary, c.Roles.Judge} {
			if r != nil {
				secrets = append(secrets, r.BaseURL)
			}
		}
	}
	for _, s := range secrets {
		if s = strings.TrimSpace(s); s != "" {
			msg = strings.ReplaceAll(msg, s, "[redacted]")
		}
	}
	return msg
}
