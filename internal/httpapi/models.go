package httpapi

// GET and PUT /api/models (V7; docs/api.md, docs/design/model-providers.md
// section 11): the model panel every server shows, and the picker's write
// route on a standalone or quickstart server.

import (
	"context"
	"errors"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/config"
	"piperoom/internal/room"
)

// Server modes of ModelPanel.Mode.
const (
	ModelModeTeam       = "team"
	ModelModeStandalone = "standalone"
	ModelModeQuickstart = "quickstart"
)

// Model states of ModelPanel.State.
const (
	ModelStateReady   = "ready"
	ModelStateNoModel = "no_model"
)

// ModelStatusReference is the gate status of a role on a Claude provider:
// the reference model, never checked against the supported list.
const ModelStatusReference = "reference"

// modelsPath is the route; its body may hold a key, so it is never logged.
const modelsPath = "/api/models"

// maxModelsBody caps a ModelChoice: a few short fields and one key.
const maxModelsBody = 64 << 10

// modelsPutTimeout bounds a PUT: building the endpoint runs the start
// checks, which may send one probe request to the chosen model.
const modelsPutTimeout = 90 * time.Second

// ModelPanel is docs/api.md's ModelPanel.
type ModelPanel struct {
	Mode      string           `json:"mode"`
	State     string           `json:"state"`
	Roles     []ModelRoleView  `json:"roles"`
	Supported []SupportedModel `json:"supported"`
}

// ModelRoleView is one role of the panel: never a key, never a whole base
// URL (host only).
type ModelRoleView struct {
	Role        string            `json:"role"`
	Provider    string            `json:"provider"`
	Flavor      string            `json:"flavor,omitempty"`
	Model       string            `json:"model"`
	BaseURLHost string            `json:"base_url_host,omitempty"`
	Region      string            `json:"region,omitempty"`
	Status      string            `json:"status"`
	PromptCache bool              `json:"prompt_cache"`
	Price       *agent.ModelPrice `json:"price"`
	Sunset      string            `json:"sunset,omitempty"`
	KeySet      bool              `json:"key_set"`
}

// SupportedModel is one supported.json row with its committed baseline. The
// rates and the cost are null while the row carries no baseline numbers.
type SupportedModel struct {
	Provider      string   `json:"provider"`
	Flavor        string   `json:"flavor,omitempty"`
	Model         string   `json:"model"`
	Status        string   `json:"status"`
	Alias         bool     `json:"alias"`
	PassRate      *float64 `json:"pass_rate"`
	RootCauseRate *float64 `json:"root_cause_rate"`
	CostPerRunUSD *float64 `json:"cost_per_run_usd"`
	GatedAt       string   `json:"gated_at"`
	Sunset        string   `json:"sunset"`
}

// ModelChoice is the PUT body. Key is a pointer so an absent key (keep the
// stored one) differs from an empty one (this target takes no key).
type ModelChoice struct {
	Provider string            `json:"provider"`
	Flavor   string            `json:"flavor,omitempty"`
	BaseURL  string            `json:"base_url,omitempty"`
	Model    string            `json:"model"`
	Key      *string           `json:"key,omitempty"`
	Roles    *ModelRoleChoices `json:"roles,omitempty"`
}

// ModelRoleChoices are the single-shot roles of a ModelChoice; a nil role
// falls back to the main one.
type ModelRoleChoices struct {
	FirstLook *ModelRoleChoice `json:"first_look,omitempty"`
	Summary   *ModelRoleChoice `json:"summary,omitempty"`
	Judge     *ModelRoleChoice `json:"judge,omitempty"`
}

// ModelRoleChoice is one role: the same fallbacks as its env variables.
type ModelRoleChoice struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
	BaseURL  string `json:"base_url,omitempty"`
}

// Models serves the model routes (WithModels).
type Models interface {
	// Panel is the current panel.
	Panel() ModelPanel
	// Editable reports a standalone or quickstart server, where PUT exists.
	Editable() bool
	// SoloUser is the one user who may PUT; "" lets no one.
	SoloUser() string
	// Apply validates and applies a choice. A refusal is a *room.Error
	// (400 validation, 400 key_required); its message never holds the key
	// or a whole base URL.
	Apply(ctx context.Context, c ModelChoice) (ModelPanel, error)
}

// WithModels mounts GET /api/models and, on a standalone or quickstart
// server, PUT /api/models.
func WithModels(m Models) Option {
	return func(s *Server) { s.models = m }
}

// mountModelsPut mounts PUT /api/models (GET is in the protected group). It
// has its own group: its checks (404 on a team server, loopback, Origin)
// answer before the login, the content type and the body, as docs/api.md
// orders them.
func (s *Server) mountModelsPut(api chi.Router) {
	if s.models == nil {
		return
	}
	api.Group(func(g chi.Router) {
		g.Use(s.modelsEditable)
		g.Use(modelsLoopback)
		g.Use(modelsOrigin)
		g.Use(s.authenticate)
		g.Use(viewerGuard)
		g.Use(middleware.Timeout(modelsPutTimeout))
		g.Put("/models", s.handlePutModels)
	})
}

func (s *Server) handleGetModels(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.models.Panel())
}

func (s *Server) handlePutModels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	solo := s.models.SoloUser()
	if solo == "" || userFrom(r.Context()).ID != solo {
		writeError(w, http.StatusForbidden, "not_allowed", "only the solo user of this standalone server can change the model")
		return
	}
	var c ModelChoice
	if err := decodeBodyLimit(w, r, &c, maxModelsBody); err != nil {
		s.writeErr(w, err)
		return
	}
	p, err := s.models.Apply(r.Context(), c)
	if err != nil {
		var re *room.Error
		if !errors.As(err, &re) {
			// Never err itself: a wrapped error could carry a value.
			s.log.Error("http: PUT /api/models failed")
			writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
			return
		}
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// modelsEditable answers PUT on a team server with 404, before the login.
func (s *Server) modelsEditable(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.models.Editable() {
			writeError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// modelsLoopback refuses a peer that is not loopback. It reads the socket's
// address only: a forwarded-for header is the client's word.
func modelsLoopback(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopbackPeer(r.RemoteAddr) {
			writeError(w, http.StatusForbidden, "not_allowed", "the model can be changed from this machine only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loopbackPeer(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	a, err := netip.ParseAddr(host)
	return err == nil && a.Unmap().IsLoopback()
}

// modelsOrigin is the Origin rule of docs/api.md: any page in a browser on
// this machine can reach a loopback server, so the request must come from the
// server's own origin, same-origin by Sec-Fetch-Site when sent, and carry a
// JSON body (no simple-request form post). Checked before the body is read.
func modelsOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !sameOriginJSON(r) {
			writeError(w, http.StatusForbidden, "origin", "this route takes a JSON request from this server's own page only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func sameOriginJSON(r *http.Request) bool {
	origins := r.Header.Values("Origin")
	if len(origins) != 1 {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if r.Host == "" || !strings.EqualFold(origins[0], scheme+"://"+r.Host) {
		return false
	}
	if sites := r.Header.Values("Sec-Fetch-Site"); len(sites) > 1 || (len(sites) == 1 && sites[0] != "same-origin") {
		return false
	}
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}

// modelsRoute reports the models route, kept out of request logging.
func modelsRoute(path string) bool {
	return strings.TrimSuffix(path, "/") == modelsPath
}

// PanelInput is what BuildModelPanel reads.
type PanelInput struct {
	Mode  string
	State string
	// Config is the effective configuration; its model variables give the
	// roles. A configuration that does not parse lists no roles.
	Config config.Config
	// Prices is the PIPEROOM_MODEL_PRICES table; nil prices nothing.
	Prices agent.Prices
	// Supported is the supported-model list (llm.SupportedModels).
	Supported []llm.SupportedModel
	// Now is the date the gates and sunsets are read at.
	Now time.Time
	// Getenv reads key variables to report key_set, never a key's value.
	Getenv func(string) string
}

// BuildModelPanel builds the panel from the configuration. It never reads a
// key's value into the panel and gives a base URL as its host only.
func BuildModelPanel(in PanelInput) ModelPanel {
	p := ModelPanel{Mode: in.Mode, State: in.State, Roles: []ModelRoleView{}, Supported: []SupportedModel{}}
	for _, m := range in.Supported {
		p.Supported = append(p.Supported, SupportedModel{
			Provider: m.Provider, Flavor: m.Flavor, Model: m.Model, Status: m.Status, Alias: m.Alias,
			GatedAt: m.GatedAt, Sunset: m.Sunset,
		})
	}
	knobs, err := config.ParseModelKnobs(in.Config)
	if err != nil {
		return p
	}
	getenv := in.Getenv
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	r := knobs.Roles
	add := func(name string, role config.ModelRole, main bool) {
		v := ModelRoleView{Role: name, Provider: role.Provider, Flavor: role.Flavor, Model: role.Model,
			Region: modelRegion(in.Config, knobs, role), KeySet: keySet(role, main, knobs, getenv),
			PromptCache: claudeProvider(role.Provider)}
		if role.BaseURL != nil {
			v.BaseURLHost = role.BaseURL.Host
		}
		if claudeProvider(role.Provider) {
			v.Status = ModelStatusReference
		} else {
			g := llm.LookupGate(in.Supported, role.Provider, role.Flavor, role.Model, in.Now)
			v.Status = g.Status
			if g.Entry != nil {
				v.Sunset = g.Entry.Sunset
			}
		}
		if mp, ok := in.Prices.PriceFor(role.Provider, role.Flavor, role.Model); ok {
			v.Price = &mp
		}
		p.Roles = append(p.Roles, v)
	}
	add("main", r.Main, true)
	if r.Triage != "" {
		t := r.Main
		t.Model = r.Triage
		add("triage", t, true)
	}
	if tp, err := config.ParseTriagePass(in.Config); err == nil && tp.On {
		fl := r.FirstLook
		fl.Model = tp.Model
		add("first_look", fl, fl.Provider == r.Main.Provider)
	}
	add("summary", r.Summary, r.Summary.Provider == r.Main.Provider)
	add("judge", r.Judge, r.Judge.Provider == r.Main.Provider)
	return p
}

func claudeProvider(p string) bool {
	return p == config.ModelProviderAnthropic || p == config.ModelProviderBedrock || p == config.ModelProviderVertex
}

// modelRegion is where a role's requests are processed, when known.
func modelRegion(c config.Config, k config.ModelKnobs, r config.ModelRole) string {
	switch r.Provider {
	case config.ModelProviderBedrock, config.ModelProviderBedrockConverse:
		return c.AWSRegion
	case config.ModelProviderVertex:
		return c.VertexRegion
	case config.ModelProviderGemini:
		if k.GeminiBackend == config.GeminiBackendVertex {
			return c.VertexRegion
		}
	}
	return ""
}

// keySet reports whether the role's provider has its credential. Cloud
// credential chains (Bedrock, Vertex, Gemini on Vertex) and flavors that take
// no key count as set: there is no key to ask for. A role on the main
// provider shares the main key variables.
func keySet(r config.ModelRole, main bool, k config.ModelKnobs, getenv func(string) string) bool {
	has := func(names ...string) bool {
		for _, n := range names {
			if strings.TrimSpace(getenv(n)) != "" {
				return true
			}
		}
		return false
	}
	mainVars := []string{}
	if main {
		mainVars = []string{config.EnvModelAPIKey, config.EnvModelAPIKeyFile}
	}
	switch r.Provider {
	case config.ModelProviderAnthropic:
		return has("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
	case config.ModelProviderOpenAI:
		std := openai.KeyEnv(r.Flavor)
		if std == "" {
			return true
		}
		return has(append(mainVars, std)...)
	case config.ModelProviderGemini:
		if k.GeminiBackend == config.GeminiBackendVertex {
			return true
		}
		return has(append(mainVars, "GEMINI_API_KEY")...)
	}
	return true
}
