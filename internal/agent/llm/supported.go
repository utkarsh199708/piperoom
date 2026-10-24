package llm

// The supported-model list of the new providers (design 5.3 and 5.4). It is
// not part of the contract: llm.go is the leads', this file and
// supported.json are A4's.

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

//go:embed supported.json
var supportedJSON []byte

// Gate status of a model (design 5): what its eval gate run found.
const (
	GateSupported    = "supported"    // passed the hard gates and the quality floor
	GateExperimental = "experimental" // passed the hard gates only, or a floating alias
	GateUnverified   = "unverified"   // not listed; allowed only by PIPEROOM_MODEL_UNVERIFIED=on
)

// SunsetWarnDays is how long before a sunset date the start warns (design 5.4).
const SunsetWarnDays = 30

// SupportedModel is one row of supported.json.
type SupportedModel struct {
	Provider string `json:"provider"`
	// Flavor is the OpenAI-compatible flavor (provider openai only); the same
	// id on two hosts is two models.
	Flavor   string `json:"flavor,omitempty"`
	Model    string `json:"model"`
	Status   string `json:"status"`
	Baseline string `json:"baseline"`
	GatedAt  string `json:"gated_at"`
	// Alias marks a floating id (…-latest, an OpenRouter route): the model
	// behind it can change without a gate, so it is experimental only.
	Alias bool `json:"alias,omitempty"`
	// Sunset is the provider's announced retirement date; after it the row
	// counts as not listed.
	Sunset string `json:"sunset,omitempty"`
}

// SupportedModels parses the embedded supported.json.
func SupportedModels() ([]SupportedModel, error) { return ParseSupported(supportedJSON) }

// ParseSupported parses and checks a supported-model list: unknown fields,
// a Claude provider (never listed: Claude is not gated by the list), a
// flavor on a provider other than openai, an unknown status, an alias that
// is not experimental, a bad date and a row given twice are errors.
func ParseSupported(b []byte) ([]SupportedModel, error) {
	var doc struct {
		Note   string           `json:"_note"`
		Models []SupportedModel `json:"models"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("llm: supported.json: %w", err)
	}
	if dec.More() {
		return nil, errors.New("llm: supported.json: data after the object")
	}
	seen := map[string]bool{}
	for i, m := range doc.Models {
		where := fmt.Sprintf("llm: supported.json: row %d (%s %s)", i, m.Provider, m.Model)
		switch m.Provider {
		case ProviderOpenAI:
			if m.Flavor == "" {
				return nil, errors.New(where + ": an openai row names its flavor")
			}
		case ProviderGemini, ProviderBedrockConverse:
			if m.Flavor != "" {
				return nil, errors.New(where + ": flavor is for provider openai only")
			}
		default:
			return nil, errors.New(where + ": provider must be openai, gemini or bedrock-converse (Claude is never gated by this list)")
		}
		if strings.TrimSpace(m.Model) == "" || strings.ContainsAny(m.Model, " \t\r\n") {
			return nil, errors.New(where + ": model must be a non-empty id without spaces")
		}
		switch m.Status {
		case GateSupported, GateExperimental:
		default:
			return nil, errors.New(where + ": status must be supported or experimental")
		}
		if m.Alias && m.Status != GateExperimental {
			return nil, errors.New(where + ": a floating alias is experimental only")
		}
		if m.Baseline == "" {
			return nil, errors.New(where + ": baseline is required")
		}
		if _, err := time.Parse(time.DateOnly, m.GatedAt); err != nil {
			return nil, errors.New(where + ": gated_at must be YYYY-MM-DD")
		}
		if m.Sunset != "" {
			if _, err := time.Parse(time.DateOnly, m.Sunset); err != nil {
				return nil, errors.New(where + ": sunset must be YYYY-MM-DD")
			}
		}
		k := m.Provider + "\x00" + m.Flavor + "\x00" + m.Model
		if seen[k] {
			return nil, errors.New(where + ": listed twice")
		}
		seen[k] = true
	}
	return doc.Models, nil
}

// Gate is what the list says about one model at a date.
type Gate struct {
	// Status is GateSupported, GateExperimental or GateUnverified (not
	// listed, or past its sunset).
	Status string
	// Entry is the row, when listed (also when past its sunset).
	Entry *SupportedModel
	// SunsetSoon is true within SunsetWarnDays of the row's sunset date.
	SunsetSoon bool
	// SunsetPassed is true after the row's sunset date: the model counts as
	// not listed.
	SunsetPassed bool
}

// LookupGate finds provider/flavor/model in list on day today (UTC).
func LookupGate(list []SupportedModel, provider, flavor, model string, today time.Time) Gate {
	for i := range list {
		m := &list[i]
		if m.Provider != provider || m.Model != model || (provider == ProviderOpenAI && m.Flavor != flavor) {
			continue
		}
		g := Gate{Status: m.Status, Entry: m}
		if m.Sunset != "" {
			sunset, _ := time.Parse(time.DateOnly, m.Sunset)
			day := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
			switch {
			case day.After(sunset):
				g.Status, g.SunsetPassed = GateUnverified, true
			case !day.Before(sunset.AddDate(0, 0, -SunsetWarnDays)):
				g.SunsetSoon = true
			}
		}
		return g
	}
	return Gate{Status: GateUnverified}
}
