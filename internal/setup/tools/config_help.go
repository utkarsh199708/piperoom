package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/tools"
)

const configHelpName = "setup_config_help"

// configHelpIndex is the read-only lookup surface the configuration help tool needs.
type configHelpIndex interface {
	Lookup(kind knowledge.Kind, name string) (knowledge.Entry, error)
}

type configHelpTool struct {
	index configHelpIndex
}

type configHelpInput struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Release string `json:"release,omitempty"`
}

type configHelpData struct {
	Requested      string          `json:"requested"`
	Name           string          `json:"name"`
	Kind           knowledge.Kind  `json:"kind"`
	Component      string          `json:"component"`
	Runtime        string          `json:"runtime"`
	Class          string          `json:"class,omitempty"`
	SinkType       string          `json:"sink_type,omitempty"`
	Versions       string          `json:"versions"`
	Release        string          `json:"release,omitempty"`
	Support        string          `json:"support,omitempty"`
	Summary        string          `json:"summary"`
	Keys           []configHelpKey `json:"keys,omitempty"`
	Checks         []string        `json:"checks"`
	Install        string          `json:"install"`
	RuntimeImage   string          `json:"runtime_image,omitempty"`
	PluginRef      string          `json:"plugin_ref,omitempty"`
	UnpinnedReason string          `json:"unpinned_reason,omitempty"`
	Documentation  string          `json:"documentation"`
	Source         string          `json:"source"`
}

type configHelpKey struct {
	Name               string            `json:"name"`
	Default            *string           `json:"default,omitempty"`
	DefaultSince       string            `json:"default_since,omitempty"`
	Required           bool              `json:"required,omitempty"`
	Values             []string          `json:"values,omitempty"`
	ValuesSince        map[string]string `json:"values_since,omitempty"`
	Since              string            `json:"since,omitempty"`
	Until              string            `json:"until,omitempty"`
	Secret             bool              `json:"secret,omitempty"`
	CredentialsInValue bool              `json:"credentials_in_value,omitempty"`
	Formerly           []string          `json:"formerly,omitempty"`
	Check              string            `json:"check,omitempty"`
}

func (t *configHelpTool) Name() string { return configHelpName }

func (t *configHelpTool) Description() string {
	return "Look up an SMT, predicate or sink in the setup knowledge index. Returns its config keys, defaults, checks, verified version range and installation notes; this is static knowledge and does not contact a target."
}

func (t *configHelpTool) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"kind": map[string]any{
			"type": "string",
			"enum": []string{string(knowledge.KindSMT), string(knowledge.KindPredicate), string(knowledge.KindSink)},
		},
		"name":    map[string]any{"type": "string", "minLength": 1, "maxLength": 256},
		"release": map[string]any{"type": "string", "maxLength": 64},
	}, "kind", "name")
}

func (t *configHelpTool) Kind() tools.Kind { return tools.KindRead }

func (t *configHelpTool) Run(ctx context.Context, raw json.RawMessage) (tools.Result, error) {
	in, err := parseConfigHelpInput(raw)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: %s: %v", tools.ErrBadInput, configHelpName, err)
	}
	if err := ctx.Err(); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w", configHelpName, err)
	}
	if t == nil || t.index == nil {
		return tools.Result{}, errors.New("setuptools: setup_config_help: knowledge index is unavailable")
	}
	entry, err := t.index.Lookup(knowledge.Kind(in.Kind), in.Name)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: %s: %w", tools.ErrBadInput, configHelpName, err)
	}
	data, err := configHelpEntry(in, entry)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: %s: %v", tools.ErrBadInput, configHelpName, err)
	}

	displayName := entry.Class
	if entry.SinkType != "" {
		displayName = entry.SinkType
	}
	if displayName == "" {
		displayName = entry.Name
	}
	summary, truncated := tools.Truncate(fmt.Sprintf("%s: %s %s (%s): %s", configHelpName, entry.Kind, entry.Name, displayName, entry.Summary))
	return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
}

func parseConfigHelpInput(raw json.RawMessage) (configHelpInput, error) {
	var in configHelpInput
	if err := decodeInput(raw, &in); err != nil {
		return in, err
	}
	in.Kind = strings.TrimSpace(in.Kind)
	in.Name = strings.TrimSpace(in.Name)
	in.Release = strings.TrimSpace(in.Release)
	if in.Kind != string(knowledge.KindSMT) && in.Kind != string(knowledge.KindPredicate) && in.Kind != string(knowledge.KindSink) {
		return in, errors.New("kind must be smt, predicate or sink")
	}
	if in.Name == "" {
		return in, errors.New("name is required")
	}
	if utf8.RuneCountInString(in.Name) > 256 {
		return in, errors.New("name is longer than 256 characters")
	}
	if utf8.RuneCountInString(in.Release) > 64 {
		return in, errors.New("release is longer than 64 characters")
	}
	return in, nil
}

func configHelpEntry(in configHelpInput, e knowledge.Entry) (configHelpData, error) {
	d := configHelpData{
		Requested:      in.Name,
		Name:           e.Name,
		Kind:           e.Kind,
		Component:      e.Component,
		Runtime:        e.Runtime,
		Class:          e.Class,
		SinkType:       e.SinkType,
		Versions:       e.Versions,
		Release:        in.Release,
		Summary:        e.Summary,
		Checks:         slices.Clone(e.Checks),
		Install:        e.Install,
		RuntimeImage:   e.RuntimeImage,
		PluginRef:      e.PluginRef,
		UnpinnedReason: e.UnpinnedReason,
		Documentation:  e.Doc,
		Source:         e.Source,
	}
	if in.Release != "" {
		support, err := e.Supports(in.Release)
		if err != nil {
			return configHelpData{}, fmt.Errorf("release %q is invalid: %w", in.Release, err)
		}
		switch support {
		case knowledge.Supported:
			d.Support = "supported"
		case knowledge.Unverified:
			d.Support = "unverified"
		case knowledge.Removed:
			d.Support = "removed"
		default:
			return configHelpData{}, fmt.Errorf("release %q has an unknown support state", in.Release)
		}
	}
	if len(e.Keys) > 0 {
		d.Keys = make([]configHelpKey, 0, len(e.Keys))
		for _, k := range e.Keys {
			d.Keys = append(d.Keys, publicConfigHelpKey(k))
		}
	}
	return d, nil
}

func publicConfigHelpKey(k knowledge.Key) configHelpKey {
	key := configHelpKey{
		Name:               k.Name,
		DefaultSince:       k.DefaultSince,
		Required:           k.Required,
		Values:             slices.Clone(k.Values),
		ValuesSince:        maps.Clone(k.ValuesSince),
		Since:              k.Since,
		Until:              k.Until,
		Secret:             k.Secret,
		CredentialsInValue: k.CredentialsInValue,
		Formerly:           slices.Clone(k.Formerly),
		Check:              k.Check,
	}
	// A secret's default is not useful to config authoring and must never be
	// included in model-visible help even if a future index row adds one.
	if !k.Secret && k.Default != nil {
		value := *k.Default
		key.Default = &value
	}
	return key
}
