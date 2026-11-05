package quickstart

// The picker's key overlay (V18 MP1): the model key the solo user picked
// reaches the adapters through agent.EndpointConfig.Getenv
// (wiring.WithGetenv), never through the process environment, so it is not
// global mutable state, not in /proc/<pid>/environ and not inherited by a
// child process.

import (
	"log/slog"
	"maps"
	"os"

	"piperoom/internal/clients/netsec"
)

// Env reads the real process environment, read only. cmd/server passes nil
// (os.LookupEnv); tests a map. The checks on an operator's standard key
// variable (OPENAI_API_KEY, GEMINI_API_KEY ...) read it, never the overlay.
type Env interface {
	Lookup(name string) (string, bool)
}

// Keys is the picker's key variables laid over the process environment: a
// variable the picker set answers its value, one it removed answers unset,
// and every other variable falls through to the environment. A Keys is
// never changed once built (with returns a copy), so an endpoint built on
// one keeps reading the same keys after a later PUT.
type Keys struct {
	env  Env
	vars map[string]keyEntry
}

type keyEntry struct {
	value netsec.Secret
	// unset hides the environment's own value: the picker removed the
	// variable (an empty key, or a key no role reads any more).
	unset bool
}

// NewKeys is the overlay at start: model.env's stored key, over env (nil =
// the process environment).
func NewKeys(env Env, stored ModelEnv) Keys {
	k := Keys{env: env, vars: map[string]keyEntry{}}
	if stored.KeyVar != "" && stored.Key != "" {
		k.vars[stored.KeyVar] = keyEntry{value: stored.Key}
	}
	return k
}

// Lookup is os.LookupEnv through the overlay.
func (k Keys) Lookup(name string) (string, bool) {
	if e, ok := k.vars[name]; ok {
		if e.unset {
			return "", false
		}
		return e.value.Reveal(), true
	}
	return lookupEnv(k.env, name)
}

// Getenv is os.Getenv through the overlay: the getenv for wiring.WithGetenv
// and the panel's key-set flags.
func (k Keys) Getenv(name string) string {
	v, _ := k.Lookup(name)
	return v
}

// lookupEnv reads the real environment env (nil = the process's), past
// any overlay.
func lookupEnv(env Env, name string) (string, bool) {
	if env == nil {
		return os.LookupEnv(name)
	}
	return env.Lookup(name)
}

// with returns a copy with set applied to it; k is unchanged.
func (k Keys) with(set func(vars map[string]keyEntry)) Keys {
	n := Keys{env: k.env, vars: maps.Clone(k.vars)}
	if n.vars == nil {
		n.vars = map[string]keyEntry{}
	}
	set(n.vars)
	return n
}

// String, GoString and LogValue keep the keys out of fmt and slog: fmt
// cannot call netsec.Secret's own String on an unexported field.
func (Keys) String() string { return "quickstart.Keys{[redacted]}" }

// GoString is String.
func (k Keys) GoString() string { return k.String() }

// LogValue is String.
func (k Keys) LogValue() slog.Value { return slog.StringValue(k.String()) }
