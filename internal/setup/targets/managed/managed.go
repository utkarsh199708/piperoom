// Package managed is the managed-cloud target of the setup runner (rows
// U13b2, U13c, U13d; docs/design/setup-managed.md): typed ops that set up a
// Debezium connector on AWS MSK Connect, Confluent Cloud or Aiven through the
// person's own cloud credentials on the runner, never through the server.
//
// One rule set for the three providers:
//   - every read op is T0; every write op is T2 and T3 when the target is
//     production (TierFor, no I/O). A managed target is never local: its
//     class is shared, or production when the account id (AWS), environment
//     or cluster id (Confluent) or project (Aiven) matches
//     PIPEROOM_SETUP_PROD_PATTERNS (plan.MatchProdPattern), or the person's
//     floor says so (plan.Raise);
//   - an op runs only in a room whose targets hold the driver's account
//     (events.SetupTarget kinds "aws", "confluent", "aiven" and their address
//     forms); on AWS every op first checks that the runner's caller identity
//     is in that account, so a profile pointed elsewhere cannot dodge the
//     production pattern;
//   - names, ids and ARNs are checked against strict patterns in Validate,
//     with no I/O; a connector config value under a secret-looking key must
//     be a ${secret:name} reference, materialised on the runner only at the
//     API call (design §2.1) and in no Data, Summary or error;
//   - a delete reaches only a connector this room created: the create ops
//     tag (AWS) or name-record what they create, and the delete op refuses
//     anything else on every class (design §4's guard row);
//   - artifacts that execute are templated: the MSK worker configuration is
//     rendered from a template under a key allow-list (worker.go).
//
// Wiring (outside this package, A9's internal/setup/setupcmd): after
// ops.New, for a room whose SetupRequest.Target is msk, confluent or aiven,
//
//	_, err := managed.RegisterMSK(reg, managed.MSKOptions{...})
//	_, err := managed.RegisterConfluent(reg, managed.ConfluentOptions{...})
//	_, err := managed.RegisterAiven(reg, managed.AivenOptions{...})
//
// with the client built from the runner's own credentials and the runner's
// secret store as Secrets.
package managed

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// Target kinds of events.SetupTarget the providers use.
const (
	KindAWS       = "aws"
	KindConfluent = "confluent"
	KindAiven     = "aiven"
)

// connectLayer is the owners layer of every managed op.
const connectLayer = "kafka-connect"

// Tags (AWS) the create ops put on what they create; the delete op reads
// TagRoom back.
const (
	TagRoom      = "piperoom-room"
	TagCreatedBy = "piperoom-created-by"
	createdBy    = "piperoom-setup"
)

// Errors callers branch on with errors.Is.
var (
	// ErrParams: params do not decode or fail a bound (wraps ops.ErrParams,
	// so the runner refuses with RefuseSchema).
	ErrParams = fmt.Errorf("managed: %w", ops.ErrParams)
	// ErrTarget: the room's targets do not hold the driver's account
	// (wraps ops.ErrTarget).
	ErrTarget = fmt.Errorf("managed: %w", ops.ErrTarget)
	// ErrAccount: the runner's cloud identity is not in the target's
	// account.
	ErrAccount = errors.New("managed: the runner's credentials are for another account")
	// ErrNotCreatedByRoom: a delete names a resource this room did not
	// create (design §4: refused on every class).
	ErrNotCreatedByRoom = errors.New("managed: the resource was not created by this room")
	// ErrSecretValue: a config value under a secret-looking key is not a
	// ${secret:name} reference, or a value looks like a credential. The
	// value is in no error.
	ErrSecretValue = errors.New("managed: a credential must be a ${secret:name} reference")
	// ErrNoSecrets: a config holds a ${secret:} reference and the driver has
	// no secret store.
	ErrNoSecrets = errors.New("managed: no secret store to materialise ${secret:} references")
	// ErrCheckFailed: a check op ran and its verdict is red; Data says which
	// check.
	ErrCheckFailed = errors.New("managed: check failed")
	// ErrNotValid: a validate-before-create found config errors; Data lists
	// them.
	ErrNotValid = errors.New("managed: the provider's validation found errors")
)

// Materialiser resolves ${secret:NAME} references from the runner's secret
// store (runner.Store.Materialise has this signature).
type Materialiser interface {
	Materialise(text string) (string, error)
}

// deriveClass is the managed class rule: shared by default, production on a
// pattern match or by the person's floor, never local.
func deriveClass(person string, patterns []string, subject string) (string, error) {
	switch person {
	case "", protocol.ClassShared, protocol.ClassProduction:
	case protocol.ClassLocal:
		// A managed target is never local; a stale "local" floor cannot
		// lower it.
		person = ""
	default:
		return "", fmt.Errorf("managed: unknown target class %q", person)
	}
	class := protocol.ClassShared
	if plan.MatchProdPattern(patterns, subject) {
		class = protocol.ClassProduction
	}
	return plan.Raise(class, person)
}

// writeTier is the tier of a write op: T2, T3 on production.
func writeTier(class string) string {
	if class == protocol.ClassProduction {
		return protocol.TierT3
	}
	return protocol.TierT2
}

// decode fills dst from params strictly (no unknown fields, no trailing
// JSON, no keys that differ only in case).
func decode(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	return nil
}

// paramErr wraps a client validation error as ErrParams.
func paramErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrParams, err)
}

// targetIn reports whether env holds a target of kind whose address
// starts with prefix (segments separated by '/').
func targetIn(env ops.Env, kind, prefix string) bool {
	for _, t := range env.Targets {
		if t.Kind != kind {
			continue
		}
		if t.Address == prefix || strings.HasPrefix(t.Address, prefix+"/") {
			return true
		}
	}
	return false
}

// secretRef is one whole ${secret:NAME} reference with the protocol's name
// rule.
var secretRef = regexp.MustCompile(`^\$\{secret:[a-zA-Z_][a-zA-Z0-9_]*\}$`)

// configKey is a connector config key.
var configKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// maxConfigValue caps one connector config value.
const maxConfigValue = 4096

// checkConnectorConfig checks a connector config without I/O: keys by
// pattern, a value under a secret-looking key a whole ${secret:} reference,
// no other value carrying a ${secret:} reference or a control character.
func checkConnectorConfig(cfg map[string]string) error {
	if len(cfg) == 0 || len(cfg) > 256 {
		return fmt.Errorf("%w: config has %d keys, want 1 to 256", ErrParams, len(cfg))
	}
	for k, v := range cfg {
		if !configKey.MatchString(k) {
			return fmt.Errorf("%w: config key %s", ErrParams, strconv.Quote(clipText(k, 64)))
		}
		if len(v) > maxConfigValue || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("%w: config value of %s is too long or has a line break", ErrParams, k)
		}
		if actions.LooksSecret(k) {
			if !secretRef.MatchString(v) {
				return fmt.Errorf("%w: %s", ErrSecretValue, k)
			}
			continue
		}
		if strings.Contains(v, "${secret:") {
			return fmt.Errorf("%w: %s is not a secret-named key and cannot take a ${secret:} reference", ErrParams, k)
		}
	}
	return nil
}

// materialise resolves the ${secret:} values of cfg into a new map.
func materialise(s Materialiser, cfg map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(cfg))
	for k, v := range cfg {
		if !secretRef.MatchString(v) {
			out[k] = v
			continue
		}
		if s == nil {
			return nil, ErrNoSecrets
		}
		m, err := s.Materialise(v)
		if err != nil {
			// The store's error names the reference, never a value.
			return nil, fmt.Errorf("managed: materialising %s: %w", k, err)
		}
		out[k] = m
	}
	return out, nil
}

// clipText cuts s for an error, with no control characters.
func clipText(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}

// stringSchema and friends build the ops' JSON Schemas.
func stringSchema(pattern string) map[string]any {
	m := map[string]any{"type": "string"}
	if pattern != "" {
		m["pattern"] = pattern
	}
	return m
}

func objectSchema(required []string, props map[string]any) map[string]any {
	m := map[string]any{"type": "object", "additionalProperties": false, "properties": props}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}
}

func stringListSchema(pattern string) map[string]any {
	return map[string]any{"type": "array", "items": stringSchema(pattern), "minItems": 1, "maxItems": 16}
}
