package integrations

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

var providerNameRe = regexp.MustCompile(spec.ProviderNamePattern)

// maxFieldLen caps one non-secret value; maxSecretLen one secret (a PEM
// chain or a service-account JSON fits easily).
const (
	maxFieldLen  = 4096
	maxSecretLen = 64 << 10
)

// merged is an Input checked against a kind and merged over a stored row.
type merged struct {
	config  map[string]any           // what Integration.Config stores: text values and {"$ref"} entries
	typed   map[string]netsec.Secret // typed-in secrets to seal
	refs    map[string]string        // secret field → secret:// reference
	changed []string                 // field names that changed, sorted
}

// secretsSet lists the secret fields that have a value, sorted.
func (m merged) secretsSet() []string {
	out := make([]string, 0, len(m.typed)+len(m.refs))
	for k := range m.typed {
		out = append(out, k)
	}
	for k := range m.refs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// textValue turns a JSON-decoded config value into its text form.
func textValue(field string, v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case bool:
		return strconv.FormatBool(x), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", &spec.ValidationError{Field: field, Message: "must be a finite number"}
		}
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case json.Number:
		return x.String(), nil
	case int:
		return strconv.Itoa(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	}
	return "", &spec.ValidationError{Field: field, Message: "must be a string, number or boolean"}
}

// checkText validates one non-secret value against its field.
func checkText(f spec.Field, s string) error {
	if len(s) > maxFieldLen {
		return &spec.ValidationError{Field: f.Name, Message: "too long"}
	}
	if strings.ContainsAny(s, "\x00\r\n") {
		return &spec.ValidationError{Field: f.Name, Message: "must be one line"}
	}
	if s == "" {
		return nil
	}
	switch f.Type {
	case spec.FieldNumber:
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return &spec.ValidationError{Field: f.Name, Message: "must be a number"}
		}
	case spec.FieldBool:
		if s != "true" && s != "false" {
			return &spec.ValidationError{Field: f.Name, Message: "must be true or false"}
		}
	case spec.FieldSelect:
		ok := false
		for _, o := range f.Options {
			ok = ok || o == s
		}
		if !ok {
			return &spec.ValidationError{Field: f.Name, Message: "must be one of " + strings.Join(f.Options, ", ")}
		}
	}
	if f.Pattern != "" {
		re, err := regexp.Compile(f.Pattern)
		if err != nil || !re.MatchString(s) {
			return &spec.ValidationError{Field: f.Name, Message: "does not match the expected format"}
		}
	}
	return nil
}

// ParseRef checks a secret:// reference and returns its provider id. It
// uses the resolver's own parser (secrets.ParseRef), so the API refuses on
// save exactly what would fail at resolve time. Errors never quote ref.
func ParseRef(ref string) (provider string, err error) {
	if !strings.HasPrefix(ref, spec.RefPrefix) {
		return "", fmt.Errorf("a reference starts with %s", spec.RefPrefix)
	}
	// Raw whitespace is refused here (encode it as %20): a reference is one
	// token in forms, env values and exports.
	if strings.ContainsAny(ref, " \t\r\n\x00") {
		return "", errors.New("a reference is one line without spaces (encode a space as %20)")
	}
	r, err := secrets.ParseRef(ref)
	if err != nil {
		return "", errors.New("not a valid reference: " + spec.RefPrefix + "<provider>/<path>[#key][?version=v], " +
			"one line, the provider id matching " + spec.ProviderNamePattern + ", no . or .. segments")
	}
	return r.Provider, nil
}

// mergeInput checks in against k and merges it over old (nil on create).
// oldTyped are old's unsealed secrets (nil on create). haveKey tells whether
// a typed-in secret can be sealed.
func mergeInput(k spec.Kind, in spec.Input, old *spec.Integration, oldTyped map[string]netsec.Secret, haveKey bool) (merged, error) {
	fields := map[string]spec.Field{}
	for _, f := range k.Fields {
		fields[f.Name] = f
	}
	m := merged{config: map[string]any{}, typed: map[string]netsec.Secret{}, refs: map[string]string{}}
	changed := map[string]bool{}
	keptTyped := map[string]bool{} // typed-in secrets carried over from old

	// Start from the stored row.
	if old != nil {
		for name, v := range old.Config {
			f, ok := fields[name]
			if !ok {
				continue // a field the kind no longer has
			}
			if f.Type.IsSecret() {
				if ref, ok := refOf(v); ok {
					m.refs[name] = ref
				}
				continue
			}
			if s, ok := v.(string); ok {
				m.config[name] = s
			}
		}
		for name, s := range oldTyped {
			if f, ok := fields[name]; ok && f.Type.IsSecret() {
				m.typed[name] = s
				keptTyped[name] = true
			}
		}
	}

	// Non-secret fields. On update, the body is the whole form: a field
	// left out is cleared (secrets are the exception, below).
	newCfg := map[string]string{}
	for name, raw := range in.Config {
		f, ok := fields[name]
		if !ok {
			return merged{}, &spec.ValidationError{Field: name, Message: "unknown field"}
		}
		if f.Type.IsSecret() {
			return merged{}, &spec.ValidationError{Field: name, Message: "secret fields go in secrets, not config"}
		}
		s, err := textValue(name, raw)
		if err != nil {
			return merged{}, err
		}
		s = strings.TrimSpace(s)
		if err := checkText(f, s); err != nil {
			return merged{}, err
		}
		if s != "" {
			newCfg[name] = s
		}
	}
	for _, f := range k.Fields {
		if f.Type.IsSecret() {
			continue
		}
		oldV, _ := m.config[f.Name].(string)
		newV, has := newCfg[f.Name]
		if oldV != newV {
			changed[f.Name] = true
		}
		if has {
			m.config[f.Name] = newV
		} else {
			delete(m.config, f.Name)
		}
		if f.Required && newV == "" && f.Default == "" {
			return merged{}, &spec.ValidationError{Field: f.Name, Message: "required"}
		}
	}

	// Secret fields.
	for name, si := range in.Secrets {
		f, ok := fields[name]
		if !ok {
			return merged{}, &spec.ValidationError{Field: name, Message: "unknown field"}
		}
		if !f.Type.IsSecret() {
			return merged{}, &spec.ValidationError{Field: name, Message: "not a secret field; put it in config"}
		}
		switch si.Op {
		case spec.SecretKeep:
		case spec.SecretSet:
			if si.Value == "" {
				continue
			}
			delete(keptTyped, name)
			if len(si.Value) > maxSecretLen {
				return merged{}, &spec.ValidationError{Field: name, Message: "too long"}
			}
			if !haveKey {
				return merged{}, spec.ErrSecretKeyMissing
			}
			m.typed[name] = si.Value
			delete(m.refs, name)
			changed[name] = true
		case spec.SecretRef:
			if _, err := ParseRef(si.Ref); err != nil {
				return merged{}, &spec.ValidationError{Field: name, Message: err.Error()}
			}
			if m.refs[name] != si.Ref {
				changed[name] = true
			}
			m.refs[name] = si.Ref
			if _, had := m.typed[name]; had {
				changed[name] = true
			}
			delete(m.typed, name)
			delete(keptTyped, name)
		case spec.SecretClear:
			_, hadT := m.typed[name]
			_, hadR := m.refs[name]
			if hadT || hadR {
				changed[name] = true
			}
			delete(m.typed, name)
			delete(m.refs, name)
			delete(keptTyped, name)
		default:
			return merged{}, &spec.ValidationError{Field: name, Message: "unknown secret operation"}
		}
	}
	for _, f := range k.Fields {
		if !f.Type.IsSecret() || !f.Required {
			continue
		}
		_, t := m.typed[f.Name]
		_, r := m.refs[f.Name]
		if !t && !r {
			return merged{}, &spec.ValidationError{Field: f.Name, Message: "required"}
		}
	}
	// A typed-in secret kept from the stored row must not follow a changed
	// destination: otherwise Test or Update with a new url/host would send
	// the saved password to a server of the caller's choice, and the
	// write-only promise (integrations.md §8) would not hold.
	if old != nil {
		if moved := destinationChanged(k, old.Config, m.config); moved != "" {
			for _, name := range sortedKeys(keptTyped) {
				if _, still := m.typed[name]; still {
					return merged{}, &spec.ValidationError{Field: name, Message: "must be entered again because " + moved + " changed"}
				}
			}
		}
	}
	for name, ref := range m.refs {
		m.config[name] = map[string]any{spec.RefKey: ref}
	}
	if k.Category == spec.CategorySecretManager {
		n, _ := m.config[spec.ProviderNameField].(string)
		if n == spec.ProviderFiles {
			return merged{}, &spec.ValidationError{Field: spec.ProviderNameField, Message: `"files" is the built-in mounted-files provider`}
		}
		// The name is the provider id references use. Renaming in place
		// would leave the old provider registered (and its credentials
		// live) under a name the store no longer knows, so it is fixed.
		if old != nil {
			if was, _ := old.Config[spec.ProviderNameField].(string); was != "" && was != n {
				return merged{}, &spec.ValidationError{Field: spec.ProviderNameField, Message: "cannot be changed; add a new secret manager with the new name and delete this one"}
			}
		}
	}
	m.changed = sortedKeys(changed)
	return m, nil
}

// destinationFields are the non-secret fields, across all kinds, that pick
// where a credential is sent or how it is protected on the way (address,
// port, TLS and SASL settings). Kept local until spec.Field can mark them.
var destinationFields = map[string]bool{
	"url": true, "host": true, "port": true, "brokers": true, "bootstrap": true,
	"account": true, "account_id": true, "address": true, "vault_url": true,
	"server_name": true, "region": true, "tenant_id": true, "project": true,
	"sslmode": true, "tls": true, "protocol": true, "mechanism": true,
}

// destinationChanged returns the first field of k (in field order) that
// picks the destination and differs between the stored and the merged
// config, or "" when none does.
func destinationChanged(k spec.Kind, oldCfg, newCfg map[string]any) string {
	for _, f := range k.Fields {
		if f.Type.IsSecret() || !destinationFields[f.Name] {
			continue
		}
		a, _ := oldCfg[f.Name].(string)
		b, _ := newCfg[f.Name].(string)
		if a == "" {
			a = f.Default
		}
		if b == "" {
			b = f.Default
		}
		if a != b {
			return f.Name
		}
	}
	return ""
}

// refOf reads a stored {"$ref": "..."} value.
func refOf(v any) (string, bool) {
	mm, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	ref, ok := mm[spec.RefKey].(string)
	return ref, ok && ref != ""
}

// textConfig returns the non-secret fields of k from cfg, with defaults.
func textConfig(k spec.Kind, cfg map[string]any) map[string]string {
	out := map[string]string{}
	for _, f := range k.Fields {
		if f.Type.IsSecret() {
			continue
		}
		s, _ := cfg[f.Name].(string)
		if s == "" {
			s = f.Default
		}
		if s != "" {
			out[f.Name] = s
		}
	}
	return out
}

// viewConfig is what a View shows of a stored config: the kind's non-secret
// fields and the references of its secret fields. Anything else is dropped,
// so a value stored by mistake can never reach a response.
func viewConfig(k spec.Kind, known bool, cfg map[string]any) map[string]any {
	out := map[string]any{}
	if !known {
		return out
	}
	for _, f := range k.Fields {
		v, ok := cfg[f.Name]
		if !ok {
			continue
		}
		if f.Type.IsSecret() {
			if ref, ok := refOf(v); ok {
				out[f.Name] = map[string]any{spec.RefKey: ref}
			}
			continue
		}
		if s, ok := v.(string); ok {
			out[f.Name] = s
		}
	}
	return out
}
