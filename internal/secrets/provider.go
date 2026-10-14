package secrets

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// Value is a resolved secret. Data is held as netsec.Secret so it never prints.
type Value struct {
	// Data is the whole secret: the raw text of a plain secret, or a JSON
	// object for a structured one (Vault data, Kubernetes data, an AWS JSON
	// secret). ExtractKey picks one field.
	Data netsec.Secret
	// Version is the provider's version label ("" when it has none). Safe to show.
	Version string
	// ExpiresAt is when a lease ends; zero = no lease (static secret).
	ExpiresAt time.Time
	// LeaseID names a renewable lease (Vault dynamic credentials); "" = none.
	LeaseID string
	// Renewable reports whether the lease can be extended.
	Renewable bool
}

// Provider reads secrets from one manager. Every method takes a context with
// a deadline. Implementations must be safe for concurrent use, must never
// put a secret value into an error, a log line or a String() result, and
// must give their exported types a String method if they hold a secret in
// an unexported field (fmt prints unexported fields raw).
type Provider interface {
	// Resolve returns the whole secret named by ref.Path at ref.Version
	// (latest when ""). It ignores ref.Key: the Registry applies the key
	// with ExtractKey, so two keys of one secret (a Vault lease's username
	// and password) come from a single read.
	Resolve(ctx context.Context, ref Ref) (Value, error)
	// List returns secret NAMES under prefix for the picker, at most limit.
	// Never values. Providers that cannot list return ErrListUnsupported.
	List(ctx context.Context, prefix string, limit int) ([]string, error)
	// Keys returns the field names of a JSON or key/value secret, sorted, for
	// the key picker; [] for a plain secret. Never values.
	Keys(ctx context.Context, ref Ref) ([]string, error)
	// Check runs the card's connection test (secret-providers.md §5).
	// sample, when not nil, is a reference to read; only its version and
	// key names are reported.
	Check(ctx context.Context, sample *Ref) []spec.Check
	// Close releases clients and stops background work (renewal, polling).
	Close()
}

// LeaseRenewer is implemented by providers whose values carry renewable
// leases (Vault). The Registry calls it at two thirds of the lease's time
// to live and returns the new expiry.
type LeaseRenewer interface {
	RenewLease(ctx context.Context, v Value) (expiresAt time.Time, err error)
}

// ChangeNotifier is implemented by providers that notice a value changed
// on their own (mounted files). The Registry sets the callback once, on
// Register; path is the Ref.Path whose value changed.
type ChangeNotifier interface {
	SetOnChange(fn func(path string))
}

// ExtractKey returns field key of a structured secret: a JSON object (string
// values as is; numbers and booleans as their JSON text) or "key=value"
// lines (the env-file format many file mounts use). key "" returns data
// unchanged. Errors never quote the data.
func ExtractKey(data netsec.Secret, key string) (netsec.Secret, error) {
	if key == "" {
		return data, nil
	}
	fields, ok := structuredFields(data.Reveal())
	if !ok {
		return "", fmt.Errorf("%w: the secret is not a JSON object or key=value list", ErrKeyNotFound)
	}
	v, ok := fields[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return netsec.Secret(v), nil
}

// KeysOf returns the sorted field names of a JSON-object secret, or [] for
// any other. It deliberately ignores the key=value form: a plain password
// such as "dGVzdA==" would parse as key "dGVzdA" and leak into the picker.
func KeysOf(data netsec.Secret) []string {
	t := strings.TrimSpace(data.Reveal())
	if !strings.HasPrefix(t, "{") {
		return []string{}
	}
	fields, ok := structuredFields(t)
	if !ok {
		return []string{}
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func structuredFields(s string) (map[string]string, bool) {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(t, "{") {
		var raw map[string]json.RawMessage
		// The decode error is dropped on purpose: its text can quote the data.
		if json.Unmarshal([]byte(t), &raw) != nil {
			return nil, false
		}
		out := make(map[string]string, len(raw))
		for k, v := range raw {
			var str string
			if json.Unmarshal(v, &str) == nil {
				out[k] = str
				continue
			}
			out[k] = string(v)
		}
		return out, true
	}
	return keyValueLines(t)
}

// keyValueLines parses "k=v" lines; blank lines and "#" comments are
// skipped. Any other line makes the text a plain secret.
func keyValueLines(s string) (map[string]string, bool) {
	if !strings.Contains(s, "=") {
		return nil, false
	}
	out := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(s))
	sc.Buffer(make([]byte, 64*1024), maxSecretBytes)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" || strings.ContainsAny(k, " \t\"'") {
			return nil, false
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
		out[k] = v
	}
	if sc.Err() != nil || len(out) == 0 {
		return nil, false
	}
	return out, true
}

// maxSecretBytes caps one secret read from any provider.
const maxSecretBytes = 1 << 20

// Check ids shared by the providers (secret-providers.md §5).
const (
	CheckReach     = "reach"
	CheckLogin     = "login"
	CheckRead      = "read_sample"
	CheckTooMuch   = "least_privilege"
	CheckLease     = "lease_renewable"
	CheckListNames = "list_names"
)

func pass(id, title, detail string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckPass, Detail: detail}
}

func fail(id, title, detail, fix string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckFail, Detail: detail, Fix: fix}
}

func warn(id, title, detail, fix string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckWarn, Detail: detail, Fix: fix}
}

func skipped(id, title, detail string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckSkipped, Detail: detail}
}

// checkTimeout is the per-check deadline (spec.Kind.Test).
const checkTimeout = 5 * time.Second

// sampleCheck reads sample through p and reports its version and whether
// the key is present. It never reports the value.
func sampleCheck(ctx context.Context, p Provider, sample *Ref) (spec.Check, Value, bool) {
	const title = "Can read the sample secret"
	if sample == nil {
		return skipped(CheckRead, title, "no sample reference given"), Value{}, false
	}
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	v, err := p.Resolve(cctx, *sample)
	if err != nil {
		return fail(CheckRead, title, errorDetail(err), "grant read on "+sample.Path+" to Piperoom's identity"), Value{}, false
	}
	detail := "read " + sample.Path
	if v.Version != "" {
		detail += ", version " + v.Version
	}
	if sample.Key != "" {
		if _, err := ExtractKey(v.Data, sample.Key); err != nil {
			return fail(CheckRead, title, detail+"; no key "+sample.Key+" (keys: "+strings.Join(KeysOf(v.Data), ", ")+")",
				"pick one of the secret's keys"), v, true
		}
		detail += "; has key " + sample.Key
	}
	return pass(CheckRead, title, detail), v, true
}

// errorDetail is a short, safe description of err for a check's Detail.
func errorDetail(err error) string {
	return truncate(err.Error(), 300)
}
