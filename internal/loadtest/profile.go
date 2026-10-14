package loadtest

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ParamKind is the type of one profile parameter.
type ParamKind int

// Parameter kinds.
const (
	KindInt ParamKind = iota
	KindIntList
	KindDuration
	KindFloat
	KindBool
)

func (k ParamKind) String() string {
	switch k {
	case KindInt:
		return "int"
	case KindIntList:
		return "int list"
	case KindDuration:
		return "duration"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	}
	return "unknown"
}

// ParamSpec declares one parameter of a profile: its type, its default for a
// full run and for a -short (CI smoke) run, and the smallest allowed value.
type ParamSpec struct {
	Name  string
	Kind  ParamKind
	Full  string // default for a full run
	Short string // default for a -short run; "" = same as Full
	Min   float64
	Help  string
}

// Params are the validated parameters of one profile run.
type Params struct {
	raw    map[string]string
	values map[string]any
}

// ErrBadParam wraps every parameter parse or validation failure.
var ErrBadParam = errors.New("loadtest: bad parameter")

// Override is one key=value parameter override from the command line. Profile
// is "" when the override applies to every selected profile that declares
// the key.
type Override struct {
	Profile string
	Key     string
	Value   string
}

// ParseOverrides parses "key=value" and "profile.key=value" strings. Empty
// keys or values, a missing '=' and the same key given twice are errors.
func ParseOverrides(args []string) ([]Override, error) {
	seen := map[string]bool{}
	out := make([]Override, 0, len(args))
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("%w: %q: want key=value", ErrBadParam, a)
		}
		if v == "" {
			return nil, fmt.Errorf("%w: %q: empty value", ErrBadParam, a)
		}
		o := Override{Key: k, Value: v}
		if p, key, dotted := strings.Cut(k, "."); dotted {
			if p == "" || key == "" {
				return nil, fmt.Errorf("%w: %q: want profile.key=value", ErrBadParam, a)
			}
			o.Profile, o.Key = p, key
		}
		id := o.Profile + "." + o.Key
		if seen[id] {
			return nil, fmt.Errorf("%w: %q given twice", ErrBadParam, k)
		}
		seen[id] = true
		out = append(out, o)
	}
	return out, nil
}

// ParseParams builds the parameters of a profile from its specs, the chosen
// defaults (short or full) and the overrides that apply to it. An override
// naming a key the profile does not declare is an error only when it names
// this profile explicitly; CheckOverrides reports keys no profile declares.
func ParseParams(profile string, specs []ParamSpec, short bool, overrides []Override) (Params, error) {
	byName := map[string]ParamSpec{}
	for _, s := range specs {
		byName[s.Name] = s
	}
	raw := map[string]string{}
	for _, s := range specs {
		v := s.Full
		if short && s.Short != "" {
			v = s.Short
		}
		raw[s.Name] = v
	}
	for _, o := range overrides {
		if o.Profile != "" && o.Profile != profile {
			continue
		}
		if _, ok := byName[o.Key]; !ok {
			if o.Profile == profile {
				return Params{}, fmt.Errorf("%w: profile %s has no parameter %q (known: %s)", ErrBadParam, profile, o.Key, knownNames(specs))
			}
			continue
		}
		raw[o.Key] = o.Value
	}
	p := Params{raw: raw, values: map[string]any{}}
	for _, s := range specs {
		v, err := parseValue(s, raw[s.Name])
		if err != nil {
			return Params{}, fmt.Errorf("%w: %s.%s=%q: %v", ErrBadParam, profile, s.Name, raw[s.Name], err)
		}
		p.values[s.Name] = v
	}
	return p, nil
}

// CheckOverrides reports an override whose key no selected profile declares,
// or whose profile prefix names no selected profile, so a typo never silently
// runs the defaults.
func CheckOverrides(profiles []Profile, overrides []Override) error {
	for _, o := range overrides {
		found := false
		for _, pr := range profiles {
			if o.Profile != "" && o.Profile != pr.Name {
				continue
			}
			if slices.ContainsFunc(pr.Params, func(s ParamSpec) bool { return s.Name == o.Key }) {
				found = true
				break
			}
		}
		if !found {
			name := o.Key
			if o.Profile != "" {
				name = o.Profile + "." + o.Key
			}
			return fmt.Errorf("%w: no selected profile has parameter %q", ErrBadParam, name)
		}
	}
	return nil
}

func knownNames(specs []ParamSpec) string {
	names := make([]string, 0, len(specs))
	for _, s := range specs {
		names = append(names, s.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func parseValue(s ParamSpec, v string) (any, error) {
	checkMin := func(f float64) error {
		if f < s.Min {
			return fmt.Errorf("must be >= %v", s.Min)
		}
		return nil
	}
	switch s.Kind {
	case KindInt:
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, errors.New("want an integer")
		}
		return n, checkMin(float64(n))
	case KindIntList:
		parts := strings.Split(v, ",")
		out := make([]int, 0, len(parts))
		for _, part := range parts {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return nil, errors.New("want a comma-separated list of integers")
			}
			if err := checkMin(float64(n)); err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return out, nil
	case KindDuration:
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, errors.New("want a Go duration such as 250ms or 2s")
		}
		return d, checkMin(float64(d))
	case KindFloat:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f != f {
			return nil, errors.New("want a number")
		}
		return f, checkMin(f)
	case KindBool:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, errors.New("want true or false")
		}
		return b, nil
	}
	return nil, fmt.Errorf("unknown kind %d", s.Kind)
}

// Int returns an int parameter (0 when the name is not an int parameter).
func (p Params) Int(name string) int { v, _ := p.values[name].(int); return v }

// Ints returns an int-list parameter.
func (p Params) Ints(name string) []int { v, _ := p.values[name].([]int); return slices.Clone(v) }

// Dur returns a duration parameter.
func (p Params) Dur(name string) time.Duration { v, _ := p.values[name].(time.Duration); return v }

// Float returns a float parameter.
func (p Params) Float(name string) float64 { v, _ := p.values[name].(float64); return v }

// Bool returns a bool parameter.
func (p Params) Bool(name string) bool { v, _ := p.values[name].(bool); return v }

// Raw returns the parameters as given (after defaults), for the report.
func (p Params) Raw() map[string]string {
	out := make(map[string]string, len(p.raw))
	for k, v := range p.raw {
		out[k] = v
	}
	return out
}
