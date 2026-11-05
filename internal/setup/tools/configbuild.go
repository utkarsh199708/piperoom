package setuptools

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/setup/knowledge"
)

// The connector config builder (row U3b): a setup adds SMTs, predicates and a
// sink to a connector config only through the knowledge index, the same
// lookup setup_config_help answers with. A class the index does not know, a
// key its entry does not list, a value outside the key's allowed values, or a
// credential that is not a ${secret:name} reference is refused before the
// config can reach a proposal; an unknown class names the nearest known one.

// ErrConfigBuild is wrapped by every refusal of the config builder.
var ErrConfigBuild = errors.New("setuptools: connector config")

// SinkRegistrationAction is the action a sink connector is registered by:
// Q5c's connect_create_connector, over the room's Connect client (in a setup
// room, the remote client of internal/setup/remoteclients).
const SinkRegistrationAction = actions.CreateConnectorName

var (
	// aliasRe is the shape of a transform or predicate alias.
	aliasRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	// buildKeyRe is the shape of a Connect config key (Q5c accepts the same).
	buildKeyRe = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)
	// secretRefRe is one whole ${secret:name} reference.
	secretRefRe = regexp.MustCompile(`^\$\{secret:[A-Za-z0-9._-]{1,128}\}$`)
	// urlPasswordRe finds a password inside a URL or DSN value: a userinfo
	// with a password, or a password= / pwd= parameter.
	// providerRefRe is a Connect config-provider reference anywhere in a
	// value (${file:...}, ${env:...}, ${secret:...}); ${topic} is not one.
	providerRefRe = regexp.MustCompile(`\$\{[A-Za-z0-9_.-]+:`)
	urlPasswordRe = regexp.MustCompile(`(?i)(//[^/@\s]*:[^/@\s]*@|[?&;](password|pwd)=)`)
)

const (
	maxBuildTransforms = 16
	maxBuildValueBytes = 8192
	maxSinkTopics      = 64
)

// sinkCommonKeys are the Kafka Connect keys every sink connector takes beside
// its own (the Connect sink connector configs); a prefix ending in "." admits
// the converter's and the consumer override's own keys.
var sinkCommonKeys = []string{
	"tasks.max", "topics", "topics.regex",
	"key.converter", "value.converter", "header.converter",
	"key.converter.", "value.converter.",
	"errors.tolerance", "errors.retry.timeout", "errors.retry.delay.max.ms",
	"errors.log.enable", "errors.log.include.messages",
	"errors.deadletterqueue.topic.name", "errors.deadletterqueue.topic.replication.factor",
	"errors.deadletterqueue.context.headers.enable",
	"consumer.override.",
}

// Versions are the releases the builder checks entries against, by index
// component ("debezium", "kafka", "confluent", ...). Debezium is required:
// it decides the Q1b renames. A component without a release is not
// range-checked.
type Versions map[string]string

// Transform is one SMT to add under transforms.<Alias>.
type Transform struct {
	Alias string
	// Class is the class as it will be configured (a $Key/$Value variant
	// where the entry has variants), or an index name.
	Class  string
	Config map[string]string
	// Predicate is the alias of a Predicate in the same call; Negate sets
	// transforms.<Alias>.negate.
	Predicate string
	Negate    bool
}

// Predicate is one predicate to add under predicates.<Alias>.
type Predicate struct {
	Alias  string
	Class  string
	Config map[string]string
}

// ConfigBuilder adds index-checked keys to connector configs. It reads only
// the shipped index.
type ConfigBuilder struct {
	ix       *knowledge.Index
	versions Versions
}

// NewConfigBuilder returns a builder over ix for the given versions.
func NewConfigBuilder(ix *knowledge.Index, v Versions) (*ConfigBuilder, error) {
	if ix == nil {
		return nil, fmt.Errorf("%w: the knowledge index is required", ErrConfigBuild)
	}
	if strings.TrimSpace(v["debezium"]) == "" {
		return nil, fmt.Errorf("%w: the Debezium release is required (it decides the renamed classes)", ErrConfigBuild)
	}
	return &ConfigBuilder{ix: ix, versions: maps.Clone(v)}, nil
}

// AddTransforms returns a copy of cfg with the predicates and transforms
// added, appended to any transforms or predicates list cfg already has. cfg is
// not changed. A key that cfg already holds is refused, never overwritten.
func (b *ConfigBuilder) AddTransforms(cfg map[string]string, preds []Predicate, ts []Transform) (map[string]string, error) {
	if len(ts)+len(preds) > maxBuildTransforms {
		return nil, fmt.Errorf("%w: at most %d transforms and predicates", ErrConfigBuild, maxBuildTransforms)
	}
	out := maps.Clone(cfg)
	if out == nil {
		out = map[string]string{}
	}
	set := func(k, v string) error {
		if _, ok := out[k]; ok {
			return fmt.Errorf("%w: %s is already set", ErrConfigBuild, k)
		}
		out[k] = v
		return nil
	}
	predAliases := listOf(out["predicates"])
	for _, p := range preds {
		if err := checkAlias(p.Alias, predAliases); err != nil {
			return nil, err
		}
		class, e, err := b.resolveClass(knowledge.KindPredicate, p.Class)
		if err != nil {
			return nil, err
		}
		prefix := "predicates." + p.Alias + "."
		if err := set(prefix+"type", class); err != nil {
			return nil, err
		}
		if err := b.addKeys(out, prefix, e, p.Config, nil); err != nil {
			return nil, err
		}
		if err := requireKeys(out, prefix, e); err != nil {
			return nil, err
		}
		predAliases = append(predAliases, p.Alias)
	}
	tAliases := listOf(out["transforms"])
	for _, t := range ts {
		if err := checkAlias(t.Alias, tAliases); err != nil {
			return nil, err
		}
		class, e, err := b.resolveClass(knowledge.KindSMT, t.Class)
		if err != nil {
			return nil, err
		}
		prefix := "transforms." + t.Alias + "."
		if err := set(prefix+"type", class); err != nil {
			return nil, err
		}
		if t.Predicate != "" {
			if !slices.Contains(predAliases, t.Predicate) {
				return nil, fmt.Errorf("%w: transform %s names predicate %q, which is not configured", ErrConfigBuild, t.Alias, clipName(t.Predicate))
			}
			if err := set(prefix+"predicate", t.Predicate); err != nil {
				return nil, err
			}
			if t.Negate {
				if err := set(prefix+"negate", "true"); err != nil {
					return nil, err
				}
			}
		} else if t.Negate {
			return nil, fmt.Errorf("%w: transform %s sets negate without a predicate", ErrConfigBuild, t.Alias)
		}
		if err := b.addKeys(out, prefix, e, t.Config, []string{"predicate", "negate", "type"}); err != nil {
			return nil, err
		}
		if err := requireKeys(out, prefix, e); err != nil {
			return nil, err
		}
		tAliases = append(tAliases, t.Alias)
	}
	if len(tAliases) > 0 {
		out["transforms"] = strings.Join(tAliases, ",")
	}
	if len(predAliases) > 0 {
		out["predicates"] = strings.Join(predAliases, ",")
	}
	return out, nil
}

// SinkOptions are the typed inputs of a sink connector config.
type SinkOptions struct {
	// Sink is the sink's index name or connector class.
	Sink string
	// Topics are the topics the sink reads (topics); TopicsRegex is the
	// alternative (topics.regex). Exactly one is set.
	Topics      []string
	TopicsRegex string
	// Config holds the sink's own keys and the Connect sink keys
	// (tasks.max, converters, errors.*, consumer.override.*).
	Config     map[string]string
	Predicates []Predicate
	Transforms []Transform
}

// SinkConfig builds a Kafka Connect sink connector config: connector.class
// from the index, the topics, the sink's keys checked against its entry
// (required keys present, credentials as ${secret:} references, no password
// inside a URL), then the transforms through AddTransforms.
func (b *ConfigBuilder) SinkConfig(o SinkOptions) (map[string]string, error) {
	class, e, err := b.resolveClass(knowledge.KindSink, o.Sink)
	if err != nil {
		return nil, err
	}
	if e.Runtime != knowledge.RuntimeConnect {
		return nil, fmt.Errorf("%w: %s runs on %s, not as a Kafka Connect sink connector", ErrConfigBuild, e.Name, e.Runtime)
	}
	switch {
	case len(o.Topics) > 0 && o.TopicsRegex != "":
		return nil, fmt.Errorf("%w: set topics or topics.regex, not both", ErrConfigBuild)
	case len(o.Topics) == 0 && o.TopicsRegex == "":
		return nil, fmt.Errorf("%w: a sink needs topics or topics.regex", ErrConfigBuild)
	case len(o.Topics) > maxSinkTopics:
		return nil, fmt.Errorf("%w: at most %d topics", ErrConfigBuild, maxSinkTopics)
	}
	out := map[string]string{"connector.class": class}
	if len(o.Topics) > 0 {
		for _, t := range o.Topics {
			if !topicRe.MatchString(t) {
				return nil, fmt.Errorf("%w: topic %q is not a Kafka topic name", ErrConfigBuild, clipName(t))
			}
		}
		out["topics"] = strings.Join(o.Topics, ",")
	} else {
		if len(o.TopicsRegex) > 512 || strings.ContainsAny(o.TopicsRegex, "\r\n${}") {
			return nil, fmt.Errorf("%w: topics.regex is not a topic pattern", ErrConfigBuild)
		}
		out["topics.regex"] = o.TopicsRegex
	}
	for _, k := range []string{"topics", "topics.regex", "connector.class", "name"} {
		if _, ok := o.Config[k]; ok {
			return nil, fmt.Errorf("%w: %s is set by the builder, not in Config", ErrConfigBuild, k)
		}
	}
	if err := b.addKeys(out, "", e, o.Config, sinkCommonKeys); err != nil {
		return nil, err
	}
	if err := requireKeys(out, "", e); err != nil {
		return nil, err
	}
	return b.AddTransforms(out, o.Predicates, o.Transforms)
}

// SinkRegistrationParams are the params of the connect_create_connector step
// (Q5c) that registers a sink connector built by SinkConfig. The connector
// name must be one of the pipeline's connectors in owners.yaml, which the
// action's scope check enforces; replace is for a re-register after a
// connect_delete_connector step of the same proposal.
func SinkRegistrationParams(connector string, cfg map[string]string, replace bool) (json.RawMessage, error) {
	if !connectorNameRe.MatchString(connector) {
		return nil, fmt.Errorf("%w: connector name %q", ErrConfigBuild, clipName(connector))
	}
	if strings.TrimSpace(cfg["connector.class"]) == "" {
		return nil, fmt.Errorf("%w: the config has no connector.class", ErrConfigBuild)
	}
	if n, ok := cfg["name"]; ok && n != connector {
		return nil, fmt.Errorf("%w: the config's name key differs from the connector name", ErrConfigBuild)
	}
	p := struct {
		Connector string            `json:"connector"`
		Config    map[string]string `json:"config"`
		Replace   bool              `json:"replace,omitempty"`
	}{connector, cfg, replace}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding the step params: %v", ErrConfigBuild, err)
	}
	return raw, nil
}

var (
	topicRe         = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	connectorNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// resolveClass turns a class or index name into the class to configure and
// its entry. A Q1b rename resolves to the new class at the builder's Debezium
// release; an unknown class is refused with the nearest known entries.
func (b *ConfigBuilder) resolveClass(kind knowledge.Kind, name string) (string, knowledge.Entry, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxHelpName || strings.Contains(name, "${") || strings.ContainsAny(name, "{} \r\n\t") {
		return "", knowledge.Entry{}, fmt.Errorf("%w: %s class %q is not a class name", ErrConfigBuild, kind, clipName(name))
	}
	var (
		class string
		e     knowledge.Entry
	)
	if looksLikeClass(name) {
		res, err := b.ix.ResolveClass(name, b.versions["debezium"])
		switch {
		case errors.Is(err, knowledge.ErrUnknownEntry):
			return "", knowledge.Entry{}, b.unknownClass(kind, name, err)
		case err != nil:
			return "", knowledge.Entry{}, fmt.Errorf("%w: %s: %w", ErrConfigBuild, clipName(name), err)
		case res.Entry == nil:
			return "", knowledge.Entry{}, b.unknownClass(kind, name, fmt.Errorf("%w: %s resolves to %s", knowledge.ErrUnknownEntry, clipName(name), res.Class))
		}
		class, e = res.Class, *res.Entry
	} else {
		var err error
		e, err = b.ix.Lookup(kind, name)
		if err != nil {
			return "", knowledge.Entry{}, b.unknownClass(kind, name, err)
		}
		if len(e.Variants) > 0 {
			return "", knowledge.Entry{}, fmt.Errorf("%w: %s is abstract; name a variant: %s$%s", ErrConfigBuild, e.Name, e.Class, strings.Join(e.Variants, " or $"))
		}
		class = e.Class
	}
	if e.Kind != kind {
		return "", knowledge.Entry{}, fmt.Errorf("%w: %s is a %s, not a %s", ErrConfigBuild, e.Name, e.Kind, kind)
	}
	if class == "" {
		return "", knowledge.Entry{}, fmt.Errorf("%w: %s has no Kafka Connect class (sink type %s)", ErrConfigBuild, e.Name, e.SinkType)
	}
	if rel := b.versions[e.Component]; rel != "" {
		sup, err := e.Supports(rel)
		if err != nil {
			return "", knowledge.Entry{}, fmt.Errorf("%w: %s release %q: %w", ErrConfigBuild, e.Component, clipName(rel), err)
		}
		if sup == knowledge.Removed {
			msg := fmt.Sprintf("%s is not in %s %s (verified %s)", e.Name, e.Component, rel, e.Versions)
			if e.Successor != "" {
				msg += "; use " + e.Successor
			}
			return "", knowledge.Entry{}, fmt.Errorf("%w: %s", ErrConfigBuild, msg)
		}
	}
	return class, e, nil
}

func (b *ConfigBuilder) unknownClass(kind knowledge.Kind, name string, err error) error {
	near := Nearest(b.ix, kind, name, maxNearest)
	if len(near) == 0 {
		return fmt.Errorf("%w: %w", ErrConfigBuild, err)
	}
	names := make([]string, len(near))
	for i, n := range near {
		names[i] = n.Class
		if names[i] == "" {
			names[i] = n.Name
		}
	}
	return fmt.Errorf("%w: unknown %s class %q; nearest known: %s: %w", ErrConfigBuild, kind, clipName(name), strings.Join(names, ", "), err)
}

// addKeys checks each key of in against entry e (or the extra names, a prefix
// ending in "." matching a group) and sets prefix+key in out. Keys are added in
// sorted order so an error names the same key every run.
func (b *ConfigBuilder) addKeys(out map[string]string, prefix string, e knowledge.Entry, in map[string]string, extra []string) error {
	keys := slices.Collect(maps.Keys(in))
	sort.Strings(keys)
	for _, k := range keys {
		v := in[k]
		if !buildKeyRe.MatchString(k) || len(k) > 128 {
			return fmt.Errorf("%w: %s key %q is not a config key", ErrConfigBuild, e.Name, clipName(k))
		}
		if len(v) > maxBuildValueBytes || strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("%w: %s.%s: the value is not one config value", ErrConfigBuild, e.Name, k)
		}
		key, known := e.Key(k)
		if !known && !extraKey(extra, k) {
			return fmt.Errorf("%w: %s has no key %q%s", ErrConfigBuild, e.Name, clipName(k), nearestKey(e, k))
		}
		// The index is the authority on its own keys (a knowledge test holds
		// every Secret key to actions.LooksSecret), so a listed key that is
		// not Secret is a credential only when its name holds a credential
		// word: primary.key.mode is not one, although its bare "key" segment
		// trips LooksSecret. A key outside the entry gets the whole rule.
		secret := (known && key.Secret) || credentialWord(k) || (!known && credentialKey(k))
		switch {
		case secret && !secretRefRe.MatchString(v):
			return fmt.Errorf("%w: %s.%s is a credential; its value must be a ${secret:name} reference", ErrConfigBuild, e.Name, k)
		case !secret && providerRefRe.MatchString(v) && !(converterKey(k) && secretRefRe.MatchString(v)):
			// Only a credential (or a converter's registry credential) carries
			// a reference: a ${file:} or ${env:} elsewhere would let Connect
			// echo a server-side value (the Q5c rule).
			return fmt.Errorf("%w: %s.%s: a ${provider:...} reference is accepted on a credential key only", ErrConfigBuild, e.Name, k)
		}
		if known && key.CredentialsInValue && urlPasswordRe.MatchString(v) {
			return fmt.Errorf("%w: %s.%s embeds a password; use the entry's user and password keys with a ${secret:name} reference", ErrConfigBuild, e.Name, k)
		}
		if known && len(key.Values) > 0 && !slices.Contains(key.Values, v) {
			return fmt.Errorf("%w: %s.%s must be one of %s", ErrConfigBuild, e.Name, k, strings.Join(key.Values, ", "))
		}
		if _, ok := out[prefix+k]; ok {
			return fmt.Errorf("%w: %s%s is already set", ErrConfigBuild, prefix, k)
		}
		out[prefix+k] = v
	}
	return nil
}

// credentialKey is the shared actions.LooksSecret rule with Q5c's exemptions
// for the bare "key" segment only (a converter class, the record-key
// settings); a key holding a credential word is a credential under any prefix.
func credentialKey(k string) bool {
	if credentialWord(k) {
		return true
	}
	lk := strings.ToLower(k)
	if !actions.LooksSecret(k) {
		return false
	}
	if converterKey(k) {
		return false
	}
	for _, suffix := range []string{"key.ignore", "message.key.columns", "key.format", "key.fields"} {
		if lk == suffix || strings.HasSuffix(lk, "."+suffix) {
			return false
		}
	}
	return true
}

// credentialWord reports whether k holds one of the credential words.
func credentialWord(k string) bool {
	lk := strings.ToLower(k)
	for _, w := range credentialWords {
		if strings.Contains(lk, w) {
			return true
		}
	}
	return false
}

// credentialWords are the substrings actions.LooksSecret documents.
var credentialWords = []string{"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private", "connectionstring"}

func converterKey(k string) bool {
	lk := strings.ToLower(k)
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return true
		}
	}
	return false
}

// requireKeys refuses a config that lacks one of e's required keys (under
// prefix), by its name or a former name.
func requireKeys(out map[string]string, prefix string, e knowledge.Entry) error {
	for _, k := range e.Keys {
		if !k.Required {
			continue
		}
		if _, ok := out[prefix+k.Name]; ok {
			continue
		}
		if slices.ContainsFunc(k.Formerly, func(f string) bool { _, ok := out[prefix+f]; return ok }) {
			continue
		}
		return fmt.Errorf("%w: %s needs %s", ErrConfigBuild, e.Name, k.Name)
	}
	return nil
}

func extraKey(extra []string, k string) bool {
	for _, x := range extra {
		if x == k || (strings.HasSuffix(x, ".") && strings.HasPrefix(k, x) && len(k) > len(x)) {
			return true
		}
	}
	return false
}

// nearestKey names the closest key of e to an unknown one, or "".
func nearestKey(e knowledge.Entry, k string) string {
	best, bestD := "", -1
	for _, key := range e.Keys {
		if d := editDistance(strings.ToLower(k), strings.ToLower(key.Name)); bestD < 0 || d < bestD {
			best, bestD = key.Name, d
		}
	}
	if best == "" {
		return ""
	}
	return "; nearest known key: " + best
}

func checkAlias(alias string, taken []string) error {
	if !aliasRe.MatchString(alias) {
		return fmt.Errorf("%w: alias %q (a letter, then letters, digits, '_' or '-')", ErrConfigBuild, clipName(alias))
	}
	if slices.Contains(taken, alias) {
		return fmt.Errorf("%w: alias %s is already used", ErrConfigBuild, alias)
	}
	return nil
}

// listOf splits a Connect list value.
func listOf(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
