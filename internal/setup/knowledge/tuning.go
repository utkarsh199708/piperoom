package knowledge

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// The tuning index (U10a): entries of kind tuning, in index/tuning-*.yaml.
// Each groups the keys of one knob (the worker heap, max.batch.size, a
// source's snapshot fetch size, a producer override), their defaults as
// the source code sets them, where they are set (Scope), the source they
// apply to (Connector) and the symptoms that justify changing them. The
// plan proposes a tuning step only after one of those symptoms is seen,
// never by default (U10b), so an entry without symptoms fails the parse.

// Scopes of a tuning entry: where its keys are set.
const (
	// ScopeConnector keys go in the connector config, including the
	// producer.override.* and consumer.override.* client overrides.
	ScopeConnector = "connector"
	// ScopeWorker keys go in the Connect worker properties (on the
	// Debezium and Confluent images, the CONNECT_* environment variables
	// that become them).
	ScopeWorker = "worker"
	// ScopeEnv keys are environment variables of the worker process, such
	// as its JVM heap options.
	ScopeEnv = "env"
)

var (
	scopes = []string{ScopeConnector, ScopeWorker, ScopeEnv}
	// A tuning entry tunes Debezium or the Kafka runtime around it.
	tuningComponents = []string{"debezium", "kafka"}
	envKeyRe         = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	propKeyRe        = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

// checkTuning validates what is particular to a tuning entry: no class,
// no install, a known scope, keys spelt for that scope, a known source
// connector and at least one symptom.
func (e *Entry) checkTuning() error {
	switch {
	case e.Class != "" || e.SinkType != "" || len(e.Variants) > 0:
		return errors.New("a tuning entry has no class, sink_type or variants")
	case e.Install != "" || len(e.Archives) > 0 || e.PluginRef != "" || e.UnpinnedReason != "" || e.Successor != "":
		return errors.New("a tuning entry installs nothing: no install, archives, plugin_ref, unpinned_reason or successor")
	case !slices.Contains(tuningComponents, e.Component):
		return fmt.Errorf("a tuning entry's component is one of %s, got %q", strings.Join(tuningComponents, ", "), e.Component)
	case !slices.Contains(scopes, e.Scope):
		return fmt.Errorf("scope %q is not one of %s", e.Scope, strings.Join(scopes, ", "))
	case len(e.Keys) == 0:
		return errors.New("a tuning entry has at least one key")
	case len(e.Symptoms) == 0:
		return errors.New("a tuning entry has at least one symptom")
	}
	for _, s := range e.Symptoms {
		if strings.TrimSpace(s) == "" {
			return errors.New("empty symptom")
		}
	}
	if e.Connector != "" {
		if e.Component != "debezium" {
			return fmt.Errorf("connector %q: only a Debezium tuning entry names a source connector", e.Connector)
		}
		if !pluginConnectorKnown(e.Connector) {
			return fmt.Errorf("connector %q is not a connector in plugins.go", e.Connector)
		}
	}
	re := propKeyRe
	if e.Scope == ScopeEnv {
		re = envKeyRe
	}
	for _, k := range e.Keys {
		if k.Secret || k.CredentialsInValue {
			return fmt.Errorf("key %s: a tuning key is never a credential", k.Name)
		}
		for _, n := range append([]string{k.Name}, k.Formerly...) {
			if !re.MatchString(n) {
				return fmt.Errorf("key %q is not spelt as a key of scope %s (%s)", n, e.Scope, re)
			}
		}
	}
	return nil
}

// Client-key prefixes Lookup folds together: the per-connector override
// (producer.override.batch.size) and the worker-wide form
// (producer.batch.size) set the same client key.
var clientPrefixes = []struct{ prefix, client string }{
	{"producer.override.", "producer"},
	{"consumer.override.", "consumer"},
	{"producer.", "producer"},
	{"consumer.", "consumer"},
}

// debeziumServerSourcePrefix is how Debezium Server passes a source
// connector key (debezium.source.max.batch.size).
const debeziumServerSourcePrefix = "debezium.source."

// splitClientKey returns the Kafka client a key configures ("producer",
// "consumer" or "" for none) and the client's own key name.
func splitClientKey(key string) (client, rest string) {
	key = strings.TrimPrefix(key, debeziumServerSourcePrefix)
	for _, p := range clientPrefixes {
		if r, ok := strings.CutPrefix(key, p.prefix); ok && r != "" {
			return p.client, r
		}
	}
	return "", key
}

// tuningKeyMatches reports whether q names one of e's keys (or a former
// name of one). The client prefixes fold together, so producer.batch.size
// (worker) and producer.override.batch.size (connector) both find the
// producer batch entry, and a bare batch.size does too; a
// debezium.source. prefix is dropped, so a Debezium Server key finds the
// connector key it sets.
func tuningKeyMatches(e Entry, q string) bool {
	qc, qr := splitClientKey(q)
	if qr == "" {
		return false
	}
	for _, k := range e.Keys {
		for _, n := range append([]string{k.Name}, k.Formerly...) {
			kc, kr := splitClientKey(n)
			if kr == qr && (kc == qc || qc == "") {
				return true
			}
		}
	}
	return false
}

// Tuning returns the tuning entries that apply to a source connector:
// those for every source and those for that connector, in index order.
// An empty connector returns every tuning entry.
func (ix *Index) Tuning(connector string) []Entry {
	var out []Entry
	for _, e := range ix.entries {
		if e.Kind != KindTuning {
			continue
		}
		if connector == "" || e.Connector == "" || e.Connector == connector {
			out = append(out, e)
		}
	}
	return out
}
