package managed

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
)

// The MSK Connect worker configuration (design §3.1): rendered from this
// template under a key allow-list, never free text. MSK Connect manages the
// offset, config and status topics, the group, the bootstrap servers and the
// REST listener, and v1 bundles no config provider, so those keys are refused
// by name with the reason.
var workerTemplate = template.Must(template.New("msk-worker").Parse(
	`# Rendered by piperoom-setup from the MSK Connect worker template.
# Allow-listed keys only; MSK Connect manages the internal topics, the group
# and the listeners.
{{range .}}{{.Key}}={{.Value}}
{{end}}`))

// workerKeys are the allowed keys and the rule each value follows.
var workerKeys = map[string]func(string) bool{
	"key.converter":                  oneOf(converterClasses...),
	"value.converter":                oneOf(converterClasses...),
	"header.converter":               oneOf(headerConverterClasses...),
	"key.converter.schemas.enable":   oneOf("true", "false"),
	"value.converter.schemas.enable": oneOf("true", "false"),
	"offset.flush.interval.ms":       intIn(1000, 600000),
	"offset.flush.timeout.ms":        intIn(1000, 600000),
}

// converterClasses need no credentials and no registry URL: an Avro or
// Protobuf converter would, and is not in v1.
var converterClasses = []string{
	"org.apache.kafka.connect.json.JsonConverter",
	"org.apache.kafka.connect.storage.StringConverter",
	"org.apache.kafka.connect.converters.ByteArrayConverter",
}

var headerConverterClasses = []string{
	"org.apache.kafka.connect.storage.SimpleHeaderConverter",
	"org.apache.kafka.connect.storage.StringConverter",
}

// workerRefused are key prefixes MSK Connect owns or v1 does not bundle,
// with the reason the refusal gives.
var workerRefused = []struct{ prefix, why string }{
	{"config.providers", "v1 bundles no config provider (design §10 D6)"},
	{"offset.storage.", "MSK Connect manages the offset topic"},
	{"config.storage.", "MSK Connect manages the config topic"},
	{"status.storage.", "MSK Connect manages the status topic"},
	{"group.id", "MSK Connect sets the group"},
	{"bootstrap.servers", "the connector's cluster comes from the cluster ARN"},
	{"plugin.path", "MSK Connect installs the custom plugin"},
	{"rest.", "MSK Connect has no reachable REST listener"},
	{"listeners", "MSK Connect has no reachable REST listener"},
}

func oneOf(vals ...string) func(string) bool {
	return func(v string) bool {
		for _, x := range vals {
			if v == x {
				return true
			}
		}
		return false
	}
}

func intIn(lo, hi int) func(string) bool {
	return func(v string) bool {
		n, err := strconv.Atoi(v)
		return err == nil && n >= lo && n <= hi
	}
}

var workerKeyShape = regexp.MustCompile(`^[a-z][a-z0-9.]{0,63}$`)

// RenderWorkerProperties renders the worker properties for entries, or
// refuses a key that is not allow-listed or a value its rule rejects. No
// I/O; the same text is the preview and what is sent.
func RenderWorkerProperties(entries map[string]string) (string, error) {
	if len(entries) == 0 {
		return "", fmt.Errorf("%w: no worker properties", ErrParams)
	}
	type kv struct{ Key, Value string }
	list := make([]kv, 0, len(entries))
	for k, v := range entries {
		if !workerKeyShape.MatchString(k) {
			return "", fmt.Errorf("%w: worker key %s", ErrParams, strconv.Quote(clipText(k, 64)))
		}
		// Every allowed value is an enumeration or a bounded number, so no
		// credential can reach the file through a value.
		for _, r := range workerRefused {
			if strings.HasPrefix(k, r.prefix) {
				return "", fmt.Errorf("%w: worker key %s is refused: %s", ErrParams, k, r.why)
			}
		}
		rule, ok := workerKeys[k]
		if !ok {
			return "", fmt.Errorf("%w: worker key %s is not in the allow-list", ErrParams, k)
		}
		if !rule(v) {
			return "", fmt.Errorf("%w: worker key %s: value %s is not allowed", ErrParams, k, strconv.Quote(clipText(v, 80)))
		}
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })
	var b bytes.Buffer
	if err := workerTemplate.Execute(&b, list); err != nil {
		return "", fmt.Errorf("managed: rendering the worker template: %w", err)
	}
	return b.String(), nil
}
