package strimzi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
)

// The KafkaConnect and KafkaConnector templates (row U12b, design §6). The
// KafkaConnect builds its image with Strimzi's build.plugins, every artifact
// with the sha512sum the knowledge index pins, never a digest read from the
// host that serves it; the KafkaConnector carries the source driver's config
// with every secret a ${secrets:<ns>/<pipeline>-db-credentials:<key>}
// reference.

func init() {
	templates["connect"] = template{kind: "KafkaConnect", render: renderConnect}
	templates["connector"] = template{kind: "KafkaConnector", render: renderConnector}
}

const placeholderBootstrap = "kafka-kafka-bootstrap.validate.svc:9093"

type connectParams struct {
	Replicas   int      `json:"replicas,omitempty"`
	Listener   string   `json:"listener"`            // the Kafka CR listener the Connect cluster uses
	TLS        bool     `json:"tls,omitempty"`       // the listener is TLS: trust the cluster CA
	Auth       string   `json:"auth,omitempty"`      // "", tls or scram-sha-512 (the Connect KafkaUser)
	Registry   string   `json:"registry"`            // image registry host[:port], an image_registry target
	Repository string   `json:"repository"`          // repository under it
	Release    string   `json:"release"`             // Debezium release (knowledge index)
	Plugins    []string `json:"plugins"`             // connector short names: postgres, mysql, ..., jdbc
	Schemas    bool     `json:"schemas,omitempty"`   // converters' schemas.enable
	Resources  string   `json:"resources,omitempty"` // "", small (500m/1Gi) or medium (1/2Gi)
	JDBCPlugin bool     `json:"-"`
}

var (
	registryRE   = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?$`)
	repositoryRE = regexp.MustCompile(`^[a-z0-9]+([._/-][a-z0-9]+){0,15}$`)
	listenerRE   = regexp.MustCompile(`^[a-z0-9]{1,11}$`)
)

// pluginArtifact resolves one build plugin from the knowledge index: a source
// connector from the pinned plugin list, the JDBC sink from its index entry.
func pluginArtifact(release, connector string) (name, artURL, sha string, err error) {
	if connector == "jdbc" {
		ix, err := knowledge.LoadIndex()
		if err != nil {
			return "", "", "", err
		}
		for _, e := range ix.Entries() {
			if e.Name != "debezium-jdbc-sink" {
				continue
			}
			for _, a := range e.Archives {
				if a.Release == release && a.Digest != "" {
					return "debezium-connector-jdbc", a.URL, strings.TrimPrefix(a.Digest, "sha512:"), nil
				}
			}
		}
		return "", "", "", fmt.Errorf("%w: no pinned jdbc archive for %s", knowledge.ErrUnknownRelease, release)
	}
	p, err := knowledge.ResolvePlugin(release, connector)
	if err != nil {
		return "", "", "", err
	}
	return p.Artifact, p.ArtifactURL(), strings.TrimPrefix(p.Digest, "sha512:"), nil
}

// renderConnect renders with placeholderBootstrap: the real bootstrap is a
// cluster fact that resolveFacts fills before a preview or a write.
func renderConnect(d *Driver, kafka string, raw json.RawMessage) (rendered, error) {
	var p connectParams
	if err := decode(raw, &p); err != nil {
		return rendered{}, err
	}
	if p.Replicas == 0 {
		p.Replicas = 1
	}
	if p.Replicas < 1 || p.Replicas > maxReplicas {
		return rendered{}, fmt.Errorf("%w: replicas must be 1 to %d", ErrParams, maxReplicas)
	}
	if !listenerRE.MatchString(p.Listener) {
		return rendered{}, fmt.Errorf("%w: listener %q is not a Kafka listener name", ErrParams, clipText(p.Listener, 32))
	}
	if !registryRE.MatchString(p.Registry) || !repositoryRE.MatchString(p.Repository) {
		return rendered{}, fmt.Errorf("%w: registry and repository must be a host[:port] and a repository path", ErrParams)
	}
	if len(p.Plugins) == 0 || len(p.Plugins) > 4 {
		return rendered{}, fmt.Errorf("%w: 1 to 4 plugins", ErrParams)
	}
	var plugins []any
	var digests []string
	seen := map[string]bool{}
	for _, c := range p.Plugins {
		if seen[c] {
			return rendered{}, fmt.Errorf("%w: plugin %s twice", ErrParams, c)
		}
		seen[c] = true
		name, u, sha, err := pluginArtifact(p.Release, c)
		if err != nil {
			return rendered{}, fmt.Errorf("%w: plugin %s: %v", ErrParams, clipText(c, 32), err)
		}
		host := ""
		if pu, err := url.Parse(u); err == nil {
			host = pu.Host
		}
		if !contains(d.pluginHosts(), host) {
			return rendered{}, fmt.Errorf("%w: plugin %s is served by %s, which is not an allowed plugin host", ErrParams, c, host)
		}
		digests = append(digests, sha)
		plugins = append(plugins, map[string]any{"name": name, "artifacts": []any{map[string]any{"type": "tgz", "url": u, "sha512sum": sha}}})
	}
	sort.Strings(digests)
	sum := sha256.Sum256([]byte(strings.Join(digests, ",")))
	image := p.Registry + "/" + p.Repository + ":piperoom-" + d.pipeline + "-" + hex.EncodeToString(sum[:])[:12]

	conv := "org.apache.kafka.connect.json.JsonConverter"
	schemas := fmt.Sprint(p.Schemas)
	internal := d.connectName() + "-connect-"
	spec := map[string]any{
		"replicas":         p.Replicas,
		"bootstrapServers": placeholderBootstrap,
		"config": map[string]any{
			"group.id": d.connectName(), "offset.storage.topic": internal + "offsets",
			"config.storage.topic": internal + "configs", "status.storage.topic": internal + "status",
			"offset.storage.replication.factor": -1, "config.storage.replication.factor": -1, "status.storage.replication.factor": -1,
			"key.converter": conv, "value.converter": conv,
			"key.converter.schemas.enable": schemas, "value.converter.schemas.enable": schemas,
			"config.providers": "secrets", "config.providers.secrets.class": secretsProviderClass,
		},
		"build": map[string]any{
			"output":  map[string]any{"type": "docker", "image": image, "pushSecret": d.pushSecretName()},
			"plugins": plugins,
		},
		"logging":       map[string]any{"type": "external", "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": d.loggingName(), "key": "log4j.properties"}}},
		"metricsConfig": map[string]any{"type": "jmxPrometheusExporter", "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": d.metricsName(), "key": "metrics-config.yml"}}},
	}
	if p.TLS {
		spec["tls"] = map[string]any{"trustedCertificates": []any{map[string]any{"secretName": kafka + "-cluster-ca-cert", "pattern": "*.crt"}}}
	}
	switch p.Auth {
	case "":
	case "tls":
		spec["authentication"] = map[string]any{"type": "tls", "certificateAndKey": map[string]any{"secretName": d.connectName(), "certificate": "user.crt", "key": "user.key"}}
	case "scram-sha-512":
		spec["authentication"] = map[string]any{"type": "scram-sha-512", "username": d.connectName(), "passwordSecret": map[string]any{"secretName": d.connectName(), "password": "password"}}
	default:
		return rendered{}, fmt.Errorf("%w: auth must be tls or scram-sha-512", ErrParams)
	}
	switch p.Resources {
	case "":
	case "small":
		spec["resources"] = map[string]any{"requests": map[string]any{"cpu": "500m", "memory": "1Gi"}, "limits": map[string]any{"cpu": "1", "memory": "1Gi"}}
	case "medium":
		spec["resources"] = map[string]any{"requests": map[string]any{"cpu": "1", "memory": "2Gi"}, "limits": map[string]any{"cpu": "2", "memory": "2Gi"}}
	default:
		return rendered{}, fmt.Errorf("%w: resources must be small or medium", ErrParams)
	}
	return rendered{kind: "KafkaConnect", name: d.connectName(), path: "/spec", pause: true, value: spec, listener: p.Listener, registry: p.Registry}, nil
}

// sourceClasses are the Debezium source connector classes a KafkaConnector
// may name (the knowledge index's plugins).
var sourceClasses = map[string]string{
	"postgres":  "io.debezium.connector.postgresql.PostgresConnector",
	"mysql":     "io.debezium.connector.mysql.MySqlConnector",
	"sqlserver": "io.debezium.connector.sqlserver.SqlServerConnector",
	"mongodb":   "io.debezium.connector.mongodb.MongoDbConnector",
	"oracle":    "io.debezium.connector.oracle.OracleConnector",
	"jdbc":      "io.debezium.connector.jdbc.JdbcSinkConnector",
}

type connectorParams struct {
	Connector string            `json:"connector"` // one of the chart's connector names
	Plugin    string            `json:"plugin"`    // postgres, mysql, ..., jdbc
	Config    map[string]string `json:"config"`
}

func renderConnector(d *Driver, _ string, raw json.RawMessage) (rendered, error) {
	var p connectorParams
	if err := decode(raw, &p); err != nil {
		return rendered{}, err
	}
	if !contains(d.connectors, p.Connector) {
		return rendered{}, fmt.Errorf("%w: connector %q is not one of the chart's", ErrName, clipText(p.Connector, 64))
	}
	cls, ok := sourceClasses[p.Plugin]
	if !ok {
		return rendered{}, fmt.Errorf("%w: plugin %q", ErrParams, clipText(p.Plugin, 32))
	}
	if len(p.Config) == 0 || len(p.Config) > 128 {
		return rendered{}, fmt.Errorf("%w: 1 to 128 config keys", ErrParams)
	}
	cfg := make(map[string]any, len(p.Config))
	for k, v := range p.Config {
		cfg[k] = v
	}
	return rendered{kind: "KafkaConnector", name: p.Connector, path: "/spec", pause: true, value: map[string]any{
		"class": cls, "tasksMax": 1, "config": cfg,
		"autoRestart": map[string]any{"enabled": true, "maxRestarts": 3},
	}}, nil
}

// targetImageRegistry is the Env.Targets kind that names the registry a
// KafkaConnect build may push to; its address is the registry host[:port].
const targetImageRegistry = "image_registry"

// resolveFacts fills what a render reads from the cluster and the room just
// before a preview or a write: for a KafkaConnect, the registry must be one of
// the room's image_registry targets and the bootstrap comes from the Kafka CR's
// status for the named listener. Other kinds pass unchanged.
func (d *Driver) resolveFacts(ctx context.Context, r rendered, ns, kafka string, env ops.Env) (rendered, error) {
	if r.kind != "KafkaConnect" {
		return r, nil
	}
	found := false
	for _, t := range env.Targets {
		if t.Kind == targetImageRegistry && t.Address == r.registry {
			found = true
		}
	}
	if !found {
		return rendered{}, fmt.Errorf("%w: registry %s is not an image_registry target of the room", ErrTarget, r.registry)
	}
	body, err := d.api.Get(ctx, objectPath(ns, "Kafka", kafka))
	if err != nil {
		return rendered{}, fmt.Errorf("strimzi: reading Kafka %s/%s: %w", ns, kafka, err)
	}
	var k struct {
		Status struct {
			Listeners []struct {
				Name             string `json:"name"`
				BootstrapServers string `json:"bootstrapServers"`
			} `json:"listeners"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &k); err != nil {
		return rendered{}, fmt.Errorf("strimzi: decoding Kafka %s/%s: %w", ns, kafka, err)
	}
	bootstrap := ""
	for _, l := range k.Status.Listeners {
		if l.Name == r.listener {
			bootstrap = l.BootstrapServers
		}
	}
	if bootstrap == "" {
		return rendered{}, fmt.Errorf("%w: Kafka %s/%s reports no bootstrap for listener %s (is it Ready?)", ErrTarget, ns, kafka, r.listener)
	}
	spec := make(map[string]any, len(r.value))
	for k, v := range r.value {
		spec[k] = v
	}
	spec["bootstrapServers"] = bootstrap
	r.value = spec
	if err := d.validate(r, kafka, ns); err != nil {
		return rendered{}, err
	}
	return r, nil
}
