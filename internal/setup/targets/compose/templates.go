package compose

import (
	"bytes"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
)

// Default host ports of the four services. A param may move a published port;
// the container port never changes.
var defaultPorts = map[string]int{
	"kafka":           29092,
	"connect":         8083,
	"schema-registry": 8081,
	"postgres":        5432,
}

// minPort and maxPort bound a published port: below 1024 needs a privilege the
// runner must not ask for, and a port above 65535 is not a port.
const (
	minPort = 1024
	maxPort = 65535
)

// maxPorts bounds the ports map, so a proposal cannot make the render do
// unbounded work.
const maxPorts = 8

// serviceNameRE is the vocabulary of a service name in a project this driver
// renders: it is also what Argv passes as a service argument.
var serviceNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// projectNameRE is a compose project name in the piperoom- namespace
// (ProjectPrefix), within compose's 63-character name vocabulary.
var projectNameRE = regexp.MustCompile(`^piperoom-[a-z0-9_-]{1,54}$`)

// WriteParams are compose_write's typed params (design §2.5: templates,
// project name, ports; there is no field that carries YAML text).
type WriteParams struct {
	// Templates names one to four templates; each service is rendered once.
	Templates []string `json:"templates"`
	// Project is the Compose project name; "" means DefaultProject.
	Project string `json:"project,omitempty"`
	// Ports moves a published host port per service name; unknown names are
	// refused.
	Ports map[string]int `json:"ports,omitempty"`
	// Registry adds the Schema Registry service to a connect render (the
	// "optional Registry" of the row); it is only valid with connect.
	Registry bool `json:"registry,omitempty"`
	// File is the written file name under the project directory; "" means
	// DefaultFile. Only a compose name is accepted.
	File string `json:"file,omitempty"`
}

// Rendered is one composed stack.
type Rendered struct {
	File     string   // project-relative compose file name
	Doc      []byte   // the YAML, header included
	Services []string // service names, sorted
	EnvKeys  []string // .env keys the file interpolates, sorted
	Mounts   []string // project-relative directories the file binds
}

// Render validates p and renders the stack. It does no I/O and is safe to
// call twice (compose_write validates and then executes with it).
func (d *Driver) Render(p WriteParams) (Rendered, error) {
	selected, err := selectTemplates(p.Templates)
	if err != nil {
		return Rendered{}, err
	}
	if (selected[TemplateConnect] || selected[TemplateRegistry]) && !selected[TemplateKafka] {
		return Rendered{}, fmt.Errorf("%w: the connect and registry templates need kafka-kraft in the same render", ErrParams)
	}
	if p.Registry && !selected[TemplateConnect] {
		return Rendered{}, fmt.Errorf("%w: registry needs the connect template", ErrParams)
	}
	project := p.Project
	if project == "" {
		project = DefaultProject
	}
	if !projectNameRE.MatchString(project) {
		return Rendered{}, fmt.Errorf("%w: project name %s is not a %s compose project name", ErrParams, q(project), ProjectPrefix)
	}
	file := p.File
	if file == "" {
		file = DefaultFile
	}
	// The path rule runs first: an escape is a refusal for the path, not for
	// the file name's shape.
	if err := ops.CheckName(file); err != nil {
		return Rendered{}, fmt.Errorf("%w: file: %v", ErrPath, err)
	}
	if !isComposeName(file) {
		return Rendered{}, fmt.Errorf("%w: file %s is not a compose file name", ErrParams, q(file))
	}
	ports, err := resolvePorts(p.Ports, selected, p.Registry)
	if err != nil {
		return Rendered{}, err
	}

	local := loopbackOnly(d.classOrDefault())
	services := make(map[string]service, 4)
	var envKeys, mounts []string
	if selected[TemplateKafka] {
		svc, err := kafkaService(ports["kafka"], local)
		if err != nil {
			return Rendered{}, err
		}
		services["kafka"] = svc
	}
	if selected[TemplatePostgres] {
		svc, err := postgresService(ports["postgres"], local)
		if err != nil {
			return Rendered{}, err
		}
		services["postgres"] = svc
		envKeys = postgresEnvKeys()
	}
	if selected[TemplateConnect] {
		svc, err := connectService(ports["connect"], local)
		if err != nil {
			return Rendered{}, err
		}
		// connect needs kafka and kafka is in this render (checked above).
		svc.DependsOn = map[string]waitFor{"kafka": {Condition: "service_healthy"}}
		services["connect"] = svc
		mounts = append(mounts, PluginDir)
	}
	if selected[TemplateRegistry] || p.Registry {
		svc, err := registryService(ports["schema-registry"], local)
		if err != nil {
			return Rendered{}, err
		}
		svc.DependsOn = map[string]waitFor{"kafka": {Condition: "service_healthy"}}
		services["schema-registry"] = svc
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(composeDoc{Name: project, Services: services}); err != nil {
		return Rendered{}, fmt.Errorf("compose: encoding the render: %w", err)
	}
	if err := enc.Close(); err != nil {
		return Rendered{}, fmt.Errorf("compose: encoding the render: %w", err)
	}
	header := "# Generated by piperoom setup from " + strings.Join(sortedKeys(selected), ", ") +
		"; compose_write regenerates this file. Do not edit it by hand.\n"
	sort.Strings(envKeys)
	return Rendered{
		File:     file,
		Doc:      append([]byte(header), buf.Bytes()...),
		Services: sortedKeys(services),
		EnvKeys:  envKeys,
		Mounts:   mounts,
	}, nil
}

// selectTemplates validates the template list.
func selectTemplates(names []string) (map[string]bool, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: templates is required", ErrParams)
	}
	sel := make(map[string]bool, len(names))
	for _, name := range names {
		switch name {
		case TemplateKafka, TemplateConnect, TemplateRegistry, TemplatePostgres:
		default:
			return nil, fmt.Errorf("%w: %s", ErrTemplate, q(name))
		}
		if sel[name] {
			return nil, fmt.Errorf("%w: duplicate template %s", ErrParams, q(name))
		}
		sel[name] = true
	}
	return sel, nil
}

// resolvePorts starts from the defaults of the selected services and applies
// the params. A name that is not in this render is refused, not ignored: a
// typo must not silently publish the default port.
func resolvePorts(want map[string]int, sel map[string]bool, registry bool) (map[string]int, error) {
	if len(want) > maxPorts {
		return nil, fmt.Errorf("%w: at most %d ports", ErrParams, maxPorts)
	}
	present := make(map[string]bool, 4)
	if sel[TemplateKafka] {
		present["kafka"] = true
	}
	if sel[TemplatePostgres] {
		present["postgres"] = true
	}
	if sel[TemplateConnect] {
		present["connect"] = true
	}
	if sel[TemplateRegistry] || registry {
		present["schema-registry"] = true
	}
	out := make(map[string]int, len(present))
	for name := range present {
		out[name] = defaultPorts[name]
	}
	for name, port := range want {
		if !present[name] {
			return nil, fmt.Errorf("%w: port for %s, which this render has no service for", ErrParams, q(name))
		}
		if port < minPort || port > maxPort {
			return nil, fmt.Errorf("%w: port %d for %s is outside %d-%d", ErrParams, port, q(name), minPort, maxPort)
		}
		out[name] = port
	}
	return out, nil
}

// composeDoc is the top-level shape this driver renders. It is deliberately
// this small: no networks, no named volumes, no extensions.
type composeDoc struct {
	Name     string             `yaml:"name"`
	Services map[string]service `yaml:"services"`
}

// service is one rendered service. Fields the validator forbids (privileged,
// build, network_mode, ...) have no field here at all.
type service struct {
	Image       string             `yaml:"image"`
	Command     []string           `yaml:"command,omitempty"`
	Environment []string           `yaml:"environment,omitempty"`
	Ports       []string           `yaml:"ports,omitempty"`
	Volumes     []string           `yaml:"volumes,omitempty"`
	DependsOn   map[string]waitFor `yaml:"depends_on,omitempty"`
	Healthcheck *healthcheck       `yaml:"healthcheck,omitempty"`
}

// waitFor is compose's depends_on condition.
type waitFor struct {
	Condition string `yaml:"condition"`
}

// healthcheck is the subset this driver renders.
type healthcheck struct {
	Test     []string `yaml:"test"`
	Interval string   `yaml:"interval,omitempty"`
	Retries  int      `yaml:"retries,omitempty"`
}

// kafkaService is a single-node KRaft broker: one process is broker and
// controller, so the stack needs no ZooKeeper. The HOST listener is what the
// runner and the Connect container on the host reach.
func kafkaService(hostPort int, local bool) (service, error) {
	img, err := imageRef("kafka")
	if err != nil {
		return service{}, err
	}
	return service{
		Image: img,
		Environment: []string{
			"KAFKA_NODE_ID=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:29092",
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,HOST://localhost:" + strconv.Itoa(hostPort),
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093",
			"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
			"KAFKA_LOG_DIRS=/var/lib/kafka/data",
		},
		Ports:   []string{published(local, hostPort, 29092)},
		Volumes: []string{"/var/lib/kafka/data"},
		Healthcheck: &healthcheck{
			Test:     []string{"CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"},
			Interval: "10s",
			Retries:  20,
		},
	}, nil
}

// postgresService is the local source database with logical WAL on: the two
// settings a Debezium Postgres connector needs before it can start.
func postgresService(hostPort int, local bool) (service, error) {
	img, err := imageRef("postgres")
	if err != nil {
		return service{}, err
	}
	return service{
		Image: img,
		Command: []string{"postgres", "-c", "wal_level=logical",
			"-c", "max_replication_slots=10", "-c", "max_wal_senders=10"},
		// The values come from the .env properties_write renders (through the
		// ${...} interpolation the pinned argv's --env-file supplies); the
		// credential itself is a ${secret:...} reference until U1-4
		// materialises it.
		Environment: []string{
			"POSTGRES_USER=${POSTGRES_USER}",
			"POSTGRES_PASSWORD=${POSTGRES_PASSWORD}",
			"POSTGRES_DB=${POSTGRES_DB}",
		},
		Ports:   []string{published(local, hostPort, 5432)},
		Volumes: []string{"/var/lib/postgresql/data"},
		Healthcheck: &healthcheck{
			Test:     []string{"CMD", "pg_isready", "-U", "${POSTGRES_USER}", "-d", "${POSTGRES_DB}"},
			Interval: "5s",
			Retries:  20,
		},
	}, nil
}

// connectService is the Debezium Kafka Connect worker with the plugin
// directory mounted from the project. The image's bundled connectors stay
// where they are; the mount is the directory plugin_install populates and
// jar_list inspects.
func connectService(hostPort int, local bool) (service, error) {
	img, err := imageRef("connect")
	if err != nil {
		return service{}, err
	}
	return service{
		Image: img,
		Environment: []string{
			"BOOTSTRAP_SERVERS=kafka:9092",
			"GROUP_ID=piperoom-connect",
			"CONFIG_STORAGE_TOPIC=_connect_configs",
			"OFFSET_STORAGE_TOPIC=_connect_offsets",
			"STATUS_STORAGE_TOPIC=_connect_status",
			"CONFIG_STORAGE_REPLICATION_FACTOR=1",
			"OFFSET_STORAGE_REPLICATION_FACTOR=1",
			"STATUS_STORAGE_REPLICATION_FACTOR=1",
			"KAFKA_CONNECT_PLUGINS_DIR=" + PluginDirContainer,
		},
		Ports:   []string{published(local, hostPort, 8083)},
		Volumes: []string{"./" + PluginDir + ":" + PluginDirContainer},
		Healthcheck: &healthcheck{
			Test:     []string{"CMD-SHELL", "curl -fs http://localhost:8083/connectors >/dev/null"},
			Interval: "5s",
			Retries:  40,
		},
	}, nil
}

// registryService is Confluent Schema Registry against the same broker.
func registryService(hostPort int, local bool) (service, error) {
	img, err := imageRef("schema-registry")
	if err != nil {
		return service{}, err
	}
	return service{
		Image: img,
		Environment: []string{
			"SCHEMA_REGISTRY_HOST_NAME=schema-registry",
			"SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:9092",
		},
		Ports: []string{published(local, hostPort, 8081)},
		Healthcheck: &healthcheck{
			Test:     []string{"CMD-SHELL", "curl -fsS http://localhost:8081/subjects >/dev/null"},
			Interval: "5s",
			Retries:  40,
		},
	}, nil
}

// postgresEnvKeys are the .env keys the postgres template interpolates. The
// runner materialises them from its secret store (U1-4); compose_write
// reports them so the caller knows which .env properties_write must render.
func postgresEnvKeys() []string {
	return []string{"POSTGRES_DB", "POSTGRES_PASSWORD", "POSTGRES_USER"}
}

// published builds the ports entry: 127.0.0.1 on a local target, the plain
// host:container form on a class the person raised.
func published(local bool, hostPort, containerPort int) string {
	entry := strconv.Itoa(hostPort) + ":" + strconv.Itoa(containerPort)
	if local {
		return "127.0.0.1:" + entry
	}
	return entry
}

// imageRef resolves a knowledge image to its pinned pull reference; a missing
// entry is an error, never a fallback tag (design §2.5: no floating tags).
func imageRef(name string) (string, error) {
	im, err := knowledge.ImageByName(name)
	if err != nil {
		return "", fmt.Errorf("compose: resolving image %s: %w", q(name), err)
	}
	return im.Ref(), nil
}

// isComposeName reports whether file names a Compose file compose will read.
// compose_write refuses any other name, so a render can never be written
// somewhere compose ignores it (or over a non-compose file).
func isComposeName(file string) bool {
	base := strings.ToLower(path.Base(strings.ReplaceAll(file, "\\", "/")))
	switch base {
	case "compose.yaml", "compose.yml", "compose.override.yaml", "compose.override.yml":
		return true
	}
	return strings.HasPrefix(base, "docker-compose") &&
		(strings.HasSuffix(base, ".yml") || strings.HasSuffix(base, ".yaml"))
}

// sortedKeys returns the keys of m in lexical order, so a render, a summary
// and a refusal are deterministic.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
