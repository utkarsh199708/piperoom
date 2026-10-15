// Package wiring constructs the real stack clients (source Postgres,
// warehouse, Kafka Connect, Kafka admin, Schema Registry), loads the owners
// config, builds the topology builder and registers the agent's read-only
// tools and write actions. It is shared by cmd/investigate and cmd/server so
// both wire the stack identically instead of keeping private copies.
package wiring

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/changes"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/debeziumtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/topology"
)

const (
	// openTimeout caps the whole construction phase (owners file, DB pings,
	// Kafka client) so a dead stack fails fast instead of hanging startup.
	openTimeout = 10 * time.Second
)

// Wired holds the constructed clients. They are shared by the topology
// builder (so pipeline_map/trace_record read real state), by A2's tools and
// actions and by A3's db tools; the room service also uses Builder and
// Owners directly.
type Wired struct {
	Source    clients.SourceDB
	Warehouse clients.Warehouse
	// WarehouseKind is the normalised WAREHOUSE_KIND ("postgres" or
	// "snowflake"). Empty means postgres.
	WarehouseKind string
	Connect       clients.ConnectClient
	Kafka         clients.KafkaAdmin
	Registry      clients.SchemaRegistryClient
	// ConnectLog reads the Connect worker log for connect_worker_log; nil when
	// CONNECT_WORKER_LOG_FILE is empty or "off", and then the tool is not
	// registered. A pipeline with its own connections: block gets none: its
	// settings start empty, and one process-wide log file is not its Connect's.
	ConnectLog clients.ConnectLogReader
	// DebeziumMetrics is the parsed DEBEZIUM_METRICS_URL/_PREFIX; an empty URL
	// leaves debezium_metrics registered but answering "not configured".
	DebeziumMetrics config.DebeziumMetrics

	// Builder derives pipeline graphs with live health from the clients above.
	Builder topology.Builder
	// Owners is the loaded owners config.
	Owners *owners.Owners
	// Rooms is where similar_incidents reads past closed rooms (the room
	// store). The caller that owns the store sets it after Open, before
	// RegisterTools; nil leaves the tool unregistered (CLI runs have no room
	// store).
	Rooms similarincidents.Source

	// close is the closer built by Open: it releases the database pools and
	// the Kafka client. Only Open sets it, so a zero Wired is safe to Close.
	close func()
}

// Close releases the database pools and the Kafka client. It never returns an
// error and is safe to call more than once, and on a zero or nil *Wired, so a
// caller may defer it as soon as it holds the value. The caller must defer it
// so the clients stay open for the whole run.
func (w *Wired) Close() {
	if w == nil || w.close == nil {
		return
	}
	w.close()
}

// a2Clients groups the three A2 clients that talk to Kafka, Kafka Connect
// and the Schema Registry. Only the Kafka client holds a resource that needs
// closing; the HTTP clients are stateless.
type a2Clients struct {
	Connect  *connect.Client
	Kafka    *kafka.Client
	Registry *schemareg.Client
	// ConnectLog is nil unless the worker log file is configured.
	ConnectLog clients.ConnectLogReader
}

// buildA2Clients constructs the A2 clients from the C8 security settings
// (internal/clients/netsec). Building performs no network I/O — kgo dials
// lazily — so a bad security configuration fails here, before the database
// pings in Open. awsCfg is the shared AWS config (nil unless LoadAWS found a
// user for it). The caller owns the Kafka client and must close it.
func buildA2Clients(cfg config.Config, awsCfg *aws.Config) (*a2Clients, error) {
	// The Schema Registry client comes first: the Kafka client uses it to
	// decode Avro keys. SCHEMA_REGISTRY_URL "" or "off" (config stores "")
	// means none: registry stays nil.
	var registry *schemareg.Client
	if cfg.SchemaRegistryURL != "" {
		r, err := schemareg.NewSecure(cfg.SchemaRegistryURL, netsec.HTTP{
			Username: cfg.SchemaRegistryUsername,
			Password: netsec.Secret(cfg.SchemaRegistryPassword),
			TLS: netsec.TLS{
				CAFile:   cfg.SchemaRegistryTLSCAFile,
				CertFile: cfg.SchemaRegistryTLSCertFile,
				KeyFile:  cfg.SchemaRegistryTLSKeyFile,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("wiring: schema registry client: %w", err)
		}
		registry = r
	}

	connectClient, err := connect.NewSecure(cfg.ConnectURL, netsec.HTTP{
		Username: cfg.ConnectUsername,
		Password: netsec.Secret(cfg.ConnectPassword),
		TLS: netsec.TLS{
			CAFile:   cfg.ConnectTLSCAFile,
			CertFile: cfg.ConnectTLSCertFile,
			KeyFile:  cfg.ConnectTLSKeyFile,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("wiring: connect client: %w", err)
	}

	sec, err := kafkaSecurity(cfg, awsCfg)
	if err != nil {
		return nil, err
	}
	kafkaClient, err := kafka.NewSecure(splitBrokers(cfg.KafkaBrokers), registry, sec)
	if err != nil {
		return nil, fmt.Errorf("wiring: kafka client: %w", err)
	}

	a2 := &a2Clients{Connect: connectClient, Kafka: kafkaClient, Registry: registry}
	// Not a typed nil in the interface: the tool is registered iff this is set.
	if cfg.ConnectWorkerLogFile != "" {
		a2.ConnectLog = connect.NewLogReader(cfg.ConnectWorkerLogFile)
	}
	return a2, nil
}

// kafkaSecurity maps the Kafka security settings of cfg onto netsec. With
// KAFKA_SASL_MECHANISM=AWS_MSK_IAM the SASL credentials come from awsCfg, the
// process's one shared AWS config; netsec then checks the protocol (SASL_SSL
// only) and that no username or password is set.
func kafkaSecurity(cfg config.Config, awsCfg *aws.Config) (netsec.Kafka, error) {
	sec := netsec.Kafka{
		Protocol:  cfg.KafkaSecurityProtocol,
		Mechanism: cfg.KafkaSASLMechanism,
		Username:  cfg.KafkaSASLUsername,
		Password:  netsec.Secret(cfg.KafkaSASLPassword),
		TLS: netsec.TLS{
			CAFile:     cfg.KafkaTLSCAFile,
			CertFile:   cfg.KafkaTLSCertFile,
			KeyFile:    cfg.KafkaTLSKeyFile,
			ServerName: cfg.KafkaTLSServerName,
		},
	}
	if usesMSKIAM(cfg) {
		if awsCfg == nil || awsCfg.Credentials == nil {
			return netsec.Kafka{}, errNoSharedAWS
		}
		sec.AWSCredentialsFunc = mskIAMCredentials(awsCfg)
	}
	return sec, nil
}

// Open constructs every client Wired needs. awsCfg is the result of LoadAWS,
// shared with NewAgentEndpoint; it may be nil, but MSK IAM is then a config
// error. On a partial failure it closes what it already opened and returns a
// wrapped error naming the failing step.
func Open(ctx context.Context, cfg config.Config, awsCfg *aws.Config) (*Wired, error) {
	o, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return nil, fmt.Errorf("wiring: load owners: %w", err)
	}
	return openClients(ctx, cfg, awsCfg, o)
}

// openClients builds one client set from the connection settings of cfg and
// gives it o as its owners config. OpenStacks calls it once per set, with the
// owners view of the pipelines that set serves.
func openClients(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners) (*Wired, error) {
	ctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	// A bad WAREHOUSE_KIND is a config mistake: fail before any client is built.
	whKind, err := warehouseKind(cfg)
	if err != nil {
		return nil, err
	}

	dm, err := config.ParseDebeziumMetrics(cfg)
	if err != nil {
		return nil, err
	}

	// Build the A2 clients before any database ping so a bad security
	// configuration fails Open even when no stack is reachable at all.
	a2, err := buildA2Clients(cfg, awsCfg)
	if err != nil {
		return nil, err
	}

	// opened collects the cleanup steps of the clients constructed so far so
	// any later failure leaves nothing running behind.
	var opened []func()
	closeOpened := func() {
		for i := len(opened) - 1; i >= 0; i-- {
			opened[i]()
		}
	}
	opened = append(opened, a2.Kafka.Close)

	src, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		closeOpened()
		return nil, fmt.Errorf("wiring: source postgres: %w", err)
	}
	opened = append(opened, src.Close)

	wh, closeWh, err := openWarehouse(ctx, cfg, defaultWarehouseOpeners)
	if err != nil {
		closeOpened()
		return nil, err
	}
	opened = append(opened, closeWh)

	w := assemble(o, whKind, a2, src, wh, closeOpened)
	w.DebeziumMetrics = dm
	return w, nil
}

// assemble builds the Wired that Open returns from the clients it opened,
// the topology builder included. It is apart from Open so a test can check
// the assembly without a database.
func assemble(o *owners.Owners, whKind string, a2 *a2Clients, src clients.SourceDB, wh clients.Warehouse, closeAll func()) *Wired {
	var once sync.Once
	w := &Wired{
		Source:        src,
		Warehouse:     wh,
		WarehouseKind: whKind,
		Connect:       a2.Connect,
		Kafka:         a2.Kafka,
		ConnectLog:    a2.ConnectLog,
		Owners:        o,
		close:         func() { once.Do(closeAll) },
	}
	// A nil *schemareg.Client in the interface would read as configured.
	if a2.Registry != nil {
		w.Registry = a2.Registry
	}
	w.Builder = newTopologyBuilder(w)
	return w
}

// newTopologyBuilder is the one place a Builder is made from w's clients, the
// Schema Registry included, for Open and RegisterTools alike.
func newTopologyBuilder(w *Wired) topology.Builder {
	return topology.NewBuilder(w.Owners, w.Connect, w.Kafka, w.Source, w.Warehouse, topology.WithSchemaRegistry(w.Registry))
}

// errNoRegistry is what the write actions get from the registry when none is
// configured, so proposing a schema action fails validation cleanly.
var errNoRegistry = errors.New("wiring: Schema Registry not configured (SCHEMA_REGISTRY_URL is empty or off)")

// noRegistry stands in for the registry in the write actions only; the read
// tools and the builder take nil and say "not configured" themselves.
type noRegistry struct{}

func (noRegistry) Subjects(context.Context) ([]string, error)             { return nil, errNoRegistry }
func (noRegistry) Versions(context.Context, string) ([]int, error)        { return nil, errNoRegistry }
func (noRegistry) SetCompatibility(context.Context, string, string) error { return errNoRegistry }
func (noRegistry) Register(context.Context, string, string) (int, error)  { return 0, errNoRegistry }
func (noRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, errNoRegistry
}
func (noRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return "", false, errNoRegistry
}

// actionRegistry is w's registry for the write actions, never nil.
func actionRegistry(w *Wired) clients.SchemaRegistryClient {
	if w.Registry == nil {
		return noRegistry{}
	}
	return w.Registry
}

// RegisterTools registers the A3 db tools, A2's read-only Kafka/Connect/
// Schema Registry tools, C11's runbook_lookup, similar_incidents (when w.Rooms is set) and the write actions the agent may propose (never
// run), reading every client from w. If w.Builder is nil it is built from
// w's clients and stored back on w, so callers can use it afterwards.
func RegisterTools(reg *tools.Registry, areg *actions.Registry, w *Wired) error {
	b := w.Builder
	if b == nil {
		b = newTopologyBuilder(w)
		w.Builder = b
	}
	if err := dbtools.Register(reg, dbtools.Deps{Src: w.Source, Wh: w.Warehouse, WarehouseKind: w.WarehouseKind, Builder: b, Owners: w.Owners, Connect: w.Connect, Kafka: w.Kafka}); err != nil {
		return fmt.Errorf("wiring: register db tools: %w", err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: w.Connect, Registry: w.Registry, Kafka: w.Kafka, Owners: w.Owners, ConnectLog: w.ConnectLog}); err != nil {
		return fmt.Errorf("wiring: register kafka tools: %w", err)
	}
	if err := debeziumtools.Register(reg, debeziumtools.Deps{Connect: w.Connect, Metrics: w.DebeziumMetrics, Owners: w.Owners}); err != nil {
		return fmt.Errorf("wiring: register debezium tools: %w", err)
	}
	// The runbook library is embedded in the binary, so it needs no client.
	if err := runbooks.Register(reg); err != nil {
		return fmt.Errorf("wiring: register runbooks: %w", err)
	}
	// similar_incidents needs the room store, which only the room service
	// owns, so it is registered when the caller provided one.
	if w.Rooms != nil {
		if err := similarincidents.Register(reg, similarincidents.Deps{Source: w.Rooms, Owners: w.Owners}); err != nil {
			return fmt.Errorf("wiring: register similar_incidents: %w", err)
		}
	}
	// what_changed is always registered: the prompt names it, and the live DDL
	// read works without the change log. The change log is the room store when
	// it implements clients.ChangeReader (F8d); without it the tool says
	// "change log unavailable".
	chg := changes.Deps{Src: w.Source, Owners: w.Owners}
	if cr, ok := w.Rooms.(clients.ChangeReader); ok {
		chg.Changes = cr
	}
	if err := changes.Register(reg, chg); err != nil {
		return fmt.Errorf("wiring: register what_changed: %w", err)
	}
	if err := actions.Register(areg, actions.Deps{Connect: w.Connect, Registry: actionRegistry(w)}); err != nil {
		return fmt.Errorf("wiring: register actions: %w", err)
	}
	return nil
}

// splitBrokers splits a comma-separated broker list, trimming blanks.
func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
