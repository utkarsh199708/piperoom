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
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/logtools"
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
	// WarehouseKind is the normalised WAREHOUSE_KIND (one of
	// warehouseKinds: postgres, snowflake, bigquery, databricks or
	// clickhouse). Empty means postgres.
	WarehouseKind string
	Connect       clients.ConnectClient
	Kafka         clients.KafkaAdmin
	Registry      clients.SchemaRegistryClient
	// DebeziumMetrics is the parsed DEBEZIUM_METRICS_URL/_PREFIX; an empty URL
	// leaves debezium_metrics registered but answering "not configured".
	DebeziumMetrics config.DebeziumMetrics

	// Orchestrator reads Airflow DAG runs and task logs, and Transform reads
	// dbt artifacts (P0 item 8). Open builds them only for a set that serves
	// a batch pipeline, and only when AIRFLOW_URL or the dbt settings are set;
	// nil otherwise, and the batch tools then answer clients.ErrNotSupported.
	// A set with no CDC pipeline has the reverse: Source, Connect, Kafka and
	// Registry stay nil (kinds.go).
	Orchestrator clients.OrchestratorReader
	Transform    clients.TransformReader
	// Now is the batch tools' clock (dag_schedule_gap, run durations); nil
	// is time.Now. Tests set it.
	Now func() time.Time

	// Builder derives pipeline graphs with live health from the clients above.
	Builder topology.Builder
	// Owners is the loaded owners config.
	Owners *owners.Owners
	// WorkerLogs reads the Connect workers' logs for connect_worker_logs
	// (Q3c). Nil leaves the tool registered and answering "not configured".
	// Like Rooms, the caller sets it after Open, before RegisterTools: the
	// Loki settings (WORKER_LOGS_LOKI_URL, _SELECTOR, _TOKEN, _USER, _ORG_ID)
	// are not in internal/config yet (Q3e contract change request), so
	// nothing in production sets it today.
	WorkerLogs logtools.Reader
	// ConnectMetricsPrefix is the metric name prefix of connect_worker_metrics
	// (CONNECT_METRICS_PREFIX, Q3b); "" is
	// debeziumtools.DefaultWorkerMetricsPrefix. Set by the caller, as
	// WorkerLogs, until internal/config has it.
	ConnectMetricsPrefix string
	// ConnectInternalTopics is CONNECT_INTERNAL_TOPICS (Q3a): the Connect
	// cluster's config, offset and status topics, comma separated, in that
	// order. "" lets connect_worker_info find them by name. Set by the caller,
	// as WorkerLogs, until internal/config has it.
	ConnectInternalTopics string

	// ProbeTools registers net_probe (Q6a) over this set. It is a hook, not
	// a direct call, because nettools imports internal/connprobe, which
	// imports this package, so wiring cannot import nettools. The caller sets
	// it to netwire.Register (internal/wiring/netwire) after Open, or in
	// StackOptions.Prepare, before RegisterTools. Nil leaves net_probe
	// unregistered.
	ProbeTools func(reg *tools.Registry, w *Wired) error

	// Rooms is where similar_incidents reads past closed rooms (the room
	// store). The caller that owns the store sets it after Open, before
	// RegisterTools; nil leaves the tool unregistered (CLI runs have no room
	// store).
	Rooms similarincidents.Source

	// conn is the connection settings this set was opened with (the
	// process-wide settings, or one pipeline's resolved connections: block).
	// net_probe lists its targets from them, so a set never names another
	// set's host. Zero for a Wired built by hand: net_probe then offers only
	// the pipeline's literal connections and its connectors' hosts.
	conn config.Config

	// write is true when the set was opened with write mode on
	// (PIPEROOM_WRITE_MODE=on and not StackOptions.ReadOnly). Only then do
	// the actions get the whole Connect and Schema Registry clients, and only
	// then does writers hold anything. A Wired built any other way is read-only.
	write bool
	// source is the source kind of the set's CDC pipelines (sourceKindOf):
	// postgres or mysql. Open sets it after checking it against the source
	// URLs' scheme; RegisterTools derives it from Owners for a Wired built by
	// hand. It picks the driver and which source's tools and actions the set
	// registers (Q8g).
	source string
	// lifecycleHosts are the endpoints this set was opened with (Kafka
	// brokers, Schema Registry, source database): the hosts a connector
	// created by connect_create_connector may name before any connector of
	// the pipeline is registered (registerLifecycle).
	lifecycleHosts []string
	writers        writeClients

	// close is the closer built by Open: it releases the database pools and
	// the Kafka clients. Only Open sets it, so a zero Wired is safe to Close.
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
		r, err := newRegistryClient(cfg)
		if err != nil {
			return nil, err
		}
		registry = r
	}

	connectClient, err := newConnectClient(cfg)
	if err != nil {
		return nil, err
	}

	sec, err := kafkaSecurity(cfg, awsCfg)
	if err != nil {
		return nil, err
	}
	kafkaClient, err := kafka.NewSecure(splitBrokers(cfg.KafkaBrokers), registry, sec)
	if err != nil {
		return nil, fmt.Errorf("wiring: kafka client: %w", err)
	}

	return &a2Clients{Connect: connectClient, Kafka: kafkaClient, Registry: registry}, nil
}

// newRegistryClient builds the Schema Registry client of cfg with its login
// and TLS settings (netsec). It is the one mapping for production and
// staging (staging.go) alike.
func newRegistryClient(cfg config.Config) (*schemareg.Client, error) {
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
	return r, nil
}

// newConnectClient builds the Kafka Connect client of cfg with its login and
// TLS settings (netsec), for production and staging alike.
func newConnectClient(cfg config.Config) (*connect.Client, error) {
	c, err := connect.NewSecure(cfg.ConnectURL, netsec.HTTP{
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
	return c, nil
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
// error. PIPEROOM_WRITE_MODE decides whether the write clients are built
// (openWriteMode). On a partial failure it closes what it already opened and
// returns a wrapped error naming the failing step.
func Open(ctx context.Context, cfg config.Config, awsCfg *aws.Config) (*Wired, error) {
	write, err := openWriteMode(cfg, false)
	if err != nil {
		return nil, err
	}
	o, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return nil, fmt.Errorf("wiring: load owners: %w", err)
	}
	return openClients(ctx, cfg, awsCfg, o, write)
}

// openClients builds one client set from the connection settings of cfg and
// gives it o as its owners config. OpenStacks calls it once per set, with the
// owners view of the pipelines that set serves. write is the process's write
// mode (never read from cfg: a pipeline's own cfg holds connections only);
// with it off no write client is built.
func openClients(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners, write bool) (*Wired, error) {
	return openClientsWith(ctx, cfg, awsCfg, o, write, defaultClientOpeners())
}

// clientOpeners are openClients' constructors; tests replace them to see which
// clients a set opens without any stack.
type clientOpeners struct {
	a2 func(cfg config.Config, awsCfg *aws.Config) (*a2Clients, error)
	// source opens a Postgres source and mysql a MySQL one; the set's source
	// kind picks which (checkSourceURLs).
	source    func(ctx context.Context, dsn string) (clients.SourceDB, func(), error)
	mysql     func(ctx context.Context, dsn string) (clients.SourceDB, func(), error)
	warehouse warehouseOpeners
	writers   writerOpeners
	batch     batchOpeners
}

func defaultClientOpeners() clientOpeners {
	return clientOpeners{
		a2: buildA2Clients,
		source: func(ctx context.Context, dsn string) (clients.SourceDB, func(), error) {
			src, err := postgres.New(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return src, src.Close, nil
		},
		mysql: func(ctx context.Context, dsn string) (clients.SourceDB, func(), error) {
			src, err := mysql.New(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return src, src.Close, nil
		},
		warehouse: defaultWarehouseOpeners,
		writers:   defaultWriterOpeners(),
		batch:     defaultBatchOpeners,
	}
}

// openClientsWith is openClients with its constructors as a parameter. The
// kinds of o's pipelines decide what is opened (kinds.go): the CDC clients
// (Kafka, Connect, Schema Registry, source Postgres, Debezium metrics and,
// with write mode on, the writers) only when a pipeline is cdc, the Airflow
// and dbt readers only when one is batch, the warehouse always.
func openClientsWith(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners, write bool, op clientOpeners) (*Wired, error) {
	ctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	ks := kindsOf(o)

	// A bad WAREHOUSE_KIND is a config mistake: fail before any client is built.
	whKind, err := warehouseKind(cfg)
	if err != nil {
		return nil, err
	}

	// The source kind of the set's CDC pipelines must be the source URLs'
	// scheme (Q8g): a mismatch is a config mistake, refused before any client.
	srcKind := events.SourceKindPostgres
	var dm config.DebeziumMetrics
	if ks.cdc() {
		if srcKind, err = checkSourceURLs(cfg, o); err != nil {
			return nil, err
		}
		if dm, err = config.ParseDebeziumMetrics(cfg); err != nil {
			return nil, err
		}
	}

	// The batch readers do no I/O when built; like the A2 clients they come
	// before any database ping so a bad setting fails without a stack.
	var orch clients.OrchestratorReader
	var tr clients.TransformReader
	if ks.batch() {
		if orch, tr, err = openBatch(cfg, op.batch); err != nil {
			return nil, err
		}
	}

	// opened collects the cleanup steps of the clients constructed so far so
	// any later failure leaves nothing running behind.
	var opened []func()
	closeOpened := func() {
		for i := len(opened) - 1; i >= 0; i-- {
			opened[i]()
		}
	}

	var a2 *a2Clients
	var src clients.SourceDB
	if ks.cdc() {
		// Build the A2 clients before any database ping so a bad security
		// configuration fails Open even when no stack is reachable at all.
		if a2, err = op.a2(cfg, awsCfg); err != nil {
			return nil, err
		}
		opened = append(opened, a2.Kafka.Close)

		openSrc := op.source
		if srcKind == events.SourceKindMySQL {
			openSrc = op.mysql
		}
		if openSrc == nil {
			closeOpened()
			return nil, fmt.Errorf("wiring: source %s: no client constructor", srcKind)
		}
		s, closeSrc, err := openSrc(ctx, cfg.SourceDBURL)
		if err != nil {
			closeOpened()
			// Both clients keep the URL out of their errors.
			return nil, fmt.Errorf("wiring: source %s: %w", srcKind, err)
		}
		src = s
		opened = append(opened, closeSrc)
	}

	wh, closeWh, err := openWarehouse(ctx, cfg, op.warehouse)
	if err != nil {
		closeOpened()
		return nil, err
	}
	opened = append(opened, closeWh)

	// The writers come last: with write mode off nothing here runs, and with
	// it on a dead source database has already failed above. Every writer is
	// a CDC system's, so a set with no CDC pipeline has none.
	var wc writeClients
	if write && ks.cdc() {
		var closeWriters func()
		wc, closeWriters, err = openWritersFor(ctx, cfg, awsCfg, op.writers, srcKind)
		if err != nil {
			closeOpened()
			return nil, err
		}
		opened = append(opened, closeWriters)
	}

	w := assemble(o, whKind, a2, src, wh, closeOpened)
	w.DebeziumMetrics = dm
	w.Orchestrator, w.Transform = orch, tr
	w.write, w.writers = write, wc
	w.source = srcKind
	w.conn = cfg
	if ks.cdc() {
		w.lifecycleHosts = lifecycleHosts(cfg)
	}
	return w, nil
}

// lifecycleHosts lists cfg's CDC endpoints as AllowedHosts entries: each
// broker, and the Schema Registry and source database URLs (the action
// reduces each to its host and drops userinfo). Unset values are left out.
func lifecycleHosts(cfg config.Config) []string {
	hosts := splitBrokers(cfg.KafkaBrokers)
	for _, u := range []string{cfg.SchemaRegistryURL, cfg.SourceDBURL} {
		if u = strings.TrimSpace(u); u != "" {
			hosts = append(hosts, u)
		}
	}
	return hosts
}

// assemble builds the Wired that Open returns from the clients it opened,
// the topology builder included. It is apart from Open so a test can check
// the assembly without a database. a2 is nil for a set with no CDC pipeline;
// src and wh may be nil interfaces.
func assemble(o *owners.Owners, whKind string, a2 *a2Clients, src clients.SourceDB, wh clients.Warehouse, closeAll func()) *Wired {
	var once sync.Once
	w := &Wired{
		Source:        src,
		Warehouse:     wh,
		WarehouseKind: whKind,
		Owners:        o,
		close:         func() { once.Do(closeAll) },
	}
	// A nil *Client in an interface would read as configured, so each is set
	// only when it exists.
	if a2 != nil {
		if a2.Connect != nil {
			w.Connect = a2.Connect
		}
		if a2.Kafka != nil {
			w.Kafka = a2.Kafka
		}
		if a2.Registry != nil {
			w.Registry = a2.Registry
		}
	}
	w.Builder = newTopologyBuilder(w)
	return w
}

// newTopologyBuilder is the one place a Builder is made from w's clients, the
// Schema Registry included, for Open and RegisterTools alike. The builder only
// reads, and the db tools reach clients through it, so it gets read-only ones.
func newTopologyBuilder(w *Wired) topology.Builder {
	return topology.NewBuilder(w.Owners, readConnect(w), w.Kafka, w.Source, w.Warehouse,
		topology.WithSchemaRegistry(readRegistry(w)),
		// The sink health guard needs to know which warehouse this client is,
		// so a declared sink of another kind is left unknown (#KAN-481).
		topology.WithWarehouseKind(w.WarehouseKind))
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

// actionRegistry is w's registry for the write actions with write mode on,
// never nil (off: readOnlyActionRegistry).
func actionRegistry(w *Wired) clients.SchemaRegistryClient {
	if w.Registry == nil {
		return noRegistry{}
	}
	return w.Registry
}

// RegisterTools registers the read tool packages of the pipeline kinds w's
// set serves (toolPackages, kinds.go): A3's db tools, A2's read-only Kafka/
// Connect/Schema Registry tools and the Debezium metrics tool for cdc (the
// batch packages for batch), C11's runbook_lookup, similar_incidents (when
// w.Rooms is set) and what_changed for every kind; and the write actions the
// agent may propose (never run), for a set with a cdc pipeline. Every client
// is read from w. If w.Builder is nil it is built from w's clients and stored
// back on w, so callers can use it afterwards. A room then sees only its own
// kind's tools: the agent filters this registry by Alert.PipelineKind.
//
// reg must come from tools.NewReadOnlyRegistry: it is agent.Deps.Tools, so a
// tool that is not KindRead fails here, at start, never in a room. The tools
// get read-only Connect and Schema Registry clients (readonly.go); only the
// actions get the writers, and only when w was opened with write mode on
// (registerActions).
func RegisterTools(reg *tools.Registry, areg *actions.Registry, w *Wired) error {
	if !reg.ReadOnly() {
		return fmt.Errorf("wiring: register tools: %w: registry is not from tools.NewReadOnlyRegistry", tools.ErrNotReadOnly)
	}
	if _, err := w.sourceKind(); err != nil {
		return fmt.Errorf("wiring: register tools: %w", err)
	}
	if w.Builder == nil {
		w.Builder = newTopologyBuilder(w)
	}
	if err := registerToolsWith(reg, w, toolPackages()); err != nil {
		return err
	}
	// Every action fixes a CDC system (Connect, Schema Registry, Kafka,
	// source Postgres), so a set with no CDC pipeline has none: the batch
	// agent says what a person should run (P4 wave).
	if !kindsOf(w.Owners).cdc() {
		return nil
	}
	if err := registerActions(areg, w, nil); err != nil {
		return fmt.Errorf("wiring: register actions: %w", err)
	}
	return nil
}

// registerActions registers the write actions over w's clients. With write
// mode on they get the whole Connect and Schema Registry clients. Off (also
// for any Wired that Open or OpenStacks did not build) they get read-only
// ones: every write refuses with ErrReadOnly before any I/O, while the reads
// and dry-runs of Validate and Preview still work, so the agent proposes and
// the room previews as before. The room's executor already runs nothing then
// (P1a-1); this keeps an action that got past it from writing anyway. The
// P3 actions follow (registerP3Actions over RegisterP3Actions, actions_p3.go),
// then the Q2 source admin actions (registerSourceActions, actions_q2.go),
// then the Q5 connector lifecycle actions (registerLifecycle, actions_q5.go),
// over a read-only Connect value with write mode off.
//
// staging is the pipeline's staging client set (X8a, staging.go), handed to
// the actions as Deps.Staging with write mode on only: off, Deps.Staging is
// nil whatever is passed, so nothing can be staged.
//
// Only the actions of the set's source kind reach areg (Q8g,
// agent.ActionServesSource): a MySQL set gets no pg_* action
// (pg_set_replica_identity and every pgactions action), and its actions get
// the MySQL client as their binlog reader (binlogReader).
func registerActions(areg *actions.Registry, w *Wired, staging *actions.Staging) error {
	src, err := w.sourceKind()
	if err != nil {
		return err
	}
	all := actions.NewRegistry()
	binlog := binlogReader(w)
	if w.write {
		if err := actions.Register(all, actions.Deps{Connect: w.Connect, Registry: actionRegistry(w), Staging: staging, Binlog: binlog}); err != nil {
			return err
		}
	} else if err := actions.Register(all, actions.Deps{Connect: readOnlyActionConnect(w), Registry: readOnlyActionRegistry(w), Binlog: binlog}); err != nil {
		return err
	}
	if err := registerTuning(all, w, staging); err != nil {
		return err
	}
	if err := registerP3Actions(all, w); err != nil {
		return err
	}
	if err := registerSourceActions(all, w); err != nil {
		return err
	}
	if err := registerLifecycle(all, w); err != nil {
		return err
	}
	for _, a := range all.All() {
		if !agent.ActionServesSource(a.Name(), src) {
			continue
		}
		if err := areg.Register(a); err != nil {
			return err
		}
	}
	return nil
}

// binlogReader is the MySQL source's binlog reader that the Connect actions
// check a MySQL position and database.server.id against (Q8e): the set's
// source client on a MySQL set, nil otherwise (a nil interface, never a typed
// nil: the actions treat a non-nil reader as a MySQL room).
func binlogReader(w *Wired) mysql.BinlogReader {
	if w.source != events.SourceKindMySQL || isNilIface(w.Source) {
		return nil
	}
	if b, ok := w.Source.(mysql.BinlogReader); ok {
		return b
	}
	return nil
}

// sourceKind returns the set's source kind: the one Open checked, or for a
// Wired built by hand the one its Owners' CDC pipelines have (sourceKindOf),
// stored on w. Two kinds in one set is an error.
func (w *Wired) sourceKind() (string, error) {
	if w.source != "" {
		return w.source, nil
	}
	k, err := sourceKindOf(w.Owners)
	if err != nil {
		return "", err
	}
	w.source = k
	return k, nil
}

// registerTuning registers the performance action connect_tune_throughput
// (X6c): the whole Connect client and the staging set with write mode on,
// the read-only Connect adapter and no staging off. Its Verify reads the
// Kafka reader's offsets by time and the room store's group_lag watch
// samples (nil without a room store: a sink tuning is then unverifiable).
func registerTuning(areg *actions.Registry, w *Wired, staging *actions.Staging) error {
	d := actions.TuningDeps{Connect: readOnlyActionConnect(w)}
	if w.write {
		d.Connect, d.Staging = w.Connect, staging
	}
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	if !isNilIface(w.Kafka) {
		d.Kafka = tuningKafka{r: w.Kafka}
	}
	if s, ok := w.Rooms.(clients.SampleReader); ok && !isNilIface(s) {
		d.Samples = s
	}
	return actions.RegisterTuning(areg, d)
}

// tuningKafka is the Kafka reader as connect_tune_throughput holds it: the
// two reads it makes and nothing else of the client. A reader without
// offsets by time answers clients.ErrNotSupported (unverifiable).
type tuningKafka struct{ r clients.KafkaReader }

func (k tuningKafka) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	return k.r.TopicOffsets(ctx, topic)
}

func (k tuningKafka) PartitionRates(ctx context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	rr, ok := k.r.(clients.RateReader)
	if !ok {
		return nil, fmt.Errorf("wiring: the Kafka client has no offsets by time: %w", clients.ErrNotSupported)
	}
	return rr.PartitionRates(ctx, since, topics...)
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
