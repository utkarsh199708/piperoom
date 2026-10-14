// Package wiring constructs the real stack clients (source Postgres,
// warehouse, Kafka Connect, Kafka admin, Schema Registry), loads the owners
// config, builds the topology builder and registers the agent's read-only
// tools and write actions. It is shared by cmd/investigate and cmd/server so
// both wire the stack identically instead of keeping private copies.
package wiring

import (
	"context"
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
	"piperoom/internal/tools/dbtools"
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
}

// buildA2Clients constructs the A2 clients from the C8 security settings
// (internal/clients/netsec). Building performs no network I/O — kgo dials
// lazily — so a bad security configuration fails here, before the database
// pings in Open. awsCfg is the shared AWS config (nil unless LoadAWS found a
// user for it). The caller owns the Kafka client and must close it.
func buildA2Clients(cfg config.Config, awsCfg *aws.Config) (*a2Clients, error) {
	// The Schema Registry client comes first: the Kafka client uses it to
	// decode Avro keys.
	registry, err := schemareg.NewSecure(cfg.SchemaRegistryURL, netsec.HTTP{
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

	return &a2Clients{Connect: connectClient, Kafka: kafkaClient, Registry: registry}, nil
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

	var once sync.Once
	return &Wired{
		Source:        src,
		Warehouse:     wh,
		WarehouseKind: whKind,
		Connect:       a2.Connect,
		Kafka:         a2.Kafka,
		Registry:      a2.Registry,
		Builder:       topology.NewBuilder(o, a2.Connect, a2.Kafka, src, wh),
		Owners:        o,
		close:         func() { once.Do(closeOpened) },
	}, nil
}

// RegisterTools registers the A3 db tools, A2's read-only Kafka/Connect/
// Schema Registry tools, C11's runbook_lookup, similar_incidents (when w.Rooms is set) and the write actions the agent may propose (never
// run), reading every client from w. If w.Builder is nil it is built from
// w's clients and stored back on w, so callers can use it afterwards.
func RegisterTools(reg *tools.Registry, areg *actions.Registry, w *Wired) error {
	b := w.Builder
	if b == nil {
		b = topology.NewBuilder(w.Owners, w.Connect, w.Kafka, w.Source, w.Warehouse)
		w.Builder = b
	}
	if err := dbtools.Register(reg, dbtools.Deps{Src: w.Source, Wh: w.Warehouse, WarehouseKind: w.WarehouseKind, Builder: b, Owners: w.Owners, Connect: w.Connect}); err != nil {
		return fmt.Errorf("wiring: register db tools: %w", err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: w.Connect, Registry: w.Registry, Kafka: w.Kafka, Owners: w.Owners}); err != nil {
		return fmt.Errorf("wiring: register kafka tools: %w", err)
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
	if err := actions.Register(areg, actions.Deps{Connect: w.Connect, Registry: w.Registry}); err != nil {
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
