// Package wiring constructs the real stack clients (source Postgres,
// warehouse, Kafka Connect, Kafka admin, Schema Registry), loads the owners
// config, builds the topology builder and registers the agent's read-only
// tools and write actions. It is shared by cmd/investigate and cmd/server so
// both wire the stack identically instead of keeping private copies.
package wiring

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/topology"
)

const (
	// openTimeout caps the whole construction phase (owners file, DB pings,
	// Kafka client) so a dead stack fails fast instead of hanging startup.
	openTimeout = 10 * time.Second
	// httpTimeout caps every Connect and Schema Registry request.
	httpTimeout = 10 * time.Second
)

// Wired holds the constructed clients. They are shared by the topology
// builder (so pipeline_map/trace_record read real state), by A2's tools and
// actions and by A3's db tools; the room service also uses Builder and
// Owners directly.
type Wired struct {
	Source    clients.SourceDB
	Warehouse clients.Warehouse
	Connect   clients.ConnectClient
	Kafka     clients.KafkaAdmin
	Registry  clients.SchemaRegistryClient

	// Builder derives pipeline graphs with live health from the clients above.
	Builder topology.Builder
	// Owners is the loaded owners config.
	Owners *owners.Owners

	// Close releases the database pools and the Kafka client. It never
	// returns an error and is safe to call more than once; the caller must
	// defer it so the clients stay open for the whole run.
	Close func()
}

// Open constructs every client Wired needs. On a partial failure it closes
// what it already opened and returns a wrapped error naming the failing step.
func Open(ctx context.Context, cfg config.Config) (*Wired, error) {
	ctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	o, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return nil, fmt.Errorf("wiring: load owners: %w", err)
	}

	// opened collects the cleanup steps of the clients constructed so far so
	// any later failure leaves nothing running behind.
	var opened []func()
	closeOpened := func() {
		for i := len(opened) - 1; i >= 0; i-- {
			opened[i]()
		}
	}

	src, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		return nil, fmt.Errorf("wiring: source postgres: %w", err)
	}
	opened = append(opened, src.Close)

	wh, err := warehouse.New(ctx, cfg.WarehouseDBURL)
	if err != nil {
		closeOpened()
		return nil, fmt.Errorf("wiring: warehouse: %w", err)
	}
	opened = append(opened, wh.Close)

	httpClient := &http.Client{Timeout: httpTimeout}
	connectClient := connect.New(cfg.ConnectURL, httpClient)
	registryClient := schemareg.New(cfg.SchemaRegistryURL, httpClient)

	kafkaClient, err := kafka.New(splitBrokers(cfg.KafkaBrokers), registryClient)
	if err != nil {
		closeOpened()
		return nil, fmt.Errorf("wiring: kafka client: %w", err)
	}
	opened = append(opened, kafkaClient.Close)

	var once sync.Once
	return &Wired{
		Source:    src,
		Warehouse: wh,
		Connect:   connectClient,
		Kafka:     kafkaClient,
		Registry:  registryClient,
		Builder:   topology.NewBuilder(o, connectClient, kafkaClient, src, wh),
		Owners:    o,
		Close:     func() { once.Do(closeOpened) },
	}, nil
}

// RegisterTools registers the A3 db tools plus A2's read-only Kafka/Connect/
// Schema Registry tools and the write actions the agent may propose (never
// run), reading every client from w. If w.Builder is nil it is built from
// w's clients and stored back on w, so callers can use it afterwards.
func RegisterTools(reg *tools.Registry, areg *actions.Registry, w *Wired) error {
	b := w.Builder
	if b == nil {
		b = topology.NewBuilder(w.Owners, w.Connect, w.Kafka, w.Source, w.Warehouse)
		w.Builder = b
	}
	if err := dbtools.Register(reg, dbtools.Deps{Src: w.Source, Wh: w.Warehouse, Builder: b, Owners: w.Owners}); err != nil {
		return fmt.Errorf("wiring: register db tools: %w", err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: w.Connect, Registry: w.Registry, Kafka: w.Kafka}); err != nil {
		return fmt.Errorf("wiring: register kafka tools: %w", err)
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
