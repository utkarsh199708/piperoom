package main

// This file is a temporary local adapter: it opens the real stack clients
// (source Postgres, warehouse, Kafka Connect, Schema Registry, Kafka) and
// registers the real read-only tools and write actions for cmd/server's
// non-demo mode. A4 is extracting this exact construction into the shared
// internal/wiring package; as soon as that lands on main, openStack is
// replaced by wiring.Open + wiring.RegisterTools and this file is deleted.
//
// TODO(a5): switch to internal/wiring after A4 lands.

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
	// stackOpenTimeout caps the whole construction phase (owners file, DB
	// pings, Kafka client) so a dead dev stack fails fast at startup instead
	// of hanging.
	stackOpenTimeout = 10 * time.Second
	// stackHTTPTimeout caps every Connect and Schema Registry request.
	stackHTTPTimeout = 10 * time.Second
)

// stackDeps holds the real stack clients plus the registries built from them,
// ready to be handed to room.Deps.
type stackDeps struct {
	Source    clients.SourceDB
	Warehouse clients.Warehouse
	Connect   clients.ConnectClient
	Kafka     clients.KafkaAdmin
	Registry  clients.SchemaRegistryClient

	// Builder derives pipeline graphs with live health from the clients above.
	Builder topology.Builder
	// Owners is the loaded owners config, used for pipeline validation and
	// action scope checks.
	Owners *owners.Owners

	Tools   *tools.Registry
	Actions *actions.Registry

	// Close releases the database pools and the Kafka client. It is safe to
	// call more than once; the caller must keep the clients open for the whole
	// run and defer Close.
	Close func()
}

// openStack constructs every real client. On a partial failure it closes what
// it already opened and returns a wrapped error naming the failing step.
func openStack(ctx context.Context, cfg config.Config) (*stackDeps, error) {
	ctx, cancel := context.WithTimeout(ctx, stackOpenTimeout)
	defer cancel()

	o, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return nil, fmt.Errorf("server: load owners: %w", err)
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
		return nil, fmt.Errorf("server: source postgres: %w", err)
	}
	opened = append(opened, src.Close)

	wh, err := warehouse.New(ctx, cfg.WarehouseDBURL)
	if err != nil {
		closeOpened()
		return nil, fmt.Errorf("server: warehouse: %w", err)
	}
	opened = append(opened, wh.Close)

	httpClient := &http.Client{Timeout: stackHTTPTimeout}
	connectClient := connect.New(cfg.ConnectURL, httpClient)
	registryClient := schemareg.New(cfg.SchemaRegistryURL, httpClient)

	kafkaClient, err := kafka.New(splitBrokers(cfg.KafkaBrokers), registryClient)
	if err != nil {
		closeOpened()
		return nil, fmt.Errorf("server: kafka client: %w", err)
	}
	opened = append(opened, kafkaClient.Close)

	builder := topology.NewBuilder(o, connectClient, kafkaClient, src, wh)

	toolsReg := tools.NewRegistry()
	if err := dbtools.Register(toolsReg, dbtools.Deps{Src: src, Wh: wh, Builder: builder, Owners: o}); err != nil {
		closeOpened()
		return nil, fmt.Errorf("server: register db tools: %w", err)
	}
	if err := kafkatools.Register(toolsReg, kafkatools.Deps{Connect: connectClient, Registry: registryClient, Kafka: kafkaClient}); err != nil {
		closeOpened()
		return nil, fmt.Errorf("server: register kafka tools: %w", err)
	}
	actionsReg := actions.NewRegistry()
	if err := actions.Register(actionsReg, actions.Deps{Connect: connectClient, Registry: registryClient}); err != nil {
		closeOpened()
		return nil, fmt.Errorf("server: register actions: %w", err)
	}

	var once sync.Once
	return &stackDeps{
		Source:    src,
		Warehouse: wh,
		Connect:   connectClient,
		Kafka:     kafkaClient,
		Registry:  registryClient,
		Builder:   builder,
		Owners:    o,
		Tools:     toolsReg,
		Actions:   actionsReg,
		Close:     func() { once.Do(closeOpened) },
	}, nil
}

// splitBrokers splits the comma-separated KAFKA_BROKERS list, trimming blanks.
func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
