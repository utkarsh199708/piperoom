package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
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

// a2Clients bundles the Connect, Kafka and Schema Registry clients. They are
// shared by the topology builder (so pipeline_map/trace_record read real state)
// and by A2's tools and actions.
type a2Clients struct {
	connect  clients.ConnectClient
	kafka    clients.KafkaAdmin
	registry clients.SchemaRegistryClient
}

// wireRealTools opens the read-only clients (source Postgres, warehouse,
// Connect, Kafka, Schema Registry), loads owners, builds the topology builder
// and registers the A3 db tools plus A2's Kafka/Connect/Schema Registry tools
// and actions. The returned cleanup closes the database pools and the Kafka
// client; the caller must defer it so the clients stay open for the whole
// agent run.
func wireRealTools(cfg config.Config, reg *tools.Registry, areg *actions.Registry) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	o, err := owners.Load(cfg.OwnersFile)
	if err != nil {
		return nil, fmt.Errorf("investigate: load owners: %w", err)
	}

	src, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		return nil, fmt.Errorf("investigate: source postgres: %w", err)
	}

	wh, err := warehouse.New(ctx, cfg.WarehouseDBURL)
	if err != nil {
		src.Close()
		return nil, fmt.Errorf("investigate: warehouse: %w", err)
	}

	a2, closeA2, err := newA2Clients(cfg)
	if err != nil {
		src.Close()
		wh.Close()
		return nil, err
	}

	if err := registerTools(reg, areg, src, wh, o, a2); err != nil {
		closeA2()
		src.Close()
		wh.Close()
		return nil, err
	}

	return func() {
		closeA2()
		src.Close()
		wh.Close()
	}, nil
}

// registerTools builds the topology builder over the given clients and
// registers the A3 db tools plus A2's tools and actions. It is separate from
// wireRealTools so tests can drive the wiring with fakes and no live stack.
func registerTools(reg *tools.Registry, areg *actions.Registry, src clients.SourceDB, wh clients.Warehouse, o *owners.Owners, a2 *a2Clients) error {
	b := topology.NewBuilder(o, a2.connect, a2.kafka, src, wh)
	if err := dbtools.Register(reg, dbtools.Deps{Src: src, Wh: wh, Builder: b}); err != nil {
		return fmt.Errorf("investigate: register db tools: %w", err)
	}
	if err := wireA2(reg, areg, a2); err != nil {
		return err
	}
	return nil
}

// newA2Clients builds the real Connect, Schema Registry and Kafka clients from
// config.
func newA2Clients(cfg config.Config) (*a2Clients, func(), error) {
	httpClient := &http.Client{Timeout: 10 * time.Second}

	connectClient := connect.New(cfg.ConnectURL, httpClient)
	registryClient := schemareg.New(cfg.SchemaRegistryURL, httpClient)
	kafkaClient, err := kafka.New(splitBrokers(cfg.KafkaBrokers), registryClient)
	if err != nil {
		return nil, nil, fmt.Errorf("investigate: kafka client: %w", err)
	}
	return &a2Clients{
		connect:  connectClient,
		kafka:    kafkaClient,
		registry: registryClient,
	}, func() { kafkaClient.Close() }, nil
}

// wireA2 registers A2's read-only Kafka/Connect/Schema Registry tools and the
// write actions the agent may propose (never run).
func wireA2(reg *tools.Registry, areg *actions.Registry, a2 *a2Clients) error {
	if err := kafkatools.Register(reg, kafkatools.Deps{
		Connect:  a2.connect,
		Registry: a2.registry,
		Kafka:    a2.kafka,
	}); err != nil {
		return fmt.Errorf("investigate: register kafka tools: %w", err)
	}
	if err := actions.Register(areg, actions.Deps{
		Connect:  a2.connect,
		Registry: a2.registry,
	}); err != nil {
		return fmt.Errorf("investigate: register actions: %w", err)
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
