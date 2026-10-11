package main

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/topology"
)

// a2Clients bundles the Connect, Kafka and Schema Registry clients. They are
// shared by the topology builder (so pipeline_map/trace_record read real state)
// and, behind -tags realtools, by A2's tools and actions. Without the tag they
// are unreachable stubs, because A2's packages have not merged yet.
type a2Clients struct {
	connect  clients.ConnectClient
	kafka    clients.KafkaAdmin
	registry clients.SchemaRegistryClient
}

// wireRealTools opens the read-only A3 clients (source Postgres, warehouse,
// owners), builds the topology builder and registers the A3 db tools. When
// built with -tags realtools it also registers A2's tools and actions. The
// returned cleanup closes the database pools and the Kafka client; the caller
// must defer it so the clients stay open for the whole agent run.
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
// registers the A3 db tools plus (behind -tags realtools) A2's tools and
// actions. It is separate from wireRealTools so tests can drive the wiring
// with fakes and no live stack.
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
