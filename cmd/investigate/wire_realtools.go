//go:build realtools

package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// newA2Clients builds the real Connect, Schema Registry and Kafka clients from
// config. Only compiled with -tags realtools, once A2's packages merge.
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

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
