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

// wireRealTools builds the A2 clients, tools and actions from config.Load().
// It compiles only with -tags realtools, once the A2/A3 packages are merged.
func wireRealTools(cfg config.Config, reg *tools.Registry, areg *actions.Registry) error {
	httpClient := &http.Client{Timeout: 10 * time.Second}

	connectClient := connect.New(cfg.ConnectURL, httpClient)
	registryClient := schemareg.New(cfg.SchemaRegistryURL, httpClient)
	kafkaClient, err := kafka.New(splitBrokers(cfg.KafkaBrokers), registryClient)
	if err != nil {
		return fmt.Errorf("investigate: kafka client: %w", err)
	}
	_ = kafkaClient // the agent loop owns no shutdown hook; the process ends with it.

	if err := kafkatools.Register(reg, kafkatools.Deps{
		Connect:  connectClient,
		Registry: registryClient,
		Kafka:    kafkaClient,
	}); err != nil {
		return fmt.Errorf("investigate: register kafka tools: %w", err)
	}
	if err := actions.Register(areg, actions.Deps{
		Connect:  connectClient,
		Registry: registryClient,
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
