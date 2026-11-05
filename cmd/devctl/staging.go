package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"piperoom/internal/config"
)

// The staging profile (X8d, dev/README.md "Staging profile"): a second Connect
// and a second Schema Registry on the same Kafka, so a fix can be tried before
// it touches production. Everything the two stacks could share is separate:
// group.id, the internal topics, the registry's store topic, the replication
// slot, the publication and the Debezium topic prefix. The prefix matters most:
// the staging connector reads the same table, and under the production prefix it
// would write into the topics the production consumer reads.
const (
	stagingProfile = "staging"

	// Host ports of connect-staging and schema-registry-staging
	// (dev/docker-compose.yml); the owners file of dev/owners.staging.yaml
	// names the same URLs.
	defaultStagingConnectURL  = "http://localhost:18083"
	defaultStagingRegistryURL = "http://localhost:18081"
	stagingConnectEnv         = "STAGING_CONNECT_URL"
	stagingRegistryEnv        = "STAGING_SCHEMA_REGISTRY_URL"

	// What the staging connector changes from dev/connectors/pg-orders-connector.json.
	// Its name stays the same: X8's Stage looks the production connector's name up on staging.
	stagingTopicPrefix = "pgstg"
	stagingSlot        = "debezium_orders_staging"
	stagingPublication = "dbz_publication_staging"
)

// stagingTopics are the topics the staging stack leaves in the shared Kafka.
// Production's own (pg.public.orders, _connect_*, _schemas) are never in it.
var stagingTopics = []string{
	stagingTopicPrefix + ".public.orders",
	"_connect_configs_staging",
	"_connect_offsets_staging",
	"_connect_status_staging",
	"_schemas_staging",
}

// stagingServices are the compose services of the profile.
var stagingServices = []string{"connect-staging", "schema-registry-staging"}

// stagingEndpoints are the staging Connect and Schema Registry base URLs.
type stagingEndpoints struct{ Connect, Registry string }

// resolveStaging reads the staging URLs (defaults, or STAGING_CONNECT_URL and
// STAGING_SCHEMA_REGISTRY_URL) and refuses one that is the production endpoint:
// the same rule the owners file has (docs/api.md, X8 staging, "Never
// production"), checked here before anything is started.
func resolveStaging(cfg config.Config, getenv func(string) string) (stagingEndpoints, error) {
	pick := func(env, def string) string {
		if v := strings.TrimSpace(getenv(env)); v != "" {
			return v
		}
		return def
	}
	eps := stagingEndpoints{
		Connect:  strings.TrimRight(pick(stagingConnectEnv, defaultStagingConnectURL), "/"),
		Registry: strings.TrimRight(pick(stagingRegistryEnv, defaultStagingRegistryURL), "/"),
	}
	for _, c := range []struct{ name, staging, prod string }{
		{stagingConnectEnv, eps.Connect, cfg.ConnectURL},
		{stagingRegistryEnv, eps.Registry, cfg.SchemaRegistryURL},
	} {
		key, err := endpointKey(c.staging)
		if err != nil {
			return stagingEndpoints{}, fmt.Errorf("devctl: %s: %w", c.name, err)
		}
		if c.prod == "" {
			continue
		}
		if prod, err := endpointKey(c.prod); err == nil && prod == key {
			return stagingEndpoints{}, fmt.Errorf("devctl: %s is the production endpoint; staging must be a different Connect and Schema Registry", c.name)
		}
	}
	return eps, nil
}

// endpointKey is scheme://host:port with the scheme's default port filled in
// and the case folded, so "http://Localhost/" and "http://localhost:80" compare equal.
func endpointKey(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", errors.New("want an http or https URL with a host")
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	return u.Scheme + "://" + strings.ToLower(u.Hostname()) + ":" + port, nil
}

// stagingConnectorOverrides is what the staging connector changes: its own
// topic prefix, slot and publication.
func stagingConnectorOverrides() map[string]string {
	return map[string]string{
		"topic.prefix":     stagingTopicPrefix,
		"slot.name":        stagingSlot,
		"publication.name": stagingPublication,
	}
}

// stagingUp waits for the staging services and registers the same connector on
// staging Connect, under its own prefix and slot. PUT /config creates or
// updates, so a second `up --staging` changes nothing.
func (s *stack) stagingUp(ctx context.Context, eps stagingEndpoints) error {
	if err := s.waitHTTP(ctx, eps.Registry+"/subjects", time.Minute); err != nil {
		return fmt.Errorf("devctl: staging schema registry: %w", err)
	}
	if err := s.waitHTTP(ctx, eps.Connect+"/connectors", 2*time.Minute); err != nil {
		return fmt.Errorf("devctl: staging connect: %w", err)
	}
	body, err := s.connectorConfigWith(stagingConnectorOverrides())
	if err != nil {
		return err
	}
	if _, err := s.do(ctx, http.MethodPut, eps.Connect+"/connectors/"+connectorName+"/config", body, http.StatusOK, http.StatusCreated); err != nil {
		return err
	}
	if err := s.waitConnectorAt(ctx, eps.Connect, "RUNNING", 90*time.Second); err != nil {
		return fmt.Errorf("devctl: staging connector: %w", err)
	}
	s.log.Info("staging is up", "connect", eps.Connect, "registry", eps.Registry, "prefix", stagingTopicPrefix, "slot", stagingSlot)
	return nil
}

// stagingDown removes only the staging stack and what it left behind, and
// leaves production running: the staging connector first (while its worker can
// still answer), the containers, then the slot, publication and topics. A slot
// would otherwise keep retaining WAL on the production Postgres. A step that
// fails does not stop the next; a second run finds nothing and succeeds.
func (s *stack) stagingDown(ctx context.Context) error {
	eps, err := resolveStaging(s.cfg, os.Getenv)
	if err != nil {
		return err
	}
	var errs []error
	if _, err := s.do(ctx, http.MethodDelete, eps.Connect+"/connectors/"+connectorName, nil, http.StatusNoContent, http.StatusOK, http.StatusNotFound); err != nil {
		// Staging not running is the usual reason; the slot is dropped below either way.
		s.log.Info("staging connector not deleted (is staging up?)", "err", err)
	}
	if err := s.compose(ctx, nil, append([]string{"--profile", stagingProfile, "rm", "--stop", "--force"}, stagingServices...)...); err != nil {
		errs = append(errs, err)
	}
	// The walsender can outlive its container for a moment: retry while the slot is active.
	step := s.stagingStepTimeout
	if step <= 0 {
		step = 30 * time.Second
	}
	var lastErr error
	if err := s.waitFor(ctx, step, func() (bool, error) {
		_, lastErr = s.psql(ctx, "orders",
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"+stagingSlot+"'",
			"DROP PUBLICATION IF EXISTS "+stagingPublication)
		return lastErr == nil, nil
	}); err != nil {
		errs = append(errs, fmt.Errorf("devctl: drop staging slot %s and publication %s: %w (last attempt: %v)", stagingSlot, stagingPublication, err, lastErr))
	}
	// kadm retries without a deadline of its own: bound a Kafka that is down.
	tctx, cancel := context.WithTimeout(ctx, step)
	defer cancel()
	if err := s.deleteTopics(tctx, stagingTopics...); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// stagingFlag reads the one optional argument of up, reset and down.
func stagingFlag(cmd string, rest []string) (bool, error) {
	switch {
	case len(rest) == 0:
		return false, nil
	case len(rest) == 1 && rest[0] == "--staging":
		return true, nil
	}
	return false, fmt.Errorf("usage: devctl %s [--staging]", cmd)
}
