package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
)

const (
	connectorName = "pg-orders-connector"
	valueSubject  = "pg.public.orders-value"
	pgContainer   = "piperoom-dev-postgres-1"

	// roomDBService is the room service's database in dev/docker-compose.yml. It
	// is not part of the pipeline: `devctl reset` keeps it (QA G19).
	roomDBService = "roomdb"
	// roomDBVolume is its named volume as Docker names it (project_volume).
	roomDBVolume = "piperoom-dev_roomdb"

	// cdcInitFile is the idempotent init script with the heartbeat table and the
	// publication, at its path inside the Postgres container (dev/postgres-init
	// is mounted at /docker-entrypoint-initdb.d).
	cdcInitFile = "/docker-entrypoint-initdb.d/04-cdc.sql"
)

// pipelineServices are the compose services `devctl reset` removes together
// with their anonymous volumes: every service except roomDBService. A unit test
// checks the list against dev/docker-compose.yml.
var pipelineServices = []string{"postgres", "kafka", "schema-registry", "connect", "devconsumer"}

// workerLogDir is where the Connect container writes its worker log on the host
// (dev/docker-compose.yml mounts it; CONNECT_WORKER_LOG_FILE points into it).
var workerLogDir = filepath.Join("dev", ".logs", "connect")

// stack knows where the repo is and how to reach every service.
type stack struct {
	root string
	cfg  config.Config
	http *http.Client
	log  *slog.Logger
	poll time.Duration // waitFor's interval; zero means 2 s (tests shorten it)
	// cdcInit, when set, replaces the docker exec of ensureCDCObjects (tests).
	cdcInit func(ctx context.Context) error
}

func newStack(log *slog.Logger) (*stack, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	return &stack{root: root, cfg: config.Load(), http: &http.Client{Timeout: 10 * time.Second}, log: log}, nil
}

// repoRoot walks up from the working directory to the folder with go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("devctl: working dir: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("devctl: run me inside the piperoom repo (no go.mod found)")
		}
		dir = parent
	}
}

// compose runs docker compose on dev/docker-compose.yml with extra env vars.
func (s *stack) compose(ctx context.Context, env map[string]string, args ...string) error {
	full := append([]string{"compose", "-f", filepath.Join(s.root, "dev", "docker-compose.yml")}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("devctl: docker %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// psql runs SQL inside the Postgres container as the superuser.
func (s *stack) psql(ctx context.Context, db string, sql ...string) (string, error) {
	args := []string{"exec", pgContainer, "psql", "-U", "piperoom", "-d", db, "-v", "ON_ERROR_STOP=1", "-At"}
	for _, q := range sql {
		args = append(args, "-c", q)
	}
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("devctl: psql: %w: %s", err, strings.TrimSpace(out.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// upArgs are the compose arguments of `devctl up`. --remove-orphans clears
// containers of services that left the compose file.
func upArgs() []string {
	return []string{"up", "-d", "--build", "--wait", "--remove-orphans"}
}

// resetArgs are the compose commands `devctl reset` runs before `up`. The default
// removes the pipeline containers with their anonymous volumes (Postgres data,
// Kafka topics, Connect offsets, schemas) and keeps roomdb and its named volume,
// so rooms survive. With rooms, `down -v` removes every volume, the rooms too.
func resetArgs(rooms bool) [][]string {
	if rooms {
		return [][]string{{"down", "-v", "--remove-orphans"}}
	}
	rm := append([]string{"rm", "--stop", "--force", "--volumes"}, pipelineServices...)
	return [][]string{rm}
}

func (s *stack) up(ctx context.Context) error {
	if err := s.prepareWorkerLogDir(); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, upArgs()...); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, s.cfg.ConnectURL+"/connectors", 2*time.Minute); err != nil {
		return err
	}
	if err := s.ensureCDCObjects(ctx); err != nil {
		return err
	}
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("stack is up", "connect", s.cfg.ConnectURL, "registry", s.cfg.SchemaRegistryURL, "kafka", s.cfg.KafkaBrokers,
		"room_db", "localhost:5433/piperoom", "worker_log", filepath.Join(workerLogDir, "connect.log"))
	return nil
}

// prepareWorkerLogDir creates dev/.logs/connect writable for everyone. The
// Connect image runs as a non-root user; on a Linux host it could not write
// into a directory Docker creates as root. (Docker Desktop ignores the mode.)
func (s *stack) prepareWorkerLogDir() error {
	dir := filepath.Join(s.root, workerLogDir)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("devctl: create worker log dir: %w", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		return fmt.Errorf("devctl: make worker log dir writable: %w", err)
	}
	return nil
}

// ensureCDCObjects runs the idempotent CDC init script (heartbeat table, the
// publication with both tables) again. A fresh volume already has them; a volume
// created before the script existed, or a publication that a "filtered" connector
// rewrote, gets them back before the connector is registered.
func (s *stack) ensureCDCObjects(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if s.cdcInit != nil {
		return s.cdcInit(ctx)
	}
	args := []string{"exec", pgContainer, "psql", "-U", "piperoom", "-d", "orders", "-v", "ON_ERROR_STOP=1", "-q", "-f", cdcInitFile}
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("devctl: apply %s: %w: %s", cdcInitFile, err, strings.TrimSpace(out.String()))
	}
	return nil
}

// reset wipes the pipeline and starts it clean. The room database survives
// unless rooms is set (`devctl reset --rooms --yes`).
func (s *stack) reset(ctx context.Context, rooms bool) error {
	for _, args := range resetArgs(rooms) {
		if err := s.compose(ctx, nil, args...); err != nil {
			return err
		}
	}
	if rooms {
		s.log.Warn("reset: room database deleted with the pipeline", "volume", roomDBVolume)
	} else {
		s.log.Info("reset: pipeline data removed; the room database is kept", "service", roomDBService, "volume", roomDBVolume)
	}
	if err := s.up(ctx); err != nil {
		return err
	}
	// Wait until the initial snapshot reached the warehouse, so "reset" ends
	// in a known-good state.
	return s.waitFor(ctx, 2*time.Minute, func() (bool, error) {
		n, err := s.psql(ctx, "warehouse", "SELECT count(*) FROM analytics.orders")
		if err != nil {
			return false, nil
		}
		return n != "0", nil
	})
}

// registerConnector registers dev/connectors/pg-orders-connector.json as is.
func (s *stack) registerConnector(ctx context.Context) error {
	return s.registerConnectorWith(ctx, nil)
}

// registerConnectorWith re-registers the dev connector config with some keys
// overridden, so a scenario can e.g. point the connector at a different role.
// PUT /config creates or updates, so this is safe to repeat.
func (s *stack) registerConnectorWith(ctx context.Context, overrides map[string]string) error {
	base, err := os.ReadFile(filepath.Join(s.root, "dev", "connectors", connectorName+".json"))
	if err != nil {
		return fmt.Errorf("devctl: read connector config: %w", err)
	}
	body, err := connectorConfig(base, overrides)
	if err != nil {
		return err
	}
	return s.putConnectorConfig(ctx, body)
}

// connectorConfig returns the JSON config base with overrides applied.
func connectorConfig(base []byte, overrides map[string]string) ([]byte, error) {
	var cfg map[string]string
	if err := json.Unmarshal(base, &cfg); err != nil {
		return nil, fmt.Errorf("devctl: decode connector config: %w", err)
	}
	for k, v := range overrides {
		cfg[k] = v
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("devctl: encode connector config: %w", err)
	}
	return b, nil
}

func (s *stack) putConnectorConfig(ctx context.Context, body []byte) error {
	_, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/config", body, http.StatusOK, http.StatusCreated)
	return err
}

// liveConnectorConfig is the config Connect runs now, which may differ from the
// file (an approved agent fix changes it).
func (s *stack) liveConnectorConfig(ctx context.Context) (map[string]string, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connectors/"+connectorName+"/config", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var cfg map[string]string
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("devctl: decode live connector config: %w", err)
	}
	return cfg, nil
}

type connectorStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"tasks"`
}

func (s *stack) connectorStatus(ctx context.Context) (connectorStatus, error) {
	var st connectorStatus
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connectors/"+connectorName+"/status", nil, http.StatusOK)
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("devctl: decode connector status: %w", err)
	}
	return st, nil
}

// waitConnector waits until the connector and task 0 are in state want.
func (s *stack) waitConnector(ctx context.Context, want string, timeout time.Duration) error {
	return s.waitFor(ctx, timeout, func() (bool, error) {
		st, err := s.connectorStatus(ctx)
		if err != nil {
			return false, nil
		}
		return st.Connector.State == "RUNNING" && len(st.Tasks) > 0 && st.Tasks[0].State == want, nil
	})
}

func (s *stack) setCompatibility(ctx context.Context, subject, level string) error {
	body := []byte(`{"compatibility":"` + level + `"}`)
	_, err := s.do(ctx, http.MethodPut, s.cfg.SchemaRegistryURL+"/config/"+url.PathEscape(subject), body, http.StatusOK)
	return err
}

// schemaVersions returns the registered version numbers of a subject.
func (s *stack) schemaVersions(ctx context.Context, subject string) ([]int, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.SchemaRegistryURL+"/subjects/"+url.PathEscape(subject)+"/versions", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var vs []int
	if err := json.Unmarshal(b, &vs); err != nil {
		return nil, fmt.Errorf("devctl: decode schema versions: %w", err)
	}
	return vs, nil
}

func (s *stack) restartTask(ctx context.Context, task int) error {
	_, err := s.do(ctx, http.MethodPost, fmt.Sprintf("%s/connectors/%s/tasks/%d/restart", s.cfg.ConnectURL, connectorName, task), nil, http.StatusOK, http.StatusNoContent, http.StatusAccepted)
	return err
}

func (s *stack) do(ctx context.Context, method, u string, body []byte, okCodes ...int) ([]byte, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, fmt.Errorf("devctl: %s %s: %w", method, u, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("devctl: %s %s: %w", method, u, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("devctl: read %s %s: %w", method, u, err)
	}
	for _, c := range okCodes {
		if resp.StatusCode == c {
			return b, nil
		}
	}
	return nil, &httpStatusError{Method: method, URL: u, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
}

// httpStatusError is an answer with an unexpected status code.
type httpStatusError struct {
	Method, URL string
	Code        int
	Body        string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("devctl: %s %s: HTTP %d: %s", e.Method, e.URL, e.Code, e.Body)
}

// restartTaskSettled restarts a task, retrying while Connect answers 409: right
// after a config change the worker group rebalances and refuses restarts.
func (s *stack) restartTaskSettled(ctx context.Context, task int, timeout time.Duration) error {
	var last error
	isConflict := func(err error) bool {
		var he *httpStatusError
		return errors.As(err, &he) && he.Code == http.StatusConflict
	}
	err := s.waitFor(ctx, timeout, func() (bool, error) {
		last = s.restartTask(ctx, task)
		if isConflict(last) {
			return false, nil
		}
		return true, last
	})
	if err != nil && isConflict(last) {
		return fmt.Errorf("devctl: restart task %d: %w (Connect still answers: %w)", task, err, last)
	}
	return err
}

func (s *stack) waitHTTP(ctx context.Context, u string, timeout time.Duration) error {
	return s.waitFor(ctx, timeout, func() (bool, error) {
		_, err := s.do(ctx, http.MethodGet, u, nil, http.StatusOK)
		return err == nil, nil
	})
}

func (s *stack) waitFor(ctx context.Context, timeout time.Duration, cond func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	every := s.poll
	if every <= 0 {
		every = 2 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("devctl: timed out after %s", timeout)
		case <-t.C:
		}
	}
}

// sourceConn opens a connection to the source database from the host.
func (s *stack) sourceConn(ctx context.Context) (*pgx.Conn, error) {
	c, err := pgx.Connect(ctx, s.cfg.SourceDBURL)
	if err != nil {
		return nil, fmt.Errorf("devctl: connect source db: %w", err)
	}
	return c, nil
}
