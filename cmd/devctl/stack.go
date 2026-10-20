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
)

// stack knows where the repo is and how to reach every service.
type stack struct {
	root string
	cfg  config.Config
	http *http.Client
	log  *slog.Logger

	// Test seams; nil or zero means the real thing. Unit tests play Postgres
	// and docker compose with them, since neither runs without Docker.
	sqlFn            func(ctx context.Context, db string, sql ...string) (string, error)
	composeFn        func(ctx context.Context, env map[string]string, args ...string) error
	clickhouseExecFn func(ctx context.Context, query string) error
	groupFn          func(ctx context.Context, group string) (groupView, error)
	// airflowCLIFn replaces `docker exec … airflow …` in unit tests.
	airflowCLIFn func(ctx context.Context, args ...string) (string, error)
	// poll is how often waitFor checks its condition (default 2 s).
	poll time.Duration
	// clickhouseRetryEvery and clickhouseRetryFor pace the ClickHouse init
	// retry (default every 2 s for up to 60 s).
	clickhouseRetryEvery, clickhouseRetryFor time.Duration
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
	if s.composeFn != nil {
		return s.composeFn(ctx, env, args...)
	}
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
	if s.sqlFn != nil {
		return s.sqlFn(ctx, db, sql...)
	}
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

func (s *stack) up(ctx context.Context) error {
	if err := s.compose(ctx, nil, "up", "-d", "--build", "--wait"); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, s.cfg.ConnectURL+"/connectors", 2*time.Minute); err != nil {
		return err
	}
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("stack is up", "connect", s.cfg.ConnectURL, "registry", s.cfg.SchemaRegistryURL, "kafka", s.cfg.KafkaBrokers)
	return nil
}

func (s *stack) reset(ctx context.Context) error {
	if err := s.compose(ctx, nil, "down", "-v", "--remove-orphans"); err != nil {
		return err
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

func (s *stack) registerConnector(ctx context.Context) error {
	body, err := os.ReadFile(filepath.Join(s.root, "dev", "connectors", connectorName+".json"))
	if err != nil {
		return fmt.Errorf("devctl: read connector config: %w", err)
	}
	// PUT /config creates or updates, so this is safe to repeat.
	_, err = s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/config", body, http.StatusOK, http.StatusCreated)
	return err
}

// registerConnectorUser re-registers the connector with database.user/password
// overridden, so a scenario can point the connector at a different role.
func (s *stack) registerConnectorUser(ctx context.Context, user, pass string) error {
	return s.registerConnectorWith(ctx, map[string]string{"database.user": user, "database.password": pass})
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
	return nil, fmt.Errorf("devctl: %s %s: HTTP %d: %s", method, u, resp.StatusCode, strings.TrimSpace(string(b)))
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
