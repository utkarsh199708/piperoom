package loadtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/migrations"
)

// PostgresImage is the database image every run uses.
const PostgresImage = "postgres:16"

// Postgres is the database server of a load run: a throwaway docker
// container the run owns, or a server given by URL. Every scenario run gets
// its own database on it, dropped afterwards.
type Postgres struct {
	AdminURL  string
	Version   string
	container string // "" when the server is not ours
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails (crypto/rand docs)
	return hex.EncodeToString(b)
}

func docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// StartPostgres runs a throwaway postgres:16 container named
// <prefix>-pg-<random> on a docker-assigned loopback port and waits until it
// accepts connections. Close removes it. tmpfsData puts the data directory in
// memory: faster to start, but no disk I/O, so only CI smoke runs use it.
func StartPostgres(ctx context.Context, prefix string, tmpfsData bool) (*Postgres, error) {
	name := fmt.Sprintf("%s-pg-%s", prefix, randHex(4))
	// The labels let a later run remove this container if this process dies
	// without cleaning up (SIGKILL, a go test timeout): see RemoveStale.
	args := []string{"run", "-d", "--rm", "--name", name,
		"--label", labelPrefix + "=" + prefix, "--label", fmt.Sprintf("%s=%d", labelOwner, os.Getpid()),
		"-e", "POSTGRES_USER=lt", "-e", "POSTGRES_PASSWORD=lt-local-only", "-e", "POSTGRES_DB=lt",
		"-p", "127.0.0.1::5432"}
	if tmpfsData {
		args = append(args, "--tmpfs", "/var/lib/postgresql/data:rw")
	}
	args = append(args, PostgresImage, "-c", "max_connections=300")
	if _, err := docker(ctx, args...); err != nil {
		return nil, fmt.Errorf("loadtest: start postgres: %w", err)
	}
	p := &Postgres{container: name}
	port, err := docker(ctx, "port", name, "5432/tcp")
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("loadtest: postgres port: %w", err)
	}
	// "127.0.0.1:49153" (possibly one line per address family).
	port = strings.TrimSpace(strings.Split(port, "\n")[0])
	p.AdminURL = "postgres://lt:lt-local-only@" + port + "/lt?sslmode=disable"
	if err := p.waitReady(ctx, 60*time.Second); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Container labels: the name prefix and the PID of the driver that owns it.
const (
	labelPrefix = "piperoom.loadtest.prefix"
	labelOwner  = "piperoom.loadtest.owner-pid"
)

// staleContainers picks, from `docker ps` lines of "<name> <owner pid>", the
// containers named <prefix>-pg-* whose owner process is gone. A line with no
// parsable PID is left alone (not ours to judge).
func staleContainers(psOutput, prefix string, alive func(int) bool) []string {
	var out []string
	for _, line := range strings.Split(psOutput, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 || !strings.HasPrefix(f[0], prefix+"-pg-") {
			continue
		}
		pid, err := strconv.Atoi(f[1])
		if err != nil || pid <= 0 {
			continue
		}
		if !alive(pid) {
			out = append(out, f[0])
		}
	}
	return out
}

// RemoveStale removes this prefix's database containers left behind by a
// driver that died without cleaning up, and returns their names. Containers
// of a live driver (another run in progress) are kept.
func RemoveStale(ctx context.Context, prefix string) ([]string, error) {
	out, err := docker(ctx, "ps", "-a", "--filter", "label="+labelPrefix+"="+prefix,
		"--format", "{{.Names}} {{.Label \""+labelOwner+"\"}}")
	if err != nil {
		return nil, fmt.Errorf("loadtest: list stale containers: %w", err)
	}
	stale := staleContainers(out, prefix, processAlive)
	for _, name := range stale {
		if _, err := docker(ctx, "rm", "-f", name); err != nil {
			return stale, fmt.Errorf("loadtest: remove stale container %s: %w", name, err)
		}
	}
	return stale, nil
}

// ExistingPostgres uses the server at adminURL; the caller's role must be
// allowed to create databases. Close leaves the server running.
func ExistingPostgres(ctx context.Context, adminURL string) (*Postgres, error) {
	p := &Postgres{AdminURL: adminURL}
	if err := p.waitReady(ctx, 10*time.Second); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Postgres) waitReady(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)
	var last error
	for time.Now().Before(deadline) {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(cctx, p.AdminURL)
		if err == nil {
			err = conn.QueryRow(cctx, "SHOW server_version").Scan(&p.Version)
			_ = conn.Close(cctx)
		}
		cancel()
		if err == nil {
			return nil
		}
		last = err
		if !sleepCtx(ctx, 300*time.Millisecond) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("loadtest: postgres not ready after %s: %w", d, last)
}

// Close removes the container when the run owns it.
func (p *Postgres) Close() {
	if p.container == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = docker(ctx, "rm", "-f", p.container)
}

// withQuery adds the k=v&k=v parameters of extra to the query of a database
// URL; a key that is already there is replaced.
func withQuery(dbURL, extra string) (string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", fmt.Errorf("loadtest: database url: %w", err)
	}
	add, err := url.ParseQuery(extra)
	if err != nil {
		return "", fmt.Errorf("loadtest: extra database parameters %q: %w", extra, err)
	}
	q := u.Query()
	for k, vs := range add {
		if k == "" || len(vs) == 0 {
			return "", fmt.Errorf("loadtest: extra database parameters %q: empty key or value", extra)
		}
		q.Set(k, vs[len(vs)-1])
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Database is one scenario run's database.
type Database struct {
	Name string
	URL  string
	p    *Postgres
}

// CreateDatabase creates a fresh database and applies the migrations, as
// cmd/server does before it opens the store.
func (p *Postgres) CreateDatabase(ctx context.Context) (*Database, error) {
	name := "lt_" + randHex(6)
	conn, err := pgx.Connect(ctx, p.AdminURL)
	if err != nil {
		return nil, fmt.Errorf("loadtest: create database: %w", err)
	}
	defer conn.Close(context.Background())
	// The name is generated (lt_ + hex), never input; Sanitize quotes it anyway.
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return nil, fmt.Errorf("loadtest: create database %s: %w", name, err)
	}
	u, err := url.Parse(p.AdminURL)
	if err != nil {
		return nil, fmt.Errorf("loadtest: admin url: %w", err)
	}
	u.Path = "/" + name
	db := &Database{Name: name, URL: u.String(), p: p}
	if err := db.migrate(ctx); err != nil {
		db.Drop()
		return nil, err
	}
	return db, nil
}

func (d *Database) migrate(ctx context.Context) error {
	cc, err := pgx.ParseConfig(d.URL)
	if err != nil {
		return fmt.Errorf("loadtest: parse db url: %w", err)
	}
	sqlDB := stdlib.OpenDB(*cc)
	defer sqlDB.Close()
	mctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if _, err := migrations.Up(mctx, sqlDB); err != nil {
		return fmt.Errorf("loadtest: migrate %s: %w", d.Name, err)
	}
	return nil
}

// Drop removes the database, closing any connection left on it.
func (d *Database) Drop() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, d.p.AdminURL)
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{d.Name}.Sanitize()+" WITH (FORCE)")
}

// DockerAvailable reports whether the docker CLI can reach a daemon.
func DockerAvailable(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := docker(cctx, "version", "--format", "{{.Server.Version}}"); err != nil {
		return errors.Join(errors.New("loadtest: docker is not available"), err)
	}
	return nil
}
