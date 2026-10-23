//go:build integration

package knowledge

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/testenv"
)

// TestReferenceProjectsBuildAndRun is the row's "done when" (U6a): each of
// the four reference projects (Maven and Gradle, Postgres and MySQL) is
// rendered, built with the real build tool and run, and its output shows a
// row inserted after start-up (the canary nonce) and then a second one that
// can only arrive by streaming. This is the CI integration job running the
// build tools, never the setup runner: the runner has no op that builds or
// runs a reference project (U6b).
//
// Postgres is the dev stack's source database; the test captures its own
// table through its own slot and publication and drops all three. The dev
// stack has no MySQL, so the MySQL projects run against a throwaway
// container of the image the knowledge index pins (ImageByName("mysql")).
//
// It skips when java, mvn, gradle or (for MySQL) a Docker daemon is
// missing. The CI full-stack job's runner image has all four, so these skips
// do not fire there and need no entry in .github/integration-skip-allow.txt.
// PIPEROOM_IT_MYSQL_IMAGE overrides the MySQL image (a registry mirror on a
// machine that cannot pull from Docker Hub).
func TestReferenceProjectsBuildAndRun(t *testing.T) {
	cfg := testenv.Require(t)
	requireJava17(t)

	pg := newPGSource(t, cfg.SourceDBURL)
	var my *mysqlSource      // started on first use, shared by both builds
	release := "3.2.0.Final" // the dev stack's Debezium release (images.go)

	parentCleanup := t.Cleanup // the container outlives the subtest that starts it
	for _, build := range []BuildTool{BuildMaven, BuildGradle} {
		for _, src := range []SourceKind{SourcePostgres, SourceMySQL} {
			t.Run(string(build)+"/"+string(src), func(t *testing.T) {
				requireTool(t, build)
				suffix := randHex(t, 4)
				var p EmbeddedParams
				var insert func(ctx context.Context, nonce string) error
				var env []string
				switch src {
				case SourcePostgres:
					p, insert, env = pg.project(t, build, release, suffix)
				case SourceMySQL:
					if my == nil {
						my = startMySQL(t, parentCleanup)
					}
					p, insert, env = my.project(t, build, release, suffix)
				}
				runReferenceProject(t, p, insert, env)
			})
		}
	}
}

// runReferenceProject renders p into a temp folder, builds it, starts it,
// inserts two nonces and waits for both in its output, then stops it.
func runReferenceProject(t *testing.T, p EmbeddedParams, insert func(context.Context, string) error, env []string) {
	t.Helper()
	files, err := RenderEmbedded(p)
	if err != nil {
		t.Fatalf("RenderEmbedded: %v", err)
	}
	dir := filepath.Join(t.TempDir(), EmbeddedRootDir)
	for _, f := range files {
		path := filepath.Join(dir, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, f.Content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmds := p.Commands()

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, cmds.Build[0], cmds.Build[1:]...)
	build.Dir = dir
	start := time.Now()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(cmds.Build, " "), err, tail(out, 8000))
	}
	t.Logf("%s built in %s", p.Build, time.Since(start).Round(time.Second))

	runArgv := cmds.Run
	if runtime.GOOS == "windows" && cmds.RunWindows != nil {
		runArgv = cmds.RunWindows
	}
	if !filepath.IsAbs(runArgv[0]) && strings.ContainsAny(runArgv[0], `/\`) {
		runArgv = append([]string{filepath.Join(dir, filepath.FromSlash(runArgv[0]))}, runArgv[1:]...)
	}
	runCtx, cancelRun := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelRun()
	run := exec.CommandContext(runCtx, runArgv[0], runArgv[1:]...)
	run.Dir = dir
	run.Env = append(os.Environ(), env...)
	// Stop it as a person would (Ctrl-C), so the shutdown hook closes the
	// engine and flushes offsets; kill only if it does not stop in time.
	run.Cancel = func() error {
		if runtime.GOOS == "windows" {
			return run.Process.Kill()
		}
		return run.Process.Signal(os.Interrupt)
	}
	run.WaitDelay = 30 * time.Second
	out := newLineLog()
	run.Stdout = out
	run.Stderr = out
	if err := run.Start(); err != nil {
		t.Fatalf("start %v: %v", runArgv, err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancelRun()
		_ = run.Wait()
	}
	defer stop()

	if !out.waitFor("piperoom-reference started", 2*time.Minute) {
		stop()
		t.Fatalf("the program did not start; output:\n%s", out.tail(8000))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The first nonce may arrive in the snapshot or the stream; the second
	// is inserted after the first is seen, so it can only be streamed.
	for _, nonce := range []string{"canary-a-" + randHex(t, 6), "canary-b-" + randHex(t, 6)} {
		if err := insert(ctx, nonce); err != nil {
			t.Fatalf("insert %s: %v", nonce, err)
		}
		if !out.waitForEvent(nonce, 3*time.Minute) {
			stop()
			t.Fatalf("no change event with %s; output:\n%s", nonce, out.tail(8000))
		}
	}
	stop()
	if strings.Contains(out.String(), "engine stopped with an error") {
		t.Errorf("the engine reported an error:\n%s", out.tail(8000))
	}
}

// pgSource is the dev stack's Postgres.
type pgSource struct {
	url  *url.URL
	conn *pgx.Conn
}

func newPGSource(t *testing.T, raw string) *pgSource {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("SOURCE_DB_URL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatalf("connect to the source Postgres: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return &pgSource{url: u, conn: conn}
}

func (s *pgSource) project(t *testing.T, build BuildTool, release, suffix string) (EmbeddedParams, func(context.Context, string) error, []string) {
	t.Helper()
	name := "it_emb_" + suffix // table, slot and publication
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.conn.Exec(ctx, "CREATE TABLE "+pgx.Identifier{"public", name}.Sanitize()+" (id serial PRIMARY KEY, note text NOT NULL)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { s.dropAll(t, name) })

	host, portStr, err := net.SplitHostPort(s.url.Host)
	if err != nil {
		host, portStr = s.url.Hostname(), "5432"
	}
	port, _ := strconv.Atoi(portStr)
	pw, _ := s.url.User.Password()
	p := EmbeddedParams{
		Build: build, Source: SourcePostgres, GroupID: "io.piperoom.it", ArtifactID: "it-emb-" + strings.ReplaceAll(suffix, "_", "-"),
		Release: release, Host: host, Port: port, Database: strings.TrimPrefix(s.url.Path, "/"),
		User: s.url.User.Username(), PasswordSecret: "it_pg_password", TopicPrefix: name,
		Tables: []string{"public." + name}, SlotName: name,
	}
	insert := func(ctx context.Context, nonce string) error {
		_, err := s.conn.Exec(ctx, "INSERT INTO "+pgx.Identifier{"public", name}.Sanitize()+" (note) VALUES ($1)", nonce)
		return err
	}
	return p, insert, []string{"PIPEROOM_SECRET_it_pg_password=" + pw}
}

// dropAll removes the test's slot, publication and table. The slot is
// dropped once the stopped engine has released it: a slot left behind would
// hold WAL on the shared dev database.
func (s *pgSource) dropAll(t *testing.T, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		var active *bool
		err := s.conn.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			t.Errorf("cleanup: reading slot %s: %v", name, err)
			break
		}
		if active != nil && !*active {
			if _, err := s.conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
				t.Errorf("cleanup: dropping slot %s: %v", name, err)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Errorf("cleanup: slot %s is still active", name)
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	if _, err := s.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Errorf("cleanup: dropping publication %s: %v", name, err)
	}
	if _, err := s.conn.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{"public", name}.Sanitize()); err != nil {
		t.Errorf("cleanup: dropping table %s: %v", name, err)
	}
}

// mysqlSource is a throwaway MySQL container of the pinned image.
type mysqlSource struct {
	container string
	port      int
	password  string
}

func startMySQL(t *testing.T, cleanup func(func())) *mysqlSource {
	t.Helper()
	requireDocker(t)
	image := os.Getenv("PIPEROOM_IT_MYSQL_IMAGE")
	if image == "" {
		im, err := ImageByName("mysql")
		if err != nil {
			t.Fatalf("ImageByName(mysql): %v", err)
		}
		image = im.Ref()
	}
	s := &mysqlSource{container: "it-emb-mysql-" + randHex(t, 4), port: freeTCPPort(t), password: "it-" + randHex(t, 8)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// MySQL 8.4 writes a row-based binlog with server_id 1 by default, which
	// is what the connector needs; the flags only make that explicit.
	args := []string{"run", "-d", "--rm", "--name", s.container,
		"-e", "MYSQL_ROOT_PASSWORD=" + s.password,
		"-p", "127.0.0.1:" + strconv.Itoa(s.port) + ":3306",
		image, "--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW"}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker run mysql: %v\n%s", err, out)
	}
	cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
		defer ccancel()
		_ = exec.CommandContext(cctx, "docker", "rm", "-f", s.container).Run()
	})
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if _, err := s.sql(ctx, "SELECT 1"); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("mysql did not become ready: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	return s
}

// sql runs a statement with the mysql client inside the container; the
// password goes in MYSQL_PWD, not on the command line.
func (s *mysqlSource) sql(ctx context.Context, stmt string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-e", "MYSQL_PWD="+s.password, s.container,
		"mysql", "-uroot", "--protocol=TCP", "-h127.0.0.1", "-N", "-e", stmt)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("mysql %q: %w: %s", stmt, err, tail(out, 2000))
	}
	return out, nil
}

func (s *mysqlSource) project(t *testing.T, build BuildTool, release, suffix string) (EmbeddedParams, func(context.Context, string) error, []string) {
	t.Helper()
	db := "it_emb_" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := s.sql(ctx, "CREATE DATABASE "+db+"; CREATE TABLE "+db+".events (id INT AUTO_INCREMENT PRIMARY KEY, note VARCHAR(64) NOT NULL)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	server, err := strconv.ParseUint(suffix, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	p := EmbeddedParams{
		Build: build, Source: SourceMySQL, GroupID: "io.piperoom.it", ArtifactID: "it-emb-" + suffix,
		Release: release, Host: "127.0.0.1", Port: s.port, Database: db, User: "root",
		PasswordSecret: "it_mysql_password", TopicPrefix: db, Tables: []string{db + ".events"},
		ServerID: uint32(server%4000000000) + 1000, // never the server's own id 1
	}
	insert := func(ctx context.Context, nonce string) error {
		if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(nonce) {
			return fmt.Errorf("nonce %q is not safe to inline", nonce)
		}
		_, err := s.sql(ctx, "INSERT INTO "+db+".events (note) VALUES ('"+nonce+"')")
		return err
	}
	return p, insert, []string{"PIPEROOM_SECRET_it_mysql_password=" + s.password}
}

// lineLog collects a process's output and lets the test wait for a line.
type lineLog struct {
	mu   sync.Mutex
	buf  strings.Builder
	cond *sync.Cond
}

func newLineLog() *lineLog {
	l := &lineLog{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *lineLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.buf.Len() < 32<<20 { // cap what a runaway process can make the test hold
		l.buf.Write(p)
	}
	l.cond.Broadcast()
	return len(p), nil
}

func (l *lineLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *lineLog) tail(n int) string { return tail([]byte(l.String()), n) }

// waitFor reports whether a line containing s appeared within d.
func (l *lineLog) waitFor(s string, d time.Duration) bool {
	return l.waitLine(d, func(line string) bool { return strings.Contains(line, s) })
}

// waitForEvent reports whether a change event line carrying nonce appeared.
func (l *lineLog) waitForEvent(nonce string, d time.Duration) bool {
	return l.waitLine(d, func(line string) bool {
		return strings.HasPrefix(line, "piperoom-reference event ") && strings.Contains(line, nonce)
	})
}

func (l *lineLog) waitLine(d time.Duration, match func(string) bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		sc := bufio.NewScanner(strings.NewReader(l.String()))
		sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
		for sc.Scan() {
			if match(sc.Text()) {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func requireJava17(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("java"); err != nil {
		t.Skipf("java is not on PATH (the CI runner image has it): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "java", "-XshowSettings:properties", "-version").CombinedOutput()
	if err != nil {
		t.Fatalf("java -version: %v\n%s", err, out)
	}
	m := regexp.MustCompile(`java\.specification\.version = (\d+)`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("cannot read the Java version from:\n%s", tail(out, 2000))
	}
	if v, _ := strconv.Atoi(string(m[1])); v < 17 {
		t.Fatalf("Java %d on PATH; Debezium 3.x needs 17 (rule dbz3-java17)", v)
	}
}

func requireTool(t *testing.T, build BuildTool) {
	t.Helper()
	tool := "mvn"
	if build == BuildGradle {
		tool = "gradle"
	}
	if _, err := exec.LookPath(tool); err != nil {
		t.Skipf("%s is not on PATH (the CI runner image has it): %v", tool, err)
	}
}

func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not on PATH (the CI full-stack job has it): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("the docker daemon is not reachable: %v\n%s", err, out)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return "..." + string(b[len(b)-n:])
}
