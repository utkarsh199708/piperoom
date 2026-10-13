package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Launcher starts server processes. Command returns the program and
// arguments that run ServeMain: `loadtest serve` for the CLI, or the test
// binary itself with an env switch in tests.
type Launcher struct {
	Command  func() (path string, args []string, extraEnv []string)
	ModelURL string // the fake model's base URL, passed as ANTHROPIC_BASE_URL
	LogDir   string // each server's stderr goes to <LogDir>/<name>.log
}

// Proc is one running server process.
type Proc struct {
	Replica int
	URL     string
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	waitErr chan error
	logFile *os.File
}

// Cluster is the server processes of one scenario run, all on one database.
type Cluster struct {
	Procs []*Proc
	http  *http.Client
}

// childEnv is the parent's environment without anything that could point a
// server at a real model or a real Piperoom config, plus the fake model.
func childEnv(parent []string, modelURL string, cfgJSON []byte, extra []string) []string {
	out := make([]string, 0, len(parent)+4)
	for _, kv := range parent {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "ANTHROPIC_") || strings.HasPrefix(k, "PIPEROOM_") || strings.HasPrefix(k, "AWS_") ||
			strings.HasPrefix(k, "VERTEX_") || strings.HasPrefix(k, "GOOGLE_") || k == ServerEnv {
			continue
		}
		out = append(out, kv)
	}
	out = append(out, extra...)
	return append(out,
		"ANTHROPIC_BASE_URL="+modelURL,
		"ANTHROPIC_API_KEY=lt-fake-key-not-a-secret",
		ServerEnv+"="+string(cfgJSON),
	)
}

// Start launches replicas servers on cfg's database. Replica 0 starts first
// and must be ready before the others start, as a rolling deploy would.
func (l Launcher) Start(ctx context.Context, cfg ServerConfig, replicas int, name string) (*Cluster, error) {
	if replicas < 1 {
		replicas = 1
	}
	c := &Cluster{http: &http.Client{Timeout: 30 * time.Second}}
	for i := 0; i < replicas; i++ {
		rc := cfg
		rc.Replica = i
		p, err := l.startOne(ctx, rc, fmt.Sprintf("%s-server-%d", name, i))
		if err != nil {
			c.Stop()
			return nil, err
		}
		c.Procs = append(c.Procs, p)
	}
	return c, nil
}

func (l Launcher) startOne(ctx context.Context, cfg ServerConfig, name string) (*Proc, error) {
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("loadtest: launch: %w", err)
	}
	path, args, extra := l.Command()
	cmd := exec.Command(path, args...)
	cmd.Env = childEnv(os.Environ(), l.ModelURL, cfgJSON, extra)
	logPath := filepath.Join(l.LogDir, name+".log")
	lf, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("loadtest: launch: log file: %w", err)
	}
	cmd.Stderr = lf
	ready := make(chan string, 1)
	cmd.Stdout = &readyWriter{ready: ready}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("loadtest: launch: stdin: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("loadtest: launch %s: %w", path, err)
	}
	p := &Proc{Replica: cfg.Replica, cmd: cmd, stdin: stdin, waitErr: make(chan error, 1), logFile: lf}
	go func() { p.waitErr <- cmd.Wait() }()

	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()
	select {
	case u := <-ready:
		p.URL = u
		return p, nil
	case err := <-p.waitErr:
		p.waitErr <- err
		p.stop()
		return nil, fmt.Errorf("loadtest: server %s exited before it was ready (%v); see %s", name, err, logPath)
	case <-timer.C:
		p.stop()
		return nil, fmt.Errorf("loadtest: server %s not ready after 60s; see %s", name, logPath)
	case <-ctx.Done():
		p.stop()
		return nil, ctx.Err()
	}
}

// stop closes stdin (the child exits on EOF), then sends SIGTERM, then kills
// after a grace period. It always reaps the process.
func (p *Proc) stop() {
	_ = p.stdin.Close()
	select {
	case <-p.waitErr:
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.waitErr:
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.waitErr
		}
	}
	_ = p.logFile.Close()
}

// Stop stops every server.
func (c *Cluster) Stop() {
	var wg sync.WaitGroup
	for _, p := range c.Procs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.stop()
		}()
	}
	wg.Wait()
}

// URL returns the base URL of replica i mod n.
func (c *Cluster) URL(i int) string { return c.Procs[i%len(c.Procs)].URL }

// Stats reads every server's stats.
func (c *Cluster) Stats(ctx context.Context) ([]ServerStats, error) {
	out := make([]ServerStats, 0, len(c.Procs))
	for _, p := range c.Procs {
		var s ServerStats
		if err := c.do(ctx, http.MethodGet, p.URL+"/loadtest/stats", &s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Reset clears every server's latency samples and sampler peaks.
func (c *Cluster) Reset(ctx context.Context) error {
	for _, p := range c.Procs {
		if err := c.do(ctx, http.MethodPost, p.URL+"/loadtest/reset", nil); err != nil {
			return err
		}
	}
	return nil
}

// GC runs a garbage collection in every server, for a clean memory baseline.
func (c *Cluster) GC(ctx context.Context) error {
	for _, p := range c.Procs {
		if err := c.do(ctx, http.MethodPost, p.URL+"/loadtest/gc", nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) do(ctx context.Context, method, u string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return fmt.Errorf("loadtest: %s %s: %w", method, u, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("loadtest: %s %s: %w", method, u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loadtest: %s %s: status %d", method, u, resp.StatusCode)
	}
	if dst == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst); err != nil {
		return fmt.Errorf("loadtest: %s %s: decode: %w", method, u, err)
	}
	return nil
}

// readyWriter is a server's stdout: it passes on the URL of the first ready
// line and drops everything else, so the child never blocks on a full pipe.
type readyWriter struct {
	mu    sync.Mutex
	buf   []byte
	ready chan string
	sent  bool
}

func (w *readyWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sent {
		return len(b), nil
	}
	w.buf = append(w.buf, b...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			if len(w.buf) > 64<<10 {
				w.buf = w.buf[:0] // no ready line in a flood of output
			}
			return len(b), nil
		}
		line := string(w.buf[:i])
		w.buf = w.buf[i+1:]
		if u, ok := strings.CutPrefix(line, ReadyPrefix); ok {
			w.sent = true
			w.buf = nil
			w.ready <- strings.TrimSpace(u)
			return len(b), nil
		}
	}
}
