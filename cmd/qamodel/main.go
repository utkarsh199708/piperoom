// Command qamodel runs the puppet model for QA: a stand-in for the Anthropic
// Messages API that lets a tester, who needs no API key, decide each model reply
// while the real Piperoom server and the real agent loop run on the real dev
// stack. Point the server at it:
//
//	go run ./cmd/qamodel --addr 127.0.0.1:9911
//	ANTHROPIC_BASE_URL=http://127.0.0.1:9911 ANTHROPIC_API_KEY=qa-puppet go run ./cmd/server
//
// See cmd/qamodel/README.md for the tester's curl loop.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/qamodel"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	// defaultAddr is loopback: the control API has no authentication.
	defaultAddr = "127.0.0.1:9911"
	// dummyKey is the API key the server is given. The puppet checks nothing,
	// but the SDK refuses to start without one.
	dummyKey = "qa-puppet"

	shutdownGrace = 5 * time.Second

	// agentModelTimeout is internal/agent's modelTimeout, the longest the
	// agent waits for one model turn.
	agentModelTimeout = 10 * time.Minute
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("qamodel", flag.ContinueOnError)
	fl.SetOutput(stderr)
	addr := fl.String("addr", defaultAddr, "listen address of the puppet model and its /qa/ control API")
	replyTimeout := fl.Duration("reply-timeout", qamodel.DefaultReplyTimeout,
		"how long a model request waits for a reply before it gets the plain text \"qamodel: no reply in time\"; keep it under the agent's 10m model-turn timeout")
	allowRemote := fl.Bool("allow-remote", false, "accept a listen address that is not loopback (the control API has no authentication)")
	fl.Usage = func() {
		fmt.Fprintln(fl.Output(), "usage: qamodel [--addr 127.0.0.1:9911] [--reply-timeout 9m] [--allow-remote]")
		fl.PrintDefaults()
	}
	if err := fl.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fl.NArg() > 0 {
		fmt.Fprintf(stderr, "qamodel: unexpected argument %q\n", fl.Arg(0))
		fl.Usage()
		return exitUsage
	}
	if *replyTimeout <= 0 {
		fmt.Fprintln(stderr, "qamodel: --reply-timeout must be positive")
		return exitUsage
	}
	if !*allowRemote && !isLoopback(*addr) {
		fmt.Fprintf(stderr, "qamodel: --addr %q is not a loopback address; the control API has no authentication. Use 127.0.0.1, or pass --allow-remote\n", *addr)
		return exitUsage
	}

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if *replyTimeout >= agentModelTimeout {
		// The agent's own deadline for a model turn would fire first, and it
		// reports a failed request instead of reading the fallback text.
		log.Warn("qamodel: --reply-timeout is not below the agent's model-turn timeout, so the fallback text will not reach it in time",
			"reply_timeout", *replyTimeout, "agent_model_timeout", agentModelTimeout)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("qamodel: listen", "addr", *addr, "err", err)
		return exitError
	}
	puppet := qamodel.New(qamodel.Config{ReplyTimeout: *replyTimeout, Logger: log})
	// No WriteTimeout: a model request is held open until a tester replies.
	srv := &http.Server{Handler: puppet, ReadHeaderTimeout: 10 * time.Second}

	url := "http://" + ln.Addr().String()
	printBanner(stdout, url, *replyTimeout)

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		puppet.Close()
		log.Error("qamodel: serve failed", "err", err)
		return exitError
	case <-ctx.Done():
	}

	// Close first: it answers the requests that still wait for a reply, which
	// Shutdown would otherwise wait on for as long as they have left.
	puppet.Close()
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("qamodel: shutdown", "err", err)
		return exitError
	}
	return exitOK
}

// isLoopback reports whether addr (host:port) listens on loopback only. An
// empty host or an unspecified address (0.0.0.0, ::) listens everywhere.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// printBanner tells the tester how to use the puppet. The first line is the
// ready line: the puppet is listening when it appears.
func printBanner(w io.Writer, url string, replyTimeout time.Duration) {
	fmt.Fprintf(w, "qamodel: puppet model listening on %s\n", url)
	fmt.Fprintf(w, `
Every model request of the Piperoom server waits here for a reply you post.

Start the server with these variables (the model id, PIPEROOM_MODEL, may be
anything: it is echoed back):

  bash:        ANTHROPIC_BASE_URL=%[1]s ANTHROPIC_API_KEY=%[2]s go run ./cmd/server
  PowerShell:  $env:ANTHROPIC_BASE_URL = "%[1]s"; $env:ANTHROPIC_API_KEY = "%[2]s"

  Optional, so that no first-look pass or post-mortem asks for a reply:
  PIPEROOM_TRIAGE_PASS=off PIPEROOM_POSTMORTEM_MODEL=off

The tester's loop (curl; every answer is JSON except a turn's text):

  curl -s '%[1]s/qa/wait?timeout=60s'      # blocks until a turn is pending, then lists them
  curl -s  %[1]s/qa/turns/t1               # read the request (?system=0 ?tools=1 ?full=1 ?from=N)
  curl -s -X POST %[1]s/qa/turns/t1/reply -d '{"text":"...","tool_calls":[{"name":"connect_status","input":{"connector":"pg-orders","why":"..."}}]}'
  curl -s  %[1]s/qa/log                    # the last 200 turns and replies, for the report

A request nobody answers within %[3]s gets the text "qamodel: no reply in time" and the agent parks.
Stop with Ctrl-C.
`, url, dummyKey, replyTimeout)
}
