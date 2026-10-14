// Command discover drafts an owners file from a live cluster. It reads Kafka
// Connect, Kafka and the Schema Registry (and, with -source-db, the source
// Postgres) and writes one pipeline per Debezium Postgres source connector for
// a human to review before copying it into OWNERS_FILE. It never writes to the
// cluster.
//
//	go run ./cmd/discover -out owners.draft.yaml
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/redact"
)

// maxLine caps one stderr line or log value.
const maxLine = 2048

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	defaultTimeout = 2 * time.Minute
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// buildFunc constructs the read-only clients discovery needs. The returned
// close func releases them and must be called even when it is a no-op.
type buildFunc func(ctx context.Context, cfg config.Config, sourceDB bool) (discover.Deps, func(), error)

// runner holds the seams that make run testable without a network: the client
// builder and the library entry points. Production uses the real ones.
type runner struct {
	build    buildFunc
	discover func(context.Context, discover.Deps, discover.Options) (discover.Result, error)
	render   func(io.Writer, discover.Result) error
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	r := runner{build: buildDeps, discover: discover.Discover, render: discover.Render}
	return r.run(ctx, args, stdout, stderr)
}

func (r runner) run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("discover", flag.ContinueOnError)
	fl.SetOutput(stderr)
	out := fl.String("out", "", "draft owners file to write (required; - = stdout)")
	force := fl.Bool("force", false, "overwrite -out if it already exists")
	sourceDB := fl.Bool("source-db", false, "also read replication slots from the source Postgres (SOURCE_DB_URL)")
	timeout := fl.Duration("timeout", defaultTimeout, "limit for the whole run")
	fl.Usage = func() {
		fmt.Fprintln(fl.Output(), "usage: discover -out FILE [-force] [-source-db] [-timeout 2m]")
		fl.PrintDefaults()
	}
	if err := fl.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		// The flag package has already printed the problem and the usage.
		return exitUsage
	}
	if fl.NArg() > 0 {
		fmt.Fprintf(stderr, "discover: unexpected argument %q\n", fl.Arg(0))
		fl.Usage()
		return exitUsage
	}
	if *out == "" {
		fmt.Fprintln(stderr, "discover: -out is required (use - for stdout)")
		fl.Usage()
		return exitUsage
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "discover: -timeout must be positive")
		return exitUsage
	}
	toStdout := *out == "-"

	// Fail before touching the cluster when the file is already there; the
	// O_EXCL open below still guards against a file that appears meanwhile.
	if !toStdout && !*force {
		if _, err := os.Lstat(*out); err == nil {
			fmt.Fprintf(stderr, "discover: %s already exists (use -force to overwrite)\n", *out)
			return exitError
		}
	}

	cfg := config.Load()
	red := newRedactor(cfg)
	// The library logs raw client errors (which can quote a URL) at Warn, so
	// every record goes through the redactor too.
	log := slog.New(redactHandler{
		next: slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}),
		red:  red,
	})

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	deps, closeDeps, err := r.build(ctx, cfg, *sourceDB)
	if err != nil {
		fmt.Fprintln(stderr, red.line(err.Error()))
		return exitError
	}
	defer closeDeps()

	start := time.Now()
	log.Info("discovering pipelines", "source_db", *sourceDB, "timeout", timeout.String())
	res, err := r.discover(ctx, deps, discover.Options{Logger: log})
	if err != nil {
		// The library already prefixes "discover: "; avoid printing it twice.
		msg := "discover: discovery failed: " + strings.TrimPrefix(err.Error(), "discover: ")
		if errors.Is(err, context.DeadlineExceeded) {
			msg += " (raise -timeout?)"
		}
		fmt.Fprintln(stderr, red.line(msg))
		return exitError
	}

	// Render fully before opening the output so a failure never creates or
	// truncates the file.
	var buf bytes.Buffer
	if err := r.render(&buf, res); err != nil {
		fmt.Fprintln(stderr, red.line(fmt.Sprintf("discover: render draft: %v", err)))
		return exitError
	}

	if toStdout {
		if _, err := stdout.Write(buf.Bytes()); err != nil {
			fmt.Fprintf(stderr, "discover: write stdout: %v\n", err)
			return exitError
		}
	} else if err := writeFile(*out, buf.Bytes(), *force); err != nil {
		if errors.Is(err, fs.ErrExist) {
			fmt.Fprintf(stderr, "discover: %s already exists (use -force to overwrite)\n", *out)
		} else {
			fmt.Fprintln(stderr, err.Error())
		}
		return exitError
	}
	log.Info("draft written", "out", *out, "bytes", buf.Len(), "took", time.Since(start).Round(time.Millisecond).String())

	printSummary(stderr, res, red)
	return exitOK
}

// writeFile writes data to path. Without force the file must not exist yet;
// if writing then fails, the half-written file is removed because this run
// created it.
func writeFile(path string, data []byte, force bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return fmt.Errorf("discover: open %s: %w", path, err)
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	if err := errors.Join(werr, cerr); err != nil {
		if !force {
			_ = os.Remove(path)
		}
		return fmt.Errorf("discover: write %s: %w", path, err)
	}
	return nil
}

// printSummary reports the counts plus every warning, so an operator sees
// what needs fixing without opening the file.
func printSummary(w io.Writer, res discover.Result, red redactor) {
	for _, warn := range res.Warnings {
		fmt.Fprintln(w, "warning: "+red.line(warn))
	}
	fmt.Fprintf(w, "discover: %d pipelines, %d skipped connectors, %d warnings\n",
		len(res.Pipelines), len(res.Skipped), len(res.Warnings))
	empty := 0
	for _, p := range res.Pipelines {
		if len(p.SourceTables) == 0 {
			empty++
		}
	}
	if empty > 0 {
		fmt.Fprintf(w, "discover: %d pipelines have no source_tables (fill them in before loading)\n", empty)
	}
}

// buildDeps builds the clients the same way internal/wiring.buildA2Clients
// does (that function is unexported and A4-owned, so it is replicated here).
// Construction errors name the env vars only: the underlying text can quote
// a URL, a DSN or a file path, so it is never shown.
func buildDeps(ctx context.Context, cfg config.Config, sourceDB bool) (discover.Deps, func(), error) {
	noop := func() {}

	// The Schema Registry client comes first: the Kafka client uses it to
	// decode Avro keys.
	registry, err := schemareg.NewSecure(cfg.SchemaRegistryURL, netsec.HTTP{
		Username: cfg.SchemaRegistryUsername,
		Password: netsec.Secret(cfg.SchemaRegistryPassword),
		TLS: netsec.TLS{
			CAFile:   cfg.SchemaRegistryTLSCAFile,
			CertFile: cfg.SchemaRegistryTLSCertFile,
			KeyFile:  cfg.SchemaRegistryTLSKeyFile,
		},
	})
	if err != nil {
		return discover.Deps{}, noop, buildError("Schema Registry", "SCHEMA_REGISTRY_URL / SCHEMA_REGISTRY_*", err)
	}

	connectClient, err := connect.NewSecure(cfg.ConnectURL, netsec.HTTP{
		Username: cfg.ConnectUsername,
		Password: netsec.Secret(cfg.ConnectPassword),
		TLS: netsec.TLS{
			CAFile:   cfg.ConnectTLSCAFile,
			CertFile: cfg.ConnectTLSCertFile,
			KeyFile:  cfg.ConnectTLSKeyFile,
		},
	})
	if err != nil {
		return discover.Deps{}, noop, buildError("Kafka Connect", "CONNECT_URL / CONNECT_*", err)
	}

	brokers := splitBrokers(cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return discover.Deps{}, noop, errors.New("discover: KAFKA_BROKERS lists no brokers")
	}
	kafkaClient, err := kafka.NewSecure(brokers, registry, netsec.Kafka{
		Protocol:  cfg.KafkaSecurityProtocol,
		Mechanism: cfg.KafkaSASLMechanism,
		Username:  cfg.KafkaSASLUsername,
		Password:  netsec.Secret(cfg.KafkaSASLPassword),
		TLS: netsec.TLS{
			CAFile:     cfg.KafkaTLSCAFile,
			CertFile:   cfg.KafkaTLSCertFile,
			KeyFile:    cfg.KafkaTLSKeyFile,
			ServerName: cfg.KafkaTLSServerName,
		},
	})
	if err != nil {
		return discover.Deps{}, noop, buildError("Kafka", "KAFKA_BROKERS / KAFKA_*", err)
	}

	deps := discover.Deps{Connect: connectClient, Kafka: kafkaClient, Registry: registry}
	if !sourceDB {
		return deps, kafkaClient.Close, nil
	}

	src, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		kafkaClient.Close()
		hint := ""
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			hint = " (timed out or interrupted)"
		}
		return discover.Deps{}, noop, fmt.Errorf("discover: cannot open the source Postgres from SOURCE_DB_URL%s", hint)
	}
	deps.Source = src
	return deps, func() {
		src.Close()
		kafkaClient.Close()
	}, nil
}

// buildError hides the cause's text but keeps a hint when the cause is a
// rejected security setting, the one case an operator can fix from the
// message alone.
func buildError(what, envs string, cause error) error {
	if errors.Is(cause, netsec.ErrInvalid) {
		return fmt.Errorf("discover: cannot build %s client: invalid security settings in %s", what, envs)
	}
	return fmt.Errorf("discover: cannot build %s client from %s", what, envs)
}

// splitBrokers splits a comma-separated broker list, trimming blanks.
func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// redactor replaces configured addresses and secrets with the env var that
// holds them. Library errors and warnings can wrap *url.Error or dial errors,
// which quote the URL or host; this is the last line of defence before text
// reaches the terminal.
type redactor struct {
	r *strings.Replacer
}

func newRedactor(cfg config.Config) redactor {
	type pair struct{ old, new string }
	var pairs []pair
	add := func(old, new string) {
		if strings.TrimSpace(old) != "" {
			pairs = append(pairs, pair{old, new})
		}
	}
	addURL := func(raw, env string) {
		add(raw, "$"+env)
		add(strings.TrimRight(raw, "/"), "$"+env)
		u, err := url.Parse(raw)
		if err != nil {
			return
		}
		add(u.Host, "<"+env+" host>")
		add(u.Hostname(), "<"+env+" host>")
		if u.User != nil {
			add(u.User.String(), "[redacted]")
			if pw, ok := u.User.Password(); ok {
				add(pw, "[redacted]")
			}
		}
	}
	addURL(cfg.ConnectURL, "CONNECT_URL")
	addURL(cfg.SchemaRegistryURL, "SCHEMA_REGISTRY_URL")
	addURL(cfg.SourceDBURL, "SOURCE_DB_URL")
	for _, b := range splitBrokers(cfg.KafkaBrokers) {
		add(b, "<KAFKA_BROKERS entry>")
		if host, _, err := net.SplitHostPort(b); err == nil {
			add(host, "<KAFKA_BROKERS entry>")
		}
	}
	for _, s := range []string{cfg.KafkaSASLPassword, cfg.SchemaRegistryPassword, cfg.ConnectPassword} {
		add(s, "[redacted]")
	}

	// strings.Replacer tries the old strings in argument order at each
	// position, so the longest must come first or a host would be replaced
	// inside its own URL and leave the rest of the URL behind.
	sort.SliceStable(pairs, func(i, j int) bool { return len(pairs[i].old) > len(pairs[j].old) })
	args := make([]string, 0, 2*len(pairs))
	for _, p := range pairs {
		args = append(args, p.old, p.new)
	}
	return redactor{r: strings.NewReplacer(args...)}
}

// line redacts s and folds it onto one line with no control characters, so
// a connector name or error text cannot inject terminal escapes or fake lines.
func (red redactor) line(s string) string {
	s = red.r.Replace(s)
	// Configured values are known; a secret quoted in an HTTP error body
	// (Connect echoes connector config in some 500s) is not, so the generic
	// masker runs too, and an unbounded body is cut.
	s, _ = redact.Text(s)
	if len(s) > maxLine {
		cut := maxLine
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + fmt.Sprintf("... [%d bytes cut]", len(s)-cut)
	}
	return strings.Map(func(c rune) rune {
		if c < 0x20 || c == 0x7f {
			return ' '
		}
		return c
	}, s)
}

// redactHandler passes every record through a redactor before the next
// handler formats it.
type redactHandler struct {
	next slog.Handler
	red  redactor
}

func (h redactHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h redactHandler) Handle(ctx context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, h.red.line(rec.Message), rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.red.attr(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h redactHandler) WithAttrs(as []slog.Attr) slog.Handler {
	red := make([]slog.Attr, len(as))
	for i, a := range as {
		red[i] = h.red.attr(a)
	}
	return redactHandler{next: h.next.WithAttrs(red), red: h.red}
}

func (h redactHandler) WithGroup(name string) slog.Handler {
	return redactHandler{next: h.next.WithGroup(name), red: h.red}
}

// attr redacts string-like values; errors and other any values are
// flattened to their text first because that text is what would be printed.
func (red redactor) attr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		a.Value = slog.StringValue(red.line(v.String()))
	case slog.KindAny:
		a.Value = slog.StringValue(red.line(fmt.Sprint(v.Any())))
	case slog.KindGroup:
		g := v.Group()
		out := make([]slog.Attr, len(g))
		for i, ga := range g {
			out[i] = red.attr(ga)
		}
		a.Value = slog.GroupValue(out...)
	default:
		a.Value = v
	}
	return a
}
