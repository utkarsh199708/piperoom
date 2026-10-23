// Command discover drafts an owners file from a live cluster. It reads Kafka
// Connect, Kafka and the Schema Registry (and, with -source-db, the source
// Postgres) and writes one pipeline per Debezium Postgres or MySQL source
// connector (a MySQL one with source_kind: mysql) for a human to review before
// copying it into OWNERS_FILE. It never writes to the cluster.
//
//	go run ./cmd/discover -out owners.draft.yaml
//
// With -write-owners it writes a file that loads as it is (every layer owned
// by one user, checked with owners.Load), plus a matching users.yaml with
// -users, and prints a summary table:
//
//	go run ./cmd/discover -write-owners owners.yaml -users users.yaml -owner alice
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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/discover"
)

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
// builder and the library entry points. Production uses the real ones; a nil
// field takes its production default (see withDefaults).
type runner struct {
	build       buildFunc
	discover    func(context.Context, discover.Deps, discover.Options) (discover.Result, error)
	render      func(io.Writer, discover.Result) error
	renderOwned func(io.Writer, discover.Result, string) error
	renderUsers func(io.Writer, string) error
	// osUser returns the OS user name, for the default of -owner.
	osUser func() (string, error)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	r := runner{build: buildDeps, discover: discover.Discover, render: discover.Render}
	return r.run(ctx, args, stdout, stderr)
}

func (r runner) withDefaults() runner {
	if r.renderOwned == nil {
		r.renderOwned = discover.RenderOwned
	}
	if r.renderUsers == nil {
		r.renderUsers = discover.RenderUsers
	}
	if r.osUser == nil {
		r.osUser = currentUsername
	}
	return r
}

func (r runner) run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	r = r.withDefaults()
	fl := flag.NewFlagSet("discover", flag.ContinueOnError)
	fl.SetOutput(stderr)
	out := fl.String("out", "", "draft owners file to write, with TODO layer owners (- = stdout)")
	writeOwners := fl.String("write-owners", "", "owners file to write that loads as it is: every layer owned by -owner, checked with owners.Load\n"+
		"(if the check fails the file is written as FILE.draft with the error on top, and the exit code is 1)")
	owner := fl.String("owner", defaultOwner(r.osUser, os.Getenv), "user id that owns every layer in the -write-owners file")
	users := fl.String("users", "", "also write a one-user users.yaml for -owner, only if the -write-owners file passes owners.Load")
	force := fl.Bool("force", false, "overwrite -out, -write-owners and -users if they already exist (never through a symbolic link)")
	sourceDB := fl.Bool("source-db", false, "also read replication slots from the source Postgres (SOURCE_DB_URL)")
	timeout := fl.Duration("timeout", defaultTimeout, "limit for the whole run")
	fl.Usage = func() {
		fmt.Fprintln(fl.Output(), "usage: discover (-out FILE | -write-owners FILE [-owner ID] [-users FILE]) [-force] [-source-db] [-timeout 2m]")
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
	ownerSet := false
	fl.Visit(func(f *flag.Flag) { ownerSet = ownerSet || f.Name == "owner" })
	if msg := checkFiles(*out, *writeOwners, *users, ownerSet); msg != "" {
		fmt.Fprintln(stderr, "discover: "+msg)
		fl.Usage()
		return exitUsage
	}
	if *writeOwners != "" {
		if err := discover.ValidateOwner(*owner); err != nil {
			fmt.Fprintln(stderr, err.Error())
			return exitUsage
		}
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "discover: -timeout must be positive")
		return exitUsage
	}
	toStdout := *out == "-"

	// Fail before touching the cluster when a file is already there, or when
	// the owners or users file has no directory to go in; the exclusive create
	// below still guards against a file that appears meanwhile.
	for _, p := range []string{*writeOwners, *users} {
		if p == "" {
			continue
		}
		if fi, err := os.Stat(filepath.Dir(p)); err != nil || !fi.IsDir() {
			fmt.Fprintf(stderr, "discover: %s: the directory %s does not exist\n", p, filepath.Dir(p))
			return exitError
		}
	}
	for _, p := range []string{*out, *writeOwners, *users} {
		if p == "" || p == "-" {
			continue
		}
		if *force {
			// -force replaces a file; through a symbolic link that would
			// overwrite whatever the link points to.
			if err := refuseSymlink(p); err != nil {
				fmt.Fprintln(stderr, err.Error())
				return exitError
			}
		} else if _, err := os.Lstat(p); err == nil {
			fmt.Fprintf(stderr, "discover: %s already exists (use -force to overwrite)\n", p)
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

	// Render everything fully before opening any output so a failure never
	// creates or truncates a file.
	var draftBuf, ownersBuf, usersBuf bytes.Buffer
	renders := []struct {
		want bool
		buf  *bytes.Buffer
		do   func() error
	}{
		{*out != "", &draftBuf, func() error { return r.render(&draftBuf, res) }},
		{*writeOwners != "", &ownersBuf, func() error { return r.renderOwned(&ownersBuf, res, *owner) }},
		{*users != "", &usersBuf, func() error { return r.renderUsers(&usersBuf, *owner) }},
	}
	for _, rd := range renders {
		if !rd.want {
			continue
		}
		if err := rd.do(); err != nil {
			fmt.Fprintln(stderr, red.line(fmt.Sprintf("discover: render draft: %v", err)))
			return exitError
		}
	}

	var written []string // files for the summary table
	if toStdout {
		if _, err := stdout.Write(draftBuf.Bytes()); err != nil {
			fmt.Fprintf(stderr, "discover: write stdout: %v\n", err)
			return exitError
		}
	} else if *out != "" {
		if code := writeOne(stderr, *out, draftBuf.Bytes(), *force); code != exitOK {
			return code
		}
		written = append(written, *out)
	}
	if *out != "" {
		log.Info("draft written", "out", *out, "bytes", draftBuf.Len(), "took", time.Since(start).Round(time.Millisecond).String())
	}

	code := exitOK
	var failure []string // lines for stderr after the summary
	var notes []string   // lines under the summary table
	if *writeOwners != "" {
		if len(res.Pipelines) == 0 {
			// An owners file without pipelines loads but does nothing; stop
			// the make-quickstart style chain here, and say why.
			printSummary(stderr, res, red)
			for _, s := range res.Skipped {
				fmt.Fprintln(stderr, "skipped: "+red.line(s.Connector+": "+s.Reason))
			}
			fmt.Fprintf(stderr, "discover: no Debezium Postgres or MySQL source connector found; nothing written to %s\n", *writeOwners)
			return exitError
		}
		oc, err := installOwners(*writeOwners, ownersBuf.Bytes(), *force, red.line)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				fmt.Fprintf(stderr, "discover: %s already exists (use -force to overwrite)\n", *writeOwners)
			} else {
				fmt.Fprintln(stderr, red.line(err.Error()))
			}
			return exitError
		}
		written = append(written, oc.path)
		if oc.draft {
			log.Warn("owners draft written: it failed owners.Load", "path", oc.path)
			code = exitError
			failure = []string{
				"discover: the owners file failed owners.Load: " + red.line(oc.reason),
				fmt.Sprintf("discover: wrote the draft to %s with the error at the top; fix it, then rename it to %s", oc.path, *writeOwners),
			}
		} else {
			log.Info("owners file written", "path", oc.path, "owner", *owner)
		}
	}
	switch {
	case *users != "" && code != exitOK:
		// The two files are a pair: a users file next to an owners draft that
		// the server cannot load would only suggest the pair is ready.
		notes = append(notes, fmt.Sprintf("users file %s was not written: it is written only with an owners file that passes owners.Load", red.line(*users)))
	case *users != "":
		if c := writeOne(stderr, *users, usersBuf.Bytes(), *force); c != exitOK {
			return c
		}
		written = append(written, *users)
		log.Info("users file written", "path", *users, "user", *owner)
	}

	printSummary(stderr, res, red)
	if *writeOwners != "" {
		// With -out - stdout carries the draft, so the table must not follow it.
		table := stdout
		if toStdout {
			table = stderr
		}
		printTable(table, res, written, red, notes...)
	}
	for _, l := range failure {
		fmt.Fprintln(stderr, l)
	}
	return code
}

// writeOne writes a file for run and reports a failure on stderr. It returns
// exitOK or the exit code to stop with.
func writeOne(stderr io.Writer, path string, data []byte, force bool) int {
	if err := writeFile(path, data, force); err != nil {
		if errors.Is(err, fs.ErrExist) {
			fmt.Fprintf(stderr, "discover: %s already exists (use -force to overwrite)\n", path)
		} else {
			fmt.Fprintln(stderr, err.Error())
		}
		return exitError
	}
	return exitOK
}

// checkFiles returns what is wrong with the file flags, or "". It runs before
// anything is read, so a usage mistake never costs a cluster round trip.
func checkFiles(out, writeOwners, users string, ownerSet bool) string {
	switch {
	case out == "" && writeOwners == "":
		return "-out or -write-owners is required (-out - writes the draft to stdout)"
	case writeOwners == "-" || users == "-":
		return "-write-owners and -users need a file path: the owners file is checked with owners.Load before it is moved into place"
	case writeOwners == "" && (users != "" || ownerSet):
		return "-owner and -users go with -write-owners"
	}
	seen := map[string]string{}
	for _, f := range []struct{ flag, path string }{{"-out", out}, {"-write-owners", writeOwners}, {"-users", users}} {
		if f.path == "" || f.path == "-" {
			continue
		}
		clean := filepath.Clean(f.path)
		if other, dup := seen[clean]; dup {
			return fmt.Sprintf("%s and %s name the same file %s", other, f.flag, f.path)
		}
		seen[clean] = f.flag
	}
	// A failed owners file is written to <write-owners>.draft; another output
	// on that name would be overwritten by it, or refuse it.
	if writeOwners != "" {
		draft := filepath.Clean(writeOwners + ".draft")
		for _, f := range []struct{ flag, path string }{{"-out", out}, {"-users", users}} {
			if f.path != "" && f.path != "-" && filepath.Clean(f.path) == draft {
				return fmt.Sprintf("%s names %s, the file -write-owners falls back to when the owners file fails owners.Load", f.flag, f.path)
			}
		}
	}
	return ""
}

// writeFile writes data to path. Without force the file must not exist yet;
// if writing then fails, the half-written file is removed because this run
// created it.
func writeFile(path string, data []byte, force bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if force {
		// O_TRUNC follows a symbolic link; O_EXCL (above) never does.
		if err := refuseSymlink(path); err != nil {
			return err
		}
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
