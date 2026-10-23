package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/events"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// X8b: Stage for connect_update_connector_config, schema_register and
// schema_set_compatibility (docs/api.md "X8 staging", Stager in action.go).
// Every Stage below reads and writes only a.d.Staging; none of them touches
// a.d.Connect or a.d.Registry, which are production.

var (
	_ Stager = (*updateConnectorConfig)(nil)
	_ Stager = (*registerSchema)(nil)
	_ Stager = (*setCompatibility)(nil)
)

const (
	// stageConnectorMissing and stageSubjectMissing are Detail when the
	// object is not on staging: a stage never creates one.
	stageConnectorMissing = "connector not present in staging"
	stageSubjectMissing   = "subject not present in staging"
	// stagePollInterval is how often Stage reads a staged connector's status.
	stagePollInterval = 2 * time.Second
	// stageSettle is how long a FAILED state right after the change is not
	// taken as the verdict: Connect restarts the tasks asynchronously, so the
	// first reads can still show the failure the change is meant to fix. A
	// FAILED task stays FAILED, so the end-of-window read still sees a real
	// one.
	stageSettle = 10 * time.Second
	// restoreRetryInterval spaces restore attempts: right after a config
	// change Connect answers 409 while its workers rebalance.
	restoreRetryInterval = 500 * time.Millisecond
)

// subjectCompatDeleter is the local adapter for removing a subject's own
// compatibility level (schemareg.Client.DeleteSubjectCompatibility); it is
// not on clients.SchemaRegistryWriter. Without it a stage of a subject that
// had no level of its own reports the level it leaves behind.
type subjectCompatDeleter interface {
	DeleteSubjectCompatibility(ctx context.Context, subject string) error
}

// stageTiming returns the observe window, poll interval and settle time in
// force.
func (d Deps) stageTiming() (window, poll, settle time.Duration) {
	window, poll, settle = StageObserveWindow, stagePollInterval, stageSettle
	if d.stageWindow > 0 {
		window = d.stageWindow
	}
	if d.stagePoll > 0 {
		poll = d.stagePoll
	}
	if d.stageSettle > 0 {
		settle = d.stageSettle
	}
	return window, poll, settle
}

// restoreRetry returns the interval between restore attempts.
func (d Deps) restoreRetry() time.Duration {
	if d.stagePoll > 0 && d.stagePoll < restoreRetryInterval {
		return d.stagePoll
	}
	return restoreRetryInterval
}

// restoreContext is the context staging is put back on: ctx's values (the
// pipeline scope the staging writes check) without its cancellation, so a
// cancelled or timed-out stage still restores, bounded by
// StageRestoreTimeout.
func (d Deps) restoreContext(ctx context.Context) (context.Context, context.CancelFunc) {
	t := StageRestoreTimeout
	if d.restoreTimeout > 0 {
		t = d.restoreTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), t)
}

// stageScope fails closed like the Connect writes: a stage needs the room's
// whole pipeline in ctx (or the explicit opt-out of the CLI and tests), and
// check must pass on it. It does no I/O.
func stageScope(ctx context.Context, check func(tools.Scope) error) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if _, ok := tools.PipelineFrom(ctx); !ok {
		if err == nil && sc.Unscoped() {
			return nil
		}
		return fmt.Errorf("actions: stage: %w: a stage needs the room's pipeline in ctx (tools.WithPipeline)", tools.ErrNoScope)
	}
	if err != nil {
		return fmt.Errorf("actions: stage: %w", err)
	}
	if err := check(sc); err != nil {
		return fmt.Errorf("actions: stage: %w", err)
	}
	return nil
}

// stagingError is an error from a staging system with its text replaced by a
// category: a client error can carry the staging URL (a transport error
// quotes it) or a config value (Connect's and the Registry's messages can
// quote one), and a stage's errors reach the room log. Unwrap keeps
// errors.Is and errors.As working.
type stagingError struct {
	msg string
	err error
}

func (e *stagingError) Error() string { return e.msg }
func (e *stagingError) Unwrap() error { return e.err }

// stagingErr wraps err from doing what (a short phrase naming the object, no
// value) as a stagingError.
func stagingErr(what string, err error) error {
	return &stagingError{msg: "actions: stage: " + what + ": " + errCategory(err), err: err}
}

// errCategory names what kind of failure err is, without its text.
func errCategory(err error) string {
	var se *schemareg.Error
	var ue *url.Error
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timed out"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, tools.ErrNoScope), errors.Is(err, tools.ErrOutOfScope):
		return err.Error() // our own scope text: names, never a value or URL
	case errors.Is(err, clients.ErrNotFound):
		return "not found on staging"
	case errors.Is(err, clients.ErrNotSupported):
		return "not supported by the staging client"
	case errors.As(err, &se):
		return "registry error " + strconv.Itoa(se.Code)
	case errors.As(err, &ue), errors.As(err, &ne):
		return "staging unreachable"
	}
	return "staging answered with an error (its text is not shown: it can quote a config value or the staging URL)"
}

// secondsText renders a duration as "60 s".
func secondsText(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + " s"
}

// finish stamps the duration from start and cleans r for the room log.
func finish(r StageResult, start time.Time) StageResult {
	r.DurationMS = time.Since(start).Milliseconds()
	return CleanStageResult(r)
}

// ---- connect_update_connector_config ----

// Stage applies the change set to the same-named connector on the staging
// Connect, watches it for the observe window, and gives it back its whole
// previous config (PUT), even when ctx ends first.
func (a *updateConnectorConfig) Stage(ctx context.Context, raw json.RawMessage) (StageResult, error) {
	set, err := a.parseParams(raw)
	if err != nil {
		return StageResult{}, err
	}
	if err := stageScope(ctx, func(sc tools.Scope) error { return sc.CheckConnector(set.connector) }); err != nil {
		return StageResult{}, err
	}
	// Staging runs the heartbeat query on the staging source database: the
	// same scope rule as production, with the pipeline required.
	if err := ctxHeartbeatScope(ctx, set, true); err != nil {
		return StageResult{}, fmt.Errorf("actions: stage: %w", err)
	}
	res := StageResult{Target: "connector " + set.connector}
	if a.d.Staging == nil || a.d.Staging.Connect == nil {
		res.Detail = events.StageNoStaging
		return res, nil
	}
	c := a.d.Staging.Connect
	fw, canPatch := c.(clients.ConnectFixWriter)
	put, canPut := c.(clients.ConnectConfigPutter)
	if !canPatch || !canPut {
		return StageResult{}, fmt.Errorf("actions: stage %s: the staging Connect client cannot PATCH and PUT a connector config: %w", set.connector, clients.ErrNotSupported)
	}
	start := time.Now()
	present, err := hasConnector(ctx, c, set.connector)
	if err != nil {
		return StageResult{}, stagingErr("list staging connectors", err)
	}
	if !present {
		res.Detail = stageConnectorMissing
		return finish(res, start), nil
	}
	before, err := c.Config(ctx, set.connector)
	if err != nil {
		return StageResult{}, stagingErr("read the config of "+set.connector, err)
	}
	// The visibility guards run on staging's own config: Validate checked
	// production's, and a guard that refuses here means staging would refuse.
	if err := set.checkGuards(before); err != nil {
		res.DryRun = &DryRunResult{Source: "staging config guard", Text: err.Error()}
		return finish(res, start), nil
	}
	todo, same := set.plan(before)
	return a.d.stageConnectorKeys(ctx, c, fw, put, set.connector, before, todo, same, res, start)
}

// stageConnectorKeys is the part of a connector config stage after the
// action's own checks passed on staging's config (before): todo are the keys
// to write, same the keys staging already has at their value. It runs
// Connect's validate on staging, applies todo (PATCH), watches the connector
// for the observe window and gives it its whole previous config back (PUT),
// even when ctx ends first. With nothing to write it only reports the
// connector's health. connect_update_connector_config and
// connect_tune_throughput share it.
func (d Deps) stageConnectorKeys(ctx context.Context, c clients.ConnectReader, fw clients.ConnectFixWriter, put clients.ConnectConfigPutter,
	connector string, before, todo map[string]string, same []string, res StageResult, start time.Time) (StageResult, error) {
	if len(todo) == 0 {
		// Staging already runs with the change: nothing to write or restore;
		// its health is still the verdict.
		healthy, detail, err := d.observeConnector(ctx, c, connector)
		if err != nil {
			return StageResult{}, err
		}
		res.Applied, res.Healthy = true, healthy
		res.Detail = fmt.Sprintf("staging already had %s, no write made; %s", strings.Join(same, ", "), detail)
		return finish(res, start), nil
	}
	dry, err := validateMerged(ctx, c, before, todo)
	if err != nil {
		dry = &DryRunResult{Source: sourceValidate, Skipped: "validate on staging failed: " + errCategory(err)}
	}
	res.DryRun = dry
	if !dry.OK && dry.Skipped == "" {
		return finish(res, start), nil // staging's validate refused it: nothing written
	}

	applyErr := fw.UpdateConnectorConfigKeys(ctx, connector, todo)
	var healthy bool
	var detail string
	var obsErr error
	if applyErr == nil {
		healthy, detail, obsErr = d.observeConnector(ctx, c, connector)
	}
	elapsed := time.Since(start)
	// Restore on every path after the write was tried: a PATCH that timed out
	// may still have landed.
	leftover := d.restoreConnector(ctx, c, put, connector, before)
	switch {
	case applyErr != nil:
		return StageResult{}, withLeftover(stagingErr("apply the change to "+connector, applyErr), leftover)
	case obsErr != nil:
		return StageResult{}, withLeftover(obsErr, leftover)
	}
	res.Applied, res.Healthy, res.Detail, res.Leftover = true, healthy, detail, leftover
	res.DurationMS = elapsed.Milliseconds()
	return CleanStageResult(res), nil
}

// withLeftover adds what a failed stage left on staging to its error.
func withLeftover(err error, leftover string) error {
	if leftover == "" {
		return err
	}
	return fmt.Errorf("%w; %s", err, leftover)
}

// hasConnector reports whether c lists the connector name.
func hasConnector(ctx context.Context, c clients.ConnectReader, name string) (bool, error) {
	names, err := c.ListConnectors(ctx)
	if err != nil {
		return false, err
	}
	return slices.Contains(names, name), nil
}

// observeConnector watches a staged connector for the observe window. A
// FAILED connector or task after the settle time ends it early as unhealthy
// with its cleaned trace; otherwise the status at the window's end decides:
// healthy only when the connector and every task (at least one) are RUNNING.
// A status read that fails in the window is retried; one that fails at its
// end, or ctx ending first, is an error: no verdict.
func (d Deps) observeConnector(ctx context.Context, c clients.ConnectReader, name string) (bool, string, error) {
	window, poll, settle := d.stageTiming()
	start := time.Now()
	end := time.NewTimer(window)
	defer end.Stop()
	for done := false; !done; {
		if st, err := c.Status(ctx, name); err == nil && time.Since(start) >= settle {
			if failed := failedText(st); failed != "" {
				return false, failed, nil
			}
		}
		select {
		case <-ctx.Done():
			return false, "", stagingErr("watch "+name, ctx.Err())
		case <-end.C:
			done = true
		case <-time.After(poll):
		}
	}
	st, err := c.Status(ctx, name)
	if err != nil {
		return false, "", stagingErr("read the status of "+name+" at the end of the window", err)
	}
	if failed := failedText(st); failed != "" {
		return false, failed, nil
	}
	desc, n, ok := connectorAndTasks(st)
	if ok {
		return true, fmt.Sprintf("RUNNING, %d of %d tasks RUNNING at the end of the %s window", n, n, secondsText(window)), nil
	}
	return false, fmt.Sprintf("not RUNNING at the end of the %s window: %s", secondsText(window), desc), nil
}

// failedText describes a FAILED connector or the first FAILED task with its
// trace cleaned like connect_status's (credentials and record values
// masked); "" when nothing is FAILED.
func failedText(st clients.ConnectorStatus) string {
	trace := func(t string) string {
		if t == "" {
			return " (no trace)"
		}
		return ": " + kafkatools.CleanTaskTrace(t)
	}
	if st.State == "FAILED" {
		return "connector FAILED" + trace(st.Trace)
	}
	var first *clients.TaskStatus
	more := 0
	for i := range st.Tasks {
		if st.Tasks[i].State != "FAILED" {
			continue
		}
		if first == nil {
			first = &st.Tasks[i]
			continue
		}
		more++
	}
	if first == nil {
		return ""
	}
	s := fmt.Sprintf("task %d FAILED", first.ID)
	if more > 0 {
		s += fmt.Sprintf(" (and %d more tasks FAILED)", more)
	}
	return s + trace(first.Trace)
}

// restoreConnector gives the staging connector its whole previous config
// back on a context of its own (Deps.restoreContext), retrying while Connect
// rebalances. It never re-creates a connector removed meanwhile (PUT would).
// It returns what it could not put back, "" when staging is as it was.
func (d Deps) restoreConnector(ctx context.Context, c clients.ConnectReader, put clients.ConnectConfigPutter, name string, cfg map[string]string) string {
	rctx, cancel := d.restoreContext(ctx)
	defer cancel()
	var last error
	for {
		present, err := hasConnector(rctx, c, name)
		switch {
		case err == nil && !present:
			return fmt.Sprintf("connector %s was removed from staging during the stage; it was not re-created", name)
		case err == nil:
			if err = put.PutConnectorConfig(rctx, name, cfg); err == nil {
				return ""
			}
		}
		last = err
		select {
		case <-rctx.Done():
			return fmt.Sprintf("restore of connector %s's previous config on staging failed (%s); staging keeps the staged change", name, errCategory(last))
		case <-time.After(d.restoreRetry()):
		}
	}
}

// ---- schema_register ----

// Stage checks the schema against the same-named subject on the staging
// Registry (no write), and only when it is compatible registers it there.
// A registered version cannot be taken back: it is reported in Leftover.
func (a *registerSchema) Stage(ctx context.Context, raw json.RawMessage) (StageResult, error) {
	var p registerSchemaParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" || p.Schema == "" {
		return StageResult{}, fmt.Errorf("actions: subject and schema are required")
	}
	if err := stageScope(ctx, func(sc tools.Scope) error { return sc.CheckSubject(p.Subject) }); err != nil {
		return StageResult{}, err
	}
	res := StageResult{Target: "subject " + p.Subject}
	if a.d.Staging == nil || a.d.Staging.Registry == nil {
		res.Detail = events.StageNoStaging
		return res, nil
	}
	r := a.d.Staging.Registry
	start := time.Now()
	present, err := hasSubject(ctx, r, p.Subject)
	if err != nil {
		return StageResult{}, stagingErr("list staging subjects", err)
	}
	if !present {
		res.Detail = stageSubjectMissing
		return finish(res, start), nil
	}
	chk, ok := r.(compatibilityChecker)
	if !ok {
		// Registering unchecked would be a write the stage cannot vouch for.
		res.DryRun = &DryRunResult{Source: sourceCompat, Skipped: noDryRun}
		res.Detail = "the staging Registry client has no compatibility check; nothing registered"
		return finish(res, start), nil
	}
	compatible, msgs, err := chk.CheckCompatibility(ctx, p.Subject, p.Schema)
	var se *schemareg.Error
	switch {
	case errors.Is(err, clients.ErrNotFound):
		res.Detail = stageSubjectMissing + " (no version to check against)"
		return finish(res, start), nil
	case errors.As(err, &se):
		// The Registry's own refusal (an invalid schema) is its verdict; its
		// message can quote the schema, so only the code is kept.
		res.DryRun = &DryRunResult{Source: sourceCompat, Text: fmt.Sprintf("refused by the staging Registry: registry error %d", se.Code)}
		return finish(res, start), nil
	case err != nil:
		return StageResult{}, stagingErr("check compatibility of "+p.Subject, err)
	}
	level := stagingLevel(ctx, r, p.Subject)
	res.DryRun = &DryRunResult{OK: compatible, Source: sourceCompat, Text: dryRunText(compatText(compatible, msgs) + "; staging level " + level)}
	if !compatible {
		return finish(res, start), nil // nothing registered
	}
	before, err := r.Versions(ctx, p.Subject)
	if err != nil {
		return StageResult{}, stagingErr("read the versions of "+p.Subject, err)
	}
	id, err := r.Register(ctx, p.Subject, p.Schema)
	if err != nil {
		return StageResult{}, stagingErr("register on "+p.Subject+" (a new version may be on staging)", err)
	}
	res.Applied, res.Healthy = true, true
	res.Detail = fmt.Sprintf("compatible under staging level %s; registered as schema id %d", level, id)
	res.DurationMS = time.Since(start).Milliseconds()
	// Bookkeeping after the write: on its own context, so a stage cancelled
	// right after the register still says what it left.
	rctx, cancel := a.d.restoreContext(ctx)
	defer cancel()
	after, err := r.Versions(rctx, p.Subject)
	switch newest := slices.Max(append([]int{0}, after...)); {
	case err != nil:
		res.Leftover = fmt.Sprintf("subject %s may keep a new version (schema id %d) on staging: its versions could not be re-read (%s)", p.Subject, id, errCategory(err))
	case newest > slices.Max(append([]int{0}, before...)):
		res.Detail += fmt.Sprintf(" (version %d)", newest)
		res.Leftover = fmt.Sprintf("subject %s keeps version %d (schema id %d) on staging: a stage does not delete a registered version", p.Subject, newest, id)
	default:
		res.Detail += " (already registered; no new version)"
	}
	return CleanStageResult(res), nil
}

// hasSubject reports whether r lists the subject.
func hasSubject(ctx context.Context, r clients.SchemaRegistryReader, subject string) (bool, error) {
	subjects, err := r.Subjects(ctx)
	if err != nil {
		return false, err
	}
	return slices.Contains(subjects, subject), nil
}

// stagingLevel is the subject's compatibility level on staging for a
// result, "unknown" when it cannot be read.
func stagingLevel(ctx context.Context, r clients.SchemaRegistryReader, subject string) string {
	level, subjectLevel, err := r.Compatibility(ctx, subject)
	if err != nil || level == "" {
		return "unknown"
	}
	return levelText(level, subjectLevel)
}

// ---- schema_set_compatibility ----

// Stage sets the level on the same-named subject on the staging Registry,
// reads it back, and puts the previous level back (or removes the subject
// level when the subject followed the global one).
func (a *setCompatibility) Stage(ctx context.Context, raw json.RawMessage) (StageResult, error) {
	var p setCompatibilityParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" {
		return StageResult{}, fmt.Errorf("actions: subject is required")
	}
	if !compatLevels[p.Level] {
		return StageResult{}, fmt.Errorf("actions: level %q is not a valid compatibility level", shortText(p.Level))
	}
	if err := stageScope(ctx, func(sc tools.Scope) error { return sc.CheckSubject(p.Subject) }); err != nil {
		return StageResult{}, err
	}
	res := StageResult{Target: "subject " + p.Subject}
	if a.d.Staging == nil || a.d.Staging.Registry == nil {
		res.Detail = events.StageNoStaging
		return res, nil
	}
	r := a.d.Staging.Registry
	start := time.Now()
	present, err := hasSubject(ctx, r, p.Subject)
	if err != nil {
		return StageResult{}, stagingErr("list staging subjects", err)
	}
	if !present {
		res.Detail = stageSubjectMissing
		return finish(res, start), nil
	}
	before, hadOwn, err := r.Compatibility(ctx, p.Subject)
	if err != nil {
		return StageResult{}, stagingErr("read the level of "+p.Subject, err)
	}
	if hadOwn && before == p.Level {
		res.Applied, res.Healthy = true, true
		res.Detail = fmt.Sprintf("staging already has level %s on %s; no write made", p.Level, p.Subject)
		return finish(res, start), nil
	}
	setErr := r.SetCompatibility(ctx, p.Subject, p.Level)
	var after string
	var afterOwn bool
	var readErr error
	if setErr == nil {
		after, afterOwn, readErr = r.Compatibility(ctx, p.Subject)
	}
	elapsed := time.Since(start)
	leftover := a.d.restoreLevel(ctx, r, p.Subject, p.Level, before, hadOwn)
	if setErr != nil {
		return StageResult{}, withLeftover(stagingErr("set the level of "+p.Subject, setErr), leftover)
	}
	res.Applied = true
	move := fmt.Sprintf("%s → %s on staging", levelText(before, hadOwn), p.Level)
	switch {
	case readErr != nil:
		res.Detail = fmt.Sprintf("%s; the re-read failed (%s)", move, errCategory(readErr))
	case !afterOwn || after != p.Level:
		res.Detail = fmt.Sprintf("%s; the re-read shows %s", move, levelText(after, afterOwn))
	default:
		res.Healthy = true
		res.Detail = move + "; accepted and read back"
	}
	res.Leftover = leftover
	res.DurationMS = elapsed.Milliseconds()
	return CleanStageResult(res), nil
}

// restoreLevel puts the subject's previous level back on its own context
// (Deps.restoreContext), retrying until it ends. It returns what it could not put
// back, "" when staging is as it was.
func (d Deps) restoreLevel(ctx context.Context, r clients.SchemaRegistryClient, subject, staged, before string, hadOwn bool) string {
	rctx, cancel := d.restoreContext(ctx)
	defer cancel()
	undo := func(ctx context.Context) error { return r.SetCompatibility(ctx, subject, before) }
	if !hadOwn {
		del, ok := r.(subjectCompatDeleter)
		if !ok {
			return fmt.Sprintf("subject %s keeps level %s on staging: it had no level of its own (global %s) and the staging client cannot remove a subject level", subject, staged, before)
		}
		undo = func(ctx context.Context) error {
			err := del.DeleteSubjectCompatibility(ctx, subject)
			if errors.Is(err, clients.ErrNotFound) {
				return nil // no subject level left: already as it was
			}
			return err
		}
	}
	for {
		err := undo(rctx)
		if err == nil {
			return ""
		}
		select {
		case <-rctx.Done():
			return fmt.Sprintf("restore of subject %s's level on staging failed (%s); staging keeps level %s", subject, errCategory(err), staged)
		case <-time.After(d.restoreRetry()):
		}
	}
}
