package wiring

import (
	"context"
	"errors"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
)

// Read tools and the topology builder get only the read half of each client
// (clients.go, Reader/Writer split). Kafka, source Postgres and the warehouse
// have no write methods, so they pass through as is; their optional read
// interfaces (DLQReader, slot and DDL listers) keep working. Kafka Connect and
// Schema Registry do write, so the tools get a value that holds only the
// Reader: the write methods below refuse, and no type assertion reaches the
// concrete client's. The write halves go to package actions only.
//
// The adapters exist because kafkatools.Deps, dbtools.Deps,
// debeziumtools.Deps and topology.NewBuilder still take the whole *Client
// interfaces. When their owners switch them to Readers (no PR does yet),
// readConnect and readRegistry can hand over the Reader itself.

// ErrReadOnly is returned by a write method reached through a read-only client.
// Not to be confused with tools.ErrNotReadOnly, which a read-only registry
// returns for a tool whose Kind is not KindRead.
var ErrReadOnly = errors.New("wiring: write through a read-only client")

// readOnlyConnect is a ConnectReader dressed as a ConnectClient whose writes
// refuse.
type readOnlyConnect struct{ r clients.ConnectReader }

func (c readOnlyConnect) ListConnectors(ctx context.Context) ([]string, error) {
	return c.r.ListConnectors(ctx)
}
func (c readOnlyConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	return c.r.Status(ctx, name)
}
func (c readOnlyConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	return c.r.Config(ctx, name)
}
func (readOnlyConnect) RestartTask(context.Context, string, int) error { return ErrReadOnly }
func (readOnlyConnect) RestartConnector(context.Context, string) error { return ErrReadOnly }

// readOnlyRegistry is a SchemaRegistryReader dressed as a
// SchemaRegistryClient whose writes refuse.
type readOnlyRegistry struct{ r clients.SchemaRegistryReader }

func (s readOnlyRegistry) Subjects(ctx context.Context) ([]string, error) { return s.r.Subjects(ctx) }
func (s readOnlyRegistry) Versions(ctx context.Context, subject string) ([]int, error) {
	return s.r.Versions(ctx, subject)
}
func (s readOnlyRegistry) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	return s.r.Schema(ctx, subject, version)
}
func (s readOnlyRegistry) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	return s.r.Compatibility(ctx, subject)
}
func (readOnlyRegistry) SetCompatibility(context.Context, string, string) error { return ErrReadOnly }
func (readOnlyRegistry) Register(context.Context, string, string) (int, error) {
	return 0, ErrReadOnly
}

// connectConfigValidator is kafkatools.ConfigValidator's shape (Q4b): the pair
// behind connect_validate_config. Both calls only read — the validate request
// asks Connect for a verdict and creates nothing — so the read tools keep them,
// while connect_validate_config finds them by a type assertion on its client.
type connectConfigValidator interface {
	connectValidator
	PluginConfigDef(ctx context.Context, class string) ([]connect.ConfigKeyDef, error)
}

// The real client has both calls, so the read tools keep them through the
// wrapper.
var _ connectConfigValidator = (*connect.Client)(nil)

// readOnlyConnectValidate is readOnlyConnect plus the config validation reads
// (Q4b). It exists so the read-only value has them exactly when the client
// does: a fake without PluginConfigDef leaves connect_validate_config
// unregistered rather than offering the model a tool that always fails.
type readOnlyConnectValidate struct {
	readOnlyConnect
	v connectConfigValidator
}

func (c readOnlyConnectValidate) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	return c.v.ValidateConfig(ctx, class, cfg)
}

func (c readOnlyConnectValidate) PluginConfigDef(ctx context.Context, class string) ([]connect.ConfigKeyDef, error) {
	return c.v.PluginConfigDef(ctx, class)
}

// readOnlyConnectTasks is readOnlyConnect plus the cluster-wide task read
// (clients.WorkerTaskReader, X6b), which connect_task_balance finds by a type
// assertion. Like readOnlyRegistryDeleted, it exists so the read-only value
// has the read exactly when the real client has it.
type readOnlyConnectTasks struct {
	readOnlyConnect
	wt clients.WorkerTaskReader
}

func (c readOnlyConnectTasks) WorkerTasks(ctx context.Context) (map[string][]clients.TaskAssignment, error) {
	return c.wt.WorkerTasks(ctx)
}

// readOnlyConnectValidateTasks is readOnlyConnectValidate plus the
// cluster-wide task read: the real client has both, and each consumer of the
// wrapper type-asserts for the one read it needs.
type readOnlyConnectValidateTasks struct {
	readOnlyConnectValidate
	wt clients.WorkerTaskReader
}

func (c readOnlyConnectValidateTasks) WorkerTasks(ctx context.Context) (map[string][]clients.TaskAssignment, error) {
	return c.wt.WorkerTasks(ctx)
}

// connectLifecycleReads are the Connect reads of the Q wave that the read
// tools find by a type assertion: the stored source offsets
// (connect_source_offsets, Q5a), the worker's version (GET /, Q5a), its
// plugin list (connect_plugins, Q1c) and its logger levels (GET
// /admin/loggers, connect_worker_info, Q3a). The real client has all four; a
// client without every one keeps none of them (the tools then stay
// unregistered or say the read is unsupported), so the wrapper types below
// need not cover every combination.
type connectLifecycleReads interface {
	connect.SourceOffsetReader
	connect.ConnectInfoReader
	connect.PluginReader
	connect.LoggerReader
}

// The real client has the four reads, so connect_plugins,
// connect_source_offsets and connect_worker_info's logger levels work over
// its read-only value.
var _ connectLifecycleReads = (*connect.Client)(nil)

// lifecycleReads forwards the four reads; each read-only Connect value
// below embeds it beside its own reads. None of them writes.
type lifecycleReads struct{ l connectLifecycleReads }

func (f lifecycleReads) SourceOffsets(ctx context.Context, connector string) ([]connect.SourceOffset, error) {
	return f.l.SourceOffsets(ctx, connector)
}
func (f lifecycleReads) ServerInfo(ctx context.Context) (connect.ServerInfo, error) {
	return f.l.ServerInfo(ctx)
}
func (f lifecycleReads) Plugins(ctx context.Context) ([]connect.Plugin, error) {
	return f.l.Plugins(ctx)
}
func (f lifecycleReads) Loggers(ctx context.Context) ([]connect.LoggerLevel, error) {
	return f.l.Loggers(ctx)
}

// The four read-only Connect values plus the Q-wave reads.
type (
	readOnlyConnectQ struct {
		readOnlyConnect
		lifecycleReads
	}
	readOnlyConnectValidateQ struct {
		readOnlyConnectValidate
		lifecycleReads
	}
	readOnlyConnectTasksQ struct {
		readOnlyConnectTasks
		lifecycleReads
	}
	readOnlyConnectValidateTasksQ struct {
		readOnlyConnectValidateTasks
		lifecycleReads
	}
)

// readConnect returns w's Connect client for read tools: nil when none is
// configured (the tools then say so), else read-only, with the worker-task
// read (X6b), the config validation pair (Q4b) and the Q-wave offsets,
// version and plugin reads (Q1c, Q5a; Q1d registers connect_plugins through
// them) when the client has them.
func readConnect(w *Wired) clients.ConnectClient {
	c := readConnectBase(w)
	if c == nil {
		return nil
	}
	l, ok := w.Connect.(connectLifecycleReads)
	if !ok {
		return c
	}
	q := lifecycleReads{l: l}
	switch v := c.(type) {
	case readOnlyConnectValidateTasks:
		return readOnlyConnectValidateTasksQ{readOnlyConnectValidateTasks: v, lifecycleReads: q}
	case readOnlyConnectValidate:
		return readOnlyConnectValidateQ{readOnlyConnectValidate: v, lifecycleReads: q}
	case readOnlyConnectTasks:
		return readOnlyConnectTasksQ{readOnlyConnectTasks: v, lifecycleReads: q}
	case readOnlyConnect:
		return readOnlyConnectQ{readOnlyConnect: v, lifecycleReads: q}
	}
	return c
}

// readConnectBase is readConnect without the Q-wave reads. The P3 and Q2
// actions with write mode off use it (readOnlyActionConnect): none of them
// reads the offsets, version or plugins. The Q5 lifecycle actions, which do,
// get readConnect with write mode off (registerLifecycle).
func readConnectBase(w *Wired) clients.ConnectClient {
	if w.Connect == nil {
		return nil
	}
	ro := readOnlyConnect{r: w.Connect}
	val, hasVal := w.Connect.(connectConfigValidator)
	wt, hasTasks := w.Connect.(clients.WorkerTaskReader)
	switch {
	case hasVal && hasTasks:
		return readOnlyConnectValidateTasks{readOnlyConnectValidate: readOnlyConnectValidate{readOnlyConnect: ro, v: val}, wt: wt}
	case hasVal:
		return readOnlyConnectValidate{readOnlyConnect: ro, v: val}
	case hasTasks:
		return readOnlyConnectTasks{readOnlyConnect: ro, wt: wt}
	}
	return ro
}

// deletedSubjectLister is the registry's deleted-state read pair (KAN-407).
// It mirrors kafkatools' deletedSubjectReader, which the registry tools find
// by a type assertion on their client; TestReadOnlyClients fails if the
// read-only wrapper stops forwarding it.
type deletedSubjectLister interface {
	SubjectsIncludingDeleted(ctx context.Context) ([]string, error)
	VersionsIncludingDeleted(ctx context.Context, subject string) ([]int, error)
}

// The real client has the deleted-state reads, so the read tools keep them
// through the wrapper.
var _ deletedSubjectLister = (*schemareg.Client)(nil)

// readOnlyRegistryDeleted is readOnlyRegistry plus the deleted-state reads.
// Like readOnlyRegistryDryRun, it exists so the read-only value keeps the
// extra reads exactly when the real client has them: a registry without them
// stays a plain readOnlyRegistry, and the tools' assertion fails as it would
// on the client itself.
type readOnlyRegistryDeleted struct {
	readOnlyRegistry
	d deletedSubjectLister
}

func (s readOnlyRegistryDeleted) SubjectsIncludingDeleted(ctx context.Context) ([]string, error) {
	return s.d.SubjectsIncludingDeleted(ctx)
}
func (s readOnlyRegistryDeleted) VersionsIncludingDeleted(ctx context.Context, subject string) ([]int, error) {
	return s.d.VersionsIncludingDeleted(ctx, subject)
}

// readRegistry returns w's Schema Registry client for read tools: nil when
// none is configured, else read-only, with the deleted-state reads when the
// client has them.
func readRegistry(w *Wired) clients.SchemaRegistryClient {
	if w.Registry == nil {
		return nil
	}
	ro := readOnlyRegistry{r: w.Registry}
	if d, ok := w.Registry.(deletedSubjectLister); ok {
		return readOnlyRegistryDeleted{readOnlyRegistry: ro, d: d}
	}
	return ro
}

// With write mode off the actions get read-only clients too (writemode.go).
// Their Validate and Preview only read, but Preview also calls two dry-run
// endpoints that change nothing (actions.Preview: "the documented dry-run
// endpoints"): Connect's config validate and the Registry's compatibility
// check. The actions find them by a type assertion on their client, so the
// read-only value keeps each one exactly when the real client has it, and the
// proposal card shows the same dry-run verdict in both modes. These two
// interfaces mirror package actions' configValidator and compatibilityChecker;
// TestWriteModeOffKeepsDryRuns fails if they drift apart.
type (
	connectValidator interface {
		ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error)
	}
	registryChecker interface {
		CheckCompatibility(ctx context.Context, subject, schema string) (ok bool, messages []string, err error)
	}
)

// The real clients have both dry-runs, so with write mode off the preview of
// a config change or a new schema keeps its verdict.
var (
	_ connectValidator = (*connect.Client)(nil)
	_ registryChecker  = (*schemareg.Client)(nil)
)

// readOnlyConnectDryRun is readOnlyConnect plus Connect's config validate.
type readOnlyConnectDryRun struct {
	readOnlyConnect
	v connectValidator
}

func (c readOnlyConnectDryRun) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	return c.v.ValidateConfig(ctx, class, cfg)
}

// readOnlyRegistryDryRun is readOnlyRegistry plus the compatibility check.
type readOnlyRegistryDryRun struct {
	readOnlyRegistry
	c registryChecker
}

func (s readOnlyRegistryDryRun) CheckCompatibility(ctx context.Context, subject, schema string) (bool, []string, error) {
	return s.c.CheckCompatibility(ctx, subject, schema)
}

// readOnlyActionConnect is w's Connect client for the actions with write mode
// off: nil when none is configured (as with write mode on), else read-only,
// with config validate when the client has it.
func readOnlyActionConnect(w *Wired) clients.ConnectClient {
	c := readConnectBase(w)
	ro, ok := c.(readOnlyConnect)
	if !ok {
		return c
	}
	if v, ok := ro.r.(connectValidator); ok {
		return readOnlyConnectDryRun{readOnlyConnect: ro, v: v}
	}
	return ro
}

// readOnlyActionRegistry is w's Schema Registry client for the actions with
// write mode off, never nil: noRegistry when none is configured (as with
// write mode on), else read-only, with the compatibility check when the
// client has it.
func readOnlyActionRegistry(w *Wired) clients.SchemaRegistryClient {
	var ro readOnlyRegistry
	switch v := readRegistry(w).(type) {
	case readOnlyRegistry:
		ro = v
	case readOnlyRegistryDeleted:
		ro = v.readOnlyRegistry
	default:
		return noRegistry{}
	}
	if c, ok := ro.r.(registryChecker); ok {
		return readOnlyRegistryDryRun{readOnlyRegistry: ro, c: c}
	}
	return ro
}
