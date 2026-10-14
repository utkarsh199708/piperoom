package wiring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/owners"
	"piperoom/internal/topology"
)

// DefaultDrainGrace is how long a swapped-out client may keep serving calls
// that started before the swap. After it the old client is closed anyway, so
// one hung call cannot keep a stale connection pool open for ever
// (docs/design/integrations.md §4.5).
const DefaultDrainGrace = 30 * time.Second

// DefaultModelDrainGrace is the drain grace of the model endpoint. LiveModel.Run
// holds its reference for a whole agent run, which normally lasts far longer
// than DefaultDrainGrace, so the model slot waits longer before it closes a
// replaced endpoint under a run. At Close the shorter grace applies to it too.
const DefaultModelDrainGrace = 15 * time.Minute

// ErrNotConfigured is the sentinel every Live wrapper returns (wrapped) when
// its category has no client: the server runs in setup mode, or the
// integration was disabled or deleted. Test it with errors.Is; the wrapping
// error's text is the plain sentence a tool can show ("Kafka Connect is not
// connected").
var ErrNotConfigured = errors.New("wiring: integration not configured")

// ErrLiveClosed is returned by Apply, Clear and the Swap methods after Close.
var ErrLiveClosed = errors.New("wiring: live clients closed")

// ErrDLQUnsupported answers the DLQReader methods when the current Kafka
// client does not implement clients.DLQReader (a fake or replay client). The
// Live Kafka wrapper always passes the clients.DLQReader type assertion, so
// callers (the watcher's dlq_growth signal, dlq_peek) tell "unsupported" from
// a real failure with errors.Is(err, errors.ErrUnsupported) and log it once.
var ErrDLQUnsupported = fmt.Errorf("wiring: the connected Kafka client cannot read dead-letter topics: %w", errors.ErrUnsupported)

// notConfiguredError carries the plain sentence for one category.
type notConfiguredError struct{ what string }

func (e *notConfiguredError) Error() string { return e.what + " is not connected" }
func (e *notConfiguredError) Unwrap() error { return ErrNotConfigured }

// categoryTitle is the human name used in the "is not connected" sentence.
var categoryTitle = map[spec.Category]string{
	spec.CategorySourceDB:  "The source database",
	spec.CategoryWarehouse: "The warehouse",
	spec.CategoryConnect:   "Kafka Connect",
	spec.CategoryKafka:     "Kafka",
	spec.CategoryRegistry:  "The Schema Registry",
	spec.CategoryModel:     "The model endpoint",
}

// slot is one generation of a client. In-flight calls hold a reference taken
// under mu, so retire and acquire never race: once retired, no new reference
// is handed out, and drained closes exactly when the last one is released.
type slot[T any] struct {
	c     T
	ok    bool // false = "not configured"
	close func()
	extra string // per-generation metadata (the warehouse kind)

	mu      sync.Mutex
	refs    int
	retired bool
	drained chan struct{} // closed once retired and refs == 0

	closeOnce sync.Once
}

func newSlot[T any](c T, ok bool, closeFn func(), extra string) *slot[T] {
	return &slot[T]{c: c, ok: ok, close: closeFn, extra: extra, drained: make(chan struct{})}
}

// acquire takes a reference; false means the slot was retired meanwhile.
func (s *slot[T]) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return false
	}
	s.refs++
	return true
}

func (s *slot[T]) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs--
	if s.retired && s.refs == 0 {
		close(s.drained)
	}
}

// retire stops new references. It is called once per slot, by the swap that
// replaced it.
func (s *slot[T]) retire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
	if s.refs == 0 {
		close(s.drained)
	}
}

// closeClient runs the client's closer at most once.
func (s *slot[T]) closeClient() {
	s.closeOnce.Do(func() {
		if s.close != nil {
			s.close()
		}
	})
}

// swapper holds the current slot of one category.
type swapper[T any] struct {
	cat   spec.Category
	grace time.Duration // how long a retired slot may drain before it is closed
	cur   atomic.Pointer[slot[T]]
}

// acquire returns the current configured slot with a reference held. The
// loop only repeats when a swap retired the loaded slot between Load and
// acquire; the swap stores its new slot before retiring the old one, so the
// next Load sees the replacement.
func (s *swapper[T]) acquire() (*slot[T], error) {
	for {
		sl := s.cur.Load()
		if sl == nil || !sl.ok {
			return nil, &notConfiguredError{what: categoryTitle[s.cat]}
		}
		if sl.acquire() {
			return sl, nil
		}
	}
}

func (s *swapper[T]) configured() bool {
	sl := s.cur.Load()
	return sl != nil && sl.ok
}

// call runs f on the current client of s, holding a reference for its whole
// duration so the client is not closed under it.
func call[T, R any](s *swapper[T], f func(T) (R, error)) (R, error) {
	sl, err := s.acquire()
	if err != nil {
		var zero R
		return zero, err
	}
	defer sl.release()
	return f(sl.c)
}

func call0[T any](s *swapper[T], f func(T) error) error {
	_, err := call(s, func(c T) (struct{}, error) { return struct{}{}, f(c) })
	return err
}

// LiveOptions configures NewLive.
type LiveOptions struct {
	// WorkspaceID is the one workspace whose integrations this Live serves.
	// Empty = spec.DefaultWorkspace. Hosted installs build one Live per workspace.
	WorkspaceID string
	// Owners is the loaded owners config, passed through to the Wired and the
	// topology builder.
	Owners *owners.Owners
	// Env lists the categories set by environment variables. Pass the keys
	// of integrations.EnvFromLookup(os.LookupEnv).Categories, the same view
	// the Integrations page shows, so the two never disagree (EnvCategories
	// gives the same answer). Env wins: Apply and Clear refuse them with
	// spec.ErrManagedByEnv, Adopt loads only them, and only the Swap methods
	// (used at start-up) may set them otherwise.
	Env []spec.Category
	// DrainGrace overrides DefaultDrainGrace; <= 0 = the default.
	DrainGrace time.Duration
	// ModelDrainGrace overrides DefaultModelDrainGrace for the model slot;
	// <= 0 = the default. It never falls below DrainGrace.
	ModelDrainGrace time.Duration
	// Logger; nil = slog.Default().
	Logger *slog.Logger
}

// Live holds one swappable wrapper per client interface Wired uses, plus the
// model endpoint, so an integration saved in the UI takes effect without a
// restart (docs/design/integrations.md §4.5). It implements spec.Applier.
type Live struct {
	ws    string
	env   map[spec.Category]bool
	grace time.Duration
	log   *slog.Logger

	source    swapper[clients.SourceDB]
	warehouse swapper[clients.Warehouse]
	connect   swapper[clients.ConnectClient]
	kafka     swapper[clients.KafkaAdmin]
	registry  swapper[clients.SchemaRegistryClient]
	model     swapper[*agent.Endpoint]

	// mu serialises swaps with each other and with Close.
	mu       sync.Mutex
	closed   bool
	drains   sync.WaitGroup // one per retired slot still waiting to close
	onClose  []func()       // extra closers (Adopt)
	shutdown chan struct{}  // closed when Close starts: drains switch to the short grace
	done     chan struct{}  // closed when Close has released everything

	wired *Wired

	// applied is the row id and version last swapped in per category, under
	// mu. Apply builds outside the lock, so two saves of one row can finish
	// out of order; the older one is dropped instead of replacing the newer.
	applied map[spec.Category]appliedRow
}

type appliedRow struct {
	id      string
	version int64
}

var _ spec.Applier = (*Live)(nil)

// NewLive builds a Live with nothing configured (setup mode) and the *Wired
// whose clients are Live's wrappers, so RegisterTools and the topology
// builder work unchanged: each call reaches whichever client is current.
// Wired.Close closes the Live.
func NewLive(opts LiveOptions) (*Live, *Wired) {
	l := &Live{
		ws:       opts.WorkspaceID,
		env:      map[spec.Category]bool{},
		applied:  map[spec.Category]appliedRow{},
		grace:    opts.DrainGrace,
		log:      opts.Logger,
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
	if l.ws == "" {
		l.ws = spec.DefaultWorkspace
	}
	if l.grace <= 0 {
		l.grace = DefaultDrainGrace
	}
	if l.log == nil {
		l.log = slog.Default()
	}
	modelGrace := opts.ModelDrainGrace
	if modelGrace <= 0 {
		modelGrace = DefaultModelDrainGrace
	}
	if modelGrace < l.grace {
		modelGrace = l.grace
	}
	for _, c := range opts.Env {
		l.env[c] = true
	}
	l.source.cat = spec.CategorySourceDB
	l.warehouse.cat = spec.CategoryWarehouse
	l.connect.cat = spec.CategoryConnect
	l.kafka.cat = spec.CategoryKafka
	l.registry.cat = spec.CategoryRegistry
	l.model.cat = spec.CategoryModel
	l.source.grace, l.warehouse.grace, l.connect.grace = l.grace, l.grace, l.grace
	l.kafka.grace, l.registry.grace = l.grace, l.grace
	l.model.grace = modelGrace

	src := &liveSource{s: &l.source}
	wh := &liveWarehouse{s: &l.warehouse}
	conn := &liveConnect{s: &l.connect}
	kaf := &liveKafka{s: &l.kafka}
	reg := &liveRegistry{s: &l.registry}
	l.wired = &Wired{
		Source:    src,
		Warehouse: wh,
		Connect:   conn,
		Kafka:     kaf,
		Registry:  reg,
		Builder:   topology.NewBuilder(opts.Owners, conn, kaf, src, wh),
		Owners:    opts.Owners,
		close:     l.Close,
	}
	return l, l.wired
}

// Wired returns the *Wired NewLive built.
func (l *Live) Wired() *Wired { return l.wired }

// Model returns the swappable model endpoint.
func (l *Live) Model() *LiveModel { return &LiveModel{s: &l.model} }

// WarehouseKind is the kind of the current warehouse client ("postgres",
// "snowflake"), "" when none is configured.
//
// Wired.WarehouseKind is a plain string read once by RegisterTools, so a swap
// from one warehouse kind to another does not reach the db tools' SQL dialect
// choice until they read it from here.
func (l *Live) WarehouseKind() string {
	sl := l.warehouse.cur.Load()
	if sl == nil || !sl.ok {
		return ""
	}
	return sl.extra
}

// Configured lists the categories that currently have a client, in
// spec.Categories order. The agent's prompt names them so it does not plan
// around tools it cannot use.
func (l *Live) Configured() []spec.Category {
	have := map[spec.Category]bool{
		spec.CategorySourceDB:  l.source.configured(),
		spec.CategoryWarehouse: l.warehouse.configured(),
		spec.CategoryConnect:   l.connect.configured(),
		spec.CategoryKafka:     l.kafka.configured(),
		spec.CategoryRegistry:  l.registry.configured(),
		spec.CategoryModel:     l.model.configured(),
	}
	var out []spec.Category
	for _, c := range spec.Categories {
		if have[c] {
			out = append(out, c)
		}
	}
	return out
}

// swapIn stores the new slot, then retires the old one and closes it once
// its in-flight calls end or after the slot's grace, whichever comes first.
// Once Close starts, a drain still waiting gets at most l.grace more, so a
// long model grace cannot hold up shutdown. A nil closeFn is fine. It must be
// called with l.mu held.
func swapIn[T any](l *Live, s *swapper[T], c T, ok bool, closeFn func(), extra string) {
	old := s.cur.Swap(newSlot(c, ok, closeFn, extra))
	if old == nil {
		return
	}
	old.retire()
	l.drains.Add(1)
	go func() {
		defer l.drains.Done()
		t := time.NewTimer(s.grace)
		defer t.Stop()
		select {
		case <-old.drained:
		case <-t.C:
			l.log.Warn("closing a replaced client with calls still in flight", "category", string(s.cat), "grace", s.grace.String())
		case <-l.shutdown:
			st := time.NewTimer(l.grace)
			defer st.Stop()
			select {
			case <-old.drained:
			case <-st.C:
				l.log.Warn("closing a replaced client with calls still in flight at shutdown", "category", string(s.cat), "grace", l.grace.String())
			}
		}
		old.closeClient()
	}()
}

// swap is the locked entry point of every Swap method.
func swap[T any](l *Live, s *swapper[T], c T, ok bool, closeFn func(), extra string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if closeFn != nil {
			closeFn()
		}
		return ErrLiveClosed
	}
	swapIn(l, s, c, ok, closeFn, extra)
	return nil
}

// swapRec is swap for a stored row (Apply and Clear): it drops a swap whose
// row version is older than the one last applied for that row, closing its
// client, so the live client always matches the newest saved version.
func swapRec[T any](l *Live, s *swapper[T], rec spec.Integration, c T, ok bool, closeFn func(), extra string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if closeFn != nil {
			closeFn()
		}
		return ErrLiveClosed
	}
	if last, seen := l.applied[rec.Category]; seen && last.id == rec.ID && rec.Version < last.version {
		if closeFn != nil {
			closeFn()
		}
		l.log.Info("wiring: dropped an out-of-order apply", "category", string(rec.Category), "id", rec.ID,
			"version", rec.Version, "live_version", last.version)
		return nil
	}
	l.applied[rec.Category] = appliedRow{id: rec.ID, version: rec.Version}
	swapIn(l, s, c, ok, closeFn, extra)
	return nil
}

// SwapSource makes c the source database client; nil = not configured.
// closeFn (may be nil) runs exactly once, when the client is replaced and
// drained, or at Close. After Close it runs at once and ErrLiveClosed is returned.
func (l *Live) SwapSource(c clients.SourceDB, closeFn func()) error {
	return swap(l, &l.source, c, c != nil, closeFn, "")
}

// SwapWarehouse makes c the warehouse client of the given kind ("postgres"
// or "snowflake"; "" = postgres); nil = not configured.
func (l *Live) SwapWarehouse(c clients.Warehouse, kind string, closeFn func()) error {
	if kind == "" && c != nil {
		kind = "postgres"
	}
	return swap(l, &l.warehouse, c, c != nil, closeFn, kind)
}

// whKind defaults an empty warehouse kind to postgres, as SwapWarehouse does.
func whKind(k string) string {
	if k == "" {
		return "postgres"
	}
	return k
}

// SwapConnect makes c the Kafka Connect client; nil = not configured.
func (l *Live) SwapConnect(c clients.ConnectClient, closeFn func()) error {
	return swap(l, &l.connect, c, c != nil, closeFn, "")
}

// SwapKafka makes c the Kafka client; nil = not configured. When c also
// implements clients.DLQReader, the wrapper forwards those calls.
func (l *Live) SwapKafka(c clients.KafkaAdmin, closeFn func()) error {
	return swap(l, &l.kafka, c, c != nil, closeFn, "")
}

// SwapRegistry makes c the Schema Registry client; nil = not configured.
func (l *Live) SwapRegistry(c clients.SchemaRegistryClient, closeFn func()) error {
	return swap(l, &l.registry, c, c != nil, closeFn, "")
}

// SwapModel makes ep the model endpoint; nil = not configured.
func (l *Live) SwapModel(ep *agent.Endpoint, closeFn func()) error {
	return swap(l, &l.model, ep, ep != nil, closeFn, "")
}

// Adopt swaps in w's clients (w is a Wired from Open, built from the
// environment) for the env categories only (LiveOptions.Env); the others stay
// as they are, normally not configured. Open builds all five clients and
// falls back to dev defaults (localhost:29092, ...) for unset variables, so
// adopting a category the operator did not set would report a client that
// points at nothing as connected. w.Close is kept for Close, because Open's
// clients share one closer: their resources, including those of the clients
// not adopted, are released only at Close. Apply and Clear refuse env
// categories, so an adopted client is replaced only by a direct Swap call.
func (l *Live) Adopt(w *Wired) error {
	if w == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		w.Close()
		return ErrLiveClosed
	}
	if l.env[spec.CategorySourceDB] && !isNil(w.Source) {
		swapIn(l, &l.source, w.Source, true, nil, "")
	}
	if l.env[spec.CategoryWarehouse] && !isNil(w.Warehouse) {
		kind := w.WarehouseKind
		if kind == "" {
			kind = "postgres"
		}
		swapIn(l, &l.warehouse, w.Warehouse, true, nil, kind)
	}
	if l.env[spec.CategoryConnect] && !isNil(w.Connect) {
		swapIn(l, &l.connect, w.Connect, true, nil, "")
	}
	if l.env[spec.CategoryKafka] && !isNil(w.Kafka) {
		swapIn(l, &l.kafka, w.Kafka, true, nil, "")
	}
	if l.env[spec.CategoryRegistry] && !isNil(w.Registry) {
		swapIn(l, &l.registry, w.Registry, true, nil, "")
	}
	l.onClose = append(l.onClose, w.Close)
	return nil
}

// checkRecord refuses a record of another workspace or scope, or an
// env-managed category.
func (l *Live) checkRecord(rec spec.Integration) error {
	ws := rec.WorkspaceID
	if ws == "" {
		ws = spec.DefaultWorkspace
	}
	if ws != l.ws {
		return fmt.Errorf("wiring: integration %s belongs to workspace %q, not %q", rec.ID, ws, l.ws)
	}
	if rec.Scope != "" && rec.Scope != spec.ScopeAll {
		return fmt.Errorf("wiring: integration %s: scope %q is not supported yet", rec.ID, rec.Scope)
	}
	if l.env[rec.Category] {
		return fmt.Errorf("wiring: %s: %w", rec.Category, spec.ErrManagedByEnv)
	}
	return nil
}

// Apply builds rec's client with kind.Build and swaps it in. A failed build
// or a client of the wrong type leaves the old client in place. Categories
// without a live client (alerts, chat, secret managers) are not handled here:
// Apply closes whatever Build returned and reports nil for alerts, an error
// for the others.
func (l *Live) Apply(ctx context.Context, rec spec.Integration, kind spec.Kind, v spec.Values) error {
	if err := l.checkRecord(rec); err != nil {
		return err
	}
	if kind.Category != rec.Category {
		return fmt.Errorf("wiring: kind %s is a %s kind, integration %s is %s", kind.ID, kind.Category, rec.ID, rec.Category)
	}
	if kind.Build == nil {
		return fmt.Errorf("wiring: kind %s cannot build a client", kind.ID)
	}
	client, closeFn, err := kind.Build(ctx, v)
	if err != nil {
		return fmt.Errorf("wiring: build %s client: %w", kind.ID, err)
	}
	bad := func() error {
		if closeFn != nil {
			closeFn()
		}
		return fmt.Errorf("wiring: kind %s built %T, not a %s client", kind.ID, client, rec.Category)
	}
	switch rec.Category {
	case spec.CategorySourceDB:
		c, ok := client.(clients.SourceDB)
		if !ok || isNil(c) {
			return bad()
		}
		return swapRec(l, &l.source, rec, clients.SourceDB(c), true, closeFn, "")
	case spec.CategoryWarehouse:
		c, ok := client.(clients.Warehouse)
		if !ok || isNil(c) {
			return bad()
		}
		return swapRec(l, &l.warehouse, rec, clients.Warehouse(c), true, closeFn, whKind(warehouseKindOf(kind.ID)))
	case spec.CategoryConnect:
		c, ok := client.(clients.ConnectClient)
		if !ok || isNil(c) {
			return bad()
		}
		return swapRec(l, &l.connect, rec, clients.ConnectClient(c), true, closeFn, "")
	case spec.CategoryKafka:
		c, ok := client.(clients.KafkaAdmin)
		if !ok || isNil(c) {
			return bad()
		}
		return swapRec(l, &l.kafka, rec, clients.KafkaAdmin(c), true, closeFn, "")
	case spec.CategoryRegistry:
		c, ok := client.(clients.SchemaRegistryClient)
		if !ok || isNil(c) {
			return bad()
		}
		return swapRec(l, &l.registry, rec, clients.SchemaRegistryClient(c), true, closeFn, "")
	case spec.CategoryModel:
		c, ok := client.(*agent.Endpoint)
		if !ok || c == nil {
			return bad()
		}
		return swapRec(l, &l.model, rec, c, true, closeFn, "")
	case spec.CategoryAlerts:
		// The row only holds the webhook secret; there is no client to swap.
		if closeFn != nil {
			closeFn()
		}
		return nil
	default:
		if closeFn != nil {
			closeFn()
		}
		return fmt.Errorf("wiring: category %s has no live client in wiring", rec.Category)
	}
}

// isNil reports whether v is nil or an interface holding a typed nil pointer
// (or other nil-able kind), such as (*connect.Client)(nil). Swapping one in
// would pass every type check and then panic on the first call.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// warehouseKindOf maps a warehouse kind id to the WAREHOUSE_KIND value the
// db tools expect.
func warehouseKindOf(kindID string) string {
	if strings.Contains(strings.ToLower(kindID), "snowflake") {
		return "snowflake"
	}
	return "postgres"
}

// Clear swaps in "not configured" for rec's category (disable, delete).
func (l *Live) Clear(_ context.Context, rec spec.Integration) error {
	if err := l.checkRecord(rec); err != nil {
		return err
	}
	switch rec.Category {
	case spec.CategorySourceDB:
		return swapRec(l, &l.source, rec, clients.SourceDB(nil), false, nil, "")
	case spec.CategoryWarehouse:
		return swapRec(l, &l.warehouse, rec, clients.Warehouse(nil), false, nil, "")
	case spec.CategoryConnect:
		return swapRec(l, &l.connect, rec, clients.ConnectClient(nil), false, nil, "")
	case spec.CategoryKafka:
		return swapRec(l, &l.kafka, rec, clients.KafkaAdmin(nil), false, nil, "")
	case spec.CategoryRegistry:
		return swapRec(l, &l.registry, rec, clients.SchemaRegistryClient(nil), false, nil, "")
	case spec.CategoryModel:
		return swapRec(l, &l.model, rec, (*agent.Endpoint)(nil), false, nil, "")
	case spec.CategoryAlerts:
		return nil
	default:
		return fmt.Errorf("wiring: category %s has no live client in wiring", rec.Category)
	}
}

// Close swaps every category to "not configured", waits until each replaced
// client is drained (or its grace ran out) and closed, then runs the closers
// from Adopt. It is safe to call more than once, concurrently, and from
// Wired.Close: every call returns only after all of that is done. It can
// block for up to the drain grace when a call is still in flight.
func (l *Live) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		<-l.done
		return
	}
	l.closed = true
	defer close(l.done)
	close(l.shutdown)
	swapIn(l, &l.source, nil, false, nil, "")
	swapIn(l, &l.warehouse, nil, false, nil, "")
	swapIn(l, &l.connect, nil, false, nil, "")
	swapIn(l, &l.kafka, nil, false, nil, "")
	swapIn(l, &l.registry, nil, false, nil, "")
	swapIn[*agent.Endpoint](l, &l.model, nil, false, nil, "")
	extra := l.onClose
	l.onClose = nil
	l.mu.Unlock()

	l.drains.Wait()
	for i := len(extra) - 1; i >= 0; i-- {
		extra[i]()
	}
}

// ---- wrappers ----

type liveSource struct{ s *swapper[clients.SourceDB] }

var _ clients.SourceDB = (*liveSource)(nil)

func (w *liveSource) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	return call(w.s, func(c clients.SourceDB) ([]clients.ReplicationSlot, error) { return c.ReplicationSlots(ctx) })
}

func (w *liveSource) RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	return call(w.s, func(c clients.SourceDB) ([]clients.DDLChange, error) { return c.RecentDDL(ctx, since, limit) })
}

func (w *liveSource) LongTransactions(ctx context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	return call(w.s, func(c clients.SourceDB) ([]clients.Transaction, error) { return c.LongTransactions(ctx, olderThan) })
}

// rowResult carries Row's two results through call.
type rowResult struct {
	row   map[string]any
	found bool
}

func (w *liveSource) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	r, err := call(w.s, func(c clients.SourceDB) (rowResult, error) {
		row, found, err := c.Row(ctx, table, pkColumn, pk)
		return rowResult{row, found}, err
	})
	return r.row, r.found, err
}

type liveWarehouse struct{ s *swapper[clients.Warehouse] }

var _ clients.Warehouse = (*liveWarehouse)(nil)

func (w *liveWarehouse) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	return call(w.s, func(c clients.Warehouse) (time.Time, error) { return c.Freshness(ctx, table, tsColumn) })
}

func (w *liveWarehouse) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	r, err := call(w.s, func(c clients.Warehouse) (rowResult, error) {
		row, found, err := c.Row(ctx, table, pkColumn, pk)
		return rowResult{row, found}, err
	})
	return r.row, r.found, err
}

type liveConnect struct {
	s *swapper[clients.ConnectClient]
}

var _ clients.ConnectClient = (*liveConnect)(nil)

func (w *liveConnect) ListConnectors(ctx context.Context) ([]string, error) {
	return call(w.s, func(c clients.ConnectClient) ([]string, error) { return c.ListConnectors(ctx) })
}

func (w *liveConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	return call(w.s, func(c clients.ConnectClient) (clients.ConnectorStatus, error) { return c.Status(ctx, name) })
}

func (w *liveConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	return call(w.s, func(c clients.ConnectClient) (map[string]string, error) { return c.Config(ctx, name) })
}

func (w *liveConnect) RestartTask(ctx context.Context, name string, task int) error {
	return call0(w.s, func(c clients.ConnectClient) error { return c.RestartTask(ctx, name, task) })
}

func (w *liveConnect) RestartConnector(ctx context.Context, name string) error {
	return call0(w.s, func(c clients.ConnectClient) error { return c.RestartConnector(ctx, name) })
}

// liveKafka also implements clients.DLQReader, because a wrapper's method
// set is fixed while the client behind it changes. When the current client
// cannot read DLQs, the DLQ methods return an error instead.
type liveKafka struct{ s *swapper[clients.KafkaAdmin] }

var (
	_ clients.KafkaAdmin = (*liveKafka)(nil)
	_ clients.DLQReader  = (*liveKafka)(nil)
)

func (w *liveKafka) Topics(ctx context.Context) ([]string, error) {
	return call(w.s, func(c clients.KafkaAdmin) ([]string, error) { return c.Topics(ctx) })
}

func (w *liveKafka) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	return call(w.s, func(c clients.KafkaAdmin) ([]clients.PartitionOffsets, error) { return c.TopicOffsets(ctx, topic) })
}

func (w *liveKafka) ConsumerGroups(ctx context.Context) ([]string, error) {
	return call(w.s, func(c clients.KafkaAdmin) ([]string, error) { return c.ConsumerGroups(ctx) })
}

func (w *liveKafka) GroupLag(ctx context.Context, group string) (clients.GroupLag, error) {
	return call(w.s, func(c clients.KafkaAdmin) (clients.GroupLag, error) { return c.GroupLag(ctx, group) })
}

func (w *liveKafka) FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
	return call(w.s, func(c clients.KafkaAdmin) ([]clients.RecordRef, error) {
		return c.FindByKey(ctx, topic, key, since, maxScan)
	})
}

func (w *liveKafka) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	return call(w.s, func(c clients.KafkaAdmin) (map[string][]clients.PartitionOffsets, error) {
		d, ok := c.(clients.DLQReader)
		if !ok {
			return nil, ErrDLQUnsupported
		}
		return d.DLQOffsets(ctx, topics...)
	})
}

func (w *liveKafka) PeekDLQ(ctx context.Context, topic string, limit int) ([]clients.DLQRecord, error) {
	return call(w.s, func(c clients.KafkaAdmin) ([]clients.DLQRecord, error) {
		d, ok := c.(clients.DLQReader)
		if !ok {
			return nil, ErrDLQUnsupported
		}
		return d.PeekDLQ(ctx, topic, limit)
	})
}

type liveRegistry struct {
	s *swapper[clients.SchemaRegistryClient]
}

var _ clients.SchemaRegistryClient = (*liveRegistry)(nil)

func (w *liveRegistry) Subjects(ctx context.Context) ([]string, error) {
	return call(w.s, func(c clients.SchemaRegistryClient) ([]string, error) { return c.Subjects(ctx) })
}

func (w *liveRegistry) Versions(ctx context.Context, subject string) ([]int, error) {
	return call(w.s, func(c clients.SchemaRegistryClient) ([]int, error) { return c.Versions(ctx, subject) })
}

func (w *liveRegistry) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	return call(w.s, func(c clients.SchemaRegistryClient) (clients.SchemaVersion, error) {
		return c.Schema(ctx, subject, version)
	})
}

// compatResult carries Compatibility's two results through call.
type compatResult struct {
	level        string
	subjectLevel bool
}

func (w *liveRegistry) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	r, err := call(w.s, func(c clients.SchemaRegistryClient) (compatResult, error) {
		level, sub, err := c.Compatibility(ctx, subject)
		return compatResult{level, sub}, err
	})
	return r.level, r.subjectLevel, err
}

func (w *liveRegistry) SetCompatibility(ctx context.Context, subject, level string) error {
	return call0(w.s, func(c clients.SchemaRegistryClient) error { return c.SetCompatibility(ctx, subject, level) })
}

func (w *liveRegistry) Register(ctx context.Context, subject, schema string) (int, error) {
	return call(w.s, func(c clients.SchemaRegistryClient) (int, error) { return c.Register(ctx, subject, schema) })
}

// LiveModel forwards to the current model endpoint. Run holds its reference
// for the whole agent run, so a key change mid-run lets the run finish on the
// old endpoint unless it outlasts the model drain grace
// (DefaultModelDrainGrace, LiveOptions.ModelDrainGrace) or the server shuts
// down; then the old endpoint is closed under the run.
type LiveModel struct{ s *swapper[*agent.Endpoint] }

// Run runs one agent investigation on the current endpoint.
func (m *LiveModel) Run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	return call0(m.s, func(e *agent.Endpoint) error { return e.Run(ctx, alert, deps, opts) })
}

// WriteHandoff writes a hand-off summary on the current endpoint.
func (m *LiveModel) WriteHandoff(ctx context.Context, roomEvents []events.Event, opts agent.Options) (events.HandoffSummary, error) {
	return call(m.s, func(e *agent.Endpoint) (events.HandoffSummary, error) {
		return e.WriteHandoff(ctx, roomEvents, opts)
	})
}

// Forget drops a room's token budget on the current endpoint; a no-op when
// none is configured.
func (m *LiveModel) Forget(roomID string) {
	_ = call0(m.s, func(e *agent.Endpoint) error { e.Forget(roomID); return nil })
}

// ---- environment ----

// envVars lists, per category, the variables that make it env-managed
// (integrations.md §4.4). internal/integrations' env view (EnvFromLookup) is
// the source of truth that the Integrations page shows; TestEnvVarsMatchIntegrations
// fails when the two lists drift apart.
var envVars = map[spec.Category][]string{
	spec.CategorySourceDB:  {"SOURCE_DB_URL"},
	spec.CategoryWarehouse: {"WAREHOUSE_DB_URL", "WAREHOUSE_KIND"},
	spec.CategoryKafka: {
		"KAFKA_BROKERS", "KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM", "KAFKA_SASL_USERNAME",
		"KAFKA_SASL_PASSWORD", "KAFKA_TLS_CA_FILE", "KAFKA_TLS_CERT_FILE", "KAFKA_TLS_KEY_FILE", "KAFKA_TLS_SERVER_NAME",
	},
	spec.CategoryConnect: {
		"CONNECT_URL", "CONNECT_USERNAME", "CONNECT_PASSWORD",
		"CONNECT_TLS_CA_FILE", "CONNECT_TLS_CERT_FILE", "CONNECT_TLS_KEY_FILE",
	},
	spec.CategoryRegistry: {
		"SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_USERNAME", "SCHEMA_REGISTRY_PASSWORD",
		"SCHEMA_REGISTRY_TLS_CA_FILE", "SCHEMA_REGISTRY_TLS_CERT_FILE", "SCHEMA_REGISTRY_TLS_KEY_FILE",
	},
	spec.CategoryModel: {"ANTHROPIC_API_KEY", "PIPEROOM_MODEL_PROVIDER", "VERTEX_PROJECT_ID", "VERTEX_REGION"},
	spec.CategoryChat:  {"SLACK_BOT_TOKEN", "SLACK_CHANNEL_ID"},
}

// EnvCategories tells which categories the environment configures (env
// wins over the Integrations page), reading it through lookup (os.LookupEnv
// in the server). It uses the same rule as integrations.EnvFromLookup: a
// variable counts when it is present and not blank. config.Load's dev
// defaults (KAFKA_BROKERS=localhost:29092, ...) are not in the environment,
// so they never count. The result is in spec.Categories order.
func EnvCategories(lookup func(string) (string, bool)) []spec.Category {
	var out []spec.Category
	for _, cat := range spec.Categories {
		for _, name := range envVars[cat] {
			if v, ok := lookup(name); ok && strings.TrimSpace(v) != "" {
				out = append(out, cat)
				break
			}
		}
	}
	return out
}
