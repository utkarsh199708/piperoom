//go:build integration

package store_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/storetest"
)

// These tests need a Postgres with the piperoom database (the dev stack, or any
// server named by PIPEROOM_DB_URL). Run: go test -race -tags integration ./internal/store/...
// They apply the Up section of migrations/001_init.sql themselves when the
// tables are missing, and only ever touch rooms named it_<random>.

var human = events.Actor{Kind: events.ActorHuman, ID: "u-ada", Name: "Ada"}

// dsn returns the database URL. explicit is true when PIPEROOM_DB_URL was set,
// in which case an unreachable database is a failure and not a skip.
func dsn() (url string, explicit bool) {
	if v := os.Getenv("PIPEROOM_DB_URL"); v != "" {
		return v, true
	}
	return config.Load().RoomDBURL, false
}

// hostPort hides the password: a failing test must never print the DSN.
func hostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "(invalid dsn)"
	}
	return u.Host
}

// admin opens a plain connection for setup, cleanup and fault injection.
func admin(t *testing.T) *pgx.Conn {
	t.Helper()
	d, explicit := dsn()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, d)
	if err != nil {
		if explicit {
			t.Fatalf("postgres unreachable at %s: %v", hostPort(d), err)
		}
		t.Skipf("postgres not reachable at %s (start the dev stack or set PIPEROOM_DB_URL): %v", hostPort(d), err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// upSQL returns the statements of the Up section of the migration.
func upSQL(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	text := string(b)
	const up, down = "-- +goose Up", "-- +goose Down"
	i, j := strings.Index(text, up), strings.Index(text, down)
	if i < 0 || j < i {
		t.Fatal("migration has no goose Up/Down markers")
	}
	return text[i+len(up) : j]
}

// ensureSchema creates the tables when missing. The advisory lock keeps two test
// processes (other packages' integration tests) from racing on CREATE TABLE.
func ensureSchema(t *testing.T) {
	t.Helper()
	conn := admin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(727274001)`); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	var have bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('public.rooms') IS NOT NULL AND to_regclass('public.room_events') IS NOT NULL`).Scan(&have); err != nil {
		t.Fatalf("check schema: %v", err)
	}
	if !have {
		if _, err := tx.Exec(ctx, upSQL(t)); err != nil {
			t.Fatalf("apply migration 001 Up: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit schema: %v", err)
	}
}

// tracked remembers the rooms a test created so they can be deleted afterwards.
type tracked struct {
	store.Store
	mu  sync.Mutex
	ids []string
}

func (s *tracked) CreateRoom(ctx context.Context, id string) error {
	if err := s.Store.CreateRoom(ctx, id); err != nil {
		return err
	}
	s.mu.Lock()
	s.ids = append(s.ids, id)
	s.mu.Unlock()
	return nil
}

func (s *tracked) cleanup(t *testing.T) {
	s.mu.Lock()
	ids := append([]string(nil), s.ids...)
	s.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	conn := admin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `DELETE FROM room_events WHERE room_id = ANY($1)`, ids); err != nil {
		t.Errorf("cleanup events: %v", err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM rooms WHERE id = ANY($1)`, ids); err != nil {
		t.Errorf("cleanup rooms: %v", err)
	}
}

// newStore opens a PG store and a room tracker; both are cleaned up with t.
func newStore(t *testing.T) (*store.PG, *tracked) {
	t.Helper()
	ensureSchema(t)
	d, _ := dsn()
	pg, err := store.NewPG(context.Background(), d, nil)
	if err != nil {
		t.Fatalf("NewPG(%s): %v", hostPort(d), err)
	}
	tr := &tracked{Store: pg}
	t.Cleanup(pg.Close) // runs last (LIFO), after the rooms are deleted
	t.Cleanup(func() { tr.cleanup(t) })
	return pg, tr
}

func newRoom(t *testing.T, s store.Store) string {
	t.Helper()
	id := storetest.NewRoomID()
	if err := s.CreateRoom(context.Background(), id); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	return id
}

func recv(t *testing.T, ch <-chan events.Event, n int, within time.Duration) []events.Event {
	t.Helper()
	out := make([]events.Event, 0, n)
	timer := time.NewTimer(within)
	defer timer.Stop()
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d of %d events", len(out), n)
			}
			out = append(out, ev)
		case <-timer.C:
			t.Fatalf("got %d of %d events within %v", len(out), n, within)
		}
	}
	return out
}

func wantSeqs(t *testing.T, got []events.Event, from int64) {
	t.Helper()
	for i, ev := range got {
		if ev.Seq != from+int64(i) {
			seqs := make([]int64, len(got))
			for j, e := range got {
				seqs[j] = e.Seq
			}
			t.Fatalf("event %d has seq %d, want %d (all: %v)", i, ev.Seq, from+int64(i), seqs)
		}
	}
}

func appendN(t *testing.T, s store.Store, room string, n int) {
	t.Helper()
	for range n {
		if _, err := s.Append(context.Background(), room, events.ControlRequested, human, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func waitClosed(t *testing.T, ch <-chan events.Event, within time.Duration) (received int) {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return received
			}
			received++
		case <-timer.C:
			t.Fatalf("channel still open after %v", within)
		}
	}
}

// TestPGSuite runs the shared behaviour suite (the same one memstore passes)
// against Postgres through ONE store, like the real process: all subtests share
// one pool and one LISTEN connection.
func TestPGSuite(t *testing.T) {
	_, tr := newStore(t)
	storetest.Run(t, func(*testing.T) store.Store { return tr })
}

// TestPGConcurrentAppendsMatchLastSeq has 20 goroutines hammer one room and then
// checks the database itself: no gaps, no duplicates, rooms.last_seq == count.
func TestPGConcurrentAppendsMatchLastSeq(t *testing.T) {
	pg, tr := newStore(t)
	room := newRoom(t, tr)
	const goroutines, each = 20, 25
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				if _, err := pg.Append(context.Background(), room, events.CommentAdded, human, events.CommentAddedPayload{Text: "x"}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Append: %v", err)
	}

	conn := admin(t)
	var count, minSeq, maxSeq, lastSeq int64
	err := conn.QueryRow(context.Background(),
		`SELECT count(*), min(seq), max(seq), (SELECT last_seq FROM rooms WHERE id = $1) FROM room_events WHERE room_id = $1`,
		room).Scan(&count, &minSeq, &maxSeq, &lastSeq)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	const n = goroutines * each
	if count != n || minSeq != 1 || maxSeq != n || lastSeq != n {
		t.Fatalf("count=%d min=%d max=%d last_seq=%d, want %d / 1 / %[1]d / %[1]d", count, minSeq, maxSeq, lastSeq, n)
	}
}

// TestPGNotifyDeliversWithoutPolling turns the safety poll off (1 h), so an event
// can only arrive through LISTEN/NOTIFY.
func TestPGNotifyDeliversWithoutPolling(t *testing.T) {
	pg, tr := newStore(t)
	pg.SetTimings(time.Hour, 5*time.Second)
	room := newRoom(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := pg.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// The hub connects asynchronously; events appended before LISTEN is up are
	// picked up by the wake-up that follows a successful LISTEN.
	for i := 1; i <= 3; i++ {
		start := time.Now()
		appendN(t, tr, room, 1)
		got := recv(t, ch, 1, 3*time.Second)
		wantSeqs(t, got, int64(i))
		t.Logf("event %d delivered in %v", i, time.Since(start).Round(time.Millisecond))
	}
}

// TestPGCrossInstance puts the writer and the subscriber in two PG instances
// (two "processes" sharing one database): only NOTIFY can connect them quickly.
func TestPGCrossInstance(t *testing.T) {
	reader, tr := newStore(t)
	reader.SetTimings(time.Hour, 5*time.Second)
	writer, _ := newStore(t)
	room := newRoom(t, tr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := reader.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for range 5 {
		if _, err := writer.Append(ctx, room, events.ControlRequested, human, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	wantSeqs(t, recv(t, ch, 5, 5*time.Second), 1)
}

// TestPGListenerReconnect kills the LISTEN backend repeatedly. Events appended
// while it is down lose their notifications; the subscriber must still get every
// event exactly once, in order, driven by the wake-up after the reconnect (the
// safety poll is off).
func TestPGListenerReconnect(t *testing.T) {
	pg, tr := newStore(t)
	pg.SetTimings(time.Hour, 5*time.Second)
	room := newRoom(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := pg.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	killer := admin(t)

	next := int64(1)
	step := func(n int) {
		t.Helper()
		appendN(t, tr, room, n)
		wantSeqs(t, recv(t, ch, n, 10*time.Second), next)
		next += int64(n)
	}
	step(5)

	waitPID := func(not uint32) uint32 {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if pid := pg.ListenerPID(); pid != 0 && pid != not {
				return pid
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("listener did not (re)connect; pid still %d", pg.ListenerPID())
		return 0
	}

	pid := waitPID(0)
	for round := 1; round <= 3; round++ {
		var terminated bool
		if err := killer.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int32(pid)).Scan(&terminated); err != nil || !terminated {
			t.Fatalf("round %d: terminate listener backend %d: %v (ok=%v)", round, pid, err, terminated)
		}
		// Append straight away: the listener is gone, so no notify reaches it.
		step(5)
		newPID := waitPID(pid)
		t.Logf("round %d: listener backend %d -> %d, all events delivered exactly once", round, pid, newPID)
		pid = newPID
	}

	// And live delivery works again on the new connection.
	step(1)
	select {
	case ev := <-ch:
		t.Fatalf("duplicate or stray event seq %d", ev.Seq)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestPGListenerKeepaliveKeepsConnection makes the hub's quiet-period check fire
// often and verifies it neither reconnects nor loses notifications.
func TestPGListenerKeepaliveKeepsConnection(t *testing.T) {
	pg, tr := newStore(t)
	pg.SetTimings(time.Hour, 5*time.Second)
	pg.SetListenKeepalive(40 * time.Millisecond)
	room := newRoom(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := pg.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for pg.ListenerPID() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pid := pg.ListenerPID()
	if pid == 0 {
		t.Fatal("listener never connected")
	}
	time.Sleep(400 * time.Millisecond) // ~10 keepalive checks
	if now := pg.ListenerPID(); now != pid {
		t.Fatalf("keepalive replaced the connection: %d -> %d", pid, now)
	}
	appendN(t, tr, room, 2)
	wantSeqs(t, recv(t, ch, 2, 3*time.Second), 1)
}

// TestPGSlowConsumerIsCutAndCanResume checks the 256-event buffer and the
// "full for too long" rule, and that resubscribing with the last seq continues
// without a gap or a repeat.
func TestPGSlowConsumerIsCutAndCanResume(t *testing.T) {
	pg, tr := newStore(t)
	pg.SetTimings(2*time.Second, 300*time.Millisecond)
	room := newRoom(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := pg.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	const total = 300
	appendN(t, tr, room, total)

	time.Sleep(1500 * time.Millisecond) // do not read: the stream must be cut meanwhile
	var got []events.Event
	for ev := range ch {
		got = append(got, ev)
	}
	if len(got) != 256 {
		t.Fatalf("slow consumer received %d events before the cut, want exactly the 256 buffered", len(got))
	}
	wantSeqs(t, got, 1)

	ch2, err := pg.Subscribe(ctx, room, got[len(got)-1].Seq)
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	rest := recv(t, ch2, total-256, 5*time.Second)
	wantSeqs(t, rest, 257)
}

// TestPGCloseStopsEverything: Close returns, every subscription channel closes,
// further use fails cleanly, and a second Close is harmless.
func TestPGCloseStopsEverything(t *testing.T) {
	ensureSchema(t)
	d, _ := dsn()
	pg, err := store.NewPG(context.Background(), d, nil)
	if err != nil {
		t.Fatalf("NewPG: %v", err)
	}
	tr := &tracked{Store: pg}
	t.Cleanup(func() { tr.cleanup(t) })
	room := newRoom(t, tr)
	appendN(t, tr, room, 2)

	var chans []<-chan events.Event
	for range 3 {
		ch, err := pg.Subscribe(context.Background(), room, 0)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		chans = append(chans, ch)
		recv(t, ch, 2, 5*time.Second)
	}

	done := make(chan struct{})
	go func() { defer close(done); pg.Close() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return: a goroutine is stuck")
	}
	for _, ch := range chans {
		waitClosed(t, ch, 2*time.Second)
	}
	if _, err := pg.Subscribe(context.Background(), room, 0); err == nil {
		t.Fatal("Subscribe after Close succeeded")
	}
	if _, err := pg.Append(context.Background(), room, events.ControlRequested, human, nil); err == nil {
		t.Fatal("Append after Close succeeded")
	}
	pg.Close() // must not panic or hang
}

// TestPGLifetimeContextEndStopsSubscriptions: the ctx given to NewPG is the
// store's lifetime.
func TestPGLifetimeContextEndStopsSubscriptions(t *testing.T) {
	ensureSchema(t)
	d, _ := dsn()
	life, endLife := context.WithCancel(context.Background())
	defer endLife()
	pg, err := store.NewPG(life, d, nil)
	if err != nil {
		t.Fatalf("NewPG: %v", err)
	}
	defer pg.Close()
	tr := &tracked{Store: pg}
	t.Cleanup(func() { tr.cleanup(t) })
	room := newRoom(t, tr)

	ch, err := pg.Subscribe(context.Background(), room, 0) // caller ctx never ends
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	endLife()
	waitClosed(t, ch, 5*time.Second)
}

// TestPGCancelledContextDoesNotHangOrBurnSeq: a call with a dead context fails
// fast and the next append still gets the next seq.
func TestPGCancelledContextDoesNotHangOrBurnSeq(t *testing.T) {
	pg, tr := newStore(t)
	room := newRoom(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := pg.Append(ctx, room, events.ControlRequested, human, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Append with cancelled ctx: %v, want context.Canceled", err)
	}
	if _, err := pg.Since(ctx, room, 0, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("Since with cancelled ctx: %v, want context.Canceled", err)
	}
	if _, err := pg.Subscribe(ctx, room, 0); err == nil {
		t.Fatal("Subscribe with cancelled ctx succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("cancelled calls took %v", d)
	}
	ev, err := pg.Append(context.Background(), room, events.ControlRequested, human, nil)
	if err != nil || ev.Seq != 1 {
		t.Fatalf("append after cancelled ones: seq %d, %v; want seq 1", ev.Seq, err)
	}
}

// TestPGNewPGErrorsDoNotLeakPassword: both a bad DSN and an unreachable server
// must fail without echoing the secret.
func TestPGNewPGErrorsDoNotLeakPassword(t *testing.T) {
	const secret = "s3cret-pass-do-not-print"
	for name, dsn := range map[string]string{
		"unparsable":  "postgres://piperoom:" + secret + "@localhost:notaport/piperoom",
		"unreachable": "postgres://piperoom:" + secret + "@127.0.0.1:1/piperoom?sslmode=disable&connect_timeout=2",
	} {
		t.Run(name, func(t *testing.T) {
			pg, err := store.NewPG(context.Background(), dsn, nil)
			if err == nil {
				pg.Close()
				t.Fatal("NewPG succeeded against a bad DSN")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaks the password: %v", err)
			}
		})
	}
}

// TestMigrationDownThenUp proves the Down section really reverses Up, in a
// scratch schema so the shared tables are untouched.
func TestMigrationDownThenUp(t *testing.T) {
	conn := admin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	text := upSQL(t)
	b, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	down := string(b[strings.Index(string(b), "-- +goose Down")+len("-- +goose Down"):])

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }() // nothing here is ever committed
	schema := "it_mig_" + strings.TrimPrefix(storetest.NewRoomID(), "it_")
	for _, q := range []string{`CREATE SCHEMA ` + pgx.Identifier{schema}.Sanitize(), `SET LOCAL search_path TO ` + pgx.Identifier{schema}.Sanitize()} {
		if _, err := tx.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := tx.Exec(ctx, text); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if _, err := tx.Exec(ctx, down); err != nil {
		t.Fatalf("Down: %v", err)
	}
	var left int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1`, schema).Scan(&left); err != nil || left != 0 {
		t.Fatalf("tables left after Down: %d, %v", left, err)
	}
	if _, err := tx.Exec(ctx, text); err != nil {
		t.Fatalf("Up again after Down: %v", err)
	}
}
