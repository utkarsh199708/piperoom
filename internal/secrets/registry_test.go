package secrets

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// fakeProvider serves values from a map and counts reads.
type fakeProvider struct {
	mu       sync.Mutex
	values   map[string]Value // by path
	err      error
	delay    time.Duration
	reads    atomic.Int64
	renews   atomic.Int64
	renewErr error
	renewTTL time.Duration
	closed   atomic.Bool
	onChange func(string)
}

func newFake(vals map[string]string) *fakeProvider {
	f := &fakeProvider{values: map[string]Value{}}
	for k, v := range vals {
		f.values[k] = Value{Data: netsec.Secret(v), Version: "1"}
	}
	return f
}

func (f *fakeProvider) set(path string, v Value) {
	f.mu.Lock()
	f.values[path] = v
	f.mu.Unlock()
}

func (f *fakeProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	f.reads.Add(1)
	if ref.Key != "" {
		return Value{}, errors.New("fake: provider got a key")
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return Value{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return Value{}, f.err
	}
	v, ok := f.values[ref.Path]
	if !ok {
		return Value{}, ErrNotFound
	}
	return v, nil
}

func (f *fakeProvider) RenewLease(ctx context.Context, v Value) (time.Time, error) {
	f.renews.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.renewErr != nil {
		return time.Time{}, f.renewErr
	}
	return time.Now().Add(f.renewTTL), nil
}

func (f *fakeProvider) SetOnChange(fn func(string)) {
	f.mu.Lock()
	f.onChange = fn
	f.mu.Unlock()
}

func (f *fakeProvider) fireChange(path string) {
	f.mu.Lock()
	fn := f.onChange
	f.mu.Unlock()
	fn(path)
}

func (f *fakeProvider) List(context.Context, string, int) ([]string, error) {
	return nil, ErrListUnsupported
}
func (f *fakeProvider) Keys(context.Context, Ref) ([]string, error) { return []string{}, nil }
func (f *fakeProvider) Check(context.Context, *Ref) []spec.Check    { return nil }
func (f *fakeProvider) Close()                                      { f.closed.Store(true) }

func newTestRegistry(t *testing.T, opts Options) *Registry {
	t.Helper()
	r := NewRegistry(opts)
	t.Cleanup(r.Close)
	return r
}

func mustRegister(t *testing.T, r *Registry, ws, name string, p Provider) {
	t.Helper()
	if err := r.Register(ws, name, p); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryResolveKeysAndCache(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"db/creds": `{"username":"u","password":"p"}`})
	mustRegister(t, r, "ws1", "vault", f)

	u, err := r.Resolve(ctx, "ws1", "secret://vault/db/creds#username")
	if err != nil || u.Reveal() != "u" {
		t.Fatalf("username = %q, %v", u.Reveal(), err)
	}
	p, err := r.Resolve(ctx, "ws1", "secret://vault/db/creds#password")
	if err != nil || p.Reveal() != "p" {
		t.Fatalf("password = %q, %v", p.Reveal(), err)
	}
	// One read serves both keys (a Vault lease's pair must match).
	if n := f.reads.Load(); n != 1 {
		t.Fatalf("reads = %d, want 1", n)
	}
	if _, err := r.Resolve(ctx, "ws1", "secret://vault/db/creds#nope"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing key err = %v", err)
	}
	if _, err := r.Resolve(ctx, "ws1", "secret://vault/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}
	if _, err := r.Resolve(ctx, "ws1", "not a ref"); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("bad ref err = %v", err)
	}
	if got := r.Entries("ws1"); len(got) != 1 || got[0].Ref != "secret://vault/db/creds" || got[0].Version != "1" {
		t.Fatalf("entries = %+v", got)
	}
}

func TestRegistryTTL(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{TTL: time.Minute})
	now := time.Now()
	r.now = func() time.Time { return now }
	f := newFake(map[string]string{"x": "v1"})
	mustRegister(t, r, "ws", "p", f)
	for i := 0; i < 3; i++ {
		if _, err := r.Resolve(ctx, "ws", "secret://p/x"); err != nil {
			t.Fatal(err)
		}
	}
	if f.reads.Load() != 1 {
		t.Fatalf("reads = %d", f.reads.Load())
	}
	f.set("x", Value{Data: "v2"})
	now = now.Add(61 * time.Second)
	v, err := r.Resolve(ctx, "ws", "secret://p/x")
	if err != nil || v.Reveal() != "v2" || f.reads.Load() != 2 {
		t.Fatalf("after TTL: %q %v reads=%d", v.Reveal(), err, f.reads.Load())
	}
}

func TestRegistryErrorsAreNotCached(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"x": "v"})
	f.err = errors.New("boom")
	mustRegister(t, r, "ws", "p", f)
	if _, err := r.Resolve(ctx, "ws", "secret://p/x"); err == nil {
		t.Fatal("want error")
	}
	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()
	if v, err := r.Resolve(ctx, "ws", "secret://p/x"); err != nil || v.Reveal() != "v" {
		t.Fatalf("%q %v", v.Reveal(), err)
	}
}

func TestRegistrySingleFlight(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"x": "v"})
	f.delay = 50 * time.Millisecond
	mustRegister(t, r, "ws", "p", f)
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := r.Resolve(ctx, "ws", "secret://p/x")
			if err == nil && v.Reveal() != "v" {
				err = errors.New("wrong value")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := f.reads.Load(); n != 1 {
		t.Fatalf("reads = %d, want 1", n)
	}
}

func TestRegistryCallerCancelDoesNotCancelSharedFetch(t *testing.T) {
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"x": "v"})
	f.delay = 50 * time.Millisecond
	mustRegister(t, r, "ws", "p", f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Resolve(ctx, "ws", "secret://p/x")
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	v, err := r.Resolve(context.Background(), "ws", "secret://p/x")
	if err != nil || v.Reveal() != "v" || f.reads.Load() != 1 {
		t.Fatalf("%q %v reads=%d", v.Reveal(), err, f.reads.Load())
	}
}

func TestRegistryTimeoutIsUnreachable(t *testing.T) {
	r := newTestRegistry(t, Options{ResolveTimeout: 20 * time.Millisecond})
	f := newFake(map[string]string{"x": "v"})
	f.delay = time.Second
	mustRegister(t, r, "ws", "p", f)
	if _, err := r.Resolve(context.Background(), "ws", "secret://p/x"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
}

func TestRegistryRefusesOtherWorkspace(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"x": "tenant-b-secret"})
	mustRegister(t, r, "tenant-b", "vault", f)
	_, err := r.Resolve(ctx, "tenant-a", "secret://vault/x")
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
	if f.reads.Load() != 0 {
		t.Fatal("provider of another workspace was called")
	}
	if _, err := r.Provider("tenant-a", "vault"); !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("Provider err = %v", err)
	}
	// Same name in two workspaces: each sees its own.
	mustRegister(t, r, "tenant-a", "vault", newFake(map[string]string{"x": "tenant-a-secret"}))
	if v, _ := r.Resolve(ctx, "tenant-a", "secret://vault/x"); v.Reveal() != "tenant-a-secret" {
		t.Fatalf("tenant-a got %q", v.Reveal())
	}
	if v, _ := r.Resolve(ctx, "tenant-b", "secret://vault/x"); v.Reveal() != "tenant-b-secret" {
		t.Fatalf("tenant-b got %q", v.Reveal())
	}
	if len(r.Entries("tenant-a")) != 1 || len(r.Entries("tenant-b")) != 1 {
		t.Fatal("entries leak across workspaces")
	}
}

func TestRegistryRegisterRules(t *testing.T) {
	r := newTestRegistry(t, Options{})
	f := newFake(nil)
	if err := r.Register("ws", "Bad Name", f); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("bad name err = %v", err)
	}
	if err := r.Register("", "p", f); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("no ws err = %v", err)
	}
	if err := r.Register("ws", spec.ProviderFiles, f); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("files name err = %v", err)
	}
	mustRegister(t, r, "ws", "p", f)
	if err := r.Register("ws", "p", newFake(nil)); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("duplicate err = %v", err)
	}
}

func TestRegistryReplaceAndUnregister(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	old := newFake(map[string]string{"x": "old"})
	mustRegister(t, r, "ws", "p", old)
	if v, _ := r.Resolve(ctx, "ws", "secret://p/x"); v.Reveal() != "old" {
		t.Fatal("old")
	}
	nw := newFake(map[string]string{"x": "new"})
	if err := r.Replace("ws", "p", nw); err != nil {
		t.Fatal(err)
	}
	if !old.closed.Load() {
		t.Fatal("old provider not closed")
	}
	if v, _ := r.Resolve(ctx, "ws", "secret://p/x"); v.Reveal() != "new" {
		t.Fatalf("after replace got %q", v.Reveal())
	}
	r.Unregister("ws", "p")
	if !nw.closed.Load() {
		t.Fatal("unregistered provider not closed")
	}
	if _, err := r.Resolve(ctx, "ws", "secret://p/x"); !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveFresh(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{})
	f := newFake(map[string]string{"db": `{"password":"p1","user":"u"}`})
	mustRegister(t, r, "ws", "p", f)
	ref := "secret://p/db#password"
	if _, err := r.Resolve(ctx, "ws", ref); err != nil {
		t.Fatal(err)
	}
	v, changed, err := r.ResolveFresh(ctx, "ws", ref)
	if err != nil || changed || v.Reveal() != "p1" || f.reads.Load() != 2 {
		t.Fatalf("unchanged: %q changed=%v %v reads=%d", v.Reveal(), changed, err, f.reads.Load())
	}
	f.set("db", Value{Data: `{"password":"p2","user":"u"}`})
	v, changed, err = r.ResolveFresh(ctx, "ws", ref)
	if err != nil || !changed || v.Reveal() != "p2" {
		t.Fatalf("rotated: %q changed=%v %v", v.Reveal(), changed, err)
	}
	// The fresh value is now cached.
	if got, _ := r.Resolve(ctx, "ws", ref); got.Reveal() != "p2" || f.reads.Load() != 3 {
		t.Fatalf("cached %q reads=%d", got.Reveal(), f.reads.Load())
	}
	// A key that did not change reports unchanged even though the secret did.
	if _, changed, _ := r.ResolveFresh(ctx, "ws", "secret://p/db#user"); changed {
		t.Fatal("user key reported changed")
	}
	// A failed fresh read keeps the cached value (no fallback is stored, but
	// the client's current value is not thrown away).
	f.mu.Lock()
	f.err = ErrUnreachable
	f.mu.Unlock()
	if _, _, err := r.ResolveFresh(ctx, "ws", ref); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("err = %v", err)
	}
	if got, err := r.Resolve(ctx, "ws", ref); err != nil || got.Reveal() != "p2" {
		t.Fatalf("cached after failed fresh: %q %v", got.Reveal(), err)
	}
}

var errAuth = errors.New("auth failed")

func TestRetryAfterAuthFailure(t *testing.T) {
	ctx := context.Background()
	isAuth := func(err error) bool { return errors.Is(err, errAuth) }
	ref := "secret://p/db"

	t.Run("rotated: rebuild and retry once", func(t *testing.T) {
		r := newTestRegistry(t, Options{})
		f := newFake(map[string]string{"db": "old"})
		mustRegister(t, r, "ws", "p", f)
		current, _ := r.Resolve(ctx, "ws", ref)
		f.set("db", Value{Data: "new"})
		calls, rebuilds := 0, 0
		err := r.RetryAfterAuthFailure(ctx, "ws", []string{ref}, isAuth,
			func(ctx context.Context) error {
				rebuilds++
				current, _ = r.Resolve(ctx, "ws", ref)
				return nil
			},
			func(context.Context) error {
				calls++
				if current.Reveal() != "new" {
					return errAuth
				}
				return nil
			})
		if err != nil || calls != 2 || rebuilds != 1 {
			t.Fatalf("err=%v calls=%d rebuilds=%d", err, calls, rebuilds)
		}
	})
	t.Run("unchanged: original error", func(t *testing.T) {
		r := newTestRegistry(t, Options{})
		mustRegister(t, r, "ws", "p", newFake(map[string]string{"db": "same"}))
		_, _ = r.Resolve(ctx, "ws", ref)
		calls, rebuilds := 0, 0
		err := r.RetryAfterAuthFailure(ctx, "ws", []string{ref}, isAuth,
			func(context.Context) error { rebuilds++; return nil },
			func(context.Context) error { calls++; return errAuth })
		if !errors.Is(err, errAuth) || calls != 1 || rebuilds != 0 {
			t.Fatalf("err=%v calls=%d rebuilds=%d", err, calls, rebuilds)
		}
	})
	t.Run("other error: no re-read", func(t *testing.T) {
		r := newTestRegistry(t, Options{})
		f := newFake(map[string]string{"db": "x"})
		mustRegister(t, r, "ws", "p", f)
		other := errors.New("other")
		err := r.RetryAfterAuthFailure(ctx, "ws", []string{ref}, isAuth,
			func(context.Context) error { return nil },
			func(context.Context) error { return other })
		if !errors.Is(err, other) || f.reads.Load() != 0 {
			t.Fatalf("err=%v reads=%d", err, f.reads.Load())
		}
	})
	t.Run("vault down: both errors", func(t *testing.T) {
		r := newTestRegistry(t, Options{})
		f := newFake(map[string]string{"db": "x"})
		f.err = ErrUnreachable
		mustRegister(t, r, "ws", "p", f)
		err := r.RetryAfterAuthFailure(ctx, "ws", []string{ref}, isAuth,
			func(context.Context) error { return nil },
			func(context.Context) error { return errAuth })
		if !errors.Is(err, errAuth) || !errors.Is(err, ErrUnreachable) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestRegistryLeaseRenewal(t *testing.T) {
	ctx := context.Background()
	r := newTestRegistry(t, Options{TTL: time.Millisecond})
	f := newFake(nil)
	f.renewTTL = 90 * time.Millisecond
	f.set("database/creds/ro", Value{Data: `{"username":"u","password":"p"}`, LeaseID: "l1", Renewable: true,
		ExpiresAt: time.Now().Add(90 * time.Millisecond)})
	mustRegister(t, r, "ws", "v", f)
	if _, err := r.Resolve(ctx, "ws", "secret://v/database/creds/ro#password"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if n := f.renews.Load(); n < 2 {
		t.Fatalf("renews = %d, want >= 2", n)
	}
	// A leased value is not dropped by the (tiny) TTL: the client still uses it.
	if _, err := r.Resolve(ctx, "ws", "secret://v/database/creds/ro#username"); err != nil || f.reads.Load() != 1 {
		t.Fatalf("err=%v reads=%d", err, f.reads.Load())
	}
	r.Close()
	n := f.renews.Load()
	time.Sleep(150 * time.Millisecond)
	if f.renews.Load() != n {
		t.Fatal("renewal continued after Close")
	}
}

func TestRegistryLeaseLostNotifies(t *testing.T) {
	ctx := context.Background()
	changed := make(chan Ref, 1)
	r := newTestRegistry(t, Options{OnChange: func(ws string, ref Ref) {
		if ws == "ws" {
			changed <- ref
		}
	}})
	f := newFake(nil)
	f.renewErr = errors.New("lease not found")
	f.set("database/creds/ro", Value{Data: `{"password":"p"}`, LeaseID: "l1", Renewable: true,
		ExpiresAt: time.Now().Add(60 * time.Millisecond)})
	mustRegister(t, r, "ws", "v", f)
	if _, err := r.Resolve(ctx, "ws", "secret://v/database/creds/ro#password"); err != nil {
		t.Fatal(err)
	}
	select {
	case ref := <-changed:
		if ref.String() != "secret://v/database/creds/ro" {
			t.Fatalf("ref = %s", ref)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OnChange after a failed renewal")
	}
	if len(r.Entries("ws")) != 0 {
		t.Fatal("lost lease still cached")
	}
	if _, err := r.Resolve(ctx, "ws", "secret://v/database/creds/ro#password"); err != nil || f.reads.Load() != 2 {
		t.Fatalf("re-read: %v reads=%d", err, f.reads.Load())
	}
}

func TestRegistryNonRenewableLeaseIsRefreshedBeforeExpiry(t *testing.T) {
	changed := make(chan struct{}, 1)
	r := newTestRegistry(t, Options{OnChange: func(string, Ref) { changed <- struct{}{} }})
	f := newFake(nil)
	f.set("x", Value{Data: "v", LeaseID: "l", Renewable: false, ExpiresAt: time.Now().Add(60 * time.Millisecond)})
	mustRegister(t, r, "ws", "v", f)
	if _, err := r.Resolve(context.Background(), "ws", "secret://v/x"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("no OnChange for an expiring non-renewable lease")
	}
	if f.renews.Load() != 0 {
		t.Fatal("non-renewable lease was renewed")
	}
}

func TestRegistryChangeNotifier(t *testing.T) {
	ctx := context.Background()
	changed := make(chan Ref, 1)
	r := newTestRegistry(t, Options{OnChange: func(_ string, ref Ref) { changed <- ref }})
	f := newFake(map[string]string{"x": "v1"})
	mustRegister(t, r, "ws", "p", f)
	if _, err := r.Resolve(ctx, "ws", "secret://p/x"); err != nil {
		t.Fatal(err)
	}
	f.set("x", Value{Data: "v2"})
	f.fireChange("x")
	if ref := <-changed; ref.Path != "x" {
		t.Fatalf("ref = %+v", ref)
	}
	if v, _ := r.Resolve(ctx, "ws", "secret://p/x"); v.Reveal() != "v2" {
		t.Fatalf("got %q", v.Reveal())
	}
}

func TestRegistryClosed(t *testing.T) {
	r := NewRegistry(Options{})
	f := newFake(map[string]string{"x": "v"})
	mustRegister(t, r, "ws", "p", f)
	r.Close()
	r.Close()
	if !f.closed.Load() {
		t.Fatal("provider not closed")
	}
	if _, err := r.Resolve(context.Background(), "ws", "secret://p/x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v", err)
	}
	if err := r.Register("ws", "q", newFake(nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("register err = %v", err)
	}
}

// A leased value read while its provider is replaced is not handed out
// unmanaged: nobody would renew it.
func TestRegistryLeaseReadDuringReplaceIsRefused(t *testing.T) {
	r := newTestRegistry(t, Options{})
	f := newFake(nil)
	f.delay = 300 * time.Millisecond
	f.set("database/creds/ro", Value{Data: `{"password":"p"}`, LeaseID: "l1", Renewable: true,
		ExpiresAt: time.Now().Add(time.Hour)})
	mustRegister(t, r, "ws", "v", f)
	errc := make(chan error, 1)
	go func() {
		_, err := r.Resolve(context.Background(), "ws", "secret://v/database/creds/ro#password")
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := r.Replace("ws", "v", newFake(nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
	if got := r.Entries("ws"); len(got) != 0 {
		t.Fatalf("stale lease cached: %+v", got)
	}
}
