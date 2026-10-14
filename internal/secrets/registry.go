package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// DefaultTTL is how long a static value stays cached when Options.TTL is 0
// (config.DefaultSecretCacheTTL).
const DefaultTTL = 5 * time.Minute

// DefaultResolveTimeout bounds one fetch from a provider (§4.1).
const DefaultResolveTimeout = 5 * time.Second

// Options configure a Registry.
type Options struct {
	// TTL is how long a static value stays in memory. 0 = DefaultTTL.
	// A value with a lease stays until the lease is lost instead (renewal
	// keeps it alive), because the client built from it still uses it.
	TTL time.Duration
	// ResolveTimeout bounds one provider fetch. 0 = DefaultResolveTimeout.
	ResolveTimeout time.Duration
	// OnChange, when set, is called after a cached value was dropped because
	// it changed or its lease was lost: a mounted file changed, a lease could
	// not be renewed. Each call runs on its own new goroutine with no lock
	// held, so a handler may Close the registry or Unregister the provider;
	// calls are not ordered.
	// ref has no Key. Consumers rebuild the clients that use it.
	OnChange func(workspaceID string, ref Ref)
	// Logger receives provider ids, reference paths and errors, never values.
	// nil = slog.Default().
	Logger *slog.Logger
}

// Registry holds the secret providers of every workspace and resolves
// references through an in-memory cache. It implements spec.Resolver.
// Create it with NewRegistry and call Close when done.
type Registry struct {
	ttl      time.Duration
	timeout  time.Duration
	onChange func(string, Ref)
	log      *slog.Logger
	now      func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	closed    bool
	gen       uint64
	providers map[provKey]provEntry
	cache     map[cacheKey]*entry
	inflight  map[flightKey]*call
}

var _ spec.Resolver = (*Registry)(nil)

type provKey struct{ ws, name string }

type provEntry struct {
	p   Provider
	gen uint64
}

type cacheKey struct{ ws, provider, path, version string }

type flightKey struct {
	cacheKey
	fresh bool
}

type entry struct {
	v         Value
	fetchedAt time.Time
	expires   time.Time // zero = until the lease is lost
	renewer   *Renewer
	gen       uint64
}

type call struct {
	done chan struct{}
	v    Value
	err  error
	prev *Value // the value cached before this fetch replaced it
}

func (c *call) prevFor(fresh bool) *Value {
	if !fresh {
		return nil
	}
	return c.prev
}

// NewRegistry returns an empty Registry.
func NewRegistry(opts Options) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{
		ttl: opts.TTL, timeout: opts.ResolveTimeout, onChange: opts.OnChange, log: opts.Logger,
		now: time.Now, ctx: ctx, cancel: cancel,
		providers: map[provKey]provEntry{}, cache: map[cacheKey]*entry{}, inflight: map[flightKey]*call{},
	}
	if r.ttl <= 0 {
		r.ttl = DefaultTTL
	}
	if r.timeout <= 0 {
		r.timeout = DefaultResolveTimeout
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	return r
}

// GoString keeps %#v from walking the cache.
func (r *Registry) GoString() string { return r.String() }

// String describes the registry without any value.
func (r *Registry) String() string { return "secrets.Registry" }

// Register adds provider p as id name in workspace ws. It fails when the
// name is invalid or taken; spec.ProviderFiles is accepted only for a
// *FilesProvider. Use Replace to swap a provider after an edit.
func (r *Registry) Register(ws, name string, p Provider) error {
	return r.put(ws, name, p, false)
}

// Replace registers p as name in ws, dropping the old provider's cached
// values and closing it. Clients built from old values keep working.
func (r *Registry) Replace(ws, name string, p Provider) error {
	return r.put(ws, name, p, true)
}

func (r *Registry) put(ws, name string, p Provider, replace bool) error {
	if ws == "" || !ValidProviderID(name) || p == nil {
		return fmt.Errorf("%w: workspace, a valid provider id and a provider are required", ErrBadProviderConfig)
	}
	if _, isFiles := p.(*FilesProvider); (name == spec.ProviderFiles) != isFiles {
		return fmt.Errorf("%w: the id %q is reserved for the mounted-files provider", ErrBadProviderConfig, spec.ProviderFiles)
	}
	k := provKey{ws, name}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	old, exists := r.providers[k]
	if exists && !replace {
		r.mu.Unlock()
		return fmt.Errorf("%w: provider %q already registered in workspace %q", ErrBadProviderConfig, name, ws)
	}
	r.gen++
	gen := r.gen
	r.providers[k] = provEntry{p: p, gen: gen}
	dropped := r.dropLocked(func(ck cacheKey) bool { return ck.ws == ws && ck.provider == name })
	r.mu.Unlock()

	stopAll(dropped)
	if exists && old.p != p {
		old.p.Close()
	}
	if n, ok := p.(ChangeNotifier); ok {
		n.SetOnChange(func(path string) { r.changed(ws, name, path, gen) })
	}
	return nil
}

// Unregister removes provider name from ws, drops its cached values and
// closes it. Unknown names are ignored.
func (r *Registry) Unregister(ws, name string) {
	k := provKey{ws, name}
	r.mu.Lock()
	old, ok := r.providers[k]
	delete(r.providers, k)
	r.gen++
	dropped := r.dropLocked(func(ck cacheKey) bool { return ck.ws == ws && ck.provider == name })
	r.mu.Unlock()
	stopAll(dropped)
	if ok {
		old.p.Close()
	}
}

// Provider returns the provider named name in workspace ws, or
// ErrUnknownProvider (also for a provider of another workspace).
func (r *Registry) Provider(ws, name string) (Provider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	pe, ok := r.providers[provKey{ws, name}]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, name)
	}
	return pe.p, nil
}

// Resolve implements spec.Resolver: it parses ref, finds its provider in
// workspaceID only, and returns the value (one key of it when the reference
// has #key), from the cache when fresh enough.
func (r *Registry) Resolve(ctx context.Context, workspaceID, ref string) (netsec.Secret, error) {
	v, err := r.ResolveValue(ctx, workspaceID, ref)
	return v.Data, err
}

// ResolveValue is Resolve with the version and lease details, for the
// health page. Data holds the key's value when the reference has #key.
func (r *Registry) ResolveValue(ctx context.Context, workspaceID, ref string) (Value, error) {
	parsed, err := ParseRef(ref)
	if err != nil {
		return Value{}, err
	}
	v, _, err := r.get(ctx, workspaceID, parsed, false)
	if err != nil {
		return Value{}, err
	}
	return withKey(v, parsed)
}

// ResolveFresh reads ref again, bypassing the cache, and stores the result.
// changed reports whether the value differs from the one cached before (true
// when nothing was cached). It is the auth-failure path (§4.2): call it once
// when a target rejects a login, and rebuild the client only when changed.
// On error the cached value stays.
func (r *Registry) ResolveFresh(ctx context.Context, workspaceID, ref string) (value netsec.Secret, changed bool, err error) {
	parsed, err := ParseRef(ref)
	if err != nil {
		return "", false, err
	}
	v, prev, err := r.get(ctx, workspaceID, parsed, true)
	if err != nil {
		return "", false, err
	}
	nv, err := withKey(v, parsed)
	if err != nil {
		return "", false, err
	}
	if prev == nil {
		return nv.Data, true, nil
	}
	old, err := withKey(*prev, parsed)
	return nv.Data, err != nil || old.Data != nv.Data, nil
}

// RetryAfterAuthFailure runs call. If it fails with an error isAuth accepts
// (compare with errors.As, never by text), it re-reads refs once, bypassing
// the cache. If any value changed it runs rebuild and then call once more;
// otherwise it returns call's original error.
func (r *Registry) RetryAfterAuthFailure(ctx context.Context, workspaceID string, refs []string,
	isAuth func(error) bool, rebuild func(context.Context) error, call func(context.Context) error) error {
	err := call(ctx)
	if err == nil || !isAuth(err) {
		return err
	}
	anyChanged := false
	for _, ref := range refs {
		_, changed, ferr := r.ResolveFresh(ctx, workspaceID, ref)
		if ferr != nil {
			return errors.Join(err, fmt.Errorf("secrets: re-reading %s after a login failure: %w", ref, ferr))
		}
		anyChanged = anyChanged || changed
	}
	if !anyChanged {
		return err
	}
	if rerr := rebuild(ctx); rerr != nil {
		return fmt.Errorf("secrets: rebuilding after a rotated secret: %w", rerr)
	}
	return call(ctx)
}

// EntryInfo describes one cached secret for the health page. No value.
type EntryInfo struct {
	Ref       string    `json:"ref"` // without #key
	Version   string    `json:"version"`
	FetchedAt time.Time `json:"fetched_at"`
	ExpiresAt time.Time `json:"expires_at"` // lease expiry; zero = static
}

// Entries lists the cached secrets of workspace ws, sorted by ref.
func (r *Registry) Entries(ws string) []EntryInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []EntryInfo{}
	for k, e := range r.cache {
		if k.ws != ws {
			continue
		}
		ref := Ref{Provider: k.provider, Path: k.path, Version: k.version}
		out = append(out, EntryInfo{Ref: ref.String(), Version: e.v.Version, FetchedAt: e.fetchedAt, ExpiresAt: e.v.ExpiresAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out
}

// Close stops every renewal, drops every value and closes every provider.
func (r *Registry) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	dropped := r.dropLocked(func(cacheKey) bool { return true })
	provs := make([]Provider, 0, len(r.providers))
	for _, pe := range r.providers {
		provs = append(provs, pe.p)
	}
	r.providers = map[provKey]provEntry{}
	r.mu.Unlock()
	stopAll(dropped)
	for _, p := range provs {
		p.Close()
	}
}

func withKey(v Value, ref Ref) (Value, error) {
	data, err := ExtractKey(v.Data, ref.Key)
	if err != nil {
		return Value{}, fmt.Errorf("secrets: %s: %w", ref.String(), err)
	}
	v.Data = data
	return v, nil
}

// get returns the whole secret of ref (no key) and, for a fresh read, the
// value cached before it.
func (r *Registry) get(ctx context.Context, ws string, ref Ref, fresh bool) (Value, *Value, error) {
	id := ref.secretID()
	ck := cacheKey{ws, id.Provider, id.Path, id.Version}
	fk := flightKey{ck, fresh}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return Value{}, nil, ErrClosed
	}
	pe, ok := r.providers[provKey{ws, id.Provider}]
	if !ok {
		r.mu.Unlock()
		return Value{}, nil, fmt.Errorf("%w: %q", ErrUnknownProvider, id.Provider)
	}
	if !fresh {
		if e, ok := r.cache[ck]; ok && (e.expires.IsZero() || r.now().Before(e.expires)) {
			v := e.v
			r.mu.Unlock()
			return v, nil, nil
		}
	}
	c, running := r.inflight[fk]
	if !running {
		c = &call{done: make(chan struct{})}
		r.inflight[fk] = c
		go r.fetch(pe, ck, fk, id, c)
	}
	r.mu.Unlock()

	select {
	case <-c.done:
	case <-ctx.Done():
		return Value{}, nil, fmt.Errorf("secrets: resolving %s: %w", id.String(), ctx.Err())
	}
	if c.err != nil {
		return Value{}, nil, c.err
	}
	return c.v, c.prevFor(fresh), nil
}

// fetch runs one provider read on its own goroutine, bounded by the resolve
// timeout and the registry's lifetime, so a caller that gives up does not
// cancel the read for the others waiting on it.
func (r *Registry) fetch(pe provEntry, ck cacheKey, fk flightKey, id Ref, c *call) {
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	v, err := pe.p.Resolve(ctx, id)
	cancel()
	if err != nil {
		err = fmt.Errorf("secrets: resolving %s: %w", id.String(), err)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(err, ErrUnreachable) {
			err = fmt.Errorf("%w: %w", ErrUnreachable, err)
		}
	}

	var stop []*Renewer
	r.mu.Lock()
	delete(r.inflight, fk)
	cur, still := r.providers[provKey{ck.ws, ck.provider}]
	if err == nil && still && cur.gen == pe.gen && !r.closed {
		if old, ok := r.cache[ck]; ok {
			// Copy under the lock: the old entry's renewer may still write
			// old.v.ExpiresAt until it is stopped below.
			pv := old.v
			c.prev = &pv
			if old.renewer != nil {
				stop = append(stop, old.renewer)
			}
		}
		e := &entry{v: v, fetchedAt: r.now(), gen: pe.gen}
		if v.ExpiresAt.IsZero() {
			e.expires = e.fetchedAt.Add(r.ttl)
		} else {
			e.renewer = r.leaseRenewer(pe.p, ck, e)
		}
		r.cache[ck] = e
		if e.renewer != nil {
			e.renewer.Start(r.ctx, v.ExpiresAt)
		}
	} else if err == nil && !v.ExpiresAt.IsZero() {
		// The provider was replaced or removed during the read. A leased
		// value would go out with no renewer and no OnChange, and the
		// caller's client would die at lease expiry; the caller retries on
		// the new provider instead.
		v = Value{}
		err = fmt.Errorf("secrets: resolving %s: %w: the secret manager changed during the read", id.String(), ErrUnreachable)
	}
	c.v, c.err = v, err
	r.mu.Unlock()
	close(c.done)
	stopAll(stop)
	if err != nil {
		r.log.Warn("secrets: resolve failed", "provider", id.Provider, "path", id.Path, "err", err)
	}
}

// leaseRenewer keeps e's lease alive. A lease that cannot be renewed (not
// renewable, or the provider has no RenewLease) is reported lost at two
// thirds of its TTL, so clients get new credentials before it ends.
func (r *Registry) leaseRenewer(p Provider, ck cacheKey, e *entry) *Renewer {
	renew := func(ctx context.Context) (time.Time, error) {
		r.mu.Lock()
		v := e.v
		r.mu.Unlock()
		lr, ok := p.(LeaseRenewer)
		if !ok || !v.Renewable {
			return time.Time{}, errors.New("secrets: lease is not renewable")
		}
		ctx, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()
		exp, err := lr.RenewLease(ctx, v)
		if err == nil {
			r.mu.Lock()
			e.v.ExpiresAt = exp
			r.mu.Unlock()
		}
		return exp, err
	}
	lost := func(err error) {
		r.mu.Lock()
		cur, ok := r.cache[ck]
		mine := ok && cur == e
		if mine {
			delete(r.cache, ck)
		}
		r.mu.Unlock()
		if !mine {
			return
		}
		ref := Ref{Provider: ck.provider, Path: ck.path, Version: ck.version}
		r.log.Info("secrets: lease lost; clients will get new credentials", "ref", ref.String(), "reason", err)
		r.notify(ck.ws, ref)
	}
	return NewRenewer(renew, lost)
}

// changed handles a ChangeNotifier callback.
func (r *Registry) changed(ws, name, path string, gen uint64) {
	r.mu.Lock()
	cur, ok := r.providers[provKey{ws, name}]
	if !ok || cur.gen != gen {
		r.mu.Unlock()
		return
	}
	dropped := r.dropLocked(func(ck cacheKey) bool { return ck.ws == ws && ck.provider == name && ck.path == path })
	r.mu.Unlock()
	stopAll(dropped)
	r.log.Info("secrets: secret changed", "ref", Ref{Provider: name, Path: path}.String())
	r.notify(ws, Ref{Provider: name, Path: path})
}

// notify runs OnChange on its own goroutine. It is called from a files
// provider's poll goroutine or a lease renewer; a handler that closes the
// registry or unregisters that provider would otherwise wait on itself.
func (r *Registry) notify(ws string, ref Ref) {
	if r.onChange != nil {
		go r.onChange(ws, ref)
	}
}

// dropLocked removes matching cache entries and returns their renewers,
// which the caller stops after unlocking (a renewer's callback takes mu).
func (r *Registry) dropLocked(match func(cacheKey) bool) []*Renewer {
	var out []*Renewer
	for k, e := range r.cache {
		if match(k) {
			if e.renewer != nil {
				out = append(out, e.renewer)
			}
			delete(r.cache, k)
		}
	}
	return out
}

func stopAll(rs []*Renewer) {
	for _, rn := range rs {
		rn.Close()
	}
}
