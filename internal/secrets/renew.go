package secrets

import (
	"context"
	"errors"
	"sync"
	"time"
)

// minRenewDelay keeps a lease that is (nearly) expired from spinning.
const minRenewDelay = 10 * time.Millisecond

// Retry backoff after a failed renewal: one blip (a Vault restart, a network
// hiccup) must not drop credentials that are still valid.
const (
	defaultRetryBase = time.Second
	maxRetryDelay    = 30 * time.Second
)

// Renewer keeps a lease alive: it calls renew at two thirds of the time left
// until each expiry. A failed renewal is retried with backoff while a quarter
// of the time left at the failure remains; then, or when renew returns an
// expiry that is not in the future (the lease hit its max TTL) or
// errLeaseNotRenewable, it calls lost once and stops; the owner then fetches
// new credentials while the old ones still work. It stops when ctx ends or Close is
// called, and Close waits for the goroutine, so it never leaks.
type Renewer struct {
	renew     func(ctx context.Context) (time.Time, error)
	lost      func(err error)
	now       func() time.Time
	retryBase time.Duration

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewRenewer returns a stopped Renewer. renew must honour its context; lost
// may be nil.
func NewRenewer(renew func(ctx context.Context) (expiresAt time.Time, err error), lost func(err error)) *Renewer {
	return &Renewer{renew: renew, lost: lost, now: time.Now, retryBase: defaultRetryBase}
}

// errLeaseEnded is passed to lost when a renewal returns no future expiry.
var errLeaseEnded = leaseEnded{}

type leaseEnded struct{}

func (leaseEnded) Error() string { return "secrets: lease reached its maximum TTL" }

// errLeaseNotRenewable is returned by a renew func for a lease that can never
// be renewed; retrying would only wait for it to expire.
var errLeaseNotRenewable = errors.New("secrets: lease is not renewable")

// finalRenewErr reports whether a renewal error cannot get better by retrying.
func finalRenewErr(err error) bool {
	return errors.Is(err, errLeaseEnded) || errors.Is(err, errLeaseNotRenewable)
}

// Start begins renewing a lease that expires at expiresAt. Calling Start
// twice has no effect.
func (r *Renewer) Start(ctx context.Context, expiresAt time.Time) {
	r.once.Do(func() {
		ctx, r.cancel = context.WithCancel(ctx)
		r.done = make(chan struct{})
		go r.loop(ctx, expiresAt)
	})
}

// Close stops renewing and waits for the renewal loop to end. It is safe to
// call before Start, more than once, and from inside lost (which may still
// be running when Close returns).
func (r *Renewer) Close() {
	r.once.Do(func() {}) // a later Start becomes a no-op
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
}

func (r *Renewer) loop(ctx context.Context, expiresAt time.Time) {
	var lostErr error
	defer func() {
		// done closes first, so lost may call Close (or code that does).
		close(r.done)
		if lostErr != nil && r.lost != nil {
			r.lost(lostErr)
		}
	}()
	for {
		delay := r.delay(expiresAt)
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		next, err := r.renewOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil && !finalRenewErr(err) {
			next, err = r.retry(ctx, expiresAt, err)
			if ctx.Err() != nil {
				return
			}
		}
		if err != nil {
			lostErr = err
			return
		}
		expiresAt = next
	}
}

func (r *Renewer) renewOnce(ctx context.Context) (time.Time, error) {
	next, err := r.renew(ctx)
	if err == nil && !next.After(r.now()) {
		err = errLeaseEnded
	}
	return next, err
}

// retry renews again with backoff after err, until it works, fails for
// good, or only a quarter of the time left at the first failure remains:
// that quarter is kept for the owner to fetch new credentials.
func (r *Renewer) retry(ctx context.Context, expiresAt time.Time, err error) (time.Time, error) {
	giveUp := expiresAt.Add(-expiresAt.Sub(r.now()) / 4)
	wait := r.retryBase
	if wait <= 0 {
		wait = defaultRetryBase
	}
	for {
		if r.now().Add(wait).After(giveUp) {
			return time.Time{}, err
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return time.Time{}, ctx.Err()
		case <-t.C:
		}
		next, rerr := r.renewOnce(ctx)
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		if rerr == nil {
			return next, nil
		}
		err = rerr
		if finalRenewErr(err) {
			return time.Time{}, err
		}
		wait = min(wait*2, maxRetryDelay)
	}
}

// delay is two thirds of the time left.
func (r *Renewer) delay(expiresAt time.Time) time.Duration {
	d := expiresAt.Sub(r.now()) * 2 / 3
	if d < minRenewDelay {
		return minRenewDelay
	}
	return d
}
