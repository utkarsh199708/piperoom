package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"
)

// Session methods (sessions.method).
const (
	MethodDev  = "dev"
	MethodOIDC = "oidc"
)

// TouchEvery is the most often a session's last_seen_at is written (design §1.3).
const TouchEvery = time.Minute

// tokenBytes is the size of the random session token. The cookie holds the
// token; only its SHA-256 is stored.
const tokenBytes = 32

// Errors returned by a Store and by Session.Check.
var (
	// ErrNotFound: no session has this token hash.
	ErrNotFound = errors.New("auth: session not found")
	// ErrRevoked: the session was revoked (logout).
	ErrRevoked = errors.New("auth: session revoked")
	// ErrExpired: the session is past its idle or absolute expiry.
	ErrExpired = errors.New("auth: session expired")
)

// Session is one server-side login (design §1.3). Times are UTC. A zero
// RevokedAt means the session is live; a zero AuthTime means unknown.
type Session struct {
	ID        string // public id, "ses_"+ULID; never the token
	TokenHash []byte // SHA-256 of the random token in the cookie
	UserID    string
	Name      string
	// Email is the IdP email, kept only when email_verified was true; "" for a
	// dev session or an unverified address. GET /api/people shows it.
	Email         string
	Iss           string
	Sub           string
	Groups        []string
	GroupsMissing bool
	AuthTime      time.Time
	AMR           []string
	ACR           string
	Method        string // MethodOIDC | MethodDev
	CreatedAt     time.Time
	LastSeenAt    time.Time
	ExpiresAt     time.Time // absolute expiry (CreatedAt + PIPEROOM_SESSION_MAX)
	RevokedAt     time.Time
}

// Check reports why the session cannot be used at now: ErrRevoked, or
// ErrExpired when it is past the absolute expiry or has been idle for idle.
// A nil result means live.
func (s Session) Check(now time.Time, idle time.Duration) error {
	if !s.RevokedAt.IsZero() {
		return ErrRevoked
	}
	if !now.Before(s.ExpiresAt) {
		return ErrExpired
	}
	if idle > 0 && !now.Before(s.LastSeenAt.Add(idle)) {
		return ErrExpired
	}
	return nil
}

// Liveness is the one rule for whether a stored session may be used now
// (#417): the per-request check, the presence tick, the person lookup and
// GET /api/people all apply it, so a session that cannot act also cannot be
// listed, given control or handed off to. Settings.Liveness builds it.
type Liveness struct {
	// Idle is PIPEROOM_SESSION_IDLE; 0 means no idle limit.
	Idle time.Duration
	// Max is the current PIPEROOM_SESSION_MAX, applied to CreatedAt: sessions
	// outlive a restart in Postgres, so a lowered max applies to sessions made
	// under the old, longer one. 0 means no limit beyond ExpiresAt.
	Max time.Duration
	// Method, when set, is the only session method accepted: MethodOIDC in
	// oidc mode, so a dev-login row made before a switch to oidc mode, or by a
	// dev server sharing the room DB, is not a live person.
	Method string
}

// Usable reports whether s may be used at now under l.
func (l Liveness) Usable(s Session, now time.Time) bool {
	if s.Check(now, l.Idle) != nil {
		return false
	}
	if l.Method != "" && s.Method != l.Method {
		return false
	}
	return l.Max <= 0 || now.Before(s.CreatedAt.Add(l.Max))
}

// Store keeps sessions. (*store.PG).Sessions() and MemStore implement it.
type Store interface {
	// Create inserts a new session.
	Create(ctx context.Context, s Session) error
	// ByTokenHash returns the session for a token hash, live or not (the
	// caller applies Check), or ErrNotFound.
	ByTokenHash(ctx context.Context, hash []byte) (Session, error)
	// ByID returns the session with this public id, live or not, or
	// ErrNotFound. The WebSocket presence tick uses it.
	ByID(ctx context.Context, id string) (Session, error)
	// LiveByUserID returns the newest session of userID (by created_at) that is
	// usable at now under live (Liveness.Usable). ErrNotFound when there is
	// none. Service.LookupPerson uses it to find a person who is not in
	// users.yaml (design §2.1).
	LiveByUserID(ctx context.Context, userID string, now time.Time, live Liveness) (Session, error)
	// LiveSessions returns the newest live session of each user (same rule as
	// LiveByUserID), at most limit of them, ordered by user id. GET /api/people
	// uses it.
	LiveSessions(ctx context.Context, now time.Time, live Liveness, limit int) ([]Session, error)
	// Touch sets last_seen_at. A missing session is not an error.
	Touch(ctx context.Context, id string, at time.Time) error
	// Revoke sets revoked_at if it is not set yet. It is idempotent and a
	// missing session is not an error.
	Revoke(ctx context.Context, id string, at time.Time) error
}

// NewToken returns a fresh cookie value (32 random bytes, base64url) and the
// SHA-256 of those bytes, which is what the store keeps.
func NewToken() (token string, hash []byte, err error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("auth: random token: %w", err)
	}
	sum := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(b), sum[:], nil
}

// HashToken returns the store key for a cookie value. ok is false when the
// value is not a well-formed token, so garbage never reaches the store.
func HashToken(token string) (hash []byte, ok bool) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) != tokenBytes {
		return nil, false
	}
	sum := sha256.Sum256(b)
	return sum[:], true
}
