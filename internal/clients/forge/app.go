package forge

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
)

// AppKey is a GitHub App's RSA private key. It prints as "[redacted]" with
// fmt and slog; only the JWT signer reads the key.
type AppKey struct{ k *rsa.PrivateKey }

// String hides the key.
func (AppKey) String() string { return "[redacted]" }

// GoString hides the key for %#v.
func (AppKey) GoString() string { return "[redacted]" }

// LogValue hides the key for log/slog.
func (AppKey) LogValue() slog.Value { return slog.StringValue("[redacted]") }

// ErrBadKey is returned for a private key that is not a PEM RSA key.
var ErrBadKey = errors.New("forge: the GitHub App private key is not a PEM RSA private key")

// ParsePrivateKey reads a PEM RSA private key: PKCS #1 ("RSA PRIVATE KEY",
// what GitHub's "Generate a private key" downloads) or PKCS #8 holding an RSA
// key. The error never quotes key bytes.
func ParsePrivateKey(pemBytes []byte) (AppKey, error) {
	blk, _ := pem.Decode(pemBytes)
	if blk == nil {
		return AppKey{}, fmt.Errorf("%w: no PEM block", ErrBadKey)
	}
	switch blk.Type {
	case "RSA PRIVATE KEY":
		k, err := x509.ParsePKCS1PrivateKey(blk.Bytes)
		if err != nil {
			return AppKey{}, fmt.Errorf("%w: PKCS #1 does not parse", ErrBadKey)
		}
		return AppKey{k: k}, nil
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
		if err != nil {
			return AppKey{}, fmt.Errorf("%w: PKCS #8 does not parse", ErrBadKey)
		}
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return AppKey{}, fmt.Errorf("%w: PKCS #8 key is %T, not RSA", ErrBadKey, k)
		}
		return AppKey{k: rk}, nil
	}
	return AppKey{}, fmt.Errorf("%w: PEM block type %q", ErrBadKey, blk.Type)
}

// LoadPrivateKeyFile reads the key file once (GITHUB_APP_PRIVATE_KEY_FILE).
// On Unix a file readable by group or others logs one warning naming the
// path, never its content.
func LoadPrivateKeyFile(path string, log *slog.Logger) (AppKey, error) {
	if fi, err := os.Stat(path); err == nil && runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 && log != nil {
		log.Warn("forge: the GitHub App private key file is readable by group or others; chmod 600 it", "path", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return AppKey{}, fmt.Errorf("forge: reading the GitHub App private key file: %w", err)
	}
	return ParsePrivateKey(b)
}

// Token lifetimes (docs/design/fix-pr.md §4.2).
const (
	jwtBackdate  = 60 * time.Second // GitHub's advice for clock drift
	jwtLife      = 9 * time.Minute  // GitHub allows at most 10
	tokenRefresh = 5 * time.Minute  // refresh this long before expires_at
)

// appJWT signs {iat: now-60s, exp: now+9m, iss: appID} with RS256.
func appJWT(key AppKey, appID int64, now time.Time) (netsec.Secret, error) {
	if key.k == nil {
		return "", fmt.Errorf("%w: no key", ErrBadKey)
	}
	enc := base64.RawURLEncoding
	head := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(struct {
		IAT int64 `json:"iat"`
		EXP int64 `json:"exp"`
		ISS int64 `json:"iss"`
	}{now.Add(-jwtBackdate).Unix(), now.Add(jwtLife).Unix(), appID})
	if err != nil {
		return "", fmt.Errorf("forge: JWT claims: %w", err)
	}
	signing := head + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(nil, key.k, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("forge: signing the App JWT: %w", err)
	}
	return netsec.Secret(signing + "." + enc.EncodeToString(sig)), nil
}

// scope is the permission set an installation token is narrowed to.
type scope int

const (
	scopeRead scope = iota
	scopeWrite
)

// permissions of each scope (design §4.2). The writer's token holds no
// checks or statuses permission; contents:write and pull_requests:write
// include the reads the writer makes.
func (s scope) permissions() map[string]string {
	if s == scopeWrite {
		return map[string]string{"contents": "write", "pull_requests": "write"}
	}
	return map[string]string{"contents": "read", "pull_requests": "read", "checks": "read", "statuses": "read"}
}

type tokenKey struct {
	repo  string // owner/name
	scope scope
}

type cachedToken struct {
	tok     netsec.Secret
	expires time.Time
}

// tokens mints and caches installation tokens, one per (repo, scope). mu
// guards the maps only and is never held across a request: each key has its
// own lock (a one-slot channel, so a waiter honours its context), so one slow
// mint delays only the calls for that repo and scope, not every repo.
type tokens struct {
	mu    sync.Mutex
	cache map[tokenKey]cachedToken
	locks map[tokenKey]chan struct{}
}

// cached returns the cached token of key when it is valid past now+tokenRefresh.
func (t *tokens) cached(key tokenKey, now time.Time) (netsec.Secret, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.cache[key]; ok && now.Add(tokenRefresh).Before(c.expires) {
		return c.tok, true
	}
	return "", false
}

// lock takes key's mint lock, or gives up when ctx ends.
func (t *tokens) lock(ctx context.Context, key tokenKey) (unlock func(), err error) {
	t.mu.Lock()
	if t.locks == nil {
		t.locks = map[tokenKey]chan struct{}{}
	}
	l, ok := t.locks[key]
	if !ok {
		l = make(chan struct{}, 1)
		t.locks[key] = l
	}
	t.mu.Unlock()
	select {
	case l <- struct{}{}:
		return func() { <-l }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("forge: waiting for an installation token for %s: %w", key.repo, ctx.Err())
	}
}

func (t *tokens) store(key tokenKey, c cachedToken) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cache == nil {
		t.cache = map[tokenKey]cachedToken{}
	}
	t.cache[key] = c
}

// token returns an installation token narrowed to repo and s, minting one
// when none is cached or the cached one expires within tokenRefresh.
// clients.RepoRef names no host: that every repos[].url is on this forge's
// web host is owners.CheckRepoHosts', run by cmd/server at start
// (docs/design/fix-pr.md §5.6).
func (c *Client) token(ctx context.Context, repo RepoRef, s scope) (netsec.Secret, error) {
	key := tokenKey{repo: repo.Owner + "/" + repo.Name, scope: s}
	if tok, ok := c.tok.cached(key, c.now()); ok {
		return tok, nil
	}
	unlock, err := c.tok.lock(ctx, key)
	if err != nil {
		return "", err
	}
	defer unlock()
	// Another call may have minted it while this one waited.
	now := c.now()
	if tok, ok := c.tok.cached(key, now); ok {
		return tok, nil
	}
	jwt, err := appJWT(c.key, c.appID, now)
	if err != nil {
		return "", err
	}
	body := struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{[]string{repo.Name}, s.permissions()}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	r := request{ep: epToken, method: http.MethodPost, path: "/app/installations/" + strconv.FormatInt(c.installationID, 10) + "/access_tokens", body: body, accept: []int{http.StatusCreated}}
	if err := c.do(ctx, jwt, r, &out); err != nil {
		var se *StatusError
		if errors.As(err, &se) && (se.Code == http.StatusNotFound || se.Code == http.StatusUnprocessableEntity || se.Code == http.StatusForbidden) {
			return "", fmt.Errorf("forge: the GitHub App installation %d cannot get a token for %s (is the App installed on that repository, with the permissions of docs/design/fix-pr.md §4.1?): %w",
				c.installationID, key.repo, err)
		}
		return "", fmt.Errorf("forge: minting an installation token for %s: %w", key.repo, err)
	}
	if out.Token == "" || out.ExpiresAt.IsZero() {
		return "", fmt.Errorf("forge: installation token answer for %s has no token or expires_at", key.repo)
	}
	t := cachedToken{tok: netsec.Secret(out.Token), expires: out.ExpiresAt}
	c.tok.store(key, t)
	return t.tok, nil
}

// botLogin returns the login of the App's bot user ("<slug>[bot]"), read
// once from GET /app with the App JWT. A failure is not cached.
func (c *Client) botLogin(ctx context.Context) (string, error) {
	c.bot.mu.Lock()
	defer c.bot.mu.Unlock()
	if c.bot.login != "" {
		return c.bot.login, nil
	}
	jwt, err := appJWT(c.key, c.appID, c.now())
	if err != nil {
		return "", err
	}
	var out struct {
		ID   int64  `json:"id"`
		Slug string `json:"slug"`
	}
	if err := c.do(ctx, jwt, request{ep: epApp, method: http.MethodGet, path: "/app"}, &out); err != nil {
		return "", fmt.Errorf("forge: reading the GitHub App: %w", err)
	}
	if out.ID != c.appID || !slugRE.MatchString(out.Slug) {
		return "", fmt.Errorf("forge: the GitHub App answer is not App %d with a valid slug", c.appID)
	}
	c.bot.login = out.Slug + "[bot]"
	return c.bot.login, nil
}

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,99}$`)
