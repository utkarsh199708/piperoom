// Package auth holds the login settings and the session store contract
// (docs/design/auth.md). Dev mode works end to end; OIDC login is auth PR 3,
// so oidc mode here only validates its configuration and refuses X-User.
package auth

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"piperoom/internal/config"
)

// Auth modes (PIPEROOM_AUTH_MODE).
const (
	ModeDev  = "dev"
	ModeOIDC = "oidc"
)

// Cookie names by mode (design §1.3). The server reads only the name of its own
// mode, never both. The __Host- prefix needs Secure, which plain-http dev
// cannot rely on.
const (
	DevSessionCookie  = "piperoom_session"
	OIDCSessionCookie = "__Host-piperoom_session"
)

// Settings is the parsed, validated login configuration.
type Settings struct {
	Mode string // ModeDev or ModeOIDC
	// DevNonLoopback is true when dev mode listens on a non-loopback address
	// with the explicit switch (PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 or the
	// deprecated --allow-remote). It adds the PIPEROOM_PUBLIC_URL host to the
	// Host allow-list and makes every dev login log a loud warning. The caller
	// sets it; ParseSettings cannot know the flag.
	DevNonLoopback bool
	// AllowedHosts are the PIPEROOM_ALLOWED_HOSTS entries, lower case.
	AllowedHosts []string
	Idle         time.Duration // PIPEROOM_SESSION_IDLE
	Max          time.Duration // PIPEROOM_SESSION_MAX
}

// Default returns dev mode with the default session lengths. httpapi uses it
// for any field a caller left unset.
func Default() Settings {
	return Settings{Mode: ModeDev, Idle: 8 * time.Hour, Max: 12 * time.Hour}
}

// IsOIDC reports whether the mode is oidc.
func (s Settings) IsOIDC() bool { return s.Mode == ModeOIDC }

// CookieName is the session cookie name for the mode.
func (s Settings) CookieName() string {
	if s.IsOIDC() {
		return OIDCSessionCookie
	}
	return DevSessionCookie
}

// hostRe is deliberately narrow, like httpapi's public host check: PIPEROOM_
// ALLOWED_HOSTS entries also become WebSocket origin patterns, and the
// websocket library matches those with path.Match, so '*', '?', '[' and '\'
// would widen the allowed origins.
var hostRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*(:[0-9]{1,5})?$`)

// ParseSettings validates the PIPEROOM_AUTH_*, PIPEROOM_SESSION_* and (in oidc
// mode) PIPEROOM_OIDC_* fields. Errors name the variable. Empty fields mean
// the default. It does not decide DevNonLoopback.
func ParseSettings(c config.Config) (Settings, error) {
	s := Default()

	mode := strings.ToLower(strings.TrimSpace(c.AuthMode))
	if mode == "" {
		mode = ModeDev
	}
	switch mode {
	case ModeDev, ModeOIDC:
		s.Mode = mode
	default:
		return Settings{}, fmt.Errorf("auth: PIPEROOM_AUTH_MODE=%q: want dev or oidc", c.AuthMode)
	}

	var err error
	if s.Idle, err = parseDuration("PIPEROOM_SESSION_IDLE", c.SessionIdle, config.DefaultSessionIdle); err != nil {
		return Settings{}, err
	}
	if s.Max, err = parseDuration("PIPEROOM_SESSION_MAX", c.SessionMax, config.DefaultSessionMax); err != nil {
		return Settings{}, err
	}

	for _, h := range strings.Split(c.AllowedHosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if !hostRe.MatchString(h) {
			return Settings{}, fmt.Errorf("auth: PIPEROOM_ALLOWED_HOSTS entry %q: want a plain host name or IPv4 address with an optional port (no wildcards)", h)
		}
		s.AllowedHosts = append(s.AllowedHosts, strings.ToLower(h))
	}

	if s.Mode == ModeOIDC {
		if err := checkOIDC(c); err != nil {
			return Settings{}, err
		}
	}
	return s, nil
}

// checkOIDC is the start-up check for oidc mode. The login itself is auth PR 3.
func checkOIDC(c config.Config) error {
	pub := strings.TrimSpace(c.PublicURL)
	if pub == "" {
		return fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_PUBLIC_URL (an https origin; the redirect URI is that plus /auth/callback)")
	}
	u, err := url.Parse(pub)
	if err != nil || !strings.EqualFold(u.Scheme, "https") {
		return fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_PUBLIC_URL to be an https URL, got %q", pub)
	}
	if strings.TrimSpace(c.OIDCIssuer) == "" {
		return fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_OIDC_ISSUER")
	}
	if strings.TrimSpace(c.OIDCClientID) == "" {
		return fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_OIDC_CLIENT_ID")
	}
	if c.OIDCClientSecret == "" && strings.TrimSpace(c.OIDCPublicClient) != "1" {
		return fmt.Errorf("auth: PIPEROOM_AUTH_MODE=oidc needs PIPEROOM_OIDC_CLIENT_SECRET (or PIPEROOM_OIDC_PUBLIC_CLIENT=1 for an IdP that wants a public client)")
	}
	return nil
}

func parseDuration(name, v, def string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		v = def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("auth: %s=%q: not a duration like 30m or 8h: %w", name, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("auth: %s=%q: must be greater than zero", name, v)
	}
	return d, nil
}
