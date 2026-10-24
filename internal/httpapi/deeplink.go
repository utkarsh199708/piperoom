package httpapi

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/auth"
)

// Deep links. Slack and alert messages link to <public URL>/rooms/<id>, but the
// web app routes on the URL hash (#/rooms/<id>), so the plain path would open
// the room list. GET /rooms/{id} turns the link into the app's own address,
// sending a signed-out person through login first so they land on the room
// and not on the home page.

// deepLinkIDRe is the room id shape a deep link accepts. Ids are r_ + a ULID;
// anything else is not a room link and goes to the web app's 404 handling.
var deepLinkIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// handleRoomDeepLink is GET (and HEAD) /rooms/{id}. Signed in (or dev mode, whose login
// is the in-app picker and keeps the hash): 302 to /#/rooms/{id}. Signed out
// in oidc mode: 302 to /auth/login?return=/rooms/{id}; the callback brings the
// browser back here, now signed in. Whether the room exists or is visible is
// left to the API the app calls next, so the link answers the same for every
// id and tells a signed-out caller nothing.
func (s *Server) handleRoomDeepLink(notFound http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if !deepLinkIDRe.MatchString(id) {
			notFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if s.authSettings.IsOIDC() {
			if _, aerr := s.resolve(r); aerr != nil {
				http.Redirect(w, r, "/auth/login?return="+url.QueryEscape("/rooms/"+id), http.StatusFound)
				return
			}
		}
		http.Redirect(w, r, "/#/rooms/"+id, http.StatusFound)
	}
}

// maxUnescape bounds how many layers of percent-encoding safeReturnPath peels
// off. Three covers every double- and triple-encoded trick; deeper nesting is
// refused outright rather than trusted.
const maxUnescape = 3

// safeReturnPath is the stricter check for the login return target: a
// same-origin path and nothing else. auth.SafeReturn already refuses schemes,
// hosts and a decoded "//" or "/\" start; this also refuses, at every level of
// percent-decoding of the path:
//
//   - a backslash anywhere (browsers treat "\" as "/", so "/a\..\\evil" can
//     become "//evil" after their normalisation);
//   - a "//" or "/\" start;
//   - control characters;
//   - a path still percent-encoded after maxUnescape rounds.
//
// Anything refused becomes "/". The result is what auth.SafeReturn makes of
// the accepted value, so both checks always agree.
func safeReturnPath(raw string) string {
	// The second-character check repeats the loop's first round on purpose:
	// it keeps "//host" and "/\host" out even if the loop below changes.
	if raw == "" || raw[0] != '/' || (len(raw) > 1 && (raw[1] == '/' || raw[1] == '\\')) {
		return "/"
	}
	p, _, _ := strings.Cut(raw, "?")
	for i := 0; ; i++ {
		if strings.HasPrefix(p, "//") || strings.ContainsRune(p, '\\') || hasCtl(p) {
			return "/"
		}
		next, err := url.PathUnescape(p)
		if err != nil {
			return "/"
		}
		if next == p {
			break
		}
		if i == maxUnescape {
			return "/"
		}
		p = next
	}
	return auth.SafeReturn(raw)
}

func hasCtl(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}
