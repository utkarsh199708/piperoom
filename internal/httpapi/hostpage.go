package httpapi

import (
	"html"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

// badHostMsg is the 421 bad_host message for API clients.
const badHostMsg = "this Host is not allowed; see PIPEROOM_PUBLIC_URL and PIPEROOM_ALLOWED_HOSTS"

// maxShownHost caps how much of the refused Host the HTML page shows. Real
// host names are far shorter; the cap keeps a crafted header from filling the
// page.
const maxShownHost = 100

// writeBadHost answers a request whose Host is not on the allow-list. A
// browser navigation gets a short HTML page that says what happened and what
// the operator should set; everything else, and anything under /api or /auth,
// keeps the JSON error shape clients parse.
//
// The page shows the Host the browser sent so the person can tell the operator
// which name they used. That header is attacker-chosen (the DNS rebinding case
// this guard exists for), so it is HTML-escaped and clipped, and the page
// carries its own CSP with no script source: even a value that got past the
// escaping could not run.
func writeBadHost(w http.ResponseWriter, r *http.Request) {
	if !wantsHTML(r) {
		writeError(w, http.StatusMisdirectedRequest, "bad_host", badHostMsg)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusMisdirectedRequest)
	_, _ = w.Write([]byte(badHostPage(r.Host)))
}

// wantsHTML reports whether r is a browser page load: a GET outside the
// server's JSON paths whose Accept header lists text/html.
func wantsHTML(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	p := strings.ToLower(r.URL.Path)
	if isServerPath(p, "/api") || isServerPath(p, "/auth") {
		return false
	}
	for _, part := range strings.Split(r.Header.Get("Accept"), ",") {
		mt, _, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err == nil && (mt == "text/html" || mt == "application/xhtml+xml") {
			return true
		}
	}
	return false
}

func badHostPage(host string) string {
	shown := clipHost(host)
	return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wrong address</title>
<style>body{font:16px/1.5 system-ui,sans-serif;max-width:36rem;margin:4rem auto;padding:0 1rem;color:#222;background:#fff}
@media (prefers-color-scheme:dark){body{color:#ddd;background:#111}}code{word-break:break-all}</style>
</head><body>
<h1>This address is not set up for Piperoom</h1>
<p>The server was reached as <code>` + html.EscapeString(shown) + `</code>, which is not one of its configured host names.</p>
<p>Use the link your team normally uses for Piperoom. If this is that link, ask whoever runs Piperoom to add this host to
<code>PIPEROOM_PUBLIC_URL</code> or <code>PIPEROOM_ALLOWED_HOSTS</code>.</p>
</body></html>
`
}

// clipHost cuts host to maxShownHost bytes on a character boundary and marks
// the cut.
func clipHost(host string) string {
	if len(host) <= maxShownHost {
		return host
	}
	cut := maxShownHost
	for cut > 0 && !utf8.RuneStart(host[cut]) {
		cut--
	}
	return host[:cut] + "…"
}
