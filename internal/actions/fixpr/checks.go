package fixpr

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/injectscan"
	"piperoom/internal/redact"
)

// The text rules of Validate (design §6.2 rules 5 to 7) and the risk flags
// of Preview (§6.3). Every refusal names the field and the rule, never the
// text: params are model text that may hold the very secret being refused.

// lfsPointer starts a Git LFS pointer file: the real content lives outside
// the repository, so an edit to the pointer would corrupt it.
const lfsPointer = "version https://git-lfs.github.com/spec/v1"

// markers are the forms the redaction passes write (internal/redact's
// "[redacted ...]", actions.RedactedValue, the forge tools' masked PEM and
// cut-line markers). The model saw them in place of a value it never saw;
// writing one back would put the mask into the file instead of the value.
var markers = []string{"[redacted", strings.ToLower(actions.RedactedValue), "[line cut]"}

// checkText is rule 5: valid UTF-8, no NUL, no control character but tab,
// LF and CR.
func checkText(field, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("%s is not valid UTF-8 text (a fix PR changes text files only)", field)
	}
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' || r == 0x7f {
			return fmt.Errorf("%s holds a NUL or control character (a fix PR changes text files only)", field)
		}
	}
	return nil
}

// checkBaseText refuses a base file that is not text or is an LFS pointer.
func checkBaseText(content []byte) error {
	if bytes.HasPrefix(content, []byte(lfsPointer)) {
		return errors.New("is a Git LFS pointer (its content is stored outside the repository; a fix PR cannot change it)")
	}
	if bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content) {
		return errors.New("is not a text file (binary content; a fix PR changes text files only)")
	}
	return nil
}

// pairRE finds key/value pairs in the forms config files use: key=value,
// key: value, "key": "value", 'key' = 'value'.
var pairRE = regexp.MustCompile(`(?:"([^"\\\n]{1,128})"|'([^'\\\n]{1,128})'|([A-Za-z_][A-Za-z0-9_.\-]{0,127}))[ \t]*[:=][ \t]*(?:"([^"\n]*)"|'([^'\n]*)'|([^\s,;}\]"']*))`)

// refRE is a ${...} reference or placeholder.
var refRE = regexp.MustCompile(`\$\{[^{}\s]{1,200}\}`)

// fqcnRE is a Java class name: lower-case package segments, then a class.
var fqcnRE = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+\.[A-Z][A-Za-z0-9_$]*$`)

// isReference reports a value that names a secret instead of holding it:
// a config provider reference or a ${...} placeholder of any templating.
func isReference(v string) bool {
	v = strings.TrimSpace(v)
	return v == "" || strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") || strings.HasPrefix(v, "{{") && strings.HasSuffix(v, "}}")
}

// keyOnlySecret reports a key that LooksSecret matches only through a bare
// "key" segment (key.converter, key.deserializer): no credential word.
func keyOnlySecret(k string) bool {
	var rest []string
	for _, seg := range strings.FieldsFunc(strings.ToLower(k), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if seg != "key" {
			rest = append(rest, seg)
		}
	}
	return !actions.LooksSecret(strings.Join(rest, "."))
}

// plainValue is a value that cannot be a credential: a boolean, a number or
// a Java class name. Only a key whose sole secret mark is a "key" segment
// may carry one: key.converter=org.apache.kafka.connect.json.JsonConverter
// is every Debezium config, and refusing it would refuse the fixes this
// action exists for.
func plainValue(v string) bool {
	v = strings.TrimSpace(v)
	switch strings.ToLower(v) {
	case "true", "false", "null", "none":
		return true
	}
	if v != "" && strings.Trim(v, "0123456789.-") == "" {
		return true
	}
	return fqcnRE.MatchString(v)
}

// checkSecrets is rule 6 on one piece of added text: no redact.Text hit, no
// redaction marker, and no pair whose key LooksSecret matches unless its
// value is a reference.
func checkSecrets(field, s string) error {
	low := strings.ToLower(s)
	for _, m := range markers {
		if strings.Contains(low, m) {
			return fmt.Errorf("%s holds a redaction marker: a masked value cannot be written back (keep the original line untouched, or use a ${secret:...} reference)", field)
		}
	}
	if strings.Contains(s, "-----BEGIN ") && strings.Contains(s, "PRIVATE KEY") {
		return fmt.Errorf("%s holds a private key", field)
	}
	// References are taken out first: redact reads "${secret:name}" as a
	// key "secret" with a value, and the reference is the one allowed form.
	if _, st := redact.Text(refRE.ReplaceAllString(s, "$${ref}")); st.Total > 0 {
		kinds := make([]string, 0, len(st.ByKind))
		for k := range st.ByKind {
			kinds = append(kinds, string(k))
		}
		sort.Strings(kinds)
		return fmt.Errorf("%s holds what the redaction rules mask (%s); a fix PR never writes a credential or personal data: use a ${secret:...}, ${file:...} or ${env:...} reference", field, strings.Join(kinds, ", "))
	}
	for _, m := range pairRE.FindAllStringSubmatch(s, -1) {
		key := m[1] + m[2] + m[3]
		val := m[4] + m[5] + m[6]
		if !actions.LooksSecret(key) || isReference(val) {
			continue
		}
		if keyOnlySecret(key) && plainValue(val) {
			continue
		}
		return fmt.Errorf("%s sets a credential-like key in plain text; a fix PR writes such a key only as a ${secret:...}, ${file:...} or ${env:...} reference", field)
	}
	return nil
}

var (
	// urlRE finds absolute URLs and protocol-relative ones.
	urlRE = regexp.MustCompile(`(?i)(?:\b[a-z][a-z0-9+.\-]*:)?//[^\s<>()\[\]"'` + "`" + `]+`)
	// wwwRE finds GFM's extended www autolinks ("www.host/path" renders as
	// a link with no scheme): a www. that does not continue a word, a host
	// or a URL already matched by urlRE.
	wwwRE = regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9_.@/\-])(www\.[^\s<>()\[\]"'` + "`" + `]+)`)
	// linkTargetRE finds Markdown link targets and reference definitions.
	linkTargetRE = regexp.MustCompile(`\]\(\s*<?([^)\s>]*)|(?m)^[ \t]{0,3}\[[^\]\n]+\]:[ \t]*<?(\S*?)>?(?:\s|$)`)
	imageRE      = regexp.MustCompile(`!\[[^\]\n]*\]\s*[(\[]`)
	htmlRE       = regexp.MustCompile(`<[A-Za-z!/?][^>\n]*>|&#?[A-Za-z0-9]+;`)
	// mentionRE finds an @name not inside a word or an e-mail address.
	mentionRE = regexp.MustCompile("(^|[^A-Za-z0-9_`./@-])@([A-Za-z0-9][A-Za-z0-9-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)?)")
)

// checkLinks is rule 7 on the title or the body: no image, no HTML, and
// every link on one of the allowed hosts (the forge's web host, Piperoom's
// own). A rendered PR body must not be a beacon that carries data out.
func checkLinks(field, s string, allowed []string) error {
	if imageRE.MatchString(s) {
		return fmt.Errorf("%s holds a Markdown image; a fix PR's title and body carry no images", field)
	}
	if htmlRE.MatchString(s) {
		return fmt.Errorf("%s holds an HTML tag or entity; a fix PR's title and body are plain Markdown", field)
	}
	check := func(raw string) error {
		h, ok := linkHost(raw)
		if !ok {
			return nil // a relative link: it resolves on the forge's own host
		}
		for _, a := range allowed {
			if h == a {
				return nil
			}
		}
		return fmt.Errorf("%s links to a host other than %s; a fix PR links only to the repository's forge and Piperoom", field, strings.Join(allowed, " and "))
	}
	for _, u := range urlRE.FindAllString(s, -1) {
		if err := check(u); err != nil {
			return err
		}
	}
	for _, m := range wwwRE.FindAllStringSubmatch(s, -1) {
		// GFM leaves trailing punctuation out of the link.
		if err := check("https://" + strings.TrimRight(m[1], "?!.,:;*_~'\"")); err != nil {
			return err
		}
	}
	for _, m := range linkTargetRE.FindAllStringSubmatch(s, -1) {
		t := m[1] + m[2]
		if strings.Contains(t, ":") && !strings.Contains(t, "//") {
			// mailto:, javascript:, data: ... are never a link to evidence.
			if i := strings.IndexByte(t, ':'); i > 0 && !strings.ContainsAny(t[:i], "/?#") {
				return fmt.Errorf("%s holds a link with a %s: scheme; only https links to the forge and Piperoom are allowed", field, strings.ToLower(oneLine(t[:i], 16)))
			}
		}
		if err := check(t); err != nil {
			return err
		}
	}
	return nil
}

// linkHost returns the lower-case host of an absolute or protocol-relative
// URL; false for a relative one. An unparsable absolute URL reports an
// empty host, which no allowed host equals.
func linkHost(raw string) (string, bool) {
	if !strings.Contains(raw, "//") {
		return "", false
	}
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", true
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", true
	}
	return strings.ToLower(u.Hostname()), true
}

// neutralizeMentions inserts a zero-width space after every @ of a mention,
// so the PR never pings a person or team the approvers did not see named.
func neutralizeMentions(s string) string {
	return mentionRE.ReplaceAllString(s, "${1}@​${2}")
}

// mdEscape escapes Markdown and mention syntax in a name the footer prints
// (an approver's display name), so it renders as the text it is.
func mdEscape(s string) string {
	s = oneLine(s, 120)
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune("\\`*_[]()<>!#|~&@", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// oneLine folds s to one line of at most n bytes, cut on a rune boundary.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == utf8.RuneError {
			return ' '
		}
		return r
	}, s)), " ")
	if len(s) <= n {
		return s
	}
	c := n - len("…")
	for c > 0 && !utf8.RuneStart(s[c]) {
		c--
	}
	return s[:c] + "…"
}

// Risk flags of design §6.3 (D11: shown to approvers, never refusals).
var (
	fromRE     = regexp.MustCompile(`(?i)^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)`)
	downloadRE = regexp.MustCompile(`(?i)\b(curl|wget|invoke-webrequest|iwr)\b|^\s*ADD\s+(?:--\S+\s+)*https?://`)
	pipeRE     = regexp.MustCompile(`(?i)\|\s*(?:sudo\s+)?(?:ba|z|k|da|a)?sh\b|\|\s*(?:sudo\s+)?(?:python[0-9.]*|perl|ruby|node|iex|powershell|pwsh)\b`)
	tlsOffRE   = regexp.MustCompile(`(?i)ssl\.endpoint\.identification\.algorithm"?\s*[:=]\s*(?:""|''|\s*$|[,}])|verify(?:_ssl|ssl|_tls)?"?\s*[:=]\s*"?false|insecure|--no-check-certificate|trustservercertificate\s*[:=]\s*"?true|sslmode\s*[:=]\s*"?disable|hostname[._-]?verification"?\s*[:=]\s*"?false`)
	aclRE      = regexp.MustCompile(`(?i)\bUser:\*|principal"?\s*[:=]\s*"?\*|allow\.everyone\.if\.no\.acl\.found"?\s*[:=]\s*"?true|--allow-principal\s+\S*\*|0\.0\.0\.0/0`)
)

// riskReport is the hosts and warnings of a step's added lines.
type riskReport struct {
	hosts    []string
	warnings []string
}

// assess flags the added lines of every file. A warning names the file, the
// line's position among the file's added lines and the flag; it quotes only
// an image name, redacted and cut.
func assess(files []fileDiff) riskReport {
	hosts := map[string]bool{}
	var warn []string
	for _, f := range files {
		for i, l := range f.added {
			at := fmt.Sprintf("%s (added line %d)", f.Path, i+1)
			for _, u := range urlRE.FindAllString(l, -1) {
				if h, ok := linkHost(u); ok && h != "" {
					hosts[h] = true
				} else if ok {
					warn = append(warn, at+": a URL with a scheme other than http(s)")
				}
			}
			if m := fromRE.FindStringSubmatch(l); m != nil && looksDockerfile(f.Path) {
				img, _ := redact.Text(m[1])
				hosts[imageHost(m[1])] = true
				warn = append(warn, fmt.Sprintf("%s: a base image (FROM %s); check its registry and tag", at, oneLine(img, 100)))
			}
			if downloadRE.MatchString(l) {
				warn = append(warn, at+": downloads at build or run time (curl, wget or ADD of a URL)")
			}
			if pipeRE.MatchString(l) {
				warn = append(warn, at+": pipes text into a shell or interpreter")
			}
			if tlsOffRE.MatchString(l) {
				warn = append(warn, at+": turns off a TLS, certificate or hostname check")
			}
			if aclRE.MatchString(l) {
				warn = append(warn, at+": widens access (a * principal, allow-everyone or an open network range)")
			}
		}
		if fs := injectscan.Scan(strings.Join(f.added, "\n")); len(fs) > 0 {
			warn = append(warn, fmt.Sprintf("%s: instruction-like text in the added lines (%s); read them as data", f.Path, strings.Join(injectscan.Kinds(fs), ", ")))
		}
	}
	out := riskReport{warnings: warn}
	for h := range hosts {
		out.hosts = append(out.hosts, h)
	}
	sort.Strings(out.hosts)
	return out
}

// looksDockerfile reports a file whose FROM lines name images.
func looksDockerfile(p string) bool {
	base := strings.ToLower(p[strings.LastIndexByte(p, '/')+1:])
	return strings.Contains(base, "dockerfile") || strings.HasSuffix(base, ".containerfile") || base == "containerfile"
}

// imageHost is the registry of an image reference: its first segment when
// that names a host (holds '.' or ':', or is localhost), else Docker Hub.
func imageHost(img string) string {
	first, _, ok := strings.Cut(img, "/")
	if ok && (strings.ContainsAny(first, ".:") || first == "localhost") {
		h, _, _ := strings.Cut(first, ":")
		return strings.ToLower(h)
	}
	return "docker.io"
}
