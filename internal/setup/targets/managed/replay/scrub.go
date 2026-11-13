package replay

import (
	"bytes"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/actions"
)

// Redacted is what every scrubbed secret value becomes.
const Redacted = "REDACTED"

// SecretKeys is the per-provider key table of design §5 on top of
// actions.LooksSecret: a JSON value under one of these keys (any case) is a
// secret wherever it appears, request or response. A new provider endpoint
// adds its keys here in the same change.
var SecretKeys = []string{
	"secret",          // Confluent IAM v2 api-keys spec.secret
	"password",        // Aiven users[].password, Connect configs
	"service_uri",     // Aiven: carries the service user's password as userinfo
	"connection_info", // Aiven: URIs and passwords of the service
	"access_key",      // Aiven service access key
	"access_cert",     // Aiven service access certificate
	"token",           // Aiven access tokens
	"full_token",      // Aiven token create response
}

// secretKey reports whether a JSON key names a secret value.
func secretKey(k string) bool {
	if actions.LooksSecret(k) {
		return true
	}
	lk := strings.ToLower(k)
	for _, s := range SecretKeys {
		if lk == s {
			return true
		}
	}
	return false
}

// droppedHeaders are never written to a cassette: they carry a credential, a
// signature or nothing stable (design §5).
var droppedHeaders = map[string]bool{
	"authorization":         true,
	"x-amz-date":            true,
	"x-amz-security-token":  true,
	"x-amz-content-sha256":  true,
	"x-amzn-requestid":      true,
	"x-amz-request-id":      true,
	"x-amz-id-2":            true,
	"amz-sdk-invocation-id": true,
	"amz-sdk-request":       true,
	"date":                  true,
	"set-cookie":            true,
	"cookie":                true,
	"user-agent":            true,
	"x-amz-user-agent":      true,
}

// keptRequestHeaders are the request headers a cassette keeps for its
// reader; the rest are dropped.
var keptRequestHeaders = []string{"Content-Type", "X-Amz-Target"}

// Identifier rewrites applied to every string (design §5 "Identifiers").
var (
	arnRE     = regexp.MustCompile(`arn:(aws[a-z-]*):([a-z0-9-]+):([a-z0-9-]*):([0-9]{12})?:`)
	accountRE = regexp.MustCompile(`(^|[^0-9])[0-9]{12}([^0-9]|$)`)
	cloudHost = regexp.MustCompile(`(?i)\b(?:[a-z0-9-]+\.)+(amazonaws\.com|confluent\.cloud|aivencloud\.com)\b`)
	ccloudID  = regexp.MustCompile(`\b(env|lkc|lcc|lsrc|pkc|sa|dlz)-[a-z0-9]{4,}\b`)
	userinfo  = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)([^/\s@"'?#]+)@`)
)

// keptHosts are API endpoints, not customer resources; they stay readable.
var keptHosts = map[string]bool{"api.confluent.cloud": true}

// awsServiceHost is an AWS service principal or endpoint
// (kafkaconnect.amazonaws.com, sts.us-east-1.amazonaws.com): not a customer
// resource, and a trust policy names it, so it stays readable.
var awsServiceHost = regexp.MustCompile(`(?i)^[a-z0-9-]+(\.[a-z]{2}(-[a-z]+)+-[0-9])?\.amazonaws\.com$`)

// Placeholders the identifier rewrites produce.
const (
	PlaceholderAccount = "000000000000"
	PlaceholderRegion  = "us-east-1"
)

// Scrubber rewrites identifiers and secrets out of an interaction. Names
// maps real names a recording session knows (an S3 bucket, an Aiven project
// and service, a cluster name) to placeholders; they are replaced literally
// everywhere, longest first.
type Scrubber struct {
	Names map[string]string
}

// Text rewrites the identifiers in s: the names, ARNs' region and account,
// bare 12-digit account ids, cloud hostnames, Confluent ids and URL
// userinfo.
func (s Scrubber) Text(v string) string {
	if v == "" {
		return v
	}
	names := make([]string, 0, len(s.Names))
	for k := range s.Names {
		if k != "" {
			names = append(names, k)
		}
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	for _, k := range names {
		v = strings.ReplaceAll(v, k, s.Names[k])
	}
	v = userinfo.ReplaceAllString(v, "${1}"+Redacted+"@")
	v = arnRE.ReplaceAllStringFunc(v, func(m string) string {
		sub := arnRE.FindStringSubmatch(m)
		region := sub[3]
		if region != "" {
			region = PlaceholderRegion
		}
		account := sub[4]
		if account != "" {
			account = PlaceholderAccount
		}
		return "arn:" + sub[1] + ":" + sub[2] + ":" + region + ":" + account + ":"
	})
	v = replaceAccounts(v)
	v = cloudHost.ReplaceAllStringFunc(v, func(h string) string {
		if keptHosts[strings.ToLower(h)] || awsServiceHost.MatchString(h) {
			return h
		}
		m := cloudHost.FindStringSubmatch(h)
		return "scrubbed." + strings.ToLower(m[1])
	})
	v = ccloudID.ReplaceAllString(v, "${1}-00000")
	// The key shapes Check refuses are rewritten too, wherever they sit: a
	// Confluent api-key id is the username half of a credential and comes
	// back under "id", a key the table cannot name as secret.
	v = awsKeyID.ReplaceAllString(v, Redacted)
	v = replaceShape(v, ccloudSecret, mixed)
	v = replaceShape(v, awsSecret, mixed)
	v = replaceShape(v, ccloudKey, func(s string) bool { return hasLetter(s) && hasDigit(s) })
	return v
}

// replaceShape rewrites group 1 of every match of re for which ok holds.
func replaceShape(v string, re *regexp.Regexp, ok func(string) bool) string {
	for _, m := range re.FindAllStringSubmatchIndex(v, -1) {
		tok := v[m[2]:m[3]]
		if ok(tok) {
			v = strings.Replace(v, tok, Redacted, 1)
		}
	}
	return v
}

// replaceAccounts rewrites every run of exactly 12 digits. A loop, because
// adjacent matches share their boundary character.
func replaceAccounts(v string) string {
	for {
		loc := accountRE.FindStringSubmatchIndex(v)
		if loc == nil {
			return v
		}
		start, end := loc[0], loc[1]
		// Keep the boundary characters the pattern consumed.
		pre := v[loc[2]:loc[3]]
		post := v[loc[4]:loc[5]]
		digits := v[start+len(pre) : end-len(post)]
		if digits == PlaceholderAccount {
			// Already a placeholder: look past it.
			rest := replaceAccounts(v[end-len(post):])
			return v[:end-len(post)] + rest
		}
		v = v[:start] + pre + PlaceholderAccount + post + v[end:]
	}
}

// Body scrubs and normalises a body by its content type: JSON is walked
// (secret keys REDACTED, every string through Text) and re-encoded with
// sorted keys; a form body has its values scrubbed and is re-encoded in key
// order; anything else goes through Text. A JSON body that does not decode
// is treated as text.
func (s Scrubber) Body(contentType string, body []byte) string {
	if len(body) == 0 {
		return ""
	}
	mt, _, _ := mime.ParseMediaType(contentType)
	switch {
	case mt == "application/x-www-form-urlencoded":
		vals, err := url.ParseQuery(string(body))
		if err == nil {
			for k, vs := range vals {
				for i := range vs {
					if secretKey(k) {
						vs[i] = Redacted
					} else {
						vs[i] = s.Text(vs[i])
					}
				}
			}
			return vals.Encode()
		}
	case strings.Contains(mt, "json") || looksJSON(body):
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err == nil && !dec.More() {
			out, err := json.Marshal(s.walk(v, false))
			if err == nil {
				return string(out)
			}
		}
	}
	return s.Text(string(body))
}

func looksJSON(b []byte) bool {
	t := bytes.TrimSpace(b)
	return len(t) > 0 && (t[0] == '{' || t[0] == '[')
}

// walk rewrites a decoded JSON value. secret says the value sits under a
// secret key: every string below it becomes REDACTED, an object or array
// below it is replaced whole.
func (s Scrubber) walk(v any, secret bool) any {
	if secret {
		switch v.(type) {
		case nil:
			return nil
		case string:
			if v.(string) == "" {
				return ""
			}
		}
		return Redacted
	}
	switch t := v.(type) {
	case map[string]any:
		// Connect's REST form {"name":"database.password","value":"..."}:
		// the sibling name is the key.
		nameSecret := false
		if n, ok := t["name"].(string); ok && secretKey(n) {
			nameSecret = true
		}
		out := make(map[string]any, len(t))
		for k, val := range t {
			sk := secretKey(k) || (nameSecret && k == "value")
			out[s.Text(k)] = s.walk(val, sk)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = s.walk(val, false)
		}
		return out
	case string:
		return s.Text(t)
	}
	return v
}

// Query normalises and scrubs a raw query: values through Text (a secret
// key's value REDACTED), keys sorted.
func (s Scrubber) Query(raw string) string {
	if raw == "" {
		return ""
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return s.Text(raw)
	}
	for k, vs := range vals {
		for i := range vs {
			if secretKey(k) {
				vs[i] = Redacted
			} else {
				vs[i] = s.Text(vs[i])
			}
		}
	}
	return vals.Encode()
}

// ResponseHeaders keeps every header except droppedHeaders, scrubbed; an
// auth-looking header that survives is REDACTED.
func (s Scrubber) ResponseHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for k, vs := range h {
		lk := strings.ToLower(k)
		if droppedHeaders[lk] || len(vs) == 0 {
			continue
		}
		v := strings.Join(vs, ", ")
		if secretKey(k) || strings.Contains(lk, "auth") {
			v = Redacted
		}
		out[http.CanonicalHeaderKey(k)] = s.Text(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// RequestHeaders keeps only keptRequestHeaders.
func (s Scrubber) RequestHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for _, k := range keptRequestHeaders {
		if v := h.Get(k); v != "" {
			out[k] = s.Text(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Path scrubs a request path in its unescaped form (url.URL.Path). A cassette
// stores and matches the unescaped path because SDKs escape an ARN in a path
// segment their own way (':' and '/' percent-encoded), and the scrubbed
// placeholder must match whichever escaping the live request used.
func (s Scrubber) Path(u *url.URL) string {
	return s.Text(u.Path)
}
