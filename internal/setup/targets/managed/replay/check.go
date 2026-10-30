package replay

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/redact"
)

// Credential shapes the loader scans for on top of redact.Text (design §5).
var (
	awsKeyID = regexp.MustCompile(`\b(?:AKIA|ASIA|AROA|AIDA)[0-9A-Z]{16}\b`)
	// 40-byte AWS secret access keys: base64 alphabet, mixed case and a
	// digit (a SHA-1 hex digest has no upper case, so it is not flagged).
	awsSecret = regexp.MustCompile(`(?:^|[^A-Za-z0-9/+=])([A-Za-z0-9/+]{40})(?:$|[^A-Za-z0-9/+=])`)
	// Confluent Cloud API keys are 16 upper-case letters and digits; their
	// secrets 64 characters of the base64 alphabet.
	ccloudKey    = regexp.MustCompile(`(?:^|[^A-Za-z0-9])([A-Z0-9]{16})(?:$|[^A-Za-z0-9])`)
	ccloudSecret = regexp.MustCompile(`(?:^|[^A-Za-z0-9/+=])([A-Za-z0-9/+]{64})(?:$|[^A-Za-z0-9/+=])`)
	aivenToken   = regexp.MustCompile(`(?i)aivenv1 `)
	urlUserinfo  = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://([^/\s@"'?#]+)@`)
)

// Check scans every string of c and returns one line per finding: where it
// is and which rule fired, never the value. An empty result means the
// cassette is clean.
func Check(c *Cassette) []string {
	var out []string
	add := func(where, why string) { out = append(out, where+": "+why) }
	for i, it := range c.Interactions {
		at := fmt.Sprintf("interaction %d", i)
		checkText(at+" request path", it.Request.Path, add)
		checkText(at+" request query", it.Request.Query, add)
		checkBody(at+" request body", it.Request.Body, add)
		checkHeaders(at+" request header", it.Request.Headers, add)
		checkBody(at+" response body", it.Response.Body, add)
		checkHeaders(at+" response header", it.Response.Headers, add)
	}
	checkText("source", c.Source, add)
	checkText("name", c.Name, add)
	return out
}

func checkHeaders(where string, h map[string]string, add func(string, string)) {
	for k, v := range h {
		lk := strings.ToLower(k)
		if droppedHeaders[lk] {
			add(where+" "+k, "a header that is dropped at record time is present")
			continue
		}
		if (secretKey(k) || strings.Contains(lk, "auth")) && v != Redacted {
			add(where+" "+k, "an auth header is not REDACTED")
			continue
		}
		checkText(where+" "+k, v, add)
	}
}

// checkBody walks a JSON body for secret keys, then scans it as text.
func checkBody(where, body string, add func(string, string)) {
	if body == "" {
		return
	}
	if looksJSON([]byte(body)) {
		var v any
		if err := json.Unmarshal([]byte(body), &v); err == nil {
			checkJSON(where, "", v, false, add)
		}
	}
	checkText(where, body, add)
}

func checkJSON(where, path string, v any, secret bool, add func(string, string)) {
	if secret {
		if s, ok := v.(string); ok && (s == "" || s == Redacted) {
			return
		}
		if v == nil {
			return
		}
		add(where+" "+path, "a value under a secret key is not REDACTED")
		return
	}
	switch t := v.(type) {
	case map[string]any:
		nameSecret := false
		if n, ok := t["name"].(string); ok && secretKey(n) {
			nameSecret = true
		}
		for k, val := range t {
			checkJSON(where, path+"."+k, val, secretKey(k) || (nameSecret && k == "value"), add)
		}
	case []any:
		for i, val := range t {
			checkJSON(where, fmt.Sprintf("%s[%d]", path, i), val, false, add)
		}
	}
}

// checkText runs redact.Text and the extra shapes over s.
func checkText(where, s string, add func(string, string)) {
	if s == "" {
		return
	}
	if _, st := redact.Text(s); st.Total > 0 {
		for k := range st.ByKind {
			add(where, "redact.Text found "+string(k))
			break
		}
	}
	if awsKeyID.MatchString(s) {
		add(where, "an AWS access key id")
	}
	for _, m := range awsSecret.FindAllStringSubmatch(s, -1) {
		if mixed(m[1]) {
			add(where, "a 40-byte AWS secret-key shape")
			break
		}
	}
	for _, m := range ccloudKey.FindAllStringSubmatch(s, -1) {
		if hasLetter(m[1]) && hasDigit(m[1]) {
			add(where, "a Confluent Cloud API key shape")
			break
		}
	}
	for _, m := range ccloudSecret.FindAllStringSubmatch(s, -1) {
		if mixed(m[1]) {
			add(where, "a 64-byte Confluent Cloud secret shape")
			break
		}
	}
	if aivenToken.MatchString(s) {
		add(where, "an Aiven token header")
	}
	for _, m := range urlUserinfo.FindAllStringSubmatch(s, -1) {
		if m[1] != Redacted {
			add(where, "URL userinfo")
			break
		}
	}
}

// mixed: upper case, lower case and a digit, as a random key has and a
// word, an ARN or a hex digest does not.
func mixed(s string) bool {
	var up, low, dig bool
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
			up = true
		case r >= 'a' && r <= 'z':
			low = true
		case r >= '0' && r <= '9':
			dig = true
		}
	}
	return up && low && dig
}

func hasLetter(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return r >= 'A' && r <= 'Z' }) >= 0
}
func hasDigit(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return r >= '0' && r <= '9' }) >= 0
}
