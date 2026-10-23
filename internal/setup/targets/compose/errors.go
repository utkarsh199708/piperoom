package compose

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"piperoom/internal/setup/ops"
)

// Sentinel errors callers branch on with errors.Is.
var (
	// ErrTemplate: the params name a template this driver does not render.
	ErrTemplate = errors.New("compose: unknown template")
	// ErrParams: the params do not decode or fail a bound.
	ErrParams = errors.New("compose: invalid params")
	// ErrPath is the U1-2 path rule (design §2.3): a file name that leaves the
	// project directory or walks a symlink is refused with it.
	ErrPath = ops.ErrPath
	// ErrSecretValue: a credential arrived in plain text where only a
	// ${secret:name} reference is allowed. The value is in no error, summary
	// or Data.
	ErrSecretValue = errors.New("compose: plain-text credential")
	// ErrStack: the rendered stack is not ready for the requested compose
	// command (no compose file, no .env, no container for a service).
	ErrStack = errors.New("compose: the stack is not ready")
	// ErrPluginHost: the plugin archive's host is not in
	// PIPEROOM_SETUP_PLUGIN_HOSTS, or the URL is not an allowed form.
	ErrPluginHost = errors.New("compose: plugin host not allowed")
	// ErrPluginDigest: the downloaded archive does not carry the digest the
	// knowledge index pins.
	ErrPluginDigest = errors.New("compose: plugin digest mismatch")
	// ErrPluginArchive: the archive has an entry this driver refuses (a link,
	// a second top-level directory, an oversized file, a traversal).
	ErrPluginArchive = errors.New("compose: plugin archive refused")
	// ErrPluginDir: the target directory holds something plugin_install did
	// not put there, or another release of the same plugin.
	ErrPluginDir = errors.New("compose: plugin directory is occupied")
	// ErrDownload: the plugin download failed (transport, status, timeout).
	ErrDownload = errors.New("compose: plugin download failed")
)

// Rule names a refusal class. A validator failure makes the step T2 with the
// reason in the preview (row U1-3a, design §2.5); the plan layer (U1-6) reads
// the rule with RuleOf and does not parse the message.
const (
	RuleYAML       = "yaml"       // the document does not parse, or is not a compose mapping
	RuleTopLevel   = "top-level"  // an unknown top-level key (include, secrets, x-*)
	RuleTemplate   = "template"   // templates/params do not describe a renderable stack
	RuleImage      = "image"      // not in the knowledge allow-list, untagged or unpinned
	RulePrivilege  = "privilege"  // privileged, cap_add, devices, security_opt, pid, ipc, userns_mode, ...
	RuleMount      = "mount"      // a bind outside the project, the Docker socket, /, env_file, ...
	RulePort       = "port"       // a port not bound to 127.0.0.1 on a local target
	RuleBuild      = "build"      // a build: section
	RuleExtends    = "extends"    // extends
	RuleNetwork    = "network"    // network_mode other than bridge/none, top-level networks
	RuleField      = "field"      // a service field outside the allow-list of rendered fields
	RuleSecret     = "secret"     // a credential value in plain text
	RuleProperties = "properties" // an unknown properties file, key or value
)

// RuleError is a refusal with the rule it broke, the service it names ("" for
// a document-level or properties refusal) and a detail that never carries a
// credential value.
type RuleError struct {
	Rule    string
	Service string
	Detail  string
}

// Error names the rule, the service and the detail.
func (e *RuleError) Error() string {
	if e.Service != "" {
		return "compose: " + e.Rule + ": service " + q(e.Service) + ": " + e.Detail
	}
	return "compose: " + e.Rule + ": " + e.Detail
}

// RuleOf returns the rule err names, or "" when err is not a refusal.
func RuleOf(err error) string {
	var re *RuleError
	if errors.As(err, &re) {
		return re.Rule
	}
	return ""
}

// Is lets errors.Is(err, ErrSecretValue) match a RuleSecret refusal, so a
// caller can branch on a plain-text credential without parsing the rule.
func (e *RuleError) Is(target error) bool {
	return target == ErrSecretValue && e.Rule == RuleSecret
}

// ruleError builds a refusal.
func ruleError(rule, service, format string, args ...any) error {
	return &RuleError{Rule: rule, Service: service, Detail: fmt.Sprintf(format, args...)}
}

// maxQuoted bounds peer text (an image reference, a mount source, a file name)
// in an error: it is text from a proposal that ends up on an approver's screen.
const maxQuoted = 120

// q quotes peer text for an error: valid UTF-8, no control characters, at
// most maxQuoted bytes. strconv.Quote then makes it unambiguous.
func q(s string) string { return strconv.Quote(clip(s)) }

// clip cuts s to maxQuoted bytes on a rune boundary and replaces control
// characters, so a refusal cannot carry a terminal escape or a forged line.
func clip(s string) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
	if len(s) <= maxQuoted {
		return s
	}
	cut := maxQuoted
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
