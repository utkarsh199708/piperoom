// Package knowledge is Piperoom's embedded setup knowledge index. It holds
// the allow-list of container images the setup templates (Compose, Strimzi,
// Debezium Server) may reference — each pinned to the registry manifest
// digest of an exact tag — and the Maven Central digests of the Debezium
// connector plugin archives, per release.
//
// Every entry is pinned, not guessed. Each Image.Digest was fetched from the
// image's registry (Docker Hub, Quay, or the Microsoft Container Registry)
// with the token + manifest flow and read from the Docker-Content-Digest
// header; each Plugin.Digest was fetched from Maven Central
// (repo1.maven.org). Every entry cites the exact URL it was read from in its
// Source field. A digest that could not be verified against its source is not
// present here, so the index intentionally covers fewer Debezium releases
// than are published; the coverage is stated in images.go and plugins.go.
//
// Start-up rule: the runner calls Check once at start and refuses to start
// on a non-nil error, so a malformed, duplicated or unsourced entry can
// never reach a template or a download. Consumers resolve references with
// LookupImage, VerifyImage and ResolvePlugin; none returns an entry whose
// digest has not passed Validate.
package knowledge

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors. Compare with errors.Is, never on text.
var (
	// ErrUnknownImage is returned when a repository and tag are not in the
	// image allow-list.
	ErrUnknownImage = errors.New("setup/knowledge: image not allow-listed")
	// ErrUnknownRelease is returned when a Debezium release has no plugin
	// digests in the index.
	ErrUnknownRelease = errors.New("setup/knowledge: release not in the index")
	// ErrUnknownPlugin is returned when a release is known but a connector is
	// not indexed for it.
	ErrUnknownPlugin = errors.New("setup/knowledge: connector not in the index")
	// ErrDigestMismatch is returned when a resolved entry exists but its
	// pinned digest differs from the one supplied.
	ErrDigestMismatch = errors.New("setup/knowledge: digest mismatch")
)

// Image is one pinned container image in the allow-list. Digest is the
// registry manifest digest (the multi-arch index digest where the image is
// multi-arch), always "sha256:<64 hex>". Repo is the image path without the
// registry hostname; Docker official images use the short name ("postgres",
// "mysql", "mongo") and are also accepted with the "library/" prefix.
type Image struct {
	Name     string // stable key used by the templates, e.g. "connect", "kafka"
	Registry string // "docker.io", "quay.io", "mcr.microsoft.com"
	Repo     string // e.g. "debezium/connect", "postgres", "mssql/server"
	Tag      string // e.g. "3.2.0.Final", "17", "2022-latest"
	Digest   string // "sha256:<64 hex>"
	Source   string // registry URL the digest was fetched from
}

// Ref returns the pinned pull reference:
// "quay.io/debezium/connect:3.2.0.Final@sha256:...".
func (i Image) Ref() string {
	return i.Registry + "/" + i.Repo + ":" + i.Tag + "@" + i.Digest
}

// Plugin is one Debezium connector plugin archive, pinned by digest. Digest
// is "sha512:<128 hex>" for releases Maven Central publishes SHA-512 for
// (3.1 and later) and "sha1:<40 hex>" for the older releases, whose .sha1 is
// the strongest digest Maven Central serves.
type Plugin struct {
	Release   string // Debezium release, e.g. "3.2.0.Final"
	Connector string // short source name, e.g. "postgres", "mysql", "oracle"
	Artifact  string // Maven artifact id, e.g. "debezium-connector-postgres"
	Digest    string // "sha512:<128 hex>" or "sha1:<40 hex>"
	Source    string // Maven Central URL the digest was fetched from
}

// ArtifactURL is the plugin archive the digest pins: the -plugin.tar.gz of
// the artifact, the unit plugin_install downloads and unpacks.
func (p Plugin) ArtifactURL() string {
	return "https://repo1.maven.org/maven2/io/debezium/" + p.Artifact + "/" +
		p.Release + "/" + p.Artifact + "-" + p.Release + "-plugin.tar.gz"
}

// parseDigest splits "algo:hex" and checks the hex is well-formed for the
// algorithm. sha256 is the registry manifest digest; sha512 and sha1 are the
// two Maven Central digest kinds for these artifacts.
func parseDigest(s string) (alg, hex string, err error) {
	alg, hex, ok := strings.Cut(s, ":")
	if !ok {
		return "", "", fmt.Errorf("digest %q: missing algorithm prefix", s)
	}
	want := 0
	switch alg {
	case "sha256":
		want = 64
	case "sha512":
		want = 128
	case "sha1":
		want = 40
	default:
		return "", "", fmt.Errorf("digest %q: unknown algorithm %q", s, alg)
	}
	if len(hex) != want {
		return "", "", fmt.Errorf("%s digest is %d hex chars, want %d", alg, len(hex), want)
	}
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return "", "", fmt.Errorf("digest %q: non-hex byte %q", s, hex[i])
		}
	}
	return alg, hex, nil
}

// Validate checks the entries for structural validity and returns the first
// error it finds: a missing field, a malformed digest (an image must be a
// sha256 and a plugin a sha512, sha256 or sha1), a non-https source, or a
// duplicate image (repo, tag) or plugin (release, connector). It is the
// start-up check; Check calls it over the shipped tables.
func Validate(images []Image, plugins []Plugin) error {
	seenImg := make(map[string]struct{}, len(images))
	for _, im := range images {
		if im.Name == "" || im.Registry == "" || im.Repo == "" || im.Tag == "" {
			return fmt.Errorf("knowledge: image %q: name, registry, repo and tag are required", im.Name)
		}
		if alg, _, err := parseDigest(im.Digest); err != nil {
			return fmt.Errorf("knowledge: image %s/%s:%s: %v", im.Registry, im.Repo, im.Tag, err)
		} else if alg != "sha256" {
			return fmt.Errorf("knowledge: image %s/%s:%s: digest must be sha256, got %s", im.Registry, im.Repo, im.Tag, alg)
		}
		if !strings.HasPrefix(im.Source, "https://") {
			return fmt.Errorf("knowledge: image %s/%s:%s: source must be an https URL", im.Registry, im.Repo, im.Tag)
		}
		key := im.Repo + "\x00" + im.Tag
		if _, dup := seenImg[key]; dup {
			return fmt.Errorf("knowledge: duplicate image %s:%s", im.Repo, im.Tag)
		}
		seenImg[key] = struct{}{}
	}
	seenPl := make(map[string]struct{}, len(plugins))
	for _, p := range plugins {
		if p.Release == "" || p.Connector == "" || p.Artifact == "" {
			return fmt.Errorf("knowledge: plugin %q/%q: release, connector and artifact are required", p.Release, p.Connector)
		}
		if _, _, err := parseDigest(p.Digest); err != nil {
			return fmt.Errorf("knowledge: plugin %s %s: %v", p.Release, p.Connector, err)
		}
		if !strings.HasPrefix(p.Source, "https://") {
			return fmt.Errorf("knowledge: plugin %s %s: source must be an https URL", p.Release, p.Connector)
		}
		key := p.Release + "\x00" + p.Connector
		if _, dup := seenPl[key]; dup {
			return fmt.Errorf("knowledge: duplicate plugin %s %s", p.Release, p.Connector)
		}
		seenPl[key] = struct{}{}
	}
	return nil
}

// Check validates the shipped index and fails when it is empty. Call it once
// at runner start-up; a non-nil error means the shipped tables are corrupt
// and the runner must refuse to start.
func Check() error {
	if len(images) == 0 || len(plugins) == 0 {
		return errors.New("knowledge: index is empty")
	}
	return Validate(images, plugins)
}

// LookupImage returns the allow-listed image for a repository and tag. repo
// is the image path without the registry hostname ("debezium/connect",
// "postgres"); the "library/" prefix of a Docker official image is accepted.
// It returns ErrUnknownImage when the pair is not allow-listed.
func LookupImage(repo, tag string) (Image, error) {
	for _, im := range images {
		if im.Repo == repo && im.Tag == tag {
			return im, nil
		}
	}
	if short, ok := strings.CutPrefix(repo, "library/"); ok {
		for _, im := range images {
			if im.Repo == short && im.Tag == tag {
				return im, nil
			}
		}
	}
	return Image{}, fmt.Errorf("%w: %s:%s", ErrUnknownImage, repo, tag)
}

// VerifyImage is LookupImage plus a digest check: it returns ErrDigestMismatch
// when the image is allow-listed but its pinned digest differs from digest.
// The comparison is case-insensitive on the hex and constant-time.
func VerifyImage(repo, tag, digest string) (Image, error) {
	im, err := LookupImage(repo, tag)
	if err != nil {
		return Image{}, err
	}
	if !digestEqual(im.Digest, digest) {
		return Image{}, fmt.Errorf("%w: %s:%s is pinned to %s, got %s",
			ErrDigestMismatch, im.Repo, im.Tag, im.Digest, digest)
	}
	return im, nil
}

// ResolvePlugin returns the pinned plugin archive for a Debezium release and
// a connector short name ("postgres", "mysql", "sqlserver", "mongodb",
// "oracle"). It returns ErrUnknownRelease when the release is not indexed and
// ErrUnknownPlugin when the release is known but the connector is not.
func ResolvePlugin(release, connector string) (Plugin, error) {
	for _, p := range plugins {
		if p.Release == release && p.Connector == connector {
			return p, nil
		}
	}
	if !hasRelease(release) {
		return Plugin{}, fmt.Errorf("%w: %s", ErrUnknownRelease, release)
	}
	return Plugin{}, fmt.Errorf("%w: %s (release %s)", ErrUnknownPlugin, connector, release)
}

// Releases returns the Debezium releases the index pins plugin digests for,
// in table order, each once.
func Releases() []string {
	seen := make(map[string]struct{})
	var out []string
	for _, p := range plugins {
		if _, dup := seen[p.Release]; dup {
			continue
		}
		seen[p.Release] = struct{}{}
		out = append(out, p.Release)
	}
	return out
}

// Connectors returns the connector short names indexed for a release, in
// table order, each once.
func Connectors(release string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, p := range plugins {
		if p.Release != release {
			continue
		}
		if _, dup := seen[p.Connector]; dup {
			continue
		}
		seen[p.Connector] = struct{}{}
		out = append(out, p.Connector)
	}
	return out
}

func hasRelease(release string) bool {
	for _, p := range plugins {
		if p.Release == release {
			return true
		}
	}
	return false
}

// digestEqual reports whether two "algo:hex" digests match: same algorithm
// and equal hex, compared case-insensitively and in constant time.
func digestEqual(a, b string) bool {
	algA, hexA, errA := parseDigest(a)
	algB, hexB, errB := parseDigest(b)
	if errA != nil || errB != nil || algA != algB {
		return false
	}
	return subtle.ConstantTimeCompare(
		[]byte(strings.ToLower(hexA)), []byte(strings.ToLower(hexB))) == 1
}
