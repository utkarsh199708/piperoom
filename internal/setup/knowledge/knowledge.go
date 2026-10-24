// Package knowledge is Piperoom's embedded setup knowledge index. It holds
// the allow-list of container images the setup templates (Compose, Strimzi,
// Debezium Server) may reference — each pinned to the registry manifest
// digest of an exact tag — and the Maven Central digests of the Debezium
// connector plugin archives, per release. It also holds the SMT and sink
// index (index.go, data in index/*.yaml): the transforms, predicates and
// sinks a setup config may name, with keys, defaults, verified release
// ranges, the checks each implies and how each is installed, and the
// tuning entries (tuning.go): worker, connector and client settings with
// their defaults and the symptoms that call for changing them.
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

// knownRegistries is the allow-list of image registries an entry may cite.
var knownRegistries = map[string]bool{
	"docker.io":         true,
	"quay.io":           true,
	"mcr.microsoft.com": true,
}

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
// is always "sha512:<128 hex>": the SHA-512 of the -plugin.tar.gz bytes, read
// from Maven Central's .sha512 file where it exists (Debezium 3.1 onwards) or
// computed over the downloaded archive bytes for the older releases.
type Plugin struct {
	Release   string // Debezium release, e.g. "3.2.0.Final"
	Connector string // short source name, e.g. "postgres", "mysql", "oracle"
	Artifact  string // Maven artifact id, e.g. "debezium-connector-postgres"
	Digest    string // "sha512:<128 hex>"
	Source    string // Maven Central URL: the .sha512 file, or the archive itself
}

// ArtifactURL is the plugin archive the digest pins: the -plugin.tar.gz of
// the artifact, the unit plugin_install downloads and unpacks.
func (p Plugin) ArtifactURL() string {
	return "https://repo1.maven.org/maven2/io/debezium/" + p.Artifact + "/" +
		p.Release + "/" + p.Artifact + "-" + p.Release + "-plugin.tar.gz"
}

// parseDigest splits "algo:hex" and checks the hex is well-formed for the
// algorithm. sha256 is the registry manifest digest; sha512 is the plugin
// archive digest. sha1 remains parseable so a stray or legacy digest is
// rejected with a clear error rather than a panic.
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

// registryManifestURL derives the manifest URL an image's Source must equal,
// from its registry, repo and tag. Docker official images (a repo with no
// slash) live under registry-1.docker.io/v2/library/; everything else uses
// the repo path as-is under the registry's /v2/ namespace.
func registryManifestURL(im Image) string {
	switch im.Registry {
	case "quay.io":
		return "https://quay.io/v2/" + im.Repo + "/manifests/" + im.Tag
	case "mcr.microsoft.com":
		return "https://mcr.microsoft.com/v2/" + im.Repo + "/manifests/" + im.Tag
	case "docker.io":
		if strings.Contains(im.Repo, "/") {
			return "https://registry-1.docker.io/v2/" + im.Repo + "/manifests/" + im.Tag
		}
		return "https://registry-1.docker.io/v2/library/" + im.Repo + "/manifests/" + im.Tag
	default:
		return ""
	}
}

// Validate checks the entries for structural validity and returns the first
// error it finds. An image must have name, registry, repo, tag, a sha256
// manifest digest and a Source equal to its registry's manifest URL for that
// repo and tag; the registry must be allow-listed and no two images may share
// a name or a (repo, tag) pair. A plugin must have release, connector,
// artifact and a sha512 digest; its Artifact must be
// "debezium-connector-"+connector, its Source must be the artifact's .sha512
// or archive URL, and no two plugins may share a (release, connector) pair.
// It is the start-up check; Check calls it over the shipped tables.
func Validate(images []Image, plugins []Plugin) error {
	seenImg := make(map[string]struct{}, len(images))
	seenName := make(map[string]struct{}, len(images))
	for _, im := range images {
		if im.Name == "" || im.Registry == "" || im.Repo == "" || im.Tag == "" {
			return fmt.Errorf("knowledge: image %q: name, registry, repo and tag are required", im.Name)
		}
		if !knownRegistries[im.Registry] {
			return fmt.Errorf("knowledge: image %s/%s:%s: registry %q is not allow-listed (docker.io, quay.io or mcr.microsoft.com)", im.Registry, im.Repo, im.Tag, im.Registry)
		}
		if alg, _, err := parseDigest(im.Digest); err != nil {
			return fmt.Errorf("knowledge: image %s/%s:%s: %v", im.Registry, im.Repo, im.Tag, err)
		} else if alg != "sha256" {
			return fmt.Errorf("knowledge: image %s/%s:%s: digest must be sha256, got %s", im.Registry, im.Repo, im.Tag, alg)
		}
		if want := registryManifestURL(im); im.Source != want {
			return fmt.Errorf("knowledge: image %s/%s:%s: source must be %s, got %s", im.Registry, im.Repo, im.Tag, want, im.Source)
		}
		if _, dup := seenName[im.Name]; dup {
			return fmt.Errorf("knowledge: duplicate image name %q", im.Name)
		}
		seenName[im.Name] = struct{}{}
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
		if alg, _, err := parseDigest(p.Digest); err != nil {
			return fmt.Errorf("knowledge: plugin %s %s: %v", p.Release, p.Connector, err)
		} else if alg != "sha512" {
			return fmt.Errorf("knowledge: plugin %s %s: digest must be sha512, got %s", p.Release, p.Connector, alg)
		}
		if want := "debezium-connector-" + p.Connector; p.Artifact != want {
			return fmt.Errorf("knowledge: plugin %s %s: artifact must be %q, got %q", p.Release, p.Connector, want, p.Artifact)
		}
		if p.Source != p.ArtifactURL()+".sha512" && p.Source != p.ArtifactURL() {
			return fmt.Errorf("knowledge: plugin %s %s: source must be the artifact's .sha512 or archive URL, got %s", p.Release, p.Connector, p.Source)
		}
		key := p.Release + "\x00" + p.Connector
		if _, dup := seenPl[key]; dup {
			return fmt.Errorf("knowledge: duplicate plugin %s %s", p.Release, p.Connector)
		}
		seenPl[key] = struct{}{}
	}
	return nil
}

// Check validates the shipped index — the image and plugin tables, the
// SMT, sink and tuning index, and the embedded-engine rule table with its
// reference projects (ValidateEmbedded) — and fails when it is empty. Call
// it once at runner start-up; a non-nil error means the shipped data is
// corrupt and the runner must refuse to start.
func Check() error {
	if len(images) == 0 || len(plugins) == 0 {
		return errors.New("knowledge: index is empty")
	}
	if err := Validate(images, plugins); err != nil {
		return err
	}
	if _, err := LoadIndex(); err != nil {
		return err
	}
	return ValidateEmbedded()
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

// ImageByName returns the allow-listed image for a stable family name ("connect",
// "server", "kafka", "schema-registry", "postgres", "mysql", "sqlserver",
// "mongodb", "oracle"). It returns ErrUnknownImage when the name is not
// allow-listed.
func ImageByName(name string) (Image, error) {
	for _, im := range images {
		if im.Name == name {
			return im, nil
		}
	}
	return Image{}, fmt.Errorf("%w: name %q", ErrUnknownImage, name)
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
