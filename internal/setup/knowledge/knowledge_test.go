package knowledge

import (
	"errors"
	"strings"
	"testing"
)

// The shipped index is what the runner validates at start-up. If this test
// fails, the tables in images.go or plugins.go are corrupt and every runner
// that ships this package would refuse to start.
func TestCheckPassesOnShippedIndex(t *testing.T) {
	if err := Check(); err != nil {
		t.Fatalf("Check on the shipped index = %v", err)
	}
	if len(images) == 0 || len(plugins) == 0 {
		t.Fatalf("shipped index is empty: %d images, %d plugins", len(images), len(plugins))
	}
}

// A bad or mismatched digest fails the start-up check, so a corrupt entry can
// never reach a template or a download.
func TestValidateRejectsBadDigest(t *testing.T) {
	goodImg := func() []Image {
		out := make([]Image, len(images))
		copy(out, images)
		return out
	}
	cases := []struct {
		name    string
		mutate  func([]Image, []Plugin)
		wantErr string // substring of the error, for a stable assertion
	}{
		{"image digest not hex", func(im []Image, _ []Plugin) { im[0].Digest = "sha256:" + strings.Repeat("z", 64) }, "non-hex"},
		{"image digest wrong length", func(im []Image, _ []Plugin) { im[0].Digest = "sha256:abcd" }, "want 64"},
		{"image digest wrong algorithm", func(im []Image, _ []Plugin) { im[0].Digest = "sha1:f441be4331e281ebc554f194d90fafe7ba82086e" }, "must be sha256"},
		{"image digest missing algorithm", func(im []Image, _ []Plugin) {
			im[0].Digest = "b79f70f10a4aa97445f61e1c7b6cc8f484018f59d9a90cabc8135d3ae70ad90b"
		}, "missing algorithm"},
		{"image missing source", func(im []Image, _ []Plugin) { im[0].Source = "" }, "source must be"},
		{"image non-https source", func(im []Image, _ []Plugin) { im[0].Source = "http://quay.io/x" }, "https"},
		{"image missing fields", func(im []Image, _ []Plugin) { im[0].Name = "" }, "required"},
		{"duplicate image", func(im []Image, _ []Plugin) { im[1] = im[0] }, "duplicate image"},
		{"plugin digest not hex", func(_ []Image, pl []Plugin) { pl[0].Digest = "sha512:" + strings.Repeat("z", 128) }, "non-hex"},
		{"plugin digest wrong length", func(_ []Image, pl []Plugin) { pl[0].Digest = "sha1:abcd" }, "want 40"},
		{"plugin digest unknown algorithm", func(_ []Image, pl []Plugin) { pl[0].Digest = "md5:abcd" }, "unknown algorithm"},
		{"plugin missing source", func(_ []Image, pl []Plugin) { pl[0].Source = "" }, "source must be"},
		{"duplicate plugin", func(_ []Image, pl []Plugin) { pl[1] = pl[0] }, "duplicate plugin"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			im := goodImg()
			pl := make([]Plugin, len(plugins))
			copy(pl, plugins)
			c.mutate(im, pl)
			err := Validate(im, pl)
			if err == nil {
				t.Fatal("Validate = nil, want a non-nil error")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("Validate error = %q, want it to contain %q", err, c.wantErr)
			}
		})
	}
}

// The table test of the "Done when": every Debezium release in the index
// resolves to its plugin digest, for every connector it claims, and the
// digest's source URL is the artifact's own digest file on Maven Central.
func TestResolvePluginPerRelease(t *testing.T) {
	releases := Releases()
	wantReleases := map[string]bool{"3.2.0.Final": true, "3.1.3.Final": true, "3.0.7.Final": true, "2.7.2.Final": true}
	if len(releases) != len(wantReleases) {
		t.Fatalf("indexed releases = %v, want exactly %d release lines", releases, len(wantReleases))
	}
	for _, release := range releases {
		if !wantReleases[release] {
			t.Errorf("release %s in index is not one of the stated coverage lines", release)
		}
		for _, connector := range Connectors(release) {
			got, err := ResolvePlugin(release, connector)
			if err != nil {
				t.Errorf("ResolvePlugin(%s, %s) = %v", release, connector, err)
				continue
			}
			wantArtifact := "debezium-connector-" + connector
			if got.Artifact != wantArtifact {
				t.Errorf("ResolvePlugin(%s, %s).Artifact = %s, want %s", release, connector, got.Artifact, wantArtifact)
			}
			// The pinned digest must equal the table's own entry and the
			// source URL must be that artifact's digest file.
			if want := pluginDigest(release, connector); got.Digest != want {
				t.Errorf("ResolvePlugin(%s, %s).Digest = %s, want %s", release, connector, got.Digest, want)
			}
			suffix := got.ArtifactURL() + ".sha512"
			if strings.HasPrefix(got.Digest, "sha1:") {
				suffix = got.ArtifactURL() + ".sha1"
			}
			if got.Source != suffix {
				t.Errorf("ResolvePlugin(%s, %s).Source = %s, want %s", release, connector, got.Source, suffix)
			}
		}
	}
}

func pluginDigest(release, connector string) string {
	for _, p := range plugins {
		if p.Release == release && p.Connector == connector {
			return p.Digest
		}
	}
	return ""
}

func TestResolvePluginUnknown(t *testing.T) {
	if _, err := ResolvePlugin("1.9.8.Final", "postgres"); !errors.Is(err, ErrUnknownRelease) {
		t.Errorf("unknown release: err = %v, want ErrUnknownRelease", err)
	}
	if _, err := ResolvePlugin("3.2.0.Final", "db2"); !errors.Is(err, ErrUnknownPlugin) {
		t.Errorf("unknown connector: err = %v, want ErrUnknownPlugin", err)
	}
}

func TestLookupAndVerifyImage(t *testing.T) {
	got, err := LookupImage("debezium/connect", "3.2.0.Final")
	if err != nil {
		t.Fatalf("LookupImage(debezium/connect, 3.2.0.Final) = %v", err)
	}
	if got.Name != "connect" || got.Registry != "quay.io" {
		t.Errorf("connect image = %+v", got)
	}
	if want := "quay.io/debezium/connect:3.2.0.Final@" + got.Digest; got.Ref() != want {
		t.Errorf("Ref = %s, want %s", got.Ref(), want)
	}
	// The "library/" alias of a Docker official image resolves.
	if _, err := LookupImage("library/postgres", "17"); err != nil {
		t.Errorf("LookupImage(library/postgres, 17) = %v", err)
	}
	if _, err := LookupImage("no/such", "1.0"); !errors.Is(err, ErrUnknownImage) {
		t.Errorf("unknown image: err = %v, want ErrUnknownImage", err)
	}
	// The pinned digest verifies; a mismatch fails with ErrDigestMismatch.
	if _, err := VerifyImage("debezium/connect", "3.2.0.Final", got.Digest); err != nil {
		t.Errorf("VerifyImage with the pinned digest = %v", err)
	}
	if _, err := VerifyImage("debezium/connect", "3.2.0.Final", "sha256:"+strings.Repeat("0", 64)); !errors.Is(err, ErrDigestMismatch) {
		t.Errorf("VerifyImage with a wrong digest: err = %v, want ErrDigestMismatch", err)
	}
	// A malformed digest never matches, even the pinned image.
	if _, err := VerifyImage("debezium/connect", "3.2.0.Final", "not-a-digest"); !errors.Is(err, ErrDigestMismatch) {
		t.Errorf("VerifyImage with a malformed digest: err = %v, want ErrDigestMismatch", err)
	}
}
