package compose

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
)

const pluginTop = "debezium-connector-postgres"

// tarEntry is one entry of a test archive.
type tarEntry struct {
	name     string
	body     string
	typeflag byte
	link     string
	mode     int64
}

// buildArchive builds a .tar.gz from entries.
func buildArchive(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o644
		}
		hdr := &tar.Header{Name: e.name, Mode: mode, Typeflag: e.typeflag, Linkname: e.link}
		if e.typeflag == tar.TypeReg || e.typeflag == tar.TypeRegA || e.typeflag == 0 {
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing tar header %q: %v", e.name, err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("writing tar body %q: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing gzip: %v", err)
	}
	return buf.Bytes()
}

// digestOf is the knowledge index's digest form for archive bytes.
func digestOf(b []byte) string {
	sum := sha512.Sum512(b)
	return "sha512:" + hex.EncodeToString(sum[:])
}

// archiveServer serves one archive and counts requests.
func archiveServer(t *testing.T, archive []byte) (*httptest.Server, *int32) {
	t.Helper()
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&reqs, 1)
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(archive)
	}))
	t.Cleanup(srv.Close)
	return srv, &reqs
}

// serverHost is the host:port form PIPEROOM_SETUP_PLUGIN_HOSTS may carry.
func serverHost(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// pluginRegistry is a registry with the driver registered against a resolver
// and an allow-list; no Docker and no registry Execute is involved.
func pluginRegistry(t *testing.T, dir string, opts Options) *ops.Registry {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := Register(reg, opts); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	return reg
}

// artifact returns a resolver that always answers with art.
func artifact(art PluginArtifact) PluginResolver {
	return func(string, string) (PluginArtifact, error) { return art, nil }
}

func TestPluginInstall(t *testing.T) {
	archive := buildArchive(t, []tarEntry{
		{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755},
		{name: pluginTop + "/debezium-connector-postgres-3.2.0.Final.jar", body: "jar bytes"},
		{name: pluginTop + "/lib/dep.jar", body: "dep bytes"},
	})
	srv, reqs := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})

	res, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	if err != nil {
		t.Fatalf("plugin_install: %v (summary %q)", err, res.Summary)
	}
	d := data(t, res)
	if d["changed"] != true || d["files"] != float64(2) {
		t.Errorf("changed/files = %v/%v, want true/2", d["changed"], d["files"])
	}
	if d["dir"] != PluginDir+"/"+pluginTop {
		t.Errorf("dir = %v", d["dir"])
	}
	if atomic.LoadInt32(reqs) != 1 {
		t.Errorf("downloads = %d, want 1", atomic.LoadInt32(reqs))
	}
	for _, name := range []string{"debezium-connector-postgres-3.2.0.Final.jar", "lib/dep.jar"} {
		path := filepath.Join(dir, PluginDir, pluginTop, filepath.FromSlash(name))
		if _, err := os.Stat(path); err != nil {
			t.Errorf("installed file %s: %v", name, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, PluginDir, pluginTop, pluginMarker))
	if err != nil {
		t.Fatalf("reading the marker: %v", err)
	}
	var marker pluginMarkerFile
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatalf("decoding the marker: %v", err)
	}
	if marker.Digest != art.Digest || marker.Release != "3.2.0.Final" || marker.Connector != "postgres" {
		t.Errorf("marker = %+v", marker)
	}

	// A second run is a no-op: the digest is the marker's, so nothing is
	// downloaded again.
	res, err = runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	if err != nil {
		t.Fatalf("second plugin_install: %v", err)
	}
	if data(t, res)["changed"] != false {
		t.Error("the second install reported a change")
	}
	if !strings.Contains(res.Summary, "already installed") {
		t.Errorf("second summary = %q", res.Summary)
	}
	if atomic.LoadInt32(reqs) != 1 {
		t.Errorf("downloads after the second run = %d, want 1", atomic.LoadInt32(reqs))
	}
}

// TestPluginInstallOneDirectoryPerPlugin: two connectors land in two sibling
// directories, each with its own marker.
func TestPluginInstallOneDirectoryPerPlugin(t *testing.T) {
	pg := buildArchive(t, []tarEntry{
		{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755},
		{name: pluginTop + "/pg.jar", body: "pg"},
	})
	my := buildArchive(t, []tarEntry{
		{name: "debezium-connector-mysql/", typeflag: tar.TypeDir, mode: 0o755},
		{name: "debezium-connector-mysql/my.jar", body: "my"},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "mysql") {
			_, _ = w.Write(my)
			return
		}
		_, _ = w.Write(pg)
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	resolve := func(_, connector string) (PluginArtifact, error) {
		if connector == "mysql" {
			return PluginArtifact{URL: srv.URL + "/mysql.tar.gz", Digest: digestOf(my), Dir: "debezium-connector-mysql"}, nil
		}
		return PluginArtifact{URL: srv.URL + "/pg.tar.gz", Digest: digestOf(pg), Dir: pluginTop}, nil
	}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: resolve})
	for _, p := range []PluginInstallParams{
		{Release: "3.2.0.Final", Connector: "postgres"},
		{Release: "3.2.0.Final", Connector: "mysql"},
	} {
		if _, err := runOp(t, reg, "plugin_install", p); err != nil {
			t.Fatalf("plugin_install(%s): %v", p.Connector, err)
		}
	}
	for _, name := range []string{pluginTop, "debezium-connector-mysql"} {
		if fi, err := os.Stat(filepath.Join(dir, PluginDir, name)); err != nil || !fi.IsDir() {
			t.Errorf("%s was not installed as its own directory: %v", name, err)
		}
	}
}

// TestPluginHostNotAllowed is the row's download allow-list: an archive URL
// outside PIPEROOM_SETUP_PLUGIN_HOSTS is refused before any request.
func TestPluginHostNotAllowed(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, reqs := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{"repo1.maven.org"}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrPluginHost) {
		t.Fatalf("plugin_install from a host outside the allow-list = %v, want ErrPluginHost", err)
	}
	if got := atomic.LoadInt32(reqs); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
	if entries, err := os.ReadDir(filepath.Join(dir, PluginDir)); err == nil && len(entries) != 0 {
		t.Fatalf("a refused install left files behind: %v", entries)
	}
}

func TestPluginDigestMismatch(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, _ := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf([]byte("something else")), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrPluginDigest) {
		t.Fatalf("plugin_install with a wrong digest = %v, want ErrPluginDigest", err)
	}
	if _, err := os.Stat(filepath.Join(dir, PluginDir, pluginTop)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a digest mismatch still created the plugin directory")
	}
	entries, err := os.ReadDir(filepath.Join(dir, PluginDir))
	if err != nil {
		t.Fatalf("reading the plugin directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("a digest mismatch left files behind: %v", entries)
	}
}

func TestPluginArchiveRefusals(t *testing.T) {
	cases := []struct {
		name    string
		entries []tarEntry
	}{
		{"a traversal", []tarEntry{{name: pluginTop + "/../evil.jar", body: "x"}}},
		{"an absolute path", []tarEntry{{name: "/etc/passwd", body: "x"}}},
		{"a symbolic link", []tarEntry{{name: pluginTop + "/link.jar", typeflag: tar.TypeSymlink, link: "/etc/passwd"}}},
		{"a hard link", []tarEntry{{name: pluginTop + "/hard.jar", typeflag: tar.TypeLink, link: pluginTop + "/x.jar"}}},
		{"a second plugin directory", []tarEntry{
			{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755},
			{name: pluginTop + "/a.jar", body: "a"},
			{name: "debezium-connector-mysql/b.jar", body: "b"},
		}},
		{"a file at the top level", []tarEntry{{name: "README.txt", body: "x"}}},
		{"an empty archive", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archive := buildArchive(t, tc.entries)
			srv, _ := archiveServer(t, archive)
			dir := t.TempDir()
			art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
			reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
			_, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
			if !errors.Is(err, ErrPluginArchive) {
				t.Fatalf("plugin_install = %v, want ErrPluginArchive", err)
			}
			if _, err := os.Stat(filepath.Join(dir, PluginDir, pluginTop)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("a refused archive created the plugin directory")
			}
			entries, readErr := os.ReadDir(filepath.Join(dir, PluginDir))
			if readErr != nil {
				t.Fatalf("reading the plugin directory: %v", readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("a refused archive left files behind: %v", entries)
			}
		})
	}
}

// TestPluginOccupiedDirectory: plugin_install never overwrites a directory it
// did not create.
func TestPluginOccupiedDirectory(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, reqs := archiveServer(t, archive)
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, PluginDir, pluginTop), 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, PluginDir, pluginTop, "keep.jar")
	if err := os.WriteFile(keep, []byte("hand copied"), 0o644); err != nil {
		t.Fatal(err)
	}
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrPluginDir) {
		t.Fatalf("plugin_install over a foreign directory = %v, want ErrPluginDir", err)
	}
	if atomic.LoadInt32(reqs) != 0 {
		t.Error("the op downloaded before checking the target directory")
	}
	raw, err := os.ReadFile(keep)
	if err != nil || string(raw) != "hand copied" {
		t.Fatalf("the occupied directory was modified: %q, %v", raw, err)
	}
}

func TestPluginRedirectRefused(t *testing.T) {
	var other int32
	otherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&other, 1)
		_, _ = w.Write([]byte("should never be fetched"))
	}))
	t.Cleanup(otherSrv.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, otherSrv.URL+"/plugin.tar.gz", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf([]byte("x")), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrDownload) {
		t.Fatalf("plugin_install through a redirect = %v, want ErrDownload", err)
	}
	if atomic.LoadInt32(&other) != 0 {
		t.Fatal("the redirect target was fetched")
	}
}

// TestPluginPlainHTTP: a network archive must be https; only a loopback test
// server may speak http.
func TestPluginPlainHTTP(t *testing.T) {
	dir := t.TempDir()
	art := PluginArtifact{URL: "http://repo1.maven.org/x.tar.gz", Digest: digestOf([]byte("x")), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{"repo1.maven.org"}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrPluginHost) {
		t.Fatalf("plugin_install over plain http = %v, want ErrPluginHost", err)
	}
}

func TestPluginParams(t *testing.T) {
	reg := pluginRegistry(t, t.TempDir(), Options{})
	cases := []struct {
		name   string
		params PluginInstallParams
		want   error
	}{
		{"a space in the release", PluginInstallParams{Release: "3.2.0 Final", Connector: "postgres"}, ErrParams},
		{"a path as the connector", PluginInstallParams{Release: "3.2.0.Final", Connector: "../postgres"}, ErrParams},
		{"an unindexed release", PluginInstallParams{Release: "9.9.9.Final", Connector: "postgres"}, knowledge.ErrUnknownRelease},
		{"an unindexed connector", PluginInstallParams{Release: "3.2.0.Final", Connector: "sqlite"}, knowledge.ErrUnknownPlugin},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runOp(t, reg, "plugin_install", tc.params); !errors.Is(err, tc.want) {
				t.Fatalf("plugin_install(%v) = %v, want %v", tc.params, err, tc.want)
			}
		})
	}
}

func TestPluginHostEntries(t *testing.T) {
	cases := []struct {
		name  string
		hosts []string
		ok    bool
	}{
		{"plain host", []string{"repo1.maven.org"}, true},
		{"host and port", []string{"127.0.0.1:8080"}, true},
		{"uppercase", []string{"Repo1.Maven.ORG"}, true},
		{"empty means the default", nil, true},
		{"a scheme", []string{"https://repo1.maven.org"}, false},
		{"a path", []string{"repo1.maven.org/maven2"}, false},
		{"user info", []string{"user@repo1.maven.org"}, false},
		{"a bad port", []string{"127.0.0.1:http"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePluginHosts(tc.hosts)
			if tc.ok {
				if err != nil {
					t.Fatalf("normalizePluginHosts(%q) = %v", tc.hosts, err)
				}
				if len(got) == 0 {
					t.Fatal("normalizePluginHosts returned no host")
				}
				return
			}
			if err == nil {
				t.Fatalf("normalizePluginHosts(%q) passed: %v", tc.hosts, got)
			}
		})
	}
	if hosts, err := normalizePluginHosts([]string{"Repo1.Maven.ORG"}); err != nil || hosts[0] != "repo1.maven.org" {
		t.Fatalf("hosts were not lower-cased: %v, %v", hosts, err)
	}
}

// TestPluginInstallDirectRefusal keeps the "downloads only from the allow-list"
// promise true when the knowledge index itself named another host: the URL
// check is on the resolved URL, not on the resolver.
func TestPluginInstallDirectRefusal(t *testing.T) {
	dir := t.TempDir()
	art := PluginArtifact{URL: "https://evil.example/plugin.tar.gz", Digest: digestOf([]byte("x")), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{"repo1.maven.org"}, PluginArtifact: artifact(art)})
	_, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	if !errors.Is(err, ErrPluginHost) {
		t.Fatalf("plugin_install from an unlisted host = %v, want ErrPluginHost", err)
	}
}

// TestPluginMarkerMismatch: a second release of the same plugin in the same
// directory is refused rather than replaced.
func TestPluginMarkerMismatch(t *testing.T) {
	first := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, _ := archiveServer(t, first)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(first), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); err != nil {
		t.Fatalf("first install: %v", err)
	}
	second := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/b.jar", body: "b"}})
	art2 := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(second), Dir: pluginTop}
	reg2 := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art2)})
	if _, err := runOp(t, reg2, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); !errors.Is(err, ErrPluginDir) {
		t.Fatalf("second release into the same directory = %v, want ErrPluginDir", err)
	}
}

// TestPluginNoLeftoverTempFiles: the download and extraction temp names never
// survive, a success included.
func TestPluginNoLeftoverTempFiles(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, _ := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	if _, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"}); err != nil {
		t.Fatalf("plugin_install: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, PluginDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Errorf("temporary entry %q survived", e.Name())
		}
	}
}

// TestPluginReceipt keeps the receipt's target a query-free URL.
func TestPluginReceipt(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, _ := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	res, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	if err != nil {
		t.Fatalf("plugin_install: %v", err)
	}
	if res.Receipt.Kind != "http" || !strings.HasSuffix(res.Receipt.Target, "/plugin.tar.gz") {
		t.Fatalf("receipt = %+v", res.Receipt)
	}
	if strings.Contains(res.Receipt.Target, "?") {
		t.Fatalf("receipt target carries a query: %q", res.Receipt.Target)
	}
}

// TestPluginInstallThroughRegistryDirectExecute proves the op also works when
// called with a hand-built Env, the way an integration test calls it with a
// longer deadline than the catalogue's default.
func TestPluginInstallThroughRegistryDirectExecute(t *testing.T) {
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: "a"}})
	srv, _ := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	op, ok := reg.Get("plugin_install")
	if !ok {
		t.Fatal("plugin_install is not registered")
	}
	confine, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = confine.Close() })
	raw, _ := json.Marshal(PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	env := ops.Env{ProjectDir: confine.Dir(), Confine: confine, MountDirs: []string{PluginDir}}
	res, err := op.Execute(context.Background(), raw, env)
	if err != nil {
		t.Fatalf("direct Execute: %v", err)
	}
	if !strings.Contains(res.Summary, "installed postgres") {
		t.Errorf("summary = %q", res.Summary)
	}
	if _, err := os.Stat(filepath.Join(dir, PluginDir, pluginTop, "a.jar")); err != nil {
		t.Fatalf("the direct install wrote no file: %v", err)
	}
}

// TestPluginSummaryNeverCarriesABody: the archive bytes are not model text.
func TestPluginSummaryNeverCarriesABody(t *testing.T) {
	secret := "s3cr3t-archive-bytes"
	archive := buildArchive(t, []tarEntry{{name: pluginTop + "/", typeflag: tar.TypeDir, mode: 0o755}, {name: pluginTop + "/a.jar", body: secret}})
	srv, _ := archiveServer(t, archive)
	dir := t.TempDir()
	art := PluginArtifact{URL: srv.URL + "/plugin.tar.gz", Digest: digestOf(archive), Dir: pluginTop}
	reg := pluginRegistry(t, dir, Options{PluginHosts: []string{serverHost(srv)}, PluginArtifact: artifact(art)})
	res, err := runOp(t, reg, "plugin_install", PluginInstallParams{Release: "3.2.0.Final", Connector: "postgres"})
	if err != nil {
		t.Fatalf("plugin_install: %v", err)
	}
	if strings.Contains(res.Summary+string(res.Data), secret) {
		t.Fatal("archive content reached the result")
	}
}
