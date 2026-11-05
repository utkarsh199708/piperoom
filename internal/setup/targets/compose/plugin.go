package compose

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/events"
	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Caps and names of plugin_install. The digest is verified before anything is
// extracted, so the archive caps are a defence against a malformed but
// correctly pinned archive (a decompression bomb), not against a wrong one.
const (
	maxPluginBytes          = 256 << 20
	maxPluginExtractedBytes = 512 << 20
	maxPluginFileBytes      = 128 << 20
	maxPluginEntries        = 4096
	// pluginMarker records what plugin_install put in a plugin directory. Its
	// presence is what makes a second run a no-op and a foreign directory an
	// error.
	pluginMarker = ".piperoom-plugin"
	// downloadTimeout bounds one archive download; the op's context deadline
	// still wins.
	downloadTimeout = 5 * time.Minute
)

// Patterns of the plugin params and of what the resolver returns. They are
// this op's allow-list: a release or connector outside them is refused before
// the knowledge index is asked.
var (
	releaseRE   = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$`)
	connectorRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	pluginDirRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sha512RE    = regexp.MustCompile(`^sha512:[0-9a-fA-F]{128}$`)
)

// PluginArtifact is one connector archive: where it is downloaded from, the
// sha512 the knowledge index pins, and the archive's one top-level directory
// (the plugin directory Connect scans).
type PluginArtifact struct {
	URL    string
	Digest string
	Dir    string
}

// PluginResolver resolves a release and connector to its pinned artifact. nil
// means internal/setup/knowledge; tests replace it with a local server.
type PluginResolver func(release, connector string) (PluginArtifact, error)

// knowledgePlugin is the production resolver: the digest comes from the
// knowledge index, never from the host that serves the archive (design §2.5).
func knowledgePlugin(release, connector string) (PluginArtifact, error) {
	p, err := knowledge.ResolvePlugin(release, connector)
	if err != nil {
		return PluginArtifact{}, err
	}
	return PluginArtifact{URL: p.ArtifactURL(), Digest: p.Digest, Dir: p.Artifact}, nil
}

// normalizePluginHosts validates and lower-cases the configured hosts, and
// falls back to the documented default. An entry is a host name or host:port;
// a scheme, a path and a user info are refused, so a typo cannot widen the
// allow-list.
func normalizePluginHosts(in []string) ([]string, error) {
	if len(in) == 0 {
		return []string{config.DefaultSetupPluginHosts}, nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, raw := range in {
		h := strings.ToLower(strings.TrimSpace(raw))
		if !validPluginHost(h) {
			return nil, fmt.Errorf("compose: PIPEROOM_SETUP_PLUGIN_HOSTS entry %s is not a host name", q(raw))
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	return out, nil
}

// validPluginHost accepts a host name, a host:port pair, or a bracketed IPv6
// host:port pair.
func validPluginHost(h string) bool {
	if h == "" || strings.ContainsAny(h, "/@ \t\r\n\\") {
		return false
	}
	if strings.HasPrefix(h, "[") {
		host, port, err := net.SplitHostPort(h)
		return err == nil && host != "" && portNumber(port)
	}
	switch strings.Count(h, ":") {
	case 0:
		return true
	case 1:
		host, port, err := net.SplitHostPort(h)
		return err == nil && host != "" && portNumber(port)
	default:
		return false // a bare IPv6 address needs brackets with a port
	}
}

// pluginHostsOrDefault is the allow-list, defaulted for a Driver built by hand
// in a test.
func (d *Driver) pluginHostsOrDefault() []string {
	if d != nil && len(d.pluginHosts) > 0 {
		return d.pluginHosts
	}
	return []string{config.DefaultSetupPluginHosts}
}

// hostAllowed reports whether the archive URL's host is in the allow-list. An
// entry with a port matches host:port; an entry without one matches the host
// on any port.
func (d *Driver) hostAllowed(u *url.URL) bool {
	host := strings.ToLower(u.Hostname())
	hostport := strings.ToLower(u.Host)
	for _, entry := range d.pluginHostsOrDefault() {
		if strings.Contains(entry, ":") {
			if entry == hostport {
				return true
			}
			continue
		}
		if entry == host {
			return true
		}
	}
	return false
}

// resolvePlugin asks the injected resolver, or the knowledge index.
func (d *Driver) resolvePlugin(release, connector string) (PluginArtifact, error) {
	if d != nil && d.pluginArtifact != nil {
		return d.pluginArtifact(release, connector)
	}
	return knowledgePlugin(release, connector)
}

// downloadHTTPClient is the one client plugin downloads use: no proxy, no
// redirects, a timeout, and a dial hook that refuses link-local and metadata
// addresses.
func (d *Driver) downloadHTTPClient() *http.Client {
	if d != nil && d.httpClient != nil {
		return d.httpClient
	}
	return newDownloadClient()
}

// downloadGuard is the dial-time denylist of the plugin download. Loopback and
// private ranges stay allowed: a person may run an internal mirror and name it
// in PIPEROOM_SETUP_PLUGIN_HOSTS; the host allow-list is what keeps the
// request to that host.
var downloadGuard = connprobe.Guard{AllowLoopback: true, AllowPrivate: true}

// newDownloadClient builds the guarded download client.
func newDownloadClient() *http.Client {
	dial := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	dial.ControlContext = func(_ context.Context, network, address string, _ syscall.RawConn) error {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("compose: plugin dial guard: %w", err)
		}
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return fmt.Errorf("compose: plugin dial guard: %q is not an IP address", host)
		}
		if err := downloadGuard.CheckAddr(addr); err != nil {
			return fmt.Errorf("compose: refusing plugin dial to %s: %w", net.JoinHostPort(host, port), err)
		}
		return nil
	}
	tr := &http.Transport{
		Proxy:                  nil,
		DialContext:            dial.DialContext,
		TLSHandshakeTimeout:    15 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		IdleConnTimeout:        30 * time.Second,
		DisableKeepAlives:      true,
		MaxResponseHeaderBytes: 64 << 10,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   downloadTimeout,
		// The last response is returned instead of followed, so a redirect to
		// a host outside the allow-list fails the status check.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// downloadURL is the URL as a summary or receipt may carry it: scheme, host
// and path, never a query.
func downloadURL(u *url.URL) string {
	return u.Scheme + "://" + u.Host + u.Path
}

// PluginInstallParams names one indexed plugin.
type PluginInstallParams struct {
	Release   string `json:"release"`
	Connector string `json:"connector"`
}

// pluginInstallOp downloads one pinned connector archive into the project's
// Connect plugin directory.
type pluginInstallOp struct{ d *Driver }

func (pluginInstallOp) Name() string   { return "plugin_install" }
func (pluginInstallOp) Target() string { return targetKind }
func (pluginInstallOp) Tier() string   { return protocol.TierT1 }
func (pluginInstallOp) Layer() string  { return "kafka-connect" }
func (pluginInstallOp) Describe() string {
	return "download one Debezium connector archive from an allowed host into the Connect plugin directory"
}

func (pluginInstallOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"release", "connector"},
		"properties": map[string]any{
			"release":   map[string]any{"type": "string", "minLength": 1, "maxLength": 32, "pattern": `^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$`},
			"connector": map[string]any{"type": "string", "minLength": 1, "maxLength": 32, "pattern": `^[a-z][a-z0-9-]{0,31}$`},
		},
	}
}

func (o pluginInstallOp) Validate(params json.RawMessage, env ops.Env) error {
	_, _, _, err := o.prepare(params)
	return err
}

// prepare decodes the params, resolves the artifact, and checks the URL and
// its host without I/O.
func (o pluginInstallOp) prepare(params json.RawMessage) (PluginInstallParams, PluginArtifact, *url.URL, error) {
	var p PluginInstallParams
	if err := decodeParams(params, &p); err != nil {
		return p, PluginArtifact{}, nil, err
	}
	p.Release = strings.TrimSpace(p.Release)
	p.Connector = strings.TrimSpace(p.Connector)
	if !releaseRE.MatchString(p.Release) {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: release %s is not a release name", ErrParams, q(p.Release))
	}
	if !connectorRE.MatchString(p.Connector) {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: connector %s is not a connector name", ErrParams, q(p.Connector))
	}
	art, err := o.d.resolvePlugin(p.Release, p.Connector)
	if err != nil {
		return p, PluginArtifact{}, nil, fmt.Errorf("compose: resolving plugin %s %s: %w", q(p.Release), q(p.Connector), err)
	}
	if !sha512RE.MatchString(art.Digest) {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: the digest of %s %s is not a sha512 digest", ErrParams, q(p.Release), q(p.Connector))
	}
	art.Digest = strings.ToLower(art.Digest)
	if !pluginDirRE.MatchString(art.Dir) {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: plugin directory %s is not a directory name", ErrParams, q(art.Dir))
	}
	u, err := url.Parse(art.URL)
	if err != nil || u.Host == "" {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: archive URL is not a URL", ErrPluginHost)
	}
	switch u.Scheme {
	case "https":
	case "http":
		// Plain http is only for a loopback archive server (the tests and a
		// local mirror); an allow-listed host on the network is https.
		if !loopbackURL(u) {
			return p, PluginArtifact{}, nil, fmt.Errorf("%w: %s serves the archive over plain http", ErrPluginHost, q(u.Host))
		}
	default:
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: archive URL scheme %s is not https", ErrPluginHost, q(u.Scheme))
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: archive URL %s carries user info, a query or a fragment", ErrPluginHost, q(downloadURL(u)))
	}
	if !strings.HasSuffix(strings.ToLower(u.Path), ".tar.gz") {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: %s is not a .tar.gz archive", ErrParams, q(downloadURL(u)))
	}
	if !o.d.hostAllowed(u) {
		return p, PluginArtifact{}, nil, fmt.Errorf("%w: %s is not in PIPEROOM_SETUP_PLUGIN_HOSTS (%s)",
			ErrPluginHost, q(u.Host), q(strings.Join(o.d.pluginHostsOrDefault(), ", ")))
	}
	return p, art, u, nil
}

// loopbackURL reports whether u names localhost or a loopback address.
func loopbackURL(u *url.URL) bool {
	h := strings.TrimSuffix(strings.TrimPrefix(u.Hostname(), "["), "]")
	if h == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(h)
	return err == nil && ip.IsLoopback()
}

func (o pluginInstallOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, art, u, err := o.prepare(params)
	if err != nil {
		return ops.Result{}, err
	}
	if env.Confine == nil {
		return ops.Result{}, errors.New("compose: the project root is not open")
	}
	start := now(env)
	if _, err := env.Confine.Check(PluginDir); err != nil {
		return ops.Result{}, err
	}
	root, err := projectRoot(env)
	if err != nil {
		return ops.Result{}, err
	}
	defer root.Close()
	if err := root.MkdirAll(PluginDir, 0o755); err != nil {
		return ops.Result{}, fmt.Errorf("compose: creating the plugin directory %s: %w", q(PluginDir), err)
	}
	target := PluginDir + "/" + art.Dir
	if err := ops.CheckName(art.Dir); err != nil {
		return ops.Result{}, fmt.Errorf("%w: %v", ErrPath, err)
	}
	installed, err := installedPlugin(root, target, art.Digest)
	if err != nil {
		return ops.Result{}, err
	}
	if installed {
		return ops.Result{
			Summary: fmt.Sprintf("%s %s is already installed in %s", p.Connector, p.Release, target),
			Data: map[string]any{
				"release": p.Release, "connector": p.Connector, "dir": target,
				"digest": art.Digest, "changed": false, "url": downloadURL(u),
			},
			Receipt: events.SetupReceipt{Kind: "http", Target: downloadURL(u), At: start, TookMS: now(env).Sub(start).Milliseconds()},
		}, nil
	}

	archiveName, err := tempName(PluginDir + "/plugin-archive")
	if err != nil {
		return ops.Result{}, err
	}
	defer func() { _ = root.Remove(archiveName) }()
	if err := downloadPlugin(ctx, o.d.downloadHTTPClient(), u, root, archiveName, art.Digest); err != nil {
		return ops.Result{}, err
	}

	extractDir, err := tempName(PluginDir + "/plugin-extract")
	if err != nil {
		return ops.Result{}, err
	}
	files, bytes, err := extractPlugin(root, archiveName, extractDir, art.Dir)
	if err != nil {
		_ = root.RemoveAll(extractDir)
		return ops.Result{}, err
	}
	// The marker goes in before the rename: a directory Connect can see always
	// says which release and digest installed it.
	marker := pluginMarkerFile{Release: p.Release, Connector: p.Connector, Dir: art.Dir, Digest: art.Digest}
	if err := writePluginMarker(root, extractDir, marker); err != nil {
		_ = root.RemoveAll(extractDir)
		return ops.Result{}, err
	}
	if err := root.Rename(extractDir, target); err != nil {
		_ = root.RemoveAll(extractDir)
		return ops.Result{}, fmt.Errorf("compose: installing %s: %w", q(target), err)
	}
	took := now(env).Sub(start).Milliseconds()
	summary := fmt.Sprintf("installed %s %s into %s (%d files, %s)", p.Connector, p.Release, target, files, humanBytes(bytes))
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"release": p.Release, "connector": p.Connector, "dir": target,
			"digest": art.Digest, "files": files, "bytes": bytes,
			"changed": true, "url": downloadURL(u),
		},
		Receipt: events.SetupReceipt{Kind: "http", Target: downloadURL(u), At: start, TookMS: took},
	}, nil
}

// downloadPlugin streams the archive into the project root while hashing it,
// and refuses it unless the sha512 matches the pinned digest. Nothing is
// extracted before the digest matches, so an archive from a compromised mirror
// (or a redirect) never reaches the plugin directory.
func downloadPlugin(ctx context.Context, client *http.Client, u *url.URL, root *os.Root, name, digest string) error {
	if client == nil {
		return errors.New("compose: no HTTP client for the plugin download")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("%w: building the request: %w", ErrDownload, err)
	}
	req.Header.Set("User-Agent", "piperoom-setup/"+ops.CatalogueVersion)
	req.Header.Set("Accept", "application/gzip, application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		// A *url.Error's text embeds the URL; the query is refused already, but
		// the inner error is the part worth keeping.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("%w: %s: %w", ErrDownload, downloadURL(u), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s: HTTP %d", ErrDownload, downloadURL(u), resp.StatusCode)
	}
	if resp.ContentLength > maxPluginBytes {
		return fmt.Errorf("%w: %s is %d bytes; the cap is %d", ops.ErrTooLarge, downloadURL(u), resp.ContentLength, maxPluginBytes)
	}
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("compose: creating the download file: %w", err)
	}
	h := sha512.New()
	n, cerr := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, maxPluginBytes+1))
	if cerr == nil {
		cerr = f.Sync()
	}
	if cl := f.Close(); cerr == nil {
		cerr = cl
	}
	if cerr != nil {
		return fmt.Errorf("%w: %s: %w", ErrDownload, downloadURL(u), cerr)
	}
	if n > maxPluginBytes {
		return fmt.Errorf("%w: %s is larger than %d bytes", ops.ErrTooLarge, downloadURL(u), maxPluginBytes)
	}
	got := "sha512:" + hex.EncodeToString(h.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(got), []byte(strings.ToLower(digest))) != 1 {
		return fmt.Errorf("%w: %s is %s, the index pins %s", ErrPluginDigest, downloadURL(u), q(got), q(digest))
	}
	return nil
}

// extractPlugin unpacks a verified archive into destDir, which must not exist
// yet. Every entry must sit under the one top-level directory (that is the
// "one plugin per directory" rule), and only regular files and directories
// are accepted.
func extractPlugin(root *os.Root, archiveName, destDir, top string) (int, int64, error) {
	f, err := root.Open(archiveName)
	if err != nil {
		return 0, 0, fmt.Errorf("compose: opening the archive: %w", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %s is not a gzip archive: %v", ErrPluginArchive, q(archiveName), err)
	}
	defer gz.Close()
	if err := root.Mkdir(destDir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("compose: creating the extraction directory: %w", err)
	}
	tr := tar.NewReader(gz)
	var files int
	var total int64
	entries := 0
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("%w: reading %s: %v", ErrPluginArchive, q(archiveName), err)
		}
		entries++
		if entries > maxPluginEntries {
			return 0, 0, fmt.Errorf("%w: %s has more than %d entries", ErrPluginArchive, q(archiveName), maxPluginEntries)
		}
		rel, perr := pluginEntryPath(hdr.Name, top)
		if perr != nil {
			return 0, 0, perr
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if rel == "" {
				continue
			}
			if err := root.MkdirAll(destDir+"/"+rel, 0o755); err != nil {
				return 0, 0, fmt.Errorf("compose: creating %s: %w", q(rel), err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if rel == "" {
				return 0, 0, fmt.Errorf("%w: %s is a file at the archive's top level", ErrPluginArchive, q(hdr.Name))
			}
			if hdr.Size < 0 || hdr.Size > maxPluginFileBytes {
				return 0, 0, fmt.Errorf("%w: %s is %d bytes", ErrPluginArchive, q(hdr.Name), hdr.Size)
			}
			total += hdr.Size
			if total > maxPluginExtractedBytes {
				return 0, 0, fmt.Errorf("%w: %s unpacks to more than %d bytes", ErrPluginArchive, q(archiveName), maxPluginExtractedBytes)
			}
			if dir := path.Dir(rel); dir != "." {
				if err := root.MkdirAll(destDir+"/"+dir, 0o755); err != nil {
					return 0, 0, fmt.Errorf("compose: creating %s: %w", q(dir), err)
				}
			}
			out, oerr := root.OpenFile(destDir+"/"+rel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
			if oerr != nil {
				return 0, 0, fmt.Errorf("compose: creating %s: %w", q(rel), oerr)
			}
			written, cerr := io.CopyN(out, tr, hdr.Size)
			if cerr == nil && written != hdr.Size {
				cerr = io.ErrUnexpectedEOF
			}
			if cerr == nil {
				cerr = out.Sync()
			}
			if cl := out.Close(); cerr == nil {
				cerr = cl
			}
			if cerr != nil {
				return 0, 0, fmt.Errorf("compose: writing %s: %w", q(rel), cerr)
			}
			files++
		default:
			return 0, 0, fmt.Errorf("%w: entry %s is a %s; only regular files and directories are installed",
				ErrPluginArchive, q(hdr.Name), tarTypeName(hdr.Typeflag))
		}
	}
	if files == 0 {
		return 0, 0, fmt.Errorf("%w: %s holds no files under %s", ErrPluginArchive, q(archiveName), q(top))
	}
	return files, total, nil
}

// pluginEntryPath validates one tar entry and returns its path under the
// plugin's top-level directory. The lexical rule is the U1-2 one: no absolute
// path, no dot-dot, no control character, no device name.
func pluginEntryPath(name, top string) (string, error) {
	n := strings.TrimSuffix(strings.ReplaceAll(name, "\\", "/"), "/")
	if n == "" {
		return "", fmt.Errorf("%w: an entry has an empty name", ErrPluginArchive)
	}
	if err := ops.CheckName(n); err != nil {
		return "", fmt.Errorf("%w: entry %s: %v", ErrPluginArchive, q(name), err)
	}
	parts := strings.Split(path.Clean(n), "/")
	if parts[0] != top {
		return "", fmt.Errorf("%w: entry %s is outside %s; one archive installs one plugin directory",
			ErrPluginArchive, q(name), q(top))
	}
	return strings.Join(parts[1:], "/"), nil
}

// tarTypeName names the entry kinds this op refuses, so an error says what was
// in the archive.
func tarTypeName(flag byte) string {
	switch flag {
	case tar.TypeSymlink:
		return "symbolic link"
	case tar.TypeLink:
		return "hard link"
	case tar.TypeChar:
		return "character device"
	case tar.TypeBlock:
		return "block device"
	case tar.TypeFifo:
		return "FIFO"
	}
	return fmt.Sprintf("type %q", flag)
}

// pluginMarkerFile is what plugin_install writes into a directory it created.
type pluginMarkerFile struct {
	Release   string `json:"release"`
	Connector string `json:"connector"`
	Dir       string `json:"dir"`
	Digest    string `json:"digest"`
}

// writePluginMarker records the installed release and digest.
func writePluginMarker(root *os.Root, dir string, m pluginMarkerFile) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("compose: encoding the plugin marker: %w", err)
	}
	f, err := root.OpenFile(dir+"/"+pluginMarker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("compose: creating the plugin marker: %w", err)
	}
	_, werr := f.Write(append(raw, '\n'))
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return fmt.Errorf("compose: writing the plugin marker: %w", werr)
	}
	return nil
}

// installedPlugin reports whether target is a directory plugin_install
// created with this digest (a no-op reinstall), and refuses anything else that
// already holds the name: a hand-copied directory or another release must not
// be overwritten silently.
func installedPlugin(root *os.Root, target, digest string) (bool, error) {
	fi, err := root.Lstat(target)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("compose: stat %s: %w", q(target), err)
	}
	if !fi.IsDir() || fi.Mode()&fs.ModeSymlink != 0 {
		return false, fmt.Errorf("%w: %s exists and is not a plain directory", ErrPluginDir, q(target))
	}
	raw, err := root.ReadFile(target + "/" + pluginMarker)
	if errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("%w: %s already holds files plugin_install did not put there", ErrPluginDir, q(target))
	}
	if err != nil {
		return false, fmt.Errorf("compose: reading the plugin marker in %s: %w", q(target), err)
	}
	var m pluginMarkerFile
	if err := json.Unmarshal(raw, &m); err != nil {
		return false, fmt.Errorf("%w: the plugin marker in %s is not readable", ErrPluginDir, q(target))
	}
	if !strings.EqualFold(m.Digest, digest) {
		return false, fmt.Errorf("%w: %s holds %s, not the pinned %s", ErrPluginDir, q(target), q(m.Digest), q(digest))
	}
	return true, nil
}

// humanBytes is a size for a summary.
func humanBytes(n int64) string {
	const mib = 1 << 20
	if n < mib {
		return fmt.Sprintf("%d KiB", (n+1023)/1024)
	}
	return fmt.Sprintf("%.1f MiB", float64(n)/mib)
}
