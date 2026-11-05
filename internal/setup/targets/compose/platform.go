package compose

import (
	"net/url"
	"os"
	"runtime"
	"strings"
)

// Docker Desktop differences this target reports (row U1-3b). The ops talk to
// the docker CLI, never to the socket directly, so the differences that matter
// are what the CLI is pointed at and what a container can reach on the host:
//
//   - the endpoint: a Unix socket on Linux, /var/run/docker.sock on macOS
//     (Docker Desktop's VM forwards to it), a named pipe on Windows;
//   - host networking: only a Linux daemon has a real host network namespace.
//     Docker Desktop runs the daemon in a VM, so `network_mode: host` reaches
//     the VM, not the person's machine — the validator refuses it anyway, and
//     docker_info says so;
//   - the host's address from inside a container: host.docker.internal on
//     Docker Desktop, the bridge gateway on a Linux daemon.
//
// DOCKER_HOST, when set, wins over the platform default: a person pointing the
// runner at a remote daemon is telling it where the daemon is.

// defaultUnixSocket is where a Linux or macOS daemon listens.
const defaultUnixSocket = "/var/run/docker.sock"

// defaultWindowsPipe is where Docker Desktop's Windows daemon listens.
const defaultWindowsPipe = "//./pipe/docker_engine"

// dockerSocketPath is the daemon endpoint the docker CLI will use on goos.
func dockerSocketPath(goos string, getenv func(string) string) string {
	if getenv != nil {
		if raw := strings.TrimSpace(getenv("DOCKER_HOST")); raw != "" {
			return normalizeDockerHost(raw)
		}
	}
	if goos == "windows" {
		return defaultWindowsPipe
	}
	return defaultUnixSocket
}

// normalizeDockerHost turns a DOCKER_HOST value into the endpoint a person and
// a test can compare: a unix:// path, an npipe path, or the tcp:// form kept
// whole. The value is operator configuration, not request text, but it is
// still clipped like every other string that reaches a summary.
func normalizeDockerHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return clip(raw)
	}
	switch u.Scheme {
	case "unix":
		return clip(u.Path)
	case "npipe":
		// npipe:////./pipe/docker_engine parses as the path
		// //./pipe/docker_engine with no host.
		p := u.Path
		if p == "" {
			p = u.Host
		}
		return clip(p)
	case "tcp", "http", "https", "ssh":
		return clip(u.Scheme + "://" + u.Host)
	}
	return clip(raw)
}

// hostNetworking reports whether the daemon has a real host network namespace
// (a Linux daemon). Docker Desktop's VM does not; the templates never use
// network_mode: host, and docker_info reports the fact for the model.
func hostNetworking(goos string) bool { return goos == "linux" }

// containerHost is the address a container uses to reach a service on the
// runner's machine. Docker Desktop publishes host.docker.internal; a Linux
// daemon uses the default bridge gateway.
func containerHost(goos string) string {
	switch goos {
	case "darwin", "windows":
		return "host.docker.internal"
	default:
		return "172.17.0.1"
	}
}

// runnerPlatform is the GOOS this driver reports: the injected one in tests,
// runtime.GOOS otherwise.
func (d *Driver) runnerPlatform() string {
	if d != nil && d.goos != "" {
		return d.goos
	}
	return runtime.GOOS
}

// lookupEnv is the environment accessor this driver uses: the injected one in
// tests, os.Getenv otherwise.
func (d *Driver) lookupEnv() func(string) string {
	if d != nil && d.getenv != nil {
		return d.getenv
	}
	return getenvOrEmpty
}

// getenvOrEmpty is os.Getenv, named so the default path never looks like the
// injected seam.
func getenvOrEmpty(key string) string { return os.Getenv(key) }
