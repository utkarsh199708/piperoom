package compose

import "testing"

func TestDockerSocketPath(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		want string
	}{
		{"linux default", "linux", nil, "/var/run/docker.sock"},
		{"macOS default", "darwin", nil, "/var/run/docker.sock"},
		{"windows default", "windows", nil, "//./pipe/docker_engine"},
		{"unix DOCKER_HOST wins", "windows", map[string]string{"DOCKER_HOST": "unix:///run/user/1000/docker.sock"}, "/run/user/1000/docker.sock"},
		{"npipe DOCKER_HOST", "windows", map[string]string{"DOCKER_HOST": "npipe:////./pipe/docker_engine"}, "//./pipe/docker_engine"},
		{"tcp DOCKER_HOST", "linux", map[string]string{"DOCKER_HOST": "tcp://10.0.0.5:2375"}, "tcp://10.0.0.5:2375"},
		{"blank DOCKER_HOST falls through", "linux", map[string]string{"DOCKER_HOST": "  "}, "/var/run/docker.sock"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(key string) string { return tc.env[key] }
			if got := dockerSocketPath(tc.goos, getenv); got != tc.want {
				t.Fatalf("dockerSocketPath(%q) = %q, want %q", tc.goos, got, tc.want)
			}
		})
	}
}

// TestDockerDesktopDifferences is the row's "macOS and Windows ... host
// networking handled with fakes": the platform decides the endpoint and what a
// container can reach on the host, and the answer comes from the injected
// GOOS, never from the machine running the test.
func TestDockerDesktopDifferences(t *testing.T) {
	cases := []struct {
		goos          string
		wantHostNet   bool
		wantContainer string
	}{
		{"linux", true, "172.17.0.1"},
		{"darwin", false, "host.docker.internal"},
		{"windows", false, "host.docker.internal"},
	}
	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			if got := hostNetworking(tc.goos); got != tc.wantHostNet {
				t.Errorf("hostNetworking(%q) = %v, want %v", tc.goos, got, tc.wantHostNet)
			}
			if got := containerHost(tc.goos); got != tc.wantContainer {
				t.Errorf("containerHost(%q) = %q, want %q", tc.goos, got, tc.wantContainer)
			}
		})
	}
	d := &Driver{goos: "windows", getenv: func(string) string { return "" }}
	if got := d.runnerPlatform(); got != "windows" {
		t.Errorf("runnerPlatform with an injected GOOS = %q", got)
	}
}
