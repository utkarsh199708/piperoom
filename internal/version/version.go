// Package version holds the build version of the Piperoom binaries.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package version

// Version is set at link time and never changed at run time:
//
//	go build -ldflags "-X piperoom/internal/version.Version=$(git describe --tags --always --dirty)" ./cmd/server
//
// It is a var only because -X can set nothing else. A plain `go build` or
// `go run` leaves Dev. GET /healthz reports it as "version" (docs/api.md), and
// cmd/server logs it once at start. It names a build, not a secret, and is
// shown without a login like the rest of /healthz.
var Version = Dev

// Dev is Version when the build set none.
const Dev = "dev"

// String returns Version, or Dev when the linker set it to "" (an -X with an
// empty value, e.g. an unset make variable), so callers never print nothing.
func String() string {
	if Version == "" {
		return Dev
	}
	return Version
}
