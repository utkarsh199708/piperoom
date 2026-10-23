// Command piperoom is the umbrella binary (U1-21): one name for the four
// Piperoom commands.
//
//	piperoom server [flags]       the room server (cmd/server)
//	piperoom discover [flags]     pipeline discovery (cmd/discover)
//	piperoom investigate [flags]  the agent loop in a terminal (cmd/investigate)
//	piperoom setup [flags]        the setup runner, exactly piperoom-setup (cmd/setup)
//
// Each subcommand takes the flags of the command it names, unchanged.
// "setup" runs in this process (internal/setup/setupcmd). server, discover and
// investigate keep their logic in package main, which Go cannot import, so
// the umbrella executes the sibling binary piperoom-<name> found next to its
// own executable: on Unix it replaces this process with it (same PID, signals
// and exit code); elsewhere it runs it as a child and returns its exit code.
//
// With no subcommand, or when the first argument is a flag, the arguments go
// to the server, so an existing "piperoom --healthcheck" or a bare "piperoom"
// entrypoint keeps meaning the server. The one exception is a first argument
// of -h, -help or --help, which prints the umbrella's own usage;
// "piperoom server --help" lists the server's flags.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"piperoom/internal/setup/setupcmd"
)

// Exit codes of the umbrella itself; a subcommand's own codes pass through.
const (
	exitOK      = 0
	exitFailed  = 1
	exitUsage   = 2
	siblingStem = "piperoom-"
)

// siblingPackage is the package that builds the binary a subcommand runs
// (named in the error when the binary is missing); ok is false for a name
// that is not a sibling subcommand.
func siblingPackage(sub string) (pkg string, ok bool) {
	switch sub {
	case "server", "discover", "investigate":
		return "./cmd/" + sub, true
	}
	return "", false
}

const usage = `Usage: piperoom <command> [flags]

Commands:
  server       run the Piperoom room server (the default when no command is given)
  discover     discover pipelines and write owners.yaml
  investigate  run the investigation agent in this terminal
  setup        run the setup runner (the same as piperoom-setup)
  help         print this help

Each command takes the flags of the binary it runs; "piperoom <command> --help"
lists them ("piperoom server --help" for the server's). Flags with no command
go to the server, so "piperoom --healthcheck" is "piperoom server --healthcheck".
server, discover and investigate run piperoom-server, piperoom-discover and
piperoom-investigate from this binary's directory.
`

// app holds the process edges so dispatch is testable without spawning.
type app struct {
	stdout, stderr io.Writer
	// setup runs piperoom-setup in-process.
	setup func(args []string, stdout, stderr io.Writer) error
	// executable is os.Executable.
	executable func() (string, error)
	// runBinary runs the binary at path with args and returns its exit code.
	runBinary func(path string, args []string) (int, error)
}

func main() {
	a := app{
		stdout:     os.Stdout,
		stderr:     os.Stderr,
		setup:      setupcmd.Run,
		executable: os.Executable,
		runBinary:  runBinary,
	}
	os.Exit(a.dispatch(os.Args[1:]))
}

// dispatch runs the subcommand args name and returns the process exit code.
func (a app) dispatch(args []string) int {
	sub, rest := "server", args
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		sub, rest = args[0], args[1:]
	}
	if len(args) > 0 {
		switch args[0] {
		case "help", "-h", "-help", "--help":
			fmt.Fprint(a.stdout, usage)
			return exitOK
		}
	}

	if sub == "setup" {
		return setupcmd.ExitCode(a.setup(rest, a.stdout, a.stderr), a.stderr)
	}
	pkg, ok := siblingPackage(sub)
	if !ok {
		fmt.Fprintf(a.stderr, "piperoom: unknown command %q\n\n%s", sub, usage)
		return exitUsage
	}
	path, err := a.siblingPath(sub)
	if err != nil {
		fmt.Fprintf(a.stderr, "piperoom: %s: %v (build it with: go build -o %s %s)\n", sub, err, binaryName(sub), pkg)
		return exitFailed
	}
	code, err := a.runBinary(path, rest)
	if err != nil {
		fmt.Fprintf(a.stderr, "piperoom: %s: %v\n", sub, err)
		return exitFailed
	}
	return code
}

// siblingPath is piperoom-<sub> in the directory of the real (symlink
// resolved) umbrella executable. It never searches PATH: the umbrella runs
// the binaries shipped with it, not whatever a PATH entry happens to hold.
// When the executable cannot be resolved it fails rather than use the
// unresolved path: os.Executable can return a symlink (macOS, BSD), and the
// link's directory is not where the shipped binaries are and may be writable
// by someone else.
func (a app) siblingPath(sub string) (string, error) {
	exe, err := a.executable()
	if err != nil {
		return "", fmt.Errorf("finding this executable: %w", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("resolving this executable: %w", err)
	}
	path := filepath.Join(filepath.Dir(exe), binaryName(sub))
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("%s not found next to %s: %w", binaryName(sub), filepath.Base(exe), err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%s is a directory", path)
	}
	return path, nil
}

// binaryName is the sibling's file name on this OS.
func binaryName(sub string) string {
	if runtime.GOOS == "windows" {
		return siblingStem + sub + ".exe"
	}
	return siblingStem + sub
}
