// Command setup is the Piperoom setup runner: piperoom-setup pairs with a
// Piperoom server over one WebSocket and executes the setup operations the
// server sends. With --standalone it runs the server for one machine itself:
// the quickstart server with write mode and setup on, loopback only, every
// room event appended to setup-log.jsonl, and --watch to restart it when the
// setup room writes owners.yaml (U1-11, docs/design/setup-agent.md section
// 2.7). The command lives in internal/setup/setupcmd so that the umbrella
// binary's "piperoom setup" (cmd/piperoom, U1-21) is the same code.
package main

import (
	"io"
	"os"

	"piperoom/internal/setup/setupcmd"
)

func main() {
	os.Exit(setupcmd.ExitCode(runWith(os.Args[1:], os.Stdout), os.Stderr))
}

func runWith(args []string, stdout io.Writer) error {
	return setupcmd.Run(args, stdout, os.Stderr)
}
