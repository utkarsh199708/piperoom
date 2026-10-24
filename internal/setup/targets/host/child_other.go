//go:build !windows

package host

import "os"

// interruptChild asks the child to stop: SIGINT on Unix, which a JVM turns into
// its shutdown hooks (Kafka Connect there leaves the group and commits its
// offsets before exiting). A platform that refuses the signal tells Terminate
// to go straight to the kill, the way Windows does.
func interruptChild(p *os.Process) bool { return p.Signal(os.Interrupt) == nil }
