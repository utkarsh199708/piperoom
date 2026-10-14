//go:build unix

package secrets

import (
	"os"
	"syscall"
)

// openNoBlock opens path read-only without blocking on a FIFO swapped in
// after the caller's Stat (O_NONBLOCK makes open(2) return at once; the
// caller then refuses the non-regular file).
func openNoBlock(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
