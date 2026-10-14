//go:build !unix

package secrets

import "os"

// openNoBlock opens path read-only. Only Unix has FIFOs that block open(2).
func openNoBlock(path string) (*os.File, error) { return os.Open(path) }
