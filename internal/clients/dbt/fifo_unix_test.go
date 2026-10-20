//go:build unix

package dbt

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A named pipe in place of an artifact is refused before it is opened:
// opening a FIFO for reading blocks until a writer appears, which no context
// deadline can interrupt.
func TestDirFIFORefusedWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, ManifestFile), 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	d, err := NewDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := d.ModelLineage(context.Background(), "fct_revenue")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("err = %v, want ErrMalformed (not a regular file)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reading a FIFO artifact blocked")
	}
}
