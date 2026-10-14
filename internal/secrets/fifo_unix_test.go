//go:build unix

package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// E13: a FIFO under a root is refused at once instead of blocking forever.
func TestFilesFIFODoesNotBlock(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "pipe")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	p, err := NewFilesProvider(FilesOptions{Roots: []string{root}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ref, err := ParseRef("secret://files" + filepath.ToSlash(fifo))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Resolve(context.Background(), ref)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		// Unblock the open so the goroutine ends.
		if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
			f.Close()
		}
		t.Fatal("Resolve blocked on a FIFO")
	}
}
