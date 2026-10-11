package wiring

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestSplitBrokers(t *testing.T) {
	got := splitBrokers(" localhost:29092 , broker2:9092 , ")
	want := []string{"localhost:29092", "broker2:9092"}
	if !slices.Equal(got, want) {
		t.Errorf("splitBrokers = %v, want %v", got, want)
	}
	if n := splitBrokers("  , , "); len(n) != 0 {
		t.Errorf("splitBrokers(blanks) = %v, want empty", n)
	}
}

func TestOpenFailsOnMissingOwners(t *testing.T) {
	_, err := Open(context.Background(), config.Config{
		OwnersFile: filepath.Join(t.TempDir(), "missing.yaml"),
	})
	if err == nil {
		t.Fatal("Open with a missing owners file: want error, got nil")
	}
	if !strings.Contains(err.Error(), "wiring: load owners") {
		t.Errorf("Open error = %v, want it to name the owners step", err)
	}
}

func TestOpenFailsOnBadSourceURL(t *testing.T) {
	// "{}" is a valid owners file (no pipelines, no layers), so Open reaches
	// the source step; "%zz" is an invalid URL escape, which fails in
	// pgxpool.ParseConfig before any connection is dialled.
	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write owners file: %v", err)
	}
	_, err := Open(context.Background(), config.Config{
		OwnersFile:  ownersFile,
		SourceDBURL: "postgres://%zz",
	})
	if err == nil {
		t.Fatal("Open with a bad source URL: want error, got nil")
	}
	if !strings.Contains(err.Error(), "wiring: source postgres") {
		t.Errorf("Open error = %v, want it to name the source step", err)
	}
}
