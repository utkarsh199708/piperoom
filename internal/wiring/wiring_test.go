package wiring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
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
	}, nil)
	if err == nil {
		t.Fatal("Open with a missing owners file: want error, got nil")
	}
	if !strings.Contains(err.Error(), "wiring: load owners") {
		t.Errorf("Open error = %v, want it to name the owners step", err)
	}
}

func TestOpenFailsOnBadSourceURL(t *testing.T) {
	// "{}" is a valid owners file (no pipelines, no layers) and the A2 client
	// settings are the dev defaults, so Open reaches the source step; "%zz" is
	// an invalid URL escape, which fails in pgxpool.ParseConfig before any
	// connection is dialled.
	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write owners file: %v", err)
	}
	cfg := devA2Config()
	cfg.OwnersFile = ownersFile
	cfg.SourceDBURL = "postgres://%zz"
	_, err := Open(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("Open with a bad source URL: want error, got nil")
	}
	if !strings.Contains(err.Error(), "wiring: source postgres") {
		t.Errorf("Open error = %v, want it to name the source step", err)
	}
}

// devA2Config is an empty-security config: the dev stack addresses with no
// SASL, no TLS and no registry/Connect login.
func devA2Config() config.Config {
	return config.Config{
		KafkaBrokers:      "localhost:29092",
		ConnectURL:        "http://localhost:8083",
		SchemaRegistryURL: "http://localhost:8081",
	}
}

func TestBuildA2ClientsEmptySecurity(t *testing.T) {
	a2, err := buildA2Clients(devA2Config(), nil)
	if err != nil {
		t.Fatalf("buildA2Clients(empty security) = %v, want nil", err)
	}
	if a2.Connect == nil || a2.Kafka == nil || a2.Registry == nil {
		t.Fatalf("buildA2Clients returned nil client(s): %+v", a2)
	}
	a2.Kafka.Close()
}

func TestBuildA2ClientsRejectsPlainWithoutTLS(t *testing.T) {
	cfg := devA2Config()
	cfg.KafkaSecurityProtocol = "SASL_PLAINTEXT"
	cfg.KafkaSASLMechanism = "PLAIN"
	cfg.KafkaSASLUsername = "agent"
	cfg.KafkaSASLPassword = "s3cret"
	_, err := buildA2Clients(cfg, nil)
	if err == nil {
		t.Fatal("buildA2Clients(PLAIN without TLS): want error, got nil")
	}
	if !errors.Is(err, netsec.ErrInvalid) {
		t.Errorf("error = %v, want it to wrap netsec.ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "PLAIN without TLS") {
		t.Errorf("error = %v, want the netsec PLAIN-without-TLS validation error", err)
	}
}

func TestOpenRejectsBadKafkaSecurity(t *testing.T) {
	// A valid owners file lets Open reach the client-construction step; the
	// database URLs stay empty, so if Open pinged the databases first the
	// error would name the source step instead of the security settings.
	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write owners file: %v", err)
	}
	cfg := devA2Config()
	cfg.OwnersFile = ownersFile
	cfg.KafkaSecurityProtocol = "SASL_PLAINTEXT"
	cfg.KafkaSASLMechanism = "PLAIN"
	cfg.KafkaSASLUsername = "agent"
	cfg.KafkaSASLPassword = "s3cret"
	_, err := Open(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("Open with bad Kafka security: want error, got nil")
	}
	if !errors.Is(err, netsec.ErrInvalid) || !strings.Contains(err.Error(), "PLAIN without TLS") {
		t.Fatalf("Open error = %v, want the netsec PLAIN-without-TLS error before any DB ping", err)
	}
}

func TestWiredCloseNilSafe(t *testing.T) {
	// Callers defer Close as soon as they hold the value, and tests build
	// Wired literals without a closer, so Close must be a no-op on a nil or
	// zero *Wired instead of a nil-func panic.
	var nilW *Wired
	nilW.Close()
	var zero Wired
	zero.Close()

	called := 0
	w := &Wired{close: func() { called++ }}
	w.Close()
	if called != 1 {
		t.Errorf("Close called the closer %d times, want 1", called)
	}
}
