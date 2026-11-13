package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

const regMainYAML = "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\nlayers: {}\n"

const regDirYAML = "pipelines:\n  - {id: refunds, topic_prefix: rf, source_tables: [public.refunds], connectors: [rf-c]}\n"

func regFiles(t *testing.T) (mainFile, dir string) {
	t.Helper()
	root := t.TempDir()
	mainFile = filepath.Join(root, "owners.yaml")
	if err := os.WriteFile(mainFile, []byte(regMainYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	dir = filepath.Join(root, "owners.d")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return mainFile, dir
}

// KAN-618: with setup on and OWNERS_DIR set, the room deps carry the owners
// reload, the live owners reader and the reload hook, so pipeline_register no
// longer refuses with "no owners reload wired"; a reload makes the new
// pipeline visible to the room service and to the fleet at once.
func TestSetupRegisterWiringReloadsOwners(t *testing.T) {
	mainFile, dir := regFiles(t)
	cfg := config.Config{OwnersFile: mainFile}
	setup := config.Setup{Enabled: true, OwnersDir: dir}
	w, err := setupRegisterWiring(cfg, nil, setup, true)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	d := w.apply(room.Deps{}, slog.New(slog.NewTextHandler(&logs, nil)))
	if d.ReloadOwners == nil || d.OwnersNow == nil || d.OnOwnersReloaded == nil {
		t.Fatalf("registration deps not wired: reload=%v now=%v hook=%v", d.ReloadOwners != nil, d.OwnersNow != nil, d.OnOwnersReloaded != nil)
	}
	if d.SetupForge != nil {
		t.Fatal("SetupForge set without FORGE_KIND")
	}
	st := memstore.New()
	fs, _ := any(st).(store.FleetStore)
	fleet := fleetOptions(w.own, w.ownersNow, st, fs, config.Watch{Mode: config.WatchOff, Interval: time.Minute}, 7)
	if _, ok := d.OwnersNow().Pipeline("refunds"); ok {
		t.Fatal("refunds visible before it is registered")
	}
	// What a registration does: write the per-pipeline file, then reload.
	if err := os.WriteFile(filepath.Join(dir, "refunds.yaml"), []byte(regDirYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.ReloadOwners(); err != nil {
		t.Fatalf("ReloadOwners: %v", err)
	}
	d.OnOwnersReloaded()
	if _, ok := d.OwnersNow().Pipeline("refunds"); !ok {
		t.Fatal("refunds not visible after the reload")
	}
	if _, ok := fleet.OwnersNow().Pipeline("refunds"); !ok {
		t.Fatal("the fleet's owners reader does not see the registered pipeline")
	}
	if !strings.Contains(logs.String(), "next restart") {
		t.Fatalf("the reload hook should say the watch starts after a restart: %q", logs.String())
	}
}

// Pipelines already in OWNERS_DIR at start are part of the starting set.
func TestSetupRegisterWiringLoadsDirAtStart(t *testing.T) {
	mainFile, dir := regFiles(t)
	if err := os.WriteFile(filepath.Join(dir, "refunds.yaml"), []byte(regDirYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := setupRegisterWiring(config.Config{OwnersFile: mainFile}, nil, config.Setup{Enabled: true, OwnersDir: dir}, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := w.own.Pipeline("refunds"); !ok {
		t.Fatal("a pipeline already in OWNERS_DIR is not in the starting set")
	}
}

// Without OWNERS_DIR, or with setup off, nothing is wired: the room keeps
// refusing and says OWNERS_DIR is missing.
func TestSetupRegisterWiringOff(t *testing.T) {
	mainFile, dir := regFiles(t)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, setup := range map[string]config.Setup{
		"no OWNERS_DIR": {Enabled: true},
		"setup off":     {OwnersDir: dir},
	} {
		w, err := setupRegisterWiring(config.Config{OwnersFile: mainFile}, nil, setup, true)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		d := w.apply(room.Deps{}, quiet)
		if d.ReloadOwners != nil || d.OnOwnersReloaded != nil || d.SetupForge != nil {
			t.Fatalf("%s: registration wired", name)
		}
	}
}

// With FORGE_KIND set, the repos: path gets the PR action and the reader.
func TestSetupRegisterWiringForge(t *testing.T) {
	mainFile, dir := regFiles(t)
	cfg := config.Config{OwnersFile: mainFile, ForgeKind: "github", GitHubAppID: "1", GitHubAppInstallationID: "2",
		GitHubAppPrivateKeyFile: writeAppKey(t)}
	w, err := setupRegisterWiring(cfg, nil, config.Setup{Enabled: true, OwnersDir: dir}, true)
	if err != nil {
		t.Fatal(err)
	}
	if w.forge == nil || w.forge.PR == nil || w.forge.Reader == nil {
		t.Fatalf("SetupForge = %+v", w.forge)
	}
	if w.forge.PR.Name() != "repo_open_pull_request" {
		t.Fatalf("PR action = %q", w.forge.PR.Name())
	}
	// A half-set forge stops the start.
	cfg.GitHubAppPrivateKeyFile = filepath.Join(t.TempDir(), "none.pem")
	if _, err := setupRegisterWiring(cfg, nil, config.Setup{Enabled: true, OwnersDir: dir}, true); err == nil {
		t.Fatal("a forge whose key file is missing must stop the start")
	}
}
