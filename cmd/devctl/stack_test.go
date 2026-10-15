package main

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
)

func TestParseResetFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		rooms   bool
		wantErr string // substring; "" = no error
	}{
		{"plain reset keeps rooms", nil, false, ""},
		{"rooms confirmed", []string{"--rooms", "--yes"}, true, ""},
		{"rooms confirmed, other order", []string{"--yes", "--rooms"}, true, ""},
		{"rooms without confirmation", []string{"--rooms"}, false, "add --yes to confirm"},
		{"yes alone", []string{"--yes"}, false, "--yes only confirms --rooms"},
		{"positional argument", []string{"now"}, false, resetUsage},
		{"unknown flag", []string{"--all"}, false, resetUsage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rooms, err := parseResetFlags(tt.args)
			if tt.wantErr == "" {
				if err != nil || rooms != tt.rooms {
					t.Fatalf("parseResetFlags(%v) = %v, %v; want %v, nil", tt.args, rooms, err, tt.rooms)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("parseResetFlags(%v) error = %v, want containing %q", tt.args, err, tt.wantErr)
			}
			if rooms {
				t.Error("a refused reset must not report rooms=true")
			}
		})
	}
}

// The --rooms refusal comes before the stack lock is read, so a wrong flag never
// depends on who holds the lock.
func TestResetRoomsNeedsYesBeforeLock(t *testing.T) {
	t.Setenv(ownerEnv, "")
	err := runCommand(t.Context(), &stack{}, "reset", []string{"--rooms"})
	if err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("reset --rooms = %v, want the --yes refusal", err)
	}
}

func TestResetArgs(t *testing.T) {
	keep := resetArgs(false)
	if len(keep) != 1 {
		t.Fatalf("reset keeps rooms: %d compose commands, want 1: %v", len(keep), keep)
	}
	if got := strings.Join(keep[0], " "); got != "rm --stop --force --volumes postgres kafka schema-registry connect devconsumer" {
		t.Errorf("reset compose args = %q", got)
	}
	for _, a := range keep[0] {
		if a == roomDBService || a == "down" || a == "-v" {
			t.Errorf("a plain reset must not touch the room database: %v", keep[0])
		}
	}
	wipe := resetArgs(true)
	if len(wipe) != 1 || strings.Join(wipe[0], " ") != "down -v --remove-orphans" {
		t.Errorf("reset --rooms compose args = %v, want down -v --remove-orphans", wipe)
	}
	if got := strings.Join(upArgs(), " "); got != "up -d --build --wait --remove-orphans" {
		t.Errorf("up compose args = %q", got)
	}
}

// composeFile is the part of dev/docker-compose.yml the checks below read.
type composeFile struct {
	Services map[string]struct {
		Image       string            `yaml:"image"`
		Profiles    []string          `yaml:"profiles"`
		Environment map[string]string `yaml:"environment"`
		Ports       []string          `yaml:"ports"`
		Volumes     []string          `yaml:"volumes"`
		Entrypoint  []string          `yaml:"entrypoint"`
		Command     []string          `yaml:"command"`
	} `yaml:"services"`
	Volumes map[string]any `yaml:"volumes"`
}

func readDevFile(t *testing.T, rel string) []byte {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestComposeFile checks dev/docker-compose.yml against what devctl assumes:
// reset removes exactly the non-room services, roomdb keeps its data on a named
// volume at the port the config default uses, and Connect writes the worker log
// where CONNECT_WORKER_LOG_FILE looks for it.
func TestComposeFile(t *testing.T) {
	var c composeFile
	if err := yaml.Unmarshal(readDevFile(t, "dev/docker-compose.yml"), &c); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	var others []string
	for name, svc := range c.Services {
		// Profile services (piperoom-shadow) are not pipeline data either: they are
		// started on request and, like a server run by hand, survive a reset.
		if name != roomDBService && len(svc.Profiles) == 0 {
			others = append(others, name)
		}
	}
	slices.Sort(others)
	want := slices.Sorted(slices.Values(pipelineServices))
	if !slices.Equal(others, want) {
		t.Errorf("compose services besides %s = %v, but reset removes %v; update pipelineServices", roomDBService, others, want)
	}

	room, ok := c.Services[roomDBService]
	if !ok {
		t.Fatalf("no %s service", roomDBService)
	}
	if _, ok := c.Volumes["roomdb"]; !ok || !slices.Contains(room.Volumes, "roomdb:/var/lib/postgresql/data") {
		t.Errorf("roomdb must keep its data on the named volume roomdb (top-level volumes %v, service volumes %v)", c.Volumes, room.Volumes)
	}
	if roomDBVolume != "piperoom-dev_roomdb" {
		t.Errorf("roomDBVolume = %q, want the compose project's name for the roomdb volume", roomDBVolume)
	}
	u, err := url.Parse(config.Load().RoomDBURL)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(room.Ports, u.Port()+":5432") {
		t.Errorf("roomdb ports %v do not publish the PIPEROOM_DB_URL default port %s", room.Ports, u.Port())
	}
	// A server the stack starts keeps its rooms in roomdb too, never in the
	// pipeline Postgres that reset wipes.
	for name, svc := range c.Services {
		if db := svc.Environment["PIPEROOM_DB_URL"]; db != "" && !strings.Contains(db, "@"+roomDBService+":5432/") {
			t.Errorf("service %s: PIPEROOM_DB_URL %q is not the %s service", name, db, roomDBService)
		}
	}
	if pg := c.Services["postgres"]; pg.Image != room.Image || !strings.Contains(pg.Image, ":17.") {
		t.Errorf("postgres image %q and roomdb image %q: want the same pinned 17.x minor", pg.Image, room.Image)
	}

	conn := c.Services["connect"]
	if !slices.Equal(conn.Command, []string{"/docker-entrypoint.sh", "start"}) {
		t.Errorf("connect command = %v, want the Debezium image's own start command", conn.Command)
	}
	if !slices.Contains(conn.Volumes, "./.logs/connect:/piperoom/logs") {
		t.Errorf("connect volumes %v do not mount the worker log dir", conn.Volumes)
	}
	if got := filepath.ToSlash(filepath.Join(workerLogDir, "connect.log")); got != config.DefaultConnectWorkerLogFile {
		t.Errorf("worker log at %q, but CONNECT_WORKER_LOG_FILE defaults to %q", got, config.DefaultConnectWorkerLogFile)
	}
}

// TestDevConnectorConfig guards the two QA fixes in the connector config: no
// publication SQL on the replication connection (G17), and a heartbeat that
// writes to the heartbeat table the init script creates and publishes (G18).
func TestDevConnectorConfig(t *testing.T) {
	var cfg map[string]string
	if err := json.Unmarshal(readDevFile(t, "dev/connectors/"+connectorName+".json"), &cfg); err != nil {
		t.Fatalf("connector config: %v", err)
	}
	if got := publicationMode(cfg); got != "disabled" {
		t.Errorf("publication.autocreate.mode = %q, want disabled (Known issues G17)", got)
	}
	if cfg["heartbeat.interval.ms"] == "" || !strings.Contains(cfg["heartbeat.action.query"], "debezium.heartbeat") {
		t.Errorf("heartbeat not configured against debezium.heartbeat: interval %q, query %q", cfg["heartbeat.interval.ms"], cfg["heartbeat.action.query"])
	}
	cdc := string(readDevFile(t, "dev/postgres-init/04-cdc.sql"))
	for _, want := range []string{"CREATE TABLE IF NOT EXISTS debezium.heartbeat", "CREATE PUBLICATION " + cfg["publication.name"] + " FOR TABLE public.orders, debezium.heartbeat"} {
		if !strings.Contains(cdc, want) {
			t.Errorf("04-cdc.sql does not contain %q", want)
		}
	}
	if !strings.HasSuffix(cdcInitFile, "/04-cdc.sql") {
		t.Errorf("cdcInitFile = %q does not name dev/postgres-init/04-cdc.sql", cdcInitFile)
	}
	if strings.Contains(string(readDevFile(t, "dev/postgres-init/03-databases.sql")), "CREATE DATABASE piperoom") {
		t.Error("the pipeline Postgres must not create the room database any more (G19)")
	}
}

func TestConnectorConfigOverrides(t *testing.T) {
	base := []byte(`{"database.user":"piperoom","publication.autocreate.mode":"disabled","tasks.max":"1"}`)
	b, err := connectorConfig(base, map[string]string{"database.user": "debezium", "publication.autocreate.mode": "filtered"})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["database.user"] != "debezium" || got["publication.autocreate.mode"] != "filtered" || got["tasks.max"] != "1" {
		t.Errorf("overridden config = %v", got)
	}
	if _, err := connectorConfig([]byte(`not json`), nil); err == nil {
		t.Error("bad base config: want an error")
	}
}

func TestPrepareWorkerLogDir(t *testing.T) {
	root := t.TempDir()
	s := &stack{root: root}
	for i := 0; i < 2; i++ { // twice: an existing directory is fine
		if err := s.prepareWorkerLogDir(); err != nil {
			t.Fatalf("prepareWorkerLogDir: %v", err)
		}
	}
	fi, err := os.Stat(filepath.Join(root, workerLogDir))
	if err != nil || !fi.IsDir() {
		t.Fatalf("worker log dir not created: %v", err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o777 {
		t.Errorf("worker log dir mode = %v, want 0777 so the container user can write it", fi.Mode().Perm())
	}
}
