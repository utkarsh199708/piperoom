package compose

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"piperoom/internal/setup/protocol"
)

// TestValidateServiceAllowList: every service field the templates do not
// render is refused, whatever its value. Each row is a field a reviewer got
// past the old deny-list into `docker compose up`; none of them is in
// knownRefusals, so only the allow-list refuses them.
func TestValidateServiceAllowList(t *testing.T) {
	pg := imageOf(t, "postgres")
	cases := []struct {
		name  string
		extra string
	}{
		{"use_api_socket", "    use_api_socket: true\n"},
		{"post_start hook as root", "    post_start:\n      - command: [\"sh\", \"-c\", \"id\"]\n        user: root\n        privileged: true\n"},
		{"pre_stop hook", "    pre_stop:\n      - command: [\"sh\", \"-c\", \"id\"]\n        privileged: true\n"},
		{"service networks", "    networks: [hostnet]\n"},
		{"gpus", "    gpus: all\n"},
		{"deploy device reservation", "    deploy:\n      resources:\n        reservations:\n          devices:\n            - capabilities: [gpu]\n"},
		{"uts host", "    uts: host\n"},
		{"label_file", "    label_file: /etc/shadow\n"},
		{"provider", "    provider:\n      type: sh\n      options: {c: id}\n"},
		{"remote syslog logging", "    logging:\n      driver: syslog\n      options: {syslog-address: \"tcp://198.51.100.1:514\"}\n"},
		{"oom_score_adj", "    oom_score_adj: -1000\n"},
		{"storage_opt", "    storage_opt: {size: 1G}\n"},
		{"user", "    user: root\n"},
		{"restart", "    restart: always\n"},
		{"labels", "    labels: [a=b]\n"},
		{"tmpfs", "    tmpfs: [/run]\n"},
		{"working_dir", "    working_dir: /\n"},
		{"an extension field", "    x-anything: 1\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateYAML(docWith(pg, tc.extra), protocol.ClassLocal)
			if err == nil {
				t.Fatal("ValidateYAML passed, want a refusal")
			}
			if got := RuleOf(err); got != RuleField {
				t.Fatalf("rule = %q, want %q (%v)", got, RuleField, err)
			}
			key := strings.TrimSpace(strings.SplitN(tc.extra, ":", 2)[0])
			if !strings.Contains(err.Error(), q(key)) {
				t.Fatalf("the refusal does not name %s: %v", key, err)
			}
		})
	}
}

// TestValidateNestedAllowLists: the values of allowed fields, the top-level
// declarations and the long volume form are allow-listed too.
func TestValidateNestedAllowLists(t *testing.T) {
	pg := imageOf(t, "postgres")
	svc := "services:\n  db:\n    image: " + pg + "\n"
	cases := []struct {
		name string
		doc  string
		rule string // "" means the document is valid
	}{
		{"host network declared external", svc + "    networks: [hostnet]\nnetworks:\n  hostnet:\n    external: true\n    name: host\n", RuleNetwork},
		{"a top-level network alone", svc + "networks:\n  hostnet:\n    external: true\n    name: host\n", RuleNetwork},
		{"a bridge network declaration", svc + "networks:\n  default:\n    driver: bridge\n", RuleNetwork},
		{"an external volume", svc + "    volumes: [\"pg:/x\"]\nvolumes:\n  pg:\n    external: true\n    name: other_project_pgdata\n", RuleTopLevel},
		{"a renamed volume", svc + "volumes:\n  pg:\n    name: other_project_pgdata\n", RuleTopLevel},
		{"a volume driver", svc + "volumes:\n  pg:\n    driver: local\n", RuleTopLevel},
		{"a volume label", svc + "volumes:\n  pg:\n    labels: [a=b]\n", RuleTopLevel},
		{"a plain volume declaration", svc + "    volumes: [\"pg:/x\"]\nvolumes:\n  pg:\n", ""},
		{"an empty volume declaration", svc + "    volumes: [\"pg:/x\"]\nvolumes:\n  pg: {}\n", ""},
		{"a bad volume name", svc + "volumes:\n  \"../x\": {}\n", RuleTopLevel},
		{"a volume subpath escape", svc + "    volumes:\n      - {type: volume, source: pg, target: /x, volume: {subpath: ../../..}}\n", RuleMount},
		{"a bind propagation option", svc + "    volumes:\n      - {type: bind, source: ./data, target: /x, read_only: true, bind: {propagation: rshared}}\n", RuleMount},
		{"a bind create_host_path", svc + "    volumes:\n      - {type: bind, source: ./data, target: /x, read_only: true, bind: {create_host_path: true}}\n", RuleMount},
		{"a consistency option", svc + "    volumes:\n      - {type: volume, source: pg, target: /x, consistency: cached}\n", RuleMount},
		{"an interpolated volume source", svc + "    volumes: [\"${POSTGRES_USER}:/host:ro\"]\n", RuleYAML},
		{"an interpolated long bind source", svc + "    volumes:\n      - {type: bind, source: \"./${POSTGRES_DB}\", target: /x, read_only: true}\n", RuleYAML},
		{"an interpolated command", svc + "    command: [\"${POSTGRES_USER}\"]\n", RuleYAML},
		{"an interpolated network_mode", svc + "    network_mode: \"${POSTGRES_DB}\"\n", RuleYAML},
		{"an interpolated project name", "name: \"${POSTGRES_DB}\"\n" + svc, RuleTopLevel},
		{"a bad project name", "name: \"Bad Name\"\n" + svc, RuleTopLevel},
		{"an interpolated environment key", svc + "    environment:\n      - ${POSTGRES_DB}=x\n", RuleYAML},
		{"a nested environment value", svc + "    environment:\n      A: {b: c}\n", RuleYAML},
		{"a depends_on restart", svc + "    depends_on:\n      kafka: {condition: service_healthy, restart: true}\n", RuleField},
		{"a depends_on condition", svc + "    depends_on:\n      kafka: {condition: whatever}\n", RuleYAML},
		{"a depends_on list", svc + "    depends_on: [kafka]\n", ""},
		{"a healthcheck start_period", svc + "    healthcheck:\n      test: [CMD, \"true\"]\n      start_period: 1s\n", RuleField},
		{"a healthcheck disable", svc + "    healthcheck:\n      disable: true\n", RuleField},
		{"a healthcheck interval", svc + "    healthcheck:\n      test: [CMD, \"true\"]\n      interval: forever\n", RuleYAML},
		{"a rendered healthcheck", svc + "    healthcheck:\n      test: [CMD, pg_isready, -U, \"${POSTGRES_USER}\"]\n      interval: 5s\n      retries: 20\n", ""},
		{"a command map", svc + "    command: {a: b}\n", RuleYAML},
		{"a rendered command", svc + "    command: [postgres, -c, wal_level=logical]\n", ""},
		// yaml.v3 resolves anchors and merge keys before the checks run, so a
		// field hidden behind an alias is still seen.
		{"a merged use_api_socket", "services:\n  a:\n    image: " + pg + "\n    healthcheck: &hc\n      test: [CMD, \"true\"]\n  db:\n    <<: {use_api_socket: true}\n    image: " + pg + "\n", RuleField},
		{"a merged anchor", "services:\n  a: &base\n    image: " + pg + "\n    post_start: [{command: [id]}]\n  db:\n    <<: *base\n", RuleField},
		{"a merged privileged", "services:\n  db:\n    <<: &p {privileged: true}\n    image: " + pg + "\n", RulePrivilege},
		{"an aliased allowed value", "services:\n  a:\n    image: " + pg + "\n    command: &c [postgres]\n  db:\n    image: " + pg + "\n    command: *c\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateYAML([]byte(tc.doc), protocol.ClassLocal)
			if tc.rule == "" {
				if err != nil {
					t.Fatalf("ValidateYAML = %v, want a pass", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateYAML passed, want the %s rule", tc.rule)
			}
			if got := RuleOf(err); got != tc.rule {
				t.Fatalf("rule = %q, want %q (%v)", got, tc.rule, err)
			}
		})
	}
}

// TestEveryRenderPassesAllowList renders every template combination the
// driver accepts, on every class, and requires each to pass the validator:
// the allow-list must refuse nothing the driver itself writes.
func TestEveryRenderPassesAllowList(t *testing.T) {
	templates := []string{TemplateKafka, TemplateConnect, TemplateRegistry, TemplatePostgres}
	rendered := 0
	for _, class := range []string{protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction} {
		d := &Driver{class: class}
		for mask := 1; mask < 1<<len(templates); mask++ {
			var sel []string
			for i, name := range templates {
				if mask&(1<<i) != 0 {
					sel = append(sel, name)
				}
			}
			for _, registry := range []bool{false, true} {
				r, err := d.Render(WriteParams{Templates: sel, Registry: registry, Project: "piperoom-proj-1", Ports: nil})
				if err != nil {
					continue // a combination the driver refuses to render
				}
				rendered++
				binds, err := validateDoc(r.Doc, class)
				if err != nil {
					t.Errorf("%s %v registry=%v: the render fails the validator: %v", class, sel, registry, err)
					continue
				}
				if strings.Join(binds, ",") != strings.Join(r.Mounts, ",") {
					t.Errorf("%s %v: binds = %q, want the render's mounts %q", class, sel, binds, r.Mounts)
				}
			}
		}
	}
	if rendered < 3*8 {
		t.Fatalf("only %d renders; the combination loop is broken", rendered)
	}
}

// tamperedDocs are bypasses written over the rendered file on disk; every
// stack op must refuse each one before docker runs.
func tamperedDocs(t *testing.T) map[string]string {
	t.Helper()
	k := imageOf(t, "kafka")
	svc := "name: piperoom-setup\nservices:\n  kafka:\n    image: " + k + "\n"
	return map[string]string{
		"use_api_socket": svc + "    use_api_socket: true\n",
		"post_start":     svc + "    post_start:\n      - command: [sh, -c, id]\n        user: root\n        privileged: true\n",
		"host network":   svc + "    networks: [hostnet]\nnetworks:\n  hostnet:\n    external: true\n    name: host\n",
		"gpus":           svc + "    gpus: all\n",
		"uts":            svc + "    uts: host\n",
		"label_file":     svc + "    label_file: /etc/shadow\n",
		"provider":       svc + "    provider: {type: sh}\n",
		"subpath":        svc + "    volumes:\n      - {type: volume, source: v, target: /x, volume: {subpath: ../../..}}\nvolumes:\n  v: {}\n",
		"external vol":   svc + "    volumes: [\"v:/x\"]\nvolumes:\n  v: {external: true}\n",
	}
}

// TestStackOpsRefuseTamperedFiles: up, restart, down and the reads that load
// the project refuse every bypass on disk before docker runs.
func TestStackOpsRefuseTamperedFiles(t *testing.T) {
	for name, doc := range tamperedDocs(t) {
		for _, op := range []string{"compose_up", "compose_restart", "compose_down", "compose_ps", "compose_config"} {
			t.Run(name+"/"+op, func(t *testing.T) {
				reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
					return []byte("[]"), 0, nil
				})
				if err := os.WriteFile(filepath.Join(dir, DefaultFile), []byte(doc), 0o644); err != nil {
					t.Fatal(err)
				}
				if _, err := runOp(t, reg, op, map[string]any{}); !errors.Is(err, ErrStack) {
					t.Fatalf("%s on a tampered file = %v, want ErrStack", op, err)
				}
				if len(fr.calls) != 0 {
					t.Fatalf("docker ran on a tampered file: %q", fr.calls)
				}
			})
		}
	}
	t.Run("container_inspect", func(t *testing.T) {
		reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
			return []byte(""), 0, nil
		})
		if err := os.WriteFile(filepath.Join(dir, DefaultFile), []byte(tamperedDocs(t)["label_file"]), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := runOp(t, reg, "container_inspect", map[string]any{"service": "kafka"}); !errors.Is(err, ErrStack) {
			t.Fatalf("container_inspect on a tampered file = %v, want ErrStack", err)
		}
		if len(fr.calls) != 0 {
			t.Fatalf("docker ran on a tampered file: %q", fr.calls)
		}
	})
}

// TestStackOpsRunValidatedSnapshot: docker reads a private copy of exactly
// the bytes the validator checked. The fake docker swaps the project file for
// a privileged one before reading what -f names (the TOCTOU window of the
// old path-based argv): it must still read the validated bytes.
func TestStackOpsRunValidatedSnapshot(t *testing.T) {
	for _, op := range []string{"compose_up", "compose_restart", "compose_down", "compose_ps", "compose_config"} {
		t.Run(op, func(t *testing.T) {
			var dir, snap string
			var seen []byte
			reg, fr, d, _ := newDockerHarness(t, Options{}, func(argv []string) ([]byte, int, error) {
				tampered := "services:\n  kafka:\n    image: x\n    privileged: true\n"
				if err := os.WriteFile(filepath.Join(dir, DefaultFile), []byte(tampered), 0o644); err != nil {
					t.Error(err)
				}
				for i := range argv {
					if argv[i] == "-f" && i+1 < len(argv) {
						snap = argv[i+1]
					}
				}
				fi, err := os.Stat(snap)
				if err != nil {
					t.Errorf("the snapshot is missing while docker runs: %v", err)
					return nil, 1, nil
				}
				// NTFS reports 0666 for every writable file and os.Chmod only
				// toggles the read-only bit, so the mode is checked on POSIX.
				if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
					t.Errorf("snapshot mode = %o, want 600", fi.Mode().Perm())
				}
				seen, _ = os.ReadFile(snap)
				if argv[len(argv)-1] == "--no-interpolate" {
					return []byte("services: {}\n"), 0, nil
				}
				return []byte("[]"), 0, nil
			})
			dir = d
			want, err := os.ReadFile(filepath.Join(dir, DefaultFile))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runOp(t, reg, op, map[string]any{}); err != nil {
				t.Fatalf("%s: %v", op, err)
			}
			if len(fr.calls) != 1 {
				t.Fatalf("docker ran %d times, want 1", len(fr.calls))
			}
			if !bytes.Equal(seen, want) {
				t.Fatalf("docker read %q, want the validated bytes", seen)
			}
			if rel, err := filepath.Rel(dir, snap); err == nil && !strings.HasPrefix(rel, "..") {
				t.Fatalf("the snapshot %s is inside the project", snap)
			}
			if _, err := os.Stat(filepath.Dir(snap)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the snapshot directory outlived the op: %v", err)
			}
		})
	}
}

// TestStackRefusesSymlinkBind: a ./x bind is lexically inside the project,
// but a symlink at x would make docker bind its target; the on-disk check
// refuses it, for the leaf and for a parent component.
func TestStackRefusesSymlinkBind(t *testing.T) {
	for _, tc := range []struct{ name, link, doc string }{
		{"the plugin directory", PluginDir, ""},
		{"a parent component", "conf", "conf/sub"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
				return []byte("ok"), 0, nil
			})
			if tc.doc != "" {
				doc := "services:\n  kafka:\n    image: " + imageOf(t, "kafka") + "\n    volumes: [\"./" + tc.doc + ":/x:ro\"]\n"
				if err := os.WriteFile(filepath.Join(dir, DefaultFile), []byte(doc), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			_ = os.RemoveAll(filepath.Join(dir, tc.link))
			if err := os.Symlink("/etc", filepath.Join(dir, tc.link)); err != nil {
				// A Windows user without SeCreateSymbolicLinkPrivilege
				// (no Developer Mode) cannot create one.
				t.Skipf("symlinks unavailable: %v", err)
			}
			if _, err := runOp(t, reg, "compose_up", map[string]any{}); !errors.Is(err, ErrStack) || RuleOf(err) != RuleMount {
				t.Fatalf("compose_up with a symlinked bind = %v, want ErrStack and the mount rule", err)
			}
			if len(fr.calls) != 0 {
				t.Fatalf("docker ran with a symlinked bind: %q", fr.calls)
			}
		})
	}
	t.Run("a real directory passes", func(t *testing.T) {
		reg, fr, dir, _ := newDockerHarness(t, Options{}, func([]string) ([]byte, int, error) {
			return []byte("ok"), 0, nil
		})
		if err := os.Mkdir(filepath.Join(dir, PluginDir), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := runOp(t, reg, "compose_up", map[string]any{}); err != nil {
			t.Fatalf("compose_up: %v", err)
		}
		if len(fr.calls) != 1 {
			t.Fatalf("docker ran %d times, want 1", len(fr.calls))
		}
	})
}

// TestProjectNamePinned: a compose file can only name a piperoom- project, and
// every stack op names that project to docker with -p, so neither a tampered
// name: nor COMPOSE_PROJECT_NAME in the project's .env can point up or down at
// another stack on the host.
func TestProjectNamePinned(t *testing.T) {
	d := &Driver{class: "local"}
	r, err := d.Render(WriteParams{Templates: []string{TemplateKafka}})
	if err != nil {
		t.Fatal(err)
	}
	// The render's own services block, with the name: line taken out.
	svc := strings.Replace(string(r.Doc), "name: "+DefaultProject+"\n", "", 1)
	if svc == string(r.Doc) {
		t.Fatal("the render carries no name: line")
	}
	for _, name := range []string{"other-app", "piperoom", "piperoomx-a", "Piperoom-a"} {
		if _, err := validateDoc([]byte("name: "+name+"\n"+svc), "local"); err == nil {
			t.Errorf("name %q passed validation", name)
		}
	}
	for doc, want := range map[string]string{
		svc:                             DefaultProject,
		"name: piperoom-room-7\n" + svc: "piperoom-room-7",
	} {
		if _, err := validateDoc([]byte(doc), "local"); err != nil {
			t.Fatalf("validateDoc(%q): %v", doc, err)
		}
		got, err := docProjectName([]byte(doc))
		if err != nil || got != want {
			t.Fatalf("docProjectName(%q) = %q, %v; want %q", doc, got, err, want)
		}
	}
	if _, err := docProjectName([]byte("name: other-app\n" + svc)); err == nil {
		t.Fatal("docProjectName accepted a name outside the piperoom- namespace")
	}
	argv, err := snapshotArgv([]string{"docker", "compose", "--project-directory", "/p", "-f", "/p/compose.yaml", "--env-file", "/p/.env", "up"}, "/s/compose.yaml", "piperoom-room-7")
	if err != nil {
		t.Fatal(err)
	}
	want := "docker compose --project-directory /p -f /s/compose.yaml -p piperoom-room-7 --env-file /p/.env up"
	if strings.Join(argv, " ") != want {
		t.Fatalf("argv = %q, want %q", argv, want)
	}
	if _, err := d.Render(WriteParams{Templates: []string{TemplateKafka}, Project: "other-app"}); err == nil {
		t.Fatal("compose_write rendered a project outside the piperoom- namespace")
	}
}
