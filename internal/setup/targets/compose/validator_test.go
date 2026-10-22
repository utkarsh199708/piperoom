package compose

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
)

// imageOf returns the pinned reference of a knowledge image.
func imageOf(t *testing.T, name string) string {
	t.Helper()
	im, err := knowledge.ImageByName(name)
	if err != nil {
		t.Fatalf("knowledge.ImageByName(%q): %v", name, err)
	}
	return im.Ref()
}

// docWith builds a one-service document with extra service fields.
func docWith(image, extra string) []byte {
	return []byte("services:\n  db:\n    image: " + image + "\n" + extra)
}

// TestValidateRules is the row's table test: one row per rule of design §2.5.
func TestValidateRules(t *testing.T) {
	pg := imageOf(t, "postgres")
	cases := []struct {
		name  string
		extra string
		class string
		rule  string // "" means the document is valid
	}{
		{"a plain service", "", protocol.ClassLocal, ""},
		{"loopback port", "    ports: [\"127.0.0.1:5432:5432\"]\n", protocol.ClassLocal, ""},
		{"port on every interface", "    ports: [\"5432:5432\"]\n", protocol.ClassLocal, RulePort},
		{"port on 0.0.0.0", "    ports: [\"0.0.0.0:5432:5432\"]\n", protocol.ClassLocal, RulePort},
		{"port without an address", "    ports: [\"5432\"]\n", protocol.ClassLocal, RulePort},
		{"a raised class may publish", "    ports: [\"5432:5432\"]\n", protocol.ClassShared, ""},
		{"privileged", "    privileged: true\n", protocol.ClassLocal, RulePrivilege},
		{"privileged false is still the field", "    privileged: false\n", protocol.ClassLocal, RulePrivilege},
		{"cap_add", "    cap_add: [NET_ADMIN]\n", protocol.ClassLocal, RulePrivilege},
		{"devices", "    devices: [\"/dev/fuse\"]\n", protocol.ClassLocal, RulePrivilege},
		{"security_opt", "    security_opt: [\"seccomp=unconfined\"]\n", protocol.ClassLocal, RulePrivilege},
		{"pid", "    pid: host\n", protocol.ClassLocal, RulePrivilege},
		{"ipc", "    ipc: host\n", protocol.ClassLocal, RulePrivilege},
		{"userns_mode", "    userns_mode: host\n", protocol.ClassLocal, RulePrivilege},
		{"sysctls", "    sysctls: {net.core.somaxconn: 1024}\n", protocol.ClassLocal, RulePrivilege},
		{"volumes_from", "    volumes_from: [other]\n", protocol.ClassLocal, RulePrivilege},
		{"network_mode host", "    network_mode: host\n", protocol.ClassLocal, RuleNetwork},
		{"network_mode service", "    network_mode: \"service:other\"\n", protocol.ClassLocal, RuleNetwork},
		{"network_mode bridge", "    network_mode: bridge\n", protocol.ClassLocal, ""},
		{"the docker socket", "    volumes: [\"/var/run/docker.sock:/var/run/docker.sock\"]\n", protocol.ClassLocal, RuleMount},
		{"the root", "    volumes: [\"/:/host\"]\n", protocol.ClassLocal, RuleMount},
		{"a bind above the project", "    volumes: [\"../outside:/data\"]\n", protocol.ClassLocal, RuleMount},
		{"the project root itself", "    volumes: [\".:/data\"]\n", protocol.ClassLocal, RuleMount},
		{"a bind under the project", "    volumes: [\"./data:/var/lib/postgresql/data\"]\n", protocol.ClassLocal, ""},
		{"a named volume", "    volumes: [\"pgdata:/var/lib/postgresql/data\"]\n", protocol.ClassLocal, ""},
		{"an anonymous volume", "    volumes: [\"/var/lib/postgresql/data\"]\n", protocol.ClassLocal, ""},
		{"a relative anonymous path", "    volumes: [\"./data\"]\n", protocol.ClassLocal, RuleMount},
		{"a long bind above the project", "    volumes:\n      - {type: bind, source: /etc, target: /x}\n", protocol.ClassLocal, RuleMount},
		{"a long named volume", "    volumes:\n      - {type: volume, source: pgdata, target: /x}\n", protocol.ClassLocal, ""},
		{"a long tmpfs", "    volumes:\n      - {type: tmpfs, target: /x}\n", protocol.ClassLocal, RuleMount},
		{"the plugin directory", "    volumes: [\"./connect-plugins:/kafka/connect\"]\n", protocol.ClassLocal, ""},
		{"another plugin source", "    volumes: [\"/opt/plugins:/kafka/connect\"]\n", protocol.ClassLocal, RuleMount},
		{"a named volume as the plugin directory", "    volumes: [\"pgdata:/kafka/connect\"]\n", protocol.ClassLocal, RuleMount},
		{"a long plugin mount", "    volumes:\n      - {type: bind, source: ./connect-plugins, target: /kafka/connect}\n", protocol.ClassLocal, ""},
		{"extends", "    extends: {service: other}\n", protocol.ClassLocal, RuleExtends},
		{"build", "    build: {context: .}\n", protocol.ClassLocal, RuleBuild},
		{"env_file", "    env_file: [.env]\n", protocol.ClassLocal, RuleMount},
		{"a service secret", "    secrets: [db]\n", protocol.ClassLocal, RuleMount},
		{"a plain-text credential", "    environment:\n      POSTGRES_PASSWORD: hunter2\n", protocol.ClassLocal, RuleSecret},
		{"an interpolated credential", "    environment:\n      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}\n", protocol.ClassLocal, ""},
		{"a converter holds the word key", "    environment:\n      - KEY_CONVERTER=io.confluent.connect.avro.AvroConverter\n", protocol.ClassLocal, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateYAML(docWith(pg, tc.extra), tc.class)
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

// TestValidatePlainSecretDoesNotEchoTheValue: a refusal never carries the
// credential it refused.
func TestValidatePlainSecretDoesNotEchoTheValue(t *testing.T) {
	err := ValidateYAML(docWith(imageOf(t, "postgres"),
		"    environment:\n      POSTGRES_PASSWORD: hunter2\n"), protocol.ClassLocal)
	if err == nil {
		t.Fatal("a plain-text credential passed")
	}
	if !errors.Is(err, ErrSecretValue) {
		t.Fatalf("errors.Is(err, ErrSecretValue) = false: %v", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("the refusal carries the value: %v", err)
	}
}

func TestValidateImages(t *testing.T) {
	pg := imageOf(t, "postgres")
	_, _, _, digest, err := parseImageRef(pg)
	if err != nil {
		t.Fatalf("parseImageRef(%q): %v", pg, err)
	}
	last := byte('0')
	if strings.HasSuffix(digest, "0") {
		last = '1'
	}
	flipped := digest[:len(digest)-1] + string(last)
	cases := []struct {
		name  string
		image string
		rule  string
	}{
		{"the pinned reference", pg, ""},
		{"the library prefix", "docker.io/library/postgres:17@" + digest, ""},
		{"a short reference", "postgres:17@" + digest, ""},
		{"a tag without a digest", "postgres:17", RuleImage},
		{"an untagged reference", "docker.io/library/postgres@" + digest, RuleImage},
		{"an unlisted image", "docker.io/library/redis:7@sha256:" + strings.Repeat("a", 64), RuleImage},
		{"an unlisted tag", "docker.io/library/postgres:18@" + digest, RuleImage},
		{"a wrong digest", "docker.io/library/postgres:17@sha256:" + flipped, RuleImage},
		{"another registry", "ghcr.io/library/postgres:17@" + digest, RuleImage},
		{"a malformed reference", "not a reference", RuleImage},
		{"no image", "", RuleImage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateYAML(docWith(tc.image, ""), protocol.ClassLocal)
			if tc.rule == "" {
				if err != nil {
					t.Fatalf("ValidateYAML = %v, want a pass", err)
				}
				return
			}
			if got := RuleOf(err); got != tc.rule {
				t.Fatalf("rule = %q, want %q (%v)", got, tc.rule, err)
			}
		})
	}
}

func TestValidateMalformed(t *testing.T) {
	pg := imageOf(t, "postgres")
	cases := []struct {
		name string
		doc  string
		rule string
	}{
		{"empty", "", RuleYAML},
		{"unclosed", "services: [unclosed", RuleYAML},
		{"a list", "- a\n- b\n", RuleYAML},
		{"a duplicate key", "services:\n  db:\n    image: " + pg + "\n  db:\n    image: " + pg + "\n", RuleYAML},
		{"no services", "name: stack\n", RuleTopLevel},
		{"empty services", "services: {}\n", RuleTopLevel},
		{"an include", "include: [other.yaml]\nservices: {}\n", RuleTopLevel},
		{"an extension", "x-defaults: {}\nservices: {}\n", RuleTopLevel},
		{"a service without an image", "services:\n  db:\n    restart: always\n", RuleImage},
		{"a bad service name", "services:\n  \"db x\":\n    image: " + pg + "\n", RuleTopLevel},
		{"a volume driver_opts", "services:\n  db:\n    image: " + pg + "\nvolumes:\n  pg:\n    driver_opts:\n      o: bind\n      device: /etc\n", RuleTopLevel},
		{"a host network driver", "services:\n  db:\n    image: " + pg + "\nnetworks:\n  default:\n    driver: host\n", RuleNetwork},
		{"a huge document", strings.Repeat("#", maxComposeBytes+1), RuleYAML},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateYAML([]byte(tc.doc), protocol.ClassLocal)
			if got := RuleOf(err); got != tc.rule {
				t.Fatalf("rule = %q, want %q (%v)", got, tc.rule, err)
			}
		})
	}
}
