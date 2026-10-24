package server

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
)

func TestRenderLocalSource(t *testing.T) {
	f := newFixture(t, Options{})
	r, err := f.drv.Render(ComposeParams{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if r.File != ComposeFile {
		t.Errorf("file = %q", r.File)
	}
	if want := []string{DefaultService, SourceService}; !reflect.DeepEqual(r.Services, want) {
		t.Errorf("services = %v, want %v", r.Services, want)
	}
	if want := []string{"POSTGRES_DB", "POSTGRES_PASSWORD", "POSTGRES_USER"}; !reflect.DeepEqual(r.EnvKeys, want) {
		t.Errorf("env keys = %v, want %v", r.EnvKeys, want)
	}
	image, err := knowledge.ImageByName("server")
	if err != nil {
		t.Fatal(err)
	}
	text := string(r.Doc)
	if !strings.Contains(text, image.Ref()) {
		t.Errorf("the render does not pin %s:\n%s", image.Ref(), text)
	}
	if !strings.Contains(text, "./"+ConfigDir+":"+ServerConfigDir+":ro") {
		t.Errorf("the config directory is not the read-only bind:\n%s", text)
	}
	if !strings.Contains(text, VolumeData+":"+ServerDataDir) {
		t.Errorf("the data directory is not the named volume:\n%s", text)
	}
	if !strings.Contains(text, "DEBEZIUM_SOURCE_DATABASE_PASSWORD=${POSTGRES_PASSWORD}") {
		t.Errorf("the source password is not an environment value:\n%s", text)
	}
	if !strings.Contains(text, "name: "+DefaultProject) {
		t.Errorf("the project name is not rendered:\n%s", text)
	}
	// The validator the Compose driver applies is applied to the render.
	if err := compose.ValidateYAML(r.Doc, protocol.ClassLocal); err != nil {
		t.Fatalf("the render does not validate: %v", err)
	}
}

func TestRenderServicesShape(t *testing.T) {
	f := newFixture(t, Options{})
	r, err := f.drv.Render(ComposeParams{})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Name     string `yaml:"name"`
		Services map[string]struct {
			Image       string              `yaml:"image"`
			Environment []string            `yaml:"environment"`
			Ports       []string            `yaml:"ports"`
			Volumes     []string            `yaml:"volumes"`
			DependsOn   map[string]struct{} `yaml:"depends_on"`
		} `yaml:"services"`
		Volumes map[string]any `yaml:"volumes"`
	}
	if err := yaml.Unmarshal(r.Doc, &doc); err != nil {
		t.Fatalf("parsing the render: %v", err)
	}
	if doc.Name != DefaultProject {
		t.Errorf("name = %q", doc.Name)
	}
	srv, ok := doc.Services[DefaultService]
	if !ok {
		t.Fatalf("no %s service: %v", DefaultService, doc.Services)
	}
	if len(srv.Environment) != 1 || len(srv.Ports) != 0 {
		t.Errorf("server environment %v, ports %v", srv.Environment, srv.Ports)
	}
	if len(srv.Volumes) != 2 || srv.Volumes[0] != "./"+ConfigDir+":"+ServerConfigDir+":ro" {
		t.Errorf("server volumes = %v", srv.Volumes)
	}
	if _, ok := srv.DependsOn[SourceService]; !ok {
		t.Errorf("the server does not depend on the source: %v", srv.DependsOn)
	}
	if _, ok := doc.Volumes[VolumeData]; !ok {
		t.Errorf("the named volume is not declared: %v", doc.Volumes)
	}
	src, ok := doc.Services[SourceService]
	if !ok {
		t.Fatalf("no %s service", SourceService)
	}
	if len(src.Environment) != 3 || !strings.Contains(strings.Join(src.Environment, ","), "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}") {
		t.Errorf("source environment = %v", src.Environment)
	}
}

func TestRenderExternalSource(t *testing.T) {
	f := newFixture(t, Options{})
	r, err := f.drv.Render(ComposeParams{Source: SourceExternal})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if want := []string{DefaultService}; !reflect.DeepEqual(r.Services, want) {
		t.Errorf("services = %v, want %v", r.Services, want)
	}
	if len(r.EnvKeys) != 0 {
		t.Errorf("env keys = %v, want none", r.EnvKeys)
	}
	if strings.Contains(string(r.Doc), "$") {
		t.Errorf("an external-source render interpolates something:\n%s", r.Doc)
	}
	if err := compose.ValidateYAML(r.Doc, protocol.ClassLocal); err != nil {
		t.Errorf("the render does not validate: %v", err)
	}
}

func TestRenderPorts(t *testing.T) {
	local := newFixture(t, Options{})
	r, err := local.drv.Render(ComposeParams{SourcePort: 15432})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(r.Doc), "127.0.0.1:15432:5432") {
		t.Errorf("a local render does not bind loopback:\n%s", r.Doc)
	}
	shared := newFixture(t, Options{Class: protocol.ClassShared})
	r, err = shared.drv.Render(ComposeParams{SourcePort: 15432})
	if err != nil {
		t.Fatalf("Render shared: %v", err)
	}
	if !strings.Contains(string(r.Doc), "15432:5432") || strings.Contains(string(r.Doc), "127.0.0.1:15432") {
		t.Errorf("a raised class publishes the plain form:\n%s", r.Doc)
	}
}

func TestRenderPassesValidatorForEveryClass(t *testing.T) {
	for _, class := range []string{protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction} {
		f := newFixture(t, Options{Class: class})
		r, err := f.drv.Render(ComposeParams{SourcePort: 15432})
		if err != nil {
			t.Fatalf("%s: Render: %v", class, err)
		}
		if err := compose.ValidateYAML(r.Doc, class); err != nil {
			t.Errorf("%s: the render does not validate: %v", class, err)
		}
	}
}

func TestRenderRefusals(t *testing.T) {
	f := newFixture(t, Options{})
	cases := []struct {
		name   string
		params ComposeParams
	}{
		{"project outside the namespace", ComposeParams{Project: "orders"}},
		{"project with a dot", ComposeParams{Project: "piperoom-a.b"}},
		{"unknown source", ComposeParams{Source: "mysql"}},
		{"a port for an external source", ComposeParams{Source: SourceExternal, SourcePort: 15432}},
		{"a privileged port", ComposeParams{SourcePort: 543}},
		{"a port above the range", ComposeParams{SourcePort: 70000}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if r, err := f.drv.Render(c.params); !errors.Is(err, ErrParams) {
				t.Errorf("Render = %v, %v; want ErrParams", r.File, err)
			}
		})
	}
}

// TestValidatorRefusesTamperedRender proves the Compose validator is the gate
// on this template's output, not decoration: the same document with a field
// the validator forbids and with a wrong digest is refused.
func TestValidatorRefusesTamperedRender(t *testing.T) {
	f := newFixture(t, Options{})
	r, err := f.drv.Render(ComposeParams{})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(r.Doc, &doc); err != nil {
		t.Fatal(err)
	}
	services := doc["services"].(map[string]any)
	server := services[DefaultService].(map[string]any)
	server["privileged"] = true
	tampered, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := compose.ValidateYAML(tampered, protocol.ClassLocal); err == nil {
		t.Fatal("a privileged server service passed the validator")
	} else if rule := compose.RuleOf(err); rule != compose.RulePrivilege {
		t.Errorf("rule = %q, want %q (%v)", rule, compose.RulePrivilege, err)
	}

	delete(server, "privileged")
	server["image"] = strings.Replace(server["image"].(string), "sha256:", "sha256:0", 1)
	tampered, err = yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := compose.ValidateYAML(tampered, protocol.ClassLocal); err == nil {
		t.Fatal("a wrong digest passed the validator")
	} else if rule := compose.RuleOf(err); rule != compose.RuleImage {
		t.Errorf("rule = %q, want %q (%v)", rule, compose.RuleImage, err)
	}
}

func TestComposeWriteOp(t *testing.T) {
	f := newFixture(t, Options{})
	res, err := f.run("server_compose_write", ComposeParams{})
	if err != nil {
		t.Fatalf("server_compose_write: %v", err)
	}
	if !f.exists(ComposeFile) {
		t.Fatalf("%s was not written", ComposeFile)
	}
	data := f.data(res)
	if data["changed"] != true || data["env_file"] != EnvFileName {
		t.Errorf("data = %v", data)
	}
	if want := []any{DefaultService, SourceService}; !reflect.DeepEqual(data["services"], want) {
		t.Errorf("services = %v, want %v", data["services"], want)
	}
	image, err := knowledge.ImageByName("server")
	if err != nil {
		t.Fatal(err)
	}
	if data["image"] != image.Ref() {
		t.Errorf("image = %v, want %s", data["image"], image.Ref())
	}
	if res.Receipt.Kind != "file" || res.Receipt.Target != ComposeFile {
		t.Errorf("receipt = %+v", res.Receipt)
	}
	again, err := f.run("server_compose_write", ComposeParams{})
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if f.data(again)["changed"] != false {
		t.Errorf("second write = %v", f.data(again))
	}
	if _, err := f.run("server_compose_write", map[string]any{"services": []string{"dbzserver"}}); !errors.Is(err, ErrParams) {
		t.Errorf("unknown field: %v", err)
	}
}
