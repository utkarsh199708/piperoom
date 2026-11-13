package compose

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/setup/protocol"
)

// updateGolden rewrites testdata/*.golden instead of comparing them:
// go test ./internal/setup/targets/compose -run RenderGolden -update
var updateGolden = flag.Bool("update", false, "update the golden files")

func TestRenderGolden(t *testing.T) {
	cases := []struct {
		name string
		p    WriteParams
	}{
		{"kafka", WriteParams{Templates: []string{TemplateKafka}}},
		{"postgres", WriteParams{Templates: []string{TemplatePostgres}}},
		{"connect", WriteParams{Templates: []string{TemplateKafka, TemplateConnect}}},
		{"registry", WriteParams{Templates: []string{TemplateKafka, TemplateRegistry}}},
		{"full", WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect}, Registry: true}},
		{"sink", WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect, TemplateSinkPostgres}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &Driver{class: protocol.ClassLocal}
			got, err := d.Render(tc.p)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if err := ValidateYAML(got.Doc, protocol.ClassLocal); err != nil {
				t.Fatalf("the render does not pass the validator: %v", err)
			}
			path := filepath.Join("testdata", tc.name+".golden")
			if *updateGolden {
				if err := os.WriteFile(path, got.Doc, 0o644); err != nil {
					t.Fatalf("writing %s: %v", path, err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if !bytes.Equal(got.Doc, want) {
				t.Errorf("the render differs from %s:\n--- got ---\n%s\n--- want ---\n%s", path, got.Doc, want)
			}
		})
	}
}

// TestRenderPinnedImages is the row's "no floating tags": every image line
// carries the digest the knowledge index pins.
func TestRenderPinnedImages(t *testing.T) {
	d := &Driver{class: protocol.ClassLocal}
	r, err := d.Render(WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect}, Registry: true})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	imageRE := regexp.MustCompile(`(?m)^\s+image: (.+)$`)
	matches := imageRE.FindAllStringSubmatch(string(r.Doc), -1)
	if len(matches) != 4 {
		t.Fatalf("got %d image lines, want 4", len(matches))
	}
	for _, m := range matches {
		if !strings.Contains(m[1], "@sha256:") {
			t.Errorf("image %q is not pinned to a digest", m[1])
		}
		if strings.HasSuffix(m[1], ":latest") {
			t.Errorf("image %q is a floating tag", m[1])
		}
	}
}

func TestRenderPortBindingFollowsClass(t *testing.T) {
	p := WriteParams{Templates: []string{TemplateKafka, TemplateConnect}}
	local, err := (&Driver{class: protocol.ClassLocal}).Render(p)
	if err != nil {
		t.Fatalf("Render(local): %v", err)
	}
	if !strings.Contains(string(local.Doc), "127.0.0.1:29092:29092") || !strings.Contains(string(local.Doc), "127.0.0.1:8083:8083") {
		t.Errorf("a local render must bind 127.0.0.1:\n%s", local.Doc)
	}
	if err := ValidateYAML(local.Doc, protocol.ClassLocal); err != nil {
		t.Errorf("a local render fails the local validator: %v", err)
	}

	raised, err := (&Driver{class: protocol.ClassShared}).Render(p)
	if err != nil {
		t.Fatalf("Render(shared): %v", err)
	}
	if strings.Contains(string(raised.Doc), "127.0.0.1:") {
		t.Errorf("a raised class publishes the port:\n%s", raised.Doc)
	}
	if !strings.Contains(string(raised.Doc), `"29092:29092"`) && !strings.Contains(string(raised.Doc), " 29092:29092") {
		t.Errorf("a raised render has no published kafka port:\n%s", raised.Doc)
	}
	if err := ValidateYAML(raised.Doc, protocol.ClassShared); err != nil {
		t.Errorf("a raised render fails the shared validator: %v", err)
	}
	if err := ValidateYAML(raised.Doc, protocol.ClassLocal); err == nil || RuleOf(err) != RulePort {
		t.Errorf("a shared render must fail a local target's port rule, got %v", err)
	}
}

func TestRenderCustomPort(t *testing.T) {
	r, err := (&Driver{}).Render(WriteParams{
		Templates: []string{TemplateKafka},
		Ports:     map[string]int{"kafka": 39092},
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	doc := string(r.Doc)
	if !strings.Contains(doc, "127.0.0.1:39092:29092") {
		t.Errorf("the host port was not applied:\n%s", doc)
	}
	if !strings.Contains(doc, "HOST://localhost:39092") {
		t.Errorf("the advertised listener does not follow the host port:\n%s", doc)
	}
}

func TestRenderRefusals(t *testing.T) {
	cases := []struct {
		name string
		p    WriteParams
		want error
	}{
		{"no templates", WriteParams{}, ErrParams},
		{"unknown template", WriteParams{Templates: []string{"nope"}}, ErrTemplate},
		{"duplicate template", WriteParams{Templates: []string{TemplateKafka, TemplateKafka}}, ErrParams},
		{"connect without kafka", WriteParams{Templates: []string{TemplateConnect}}, ErrParams},
		{"registry without kafka", WriteParams{Templates: []string{TemplateRegistry}}, ErrParams},
		{"registry flag without connect", WriteParams{Templates: []string{TemplateKafka}, Registry: true}, ErrParams},
		{"bad project", WriteParams{Templates: []string{TemplateKafka}, Project: "My Project"}, ErrParams},
		{"compose file named .env", WriteParams{Templates: []string{TemplateKafka}, File: ".env"}, ErrParams},
		{"escape", WriteParams{Templates: []string{TemplateKafka}, File: "../compose.yaml"}, ErrPath},
		{"port for absent service", WriteParams{Templates: []string{TemplateKafka}, Ports: map[string]int{"postgres": 5433}}, ErrParams},
		{"privileged port", WriteParams{Templates: []string{TemplateKafka}, Ports: map[string]int{"kafka": 80}}, ErrParams},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := (&Driver{}).Render(tc.p); !errors.Is(err, tc.want) {
				t.Fatalf("Render = %v, want errors.Is %v", err, tc.want)
			}
		})
	}
}

// TestRenderIsDeterministic guards the golden files: the same params render
// the same bytes whatever order the templates arrive in.
func TestRenderIsDeterministic(t *testing.T) {
	a, err := (&Driver{}).Render(WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	b, err := (&Driver{}).Render(WriteParams{Templates: []string{TemplateConnect, TemplatePostgres, TemplateKafka}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !bytes.Equal(a.Doc, b.Doc) {
		t.Errorf("the render depends on template order:\n%s\n%s", a.Doc, b.Doc)
	}
}
