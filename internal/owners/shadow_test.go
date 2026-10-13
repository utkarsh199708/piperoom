package owners

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestShadowBlock(t *testing.T) {
	cases := []struct {
		name    string
		block   string
		want    bool
		wantErr string
	}{
		{name: "absent", block: "", want: false},
		{name: "enabled true", block: "    shadow:\n      enabled: true\n", want: true},
		{name: "enabled false", block: "    shadow:\n      enabled: false\n", want: false},
		{name: "flow form", block: "    shadow: {enabled: true}\n", want: true},
		{name: "empty block", block: "    shadow: {}\n", want: false},
		{name: "unknown key", block: "    shadow:\n      enable: true\n", wantErr: `shadow: unknown field "enable"`},
		{name: "extra key", block: "    shadow: {enabled: true, budget: 5}\n", wantErr: `unknown field "budget"`},
		{name: "not a mapping", block: "    shadow: true\n", wantErr: "parse"},
		{name: "enabled not bool", block: "    shadow: {enabled: maybe}\n", wantErr: "parse"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, withWatch(tc.block)))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders")
			if p.Shadow() != tc.want {
				t.Fatalf("Shadow() = %v, want %v", p.Shadow(), tc.want)
			}
			if o.AnyShadow() != tc.want {
				t.Fatalf("AnyShadow() = %v, want %v", o.AnyShadow(), tc.want)
			}
		})
	}
}

func TestShadowZeroValues(t *testing.T) {
	if (Pipeline{ID: "x"}).Shadow() {
		t.Fatal("a pipeline built in code must not be shadow")
	}
	var nilOwners *Owners
	if nilOwners.AnyShadow() {
		t.Fatal("nil owners must have no shadow pipeline")
	}
}

// Every owners file in the repo must still load with the shadow field added.
func TestRepoOwnersFilesStillLoad(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(here), "..", "..")
	for _, rel := range []string{"dev/owners.yaml", "deploy/config/owners.example.yaml"} {
		o, err := Load(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if o.AnyShadow() {
			t.Fatalf("%s: no shipped owners file turns shadow on", rel)
		}
	}
}
