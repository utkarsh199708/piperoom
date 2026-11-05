package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// fakeDeletedRegistry adds the optional deleted-state reads a real registry
// client has to fakeRegistry, so the tool's soft- vs hard-deleted logic is
// testable. (#KAN-407)
type fakeDeletedRegistry struct {
	*fakeRegistry
	subjectsDeletedFn func(context.Context) ([]string, error)
	versionsDeletedFn func(context.Context, string) ([]int, error)
}

func (f *fakeDeletedRegistry) SubjectsIncludingDeleted(ctx context.Context) ([]string, error) {
	return f.subjectsDeletedFn(ctx)
}

func (f *fakeDeletedRegistry) VersionsIncludingDeleted(ctx context.Context, subject string) ([]int, error) {
	return f.versionsDeletedFn(ctx, subject)
}

// registryTool registers the kafkatools over one registry client and returns
// the named tool.
func registryTool(t *testing.T, r clients.SchemaRegistryClient, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Registry: r}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	return tool
}

// TestSchemaRegistryVersionsMissingSubject is TC3: a subject the live listing
// does not have is a finding, not an error, and its deleted state is named
// when the client can tell. An unreachable registry stays an explicit error.
func TestSchemaRegistryVersionsMissingSubject(t *testing.T) {
	const subject = "pg.public.orders-value"
	const notFoundSummary = "pg.public.orders-value: subject not found (soft-deleted subjects can be listed with deleted=true)"
	unreachable := errors.New("dial tcp 127.0.0.1:8081: connect: connection refused")

	tests := []struct {
		name         string
		registry     clients.SchemaRegistryClient
		wantErr      error  // non-nil: the tool call must fail with this error
		wantSummary  string // substring the Summary must contain
		wantState    string // Data["deleted_state"]
		wantVersions []int  // Data["versions"], when soft-deleted
		wantProbeErr bool   // Data["deleted_probe_error"] is set
	}{
		{
			name: "missing, client cannot list deleted",
			registry: &fakeRegistry{
				versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
			},
			wantSummary: notFoundSummary,
			wantState:   "unknown",
		},
		{
			name: "soft-deleted subject",
			registry: &fakeDeletedRegistry{
				fakeRegistry: &fakeRegistry{
					versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
				},
				versionsDeletedFn: func(context.Context, string) ([]int, error) { return []int{1, 2, 3}, nil },
			},
			wantSummary:  notFoundSummary + "; soft-deleted: versions [1 2 3] still exist and can be restored",
			wantState:    "soft",
			wantVersions: []int{1, 2, 3},
		},
		{
			name: "hard-deleted subject, deleted listing 404",
			registry: &fakeDeletedRegistry{
				fakeRegistry: &fakeRegistry{
					versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
				},
				versionsDeletedFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
			},
			wantSummary: notFoundSummary + "; no soft-deleted versions exist (hard-deleted or never registered)",
			wantState:   "hard",
		},
		{
			name: "never registered, deleted listing empty",
			registry: &fakeDeletedRegistry{
				fakeRegistry: &fakeRegistry{
					versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
				},
				versionsDeletedFn: func(context.Context, string) ([]int, error) { return []int{}, nil },
			},
			wantSummary: notFoundSummary + "; no soft-deleted versions exist (hard-deleted or never registered)",
			wantState:   "hard",
		},
		{
			name: "deleted-state probe fails",
			registry: &fakeDeletedRegistry{
				fakeRegistry: &fakeRegistry{
					versionsFn: func(context.Context, string) ([]int, error) { return nil, clients.ErrNotFound },
				},
				versionsDeletedFn: func(context.Context, string) ([]int, error) { return nil, unreachable },
			},
			wantSummary:  notFoundSummary + "; deleted-state check failed",
			wantState:    "unknown",
			wantProbeErr: true,
		},
		{
			name: "registry unreachable is not reported as missing",
			registry: &fakeRegistry{
				versionsFn: func(context.Context, string) ([]int, error) { return nil, unreachable },
			},
			wantErr: unreachable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if f, ok := tc.registry.(writeCounter); ok {
				guardWrites(t, f)
			}
			tool := registryTool(t, tc.registry, "schema_registry_versions")
			r, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"subject":"`+subject+`"}`))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("missing subject must be a finding, got error %v", err)
			}
			if !strings.Contains(r.Summary, tc.wantSummary) {
				t.Fatalf("summary = %q, want it to contain %q", r.Summary, tc.wantSummary)
			}
			data, ok := r.Data.(map[string]any)
			if !ok {
				t.Fatalf("data = %#v, want a map", r.Data)
			}
			if data["found"] != false {
				t.Fatalf("found = %v, want false", data["found"])
			}
			if data["deleted_state"] != tc.wantState {
				t.Fatalf("deleted_state = %v, want %q", data["deleted_state"], tc.wantState)
			}
			if tc.wantVersions != nil {
				got, ok := data["versions"].([]int)
				if !ok || len(got) != len(tc.wantVersions) {
					t.Fatalf("versions = %#v, want %v", data["versions"], tc.wantVersions)
				}
				for i := range got {
					if got[i] != tc.wantVersions[i] {
						t.Fatalf("versions = %v, want %v", got, tc.wantVersions)
					}
				}
			}
			if tc.wantProbeErr && data["deleted_probe_error"] == nil {
				t.Fatalf("deleted_probe_error missing from %#v", data)
			}
		})
	}
}

// A live subject keeps its exact existing summary; a soft-deleted version
// that the plain listing hides is named, and an enrichment failure never
// breaks the valid live listing. (#KAN-407)
func TestSchemaRegistryVersionsSoftDeletedVersions(t *testing.T) {
	base := func() *fakeRegistry {
		return &fakeRegistry{
			versionsFn: func(context.Context, string) ([]int, error) { return []int{1, 2}, nil },
			schemaFn: func(_ context.Context, s string, _ int) (clients.SchemaVersion, error) {
				return clients.SchemaVersion{Subject: s, Version: 2, ID: 5, Schema: "{}"}, nil
			},
			compatFn: func(context.Context, string) (string, bool, error) { return "BACKWARD", true, nil },
		}
	}
	tests := []struct {
		name        string
		deletedFn   func(context.Context, string) ([]int, error)
		wantExtra   string
		wantErrInDa bool
	}{
		{
			name:      "no soft-deleted versions",
			deletedFn: func(context.Context, string) ([]int, error) { return []int{1, 2}, nil },
		},
		{
			name:      "soft-deleted version 3",
			deletedFn: func(context.Context, string) ([]int, error) { return []int{1, 2, 3}, nil },
			wantExtra: "; soft-deleted versions [3] still exist and can be restored (list with deleted=true)",
		},
		{
			name:        "deleted-version check fails",
			deletedFn:   func(context.Context, string) ([]int, error) { return nil, errors.New("boom") },
			wantErrInDa: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fr := &fakeDeletedRegistry{fakeRegistry: base(), versionsDeletedFn: tc.deletedFn}
			guardWrites(t, fr)
			tool := registryTool(t, fr, "schema_registry_versions")
			r := runTool(t, tool, `{"subject":"pg.public.orders-value"}`)
			const baseSummary = "pg.public.orders-value: versions [1 2], latest id 5, compatibility BACKWARD (subject)"
			if !strings.HasPrefix(r.Summary, baseSummary) {
				t.Fatalf("summary = %q, want prefix %q", r.Summary, baseSummary)
			}
			if tc.wantExtra != "" && !strings.Contains(r.Summary, tc.wantExtra) {
				t.Fatalf("summary = %q, want %q", r.Summary, tc.wantExtra)
			}
			data := r.Data.(map[string]any)
			if tc.wantErrInDa {
				if _, ok := data["deleted_versions_error"]; !ok {
					t.Fatalf("deleted_versions_error missing from %#v", data)
				}
				return
			}
			if tc.wantExtra == "" {
				if _, ok := data["deleted_versions"]; ok {
					t.Fatalf("unexpected deleted_versions in %#v", data)
				}
				return
			}
			got, ok := data["deleted_versions"].([]int)
			if !ok || len(got) != 1 || got[0] != 3 {
				t.Fatalf("deleted_versions = %#v, want [3]", data["deleted_versions"])
			}
		})
	}
}

// schema_registry_subjects with include_deleted lists a soft-deleted subject
// and marks it; the default listing is unchanged and a client that cannot
// tell says so instead of guessing. (#KAN-407)
func TestSchemaRegistrySubjectsIncludeDeleted(t *testing.T) {
	newRegistry := func() *fakeDeletedRegistry {
		return &fakeDeletedRegistry{
			fakeRegistry: &fakeRegistry{
				subjectsFn: func(context.Context) ([]string, error) {
					// The value subject was soft-deleted: only the key is live.
					return []string{"pg.public.orders-key"}, nil
				},
			},
			subjectsDeletedFn: func(context.Context) ([]string, error) {
				return []string{"pg.public.orders-key", "pg.public.orders-value"}, nil
			},
		}
	}
	t.Run("include_deleted", func(t *testing.T) {
		fr := newRegistry()
		guardWrites(t, fr)
		tool := registryTool(t, fr, "schema_registry_subjects")
		r := runTool(t, tool, `{"include_deleted":true}`)
		if !strings.Contains(r.Summary, "pg.public.orders-value (soft-deleted)") {
			t.Fatalf("summary = %q, want the soft-deleted subject marked", r.Summary)
		}
		data, ok := r.Data.(map[string]any)
		if !ok {
			t.Fatalf("data = %#v, want a map", r.Data)
		}
		live, _ := data["subjects"].([]string)
		deleted, _ := data["deleted_subjects"].([]string)
		if len(live) != 1 {
			t.Fatalf("subjects = %v, want only the live key subject", data["subjects"])
		}
		if len(deleted) != 1 || deleted[0] != "pg.public.orders-value" {
			t.Fatalf("deleted_subjects = %v", data["deleted_subjects"])
		}
	})
	t.Run("default unchanged", func(t *testing.T) {
		fr := newRegistry()
		guardWrites(t, fr)
		tool := registryTool(t, fr, "schema_registry_subjects")
		r := runTool(t, tool, `{}`)
		if strings.Contains(r.Summary, "soft-deleted") {
			t.Fatalf("default listing mentions soft-deleted: %q", r.Summary)
		}
		if got, ok := r.Data.([]string); !ok || len(got) != 1 {
			t.Fatalf("data = %#v, want the live key subject as []string", r.Data)
		}
	})
	t.Run("client without deleted listing", func(t *testing.T) {
		fr := &fakeRegistry{
			subjectsFn: func(context.Context) ([]string, error) {
				return []string{"pg.public.orders-value"}, nil
			},
		}
		guardWrites(t, fr)
		tool := registryTool(t, fr, "schema_registry_subjects")
		_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"include_deleted":true}`))
		if err == nil || !strings.Contains(err.Error(), "does not support listing soft-deleted subjects") {
			t.Fatalf("err = %v, want an explicit unsupported-client error", err)
		}
	})
}
