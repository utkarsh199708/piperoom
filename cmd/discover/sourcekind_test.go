package main

import (
	"strings"
	"testing"

	"piperoom/internal/discover"
)

func TestKeepOneSourceKind(t *testing.T) {
	mixed := discover.Result{
		Pipelines: []discover.Pipeline{{ID: "orders-mysql", Connector: "orders-mysql", SourceKind: "mysql"}, {ID: "pg-orders", Connector: "pg-orders"}},
		Skipped:   []discover.Skipped{{Connector: "zz-sink", Reason: "not a source"}},
		Warnings:  []string{mixedKindsWarning + " (pg-orders) and MySQL ones (orders-mysql); ...", "other warning"},
	}
	for _, tc := range []struct {
		url, keep, drop string
	}{
		{"postgres://u@h:5432/db", "pg-orders", "orders-mysql"},
		{"", "pg-orders", "orders-mysql"},
		{"MySQL://u@h:3306/shop", "orders-mysql", "pg-orders"},
	} {
		got := keepOneSourceKind(mixed, ownedSourceKind(tc.url))
		if len(got.Pipelines) != 1 || got.Pipelines[0].Connector != tc.keep {
			t.Fatalf("%q: pipelines %+v, want only %s", tc.url, got.Pipelines, tc.keep)
		}
		var found bool
		for _, s := range got.Skipped {
			if s.Connector == tc.drop && strings.Contains(s.Reason, "one server serves one source kind") {
				found = true
			}
		}
		if !found || len(got.Skipped) != 2 {
			t.Fatalf("%q: skipped %+v, want %s listed with the reason", tc.url, got.Skipped, tc.drop)
		}
		if len(got.Warnings) != 1 || got.Warnings[0] != "other warning" {
			t.Fatalf("%q: warnings %q, want the mixed-kinds one dropped", tc.url, got.Warnings)
		}
	}
	if len(mixed.Skipped) != 1 || len(mixed.Pipelines) != 2 {
		t.Fatal("the input result was changed")
	}
	one := discover.Result{Pipelines: []discover.Pipeline{{Connector: "pg-orders"}}, Warnings: []string{"w"}}
	if got := keepOneSourceKind(one, ""); len(got.Pipelines) != 1 || len(got.Warnings) != 1 {
		t.Fatalf("single kind changed: %+v", got)
	}
}
