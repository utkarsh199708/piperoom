package main

import (
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/discover"
)

// ownedSourceKind is the source kind a -write-owners file keeps: the scheme
// of SOURCE_DB_URL ("mysql" for mysql://, else "" for postgres, which
// owners.yaml leaves out). One server client set serves one source kind
// (wiring.Open refuses a mix without connections: blocks), and the file is
// promised to load and serve as written.
func ownedSourceKind(sourceDBURL string) string {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sourceDBURL)), "mysql://") {
		return "mysql"
	}
	return ""
}

// keepOneSourceKind drops the pipelines whose source kind is not kind and
// lists each as skipped with the reason, so the summary says what was left
// out and how to serve it. With one kind in the result it changes nothing.
func keepOneSourceKind(res discover.Result, kind string) discover.Result {
	var kept []discover.Pipeline
	var dropped []discover.Skipped
	for _, p := range res.Pipelines {
		if p.SourceKind == kind {
			kept = append(kept, p)
			continue
		}
		dropped = append(dropped, discover.Skipped{
			Connector: p.Connector,
			Reason: fmt.Sprintf("a %s source, but SOURCE_DB_URL is a %s URL and one server serves one source kind; run discover again with SOURCE_DB_URL set to its database (another deployment), or add the pipeline by hand with a connections: block",
				kindName(p.SourceKind), kindName(kind)),
		})
	}
	if len(dropped) == 0 {
		return res
	}
	res.Pipelines = kept
	// The mixed-kinds warning says the server refuses the file; this one now
	// holds a single kind, so the warning no longer applies to it.
	var warns []string
	for _, w := range res.Warnings {
		if !strings.HasPrefix(w, mixedKindsWarning) {
			warns = append(warns, w)
		}
	}
	res.Warnings = warns
	res.Skipped = append(append([]discover.Skipped(nil), res.Skipped...), dropped...)
	sort.Slice(res.Skipped, func(i, j int) bool { return res.Skipped[i].Connector < res.Skipped[j].Connector })
	return res
}

// mixedKindsWarning is how discover.warnMixedKinds starts its warning.
const mixedKindsWarning = "the cluster has Postgres source connectors"

func kindName(kind string) string {
	if kind == "mysql" {
		return "MySQL"
	}
	return "Postgres"
}
