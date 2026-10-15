package evals

import "testing"

// "no doubt", "without doubt" and "no question" affirm what follows.
func TestNoDoubtAffirms(t *testing.T) {
	text := "There is no doubt that the replication slot was dropped."
	if !matchGroups([][]string{{"slot"}}, text) {
		t.Errorf("matchGroups: %q does not state slot", text)
	}
	if !matchGroups([][]string{{"slot"}}, "Without doubt the replication slot was dropped.") {
		t.Error("matchGroups: without doubt read as a denial")
	}
	if hit, _ := termHits(clauseTokens("There is no doubt the poll timeout is too low."), "poll timeout"); !hit {
		t.Error("termHits: no doubt read as a denial")
	}
	if matchGroups([][]string{{"slot"}}, "I doubt the replication slot was dropped.") {
		t.Error("matchGroups: a plain doubt no longer denies")
	}
}

// "ruled out X" denies X going forward, as "X was ruled out" does going back;
// "cannot rule out X" keeps X a candidate.
func TestRuledOutDeniesForward(t *testing.T) {
	if matchGroups([][]string{{"registry"}}, "We ruled out the registry.") {
		t.Error("matchGroups: 'ruled out the registry' states registry")
	}
	if namesLayer(layerTerms["kafka-connect"], "I ruled out the connector; order-sync is stuck.") {
		t.Error("namesLayer: 'ruled out the connector' names kafka-connect")
	}
	for _, text := range []string{"We cannot rule out the registry.", "We can't yet rule out the registry.",
		"We ruled out the connector and the registry rejected the schema."} {
		if !matchGroups([][]string{{"registry"}}, text) {
			t.Errorf("%q: registry denied", text)
		}
	}
}

// tokenize splits "PostgreSQL" into "postgre sql"; it still names postgres.
func TestPostgreSQLNamesPostgres(t *testing.T) {
	if text := "PostgreSQL dropped the column."; !namesLayer(layerTerms["postgres"], text) {
		t.Errorf("namesLayer postgres: %q -> tokens %v", text, tokenize(text))
	}
}

// Committed single-cause diagnoses are not hedges; real hedges still are.
func TestCauseHedgeNeedsAlternatives(t *testing.T) {
	for _, text := range []string{
		"The amount column changed to numeric. Connect is healthy and the registry did not change either.",
		"The amount column changed to numeric; I ruled out the other possible causes.",
		"There are no other possibilities: the amount column changed to numeric.",
	} {
		if m, h := causeHedge(text); h {
			t.Errorf("causeHedge(%q) = true on %q", text, m)
		}
	}
	for _, text := range []string{
		"It is either the amount change or a consumer crash.",
		"Is it either the registry or the connector?",
		"Possible causes: the amount change, a consumer crash.",
		"The amount change; another possibility is a consumer crash.",
		"Not sure whether the registry or the connector failed.",
		// A negated rule-out keeps the alternatives open.
		"I can't rule out other possibilities, but the amount column changed.",
		"We cannot rule out another possibility: a consumer crash.",
		"I could not rule out other possible causes.",
		// "either" whose "or" is in a later clause or list item.
		"Either the registry rejected the schema. Or the connector crashed.",
		"Either the registry rejected the schema,\nor the connector crashed.",
		"Either:\n- the registry\n- the connector",
	} {
		if _, h := causeHedge(text); !h {
			t.Errorf("causeHedge(%q) = false, want a hedge", text)
		}
	}
}

// A zero count is not a current fault; a non-zero one still is.
func TestZeroCountIsNoFault(t *testing.T) {
	for _, text := range []string{
		"Pipeline is healthy: lag 0, 0 errors in the last hour.",
		"Healthy. Connector RUNNING, 0 failed tasks.",
		"All tasks running, zero errors, consumer caught up. Healthy.",
	} {
		if !healthyVerdict(text) {
			t.Errorf("healthyVerdict(%q) = false (hasFault=%v)", text, hasFault(text))
		}
	}
	for _, text := range []string{
		"Healthy, but errors rose from 0 to 500 errors.",
		"Healthy, but 10.0 errors in the connector.",
		"Healthy, 5.0 failed tasks.",
	} {
		if !hasFault(text) {
			t.Errorf("hasFault(%q) = false: a non-zero count read as no fault", text)
		}
	}
}

// A clearing word after a negator does not clear: "the connector is not
// healthy" states the connector.
func TestNotHealthyDoesNotClear(t *testing.T) {
	for _, c := range []struct{ text, term string }{
		{"The connector is not healthy.", "connector"},
		{"The connector isn't healthy.", "connector"},
		{"The poll timeout is not ok for this group.", "poll timeout"},
	} {
		if hit, neg := termHits(clauseTokens(c.text), c.term); !hit {
			t.Errorf("termHits(%q, %q) = hit %v neg %v", c.text, c.term, hit, neg)
		}
	}
	if hit, _ := termHits(clauseTokens("The connector is healthy."), "connector"); hit {
		t.Error("a plain healthy no longer clears")
	}
}

// The verb "connect to" does not name the Kafka Connect layer; the noun does.
func TestConnectVerbIsNoConnectLayer(t *testing.T) {
	if text := "order-sync could not connect to the broker after the TLS cert rotated."; namesLayer(layerTerms["kafka-connect"], text) {
		t.Errorf("namesLayer kafka-connect: %q", text)
	}
	if !namesLayer(layerTerms["kafka-connect"], "Kafka Connect rejected the config.") {
		t.Error("Kafka Connect no longer names the layer")
	}
}
