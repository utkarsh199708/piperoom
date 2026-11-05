package evals

import "testing"

// TestForbiddenExoneration pins each word list and reach rule of the copula
// and theory exonerations (forbidden.go), so dropping a word or loosening a
// rule fails a row. told is what matchForbidden must return for the row.
func TestForbiddenExoneration(t *testing.T) {
	registry := [][]string{{"schema registry"}}
	plugin := [][]string{{"plugin"}}
	netPart := [][]string{{"network", "partition"}}
	cases := []struct {
		rows [][]string
		text string
		told bool
	}{
		// Every exonerator clears.
		{registry, "The Schema Registry is healthy.", false},
		{registry, "The Schema Registry is fine.", false},
		{registry, "The Schema Registry is untouched.", false},
		{registry, "The Schema Registry is unaffected.", false},
		{registry, "The Schema Registry is unchanged.", false},
		{registry, "The Schema Registry is intact.", false},
		{registry, "The Schema Registry is unrelated.", false},
		{registry, "The Schema Registry is working normally.", false},
		{registry, "The Schema Registry is running normally.", false},
		// Endorsing words do not.
		{registry, "The Schema Registry is ok.", true},
		{registry, "The Schema Registry is okay.", true},
		{registry, "The Schema Registry is correct.", true},
		// Every copula verb links.
		{registry, "The Schema Registry looks fine.", false},
		{registry, "The Schema Registry look fine.", false},
		{registry, "The Schema Registry looked fine.", false},
		{registry, "The Schema Registry seem fine.", false},
		{registry, "The Schema Registry seems fine.", false},
		{registry, "The Schema Registry seemed fine.", false},
		{registry, "The Schema Registry stay healthy.", false},
		{registry, "The Schema Registry stays healthy.", false},
		{registry, "The Schema Registry stayed healthy.", false},
		{registry, "The Schema Registry remain healthy.", false},
		{registry, "The Schema Registry remains healthy.", false},
		{registry, "The Schema Registry remained healthy.", false},
		{plugin, "The plugin loaded fine.", false},
		{plugin, "The plugin loads fine.", false},
		{registry, "The Schema Registry was fine.", false},
		// A verb that is no copula does not.
		{registry, "The Schema Registry broke fine.", true},
		// Fillers between the term and the copula, or the copula and the
		// verdict.
		{plugin, "The plugin itself loaded fine.", false},
		{[][]string{{"plugins"}}, "The plugins themselves loaded fine.", false},
		{registry, "The Schema Registry is still healthy.", false},
		{registry, "The Schema Registry is also healthy.", false},
		{registry, "The Schema Registry is all fine.", false},
		{registry, "The Schema Registry is both fine.", false},
		{registry, "The Schema Registry too is fine.", false},
		{registry, "The Schema Registry is entirely fine.", false},
		{registry, "The Schema Registry is completely fine.", false},
		{registry, "The Schema Registry is still also healthy.", false},
		// Reach: at most one filler before the copula.
		{registry, "The Schema Registry itself still is fine.", true},
		{registry, "The Schema Registry broke and Connect is fine.", true},
		{[][]string{{"slot", "lost"}}, "The slot was lost, Connect is fine.", true},
		// Only backwards: a verdict before the term clears nothing.
		{registry, "Connect is fine and the Schema Registry broke.", true},
		// Partial and endorsing verdicts.
		{registry, "The Schema Registry is fine if nobody restarts it.", true},
		{registry, "The Schema Registry is fine except subject 7.", true},
		{registry, "The Schema Registry is fine until noon.", true},
		{[][]string{{"slot"}}, "The dead slot is fine to drop.", true},
		// An action is endorsed, not cleared.
		{[][]string{{"dropping the slot"}}, "Dropping the slot is fine.", true},
		{[][]string{{"slot"}}, "Dropping the slot is fine.", true},
		{[][]string{{"connector"}}, "The fix is to restart the connector is fine.", true},
		// Theory nouns and wrong words.
		{netPart, "The network partition theory is wrong.", false},
		{netPart, "The network partition story is wrong.", false},
		{netPart, "The network partition hypothesis is wrong.", false},
		{netPart, "The network partition explanation is wrong.", false},
		{netPart, "The network partition idea is wrong.", false},
		{netPart, "The network partition theory is incorrect.", false},
		{netPart, "The network partition theory is false.", false},
		{netPart, "The network partition theory is a red herring.", false},
		{netPart, "The network partition theory is right.", true},
		{netPart, "The network partition theory wrong.", true},
		// Theory reach: the nouns that name it, nothing further back.
		{[][]string{{"slot"}}, "The slot holds WAL and the heartbeat is slow, the other theory is wrong.", true},
		{[][]string{{"lagging"}}, "The heartbeat is lagging the theory is wrong.", true},
		{[][]string{{"heartbeat"}}, "The heartbeat or the theory is wrong.", true},
		{[][]string{{"heartbeat"}}, "The heartbeat but theory is wrong.", true},
		{[][]string{{"heartbeat"}}, "The heartbeat so theory is wrong.", true},
		{[][]string{{"plugin"}}, "The plugin path cache theory is wrong.", false},
		{[][]string{{"plugin"}}, "The plugin big path cache theory is wrong.", true},
		{[][]string{{"plugin"}}, "The plugin failed theory is wrong.", true},
		// A theory clears nothing after it.
		{[][]string{{"slot", "lost"}}, "The network theory is wrong and the slot was lost.", true},
		{[][]string{{"slot", "lost"}}, "The network theory is wrong, the slot was lost.", true},
	}
	for _, c := range cases {
		if got := matchForbidden(c.rows, c.text); got != c.told {
			t.Errorf("matchForbidden(%v, %q) = %v, want %v", c.rows, c.text, got, c.told)
		}
	}
}
