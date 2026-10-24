package evals

import "testing"

// W4b-2 (KAN-493 test case 2b): a hypothesis that says no data was lost does
// not name the loss. Each row is read against the loss group of every
// scenario that has one; want says whether that group must accept it.
func TestNegatedLossTerm(t *testing.T) {
	lossGroups := []struct {
		path  string
		group int
	}{
		{"scenarios/topic-retention-data-loss.yaml", 2},
		{"scenarios/consumer-offsets-out-of-range-retention.yaml", 2},
		{"scenarios/rows-lost-after-retention.yaml", 2},
	}
	cases := []struct {
		text string
		want bool
		// only limits the row to the scenarios whose loss group holds its
		// loss word.
		only []string
	}{
		// Negatives: no loss is stated.
		{text: "Nothing was lost; order-sync is just slow.", only: lostGroups},
		{text: "Nothing is lost, order-sync is just slow after the re-pricing job.", only: lostGroups},
		{text: "No records were lost.", only: lostGroups},
		{text: "No data loss; order-sync is just slow.", only: lostGroups},
		{text: "There was no data loss.", only: lostGroups},
		{text: "Records were not lost, order-sync is just slow.", only: lostGroups},
		{text: "The 01:00-03:00 changes weren't lost.", only: lostGroups},
		{text: "The changes were never lost.", only: lostGroups},
		{text: "None of the re-pricing changes were lost.", only: lostGroups},
		{text: "order-sync restarted without any data loss.", only: lostGroups},
		{text: "Zero data loss: order-sync is just slow.", only: lostGroups},
		{text: "Nothing has been lost yet.", only: lostGroups},
		{text: "Nothing was deleted before order-sync read it.", only: delGroups},
		{text: "Nothing expired before order-sync read it.", only: delGroups},
		{text: "None of the unread changes were removed.", only: []string{"rows-lost-after-retention"}},
		{text: "Nothing was purged before order-sync read it.", only: delGroups},
		{text: "Nothing is gone from the topic.", only: []string{"topic-retention-data-loss"}},
		{text: "Nothing was skipped; order-sync is just slow.", only: []string{"topic-retention-data-loss"}},

		// Real loss phrasings must still match.
		{want: true, text: "Records were lost before the consumer read them.", only: lostGroups},
		{want: true, text: "Data loss already happened.", only: lostGroups},
		{want: true, text: "The 01:00-03:00 changes were lost: order-sync never read them.", only: lostGroups},
		{want: true, text: "order-sync could not keep up, so the changes were lost.", only: lostGroups},
		{want: true, text: "No consumer read the changes before they were lost to retention.", only: lostGroups},
		{want: true, text: "Not all changes were lost, but the 01:00-03:00 ones were.", only: lostGroups},
		{want: true, text: "order-sync did not read the changes in time and they were lost.", only: lostGroups},
		{want: true, text: "No consumer read them before they were deleted.", only: delGroups},
		{want: true, text: "The changes that order-sync never read were deleted by retention.", only: delGroups},
		{want: true, text: "order-sync was not running when retention deleted the changes.", only: delGroups},
		{want: true, text: "No one noticed until the changes had expired.", only: delGroups},
	}
	for _, lg := range lossGroups {
		sc := loadScenario(t, lg.path)
		g := groupOf(t, sc, lg.group)
		for _, c := range cases {
			if !contains(c.only, sc.Name) {
				continue
			}
			if got := matchGroups(g, c.text); got != c.want {
				t.Errorf("%s group %d: matchGroups(%q) = %v, want %v", sc.Name, lg.group, c.text, got, c.want)
			}
		}
	}
}

// lostGroups are the scenarios whose loss group takes lost and loss*.
var lostGroups = []string{"topic-retention-data-loss", "consumer-offsets-out-of-range-retention"}

// delGroups are the scenarios whose loss group takes delet*, expir* and purg*.
var delGroups = []string{"consumer-offsets-out-of-range-retention", "rows-lost-after-retention"}

// A negated loss word is cleared only as a loss word: a negator before
// another root-cause term keeps its earlier reading, and a negation does not
// reach a loss word past a subordinating word.
func TestNegatedLossScope(t *testing.T) {
	cases := []struct {
		groups [][]string
		text   string
		want   bool
	}{
		// Not a loss word: the weak negator before a verb still describes the failure.
		{[][]string{{"deserializ*"}}, "order-sync cannot deserialize the record.", true},
		{[][]string{{"slot*"}}, "Debezium cannot read slot debezium_orders.", true},
		// "nothing" before a non-loss term is unchanged.
		{[][]string{{"retention"}}, "Retention is fine: nothing was deleted before order-sync read it.", true},
		// A loss word after "because" is affirmed.
		{[][]string{{"lost"}}, "No events reach the warehouse because they were lost to retention.", true},
		// A loss word inside a longer term ("can no longer be replayed") is
		// not negated by the term's own negator.
		{[][]string{{"no longer replay*"}}, "The expired changes no longer replay from pg.public.orders.", true},
	}
	for _, c := range cases {
		if got := matchGroups(c.groups, c.text); got != c.want {
			t.Errorf("matchGroups(%v, %q) = %v, want %v", c.groups, c.text, got, c.want)
		}
	}
}
