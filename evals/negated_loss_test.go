package evals

import "testing"

// W4b-2 (KAN-493 test case 2b): a hypothesis that says no data was lost does
// not name the loss. Each row is read against the loss group of the
// scenarios its filter names; want says whether that group must accept it.
func TestNegatedLossTerm(t *testing.T) {
	lossGroups := []struct {
		path  string
		group int
	}{
		{"scenarios/topic-retention-data-loss.yaml", 2},
		{"scenarios/consumer-offsets-out-of-range-retention.yaml", 2},
		{"scenarios/rows-lost-after-retention.yaml", 2},
		{"scenarios/source-column-dropped.yaml", 2},
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
		// Review round 1 (#991): a verb negator across fillers, a counter
		// before a count.
		{text: "The changes have not yet been deleted.", only: delGroups},
		{text: "Zero records were lost.", only: lostGroups},
		{text: "Zero records lost.", only: lostGroups},
		// A temporal preposition is no barrier: the denial reaches on.
		{text: "No events since the restart were lost.", only: lostGroups},
		{text: "No changes before the outage were lost.", only: lostGroups},
		{text: "No records after the restart were deleted.", only: delGroups},
		{text: "Nothing after the restart was deleted.", only: delGroups},
		{text: "None of the changes before the outage were lost.", only: lostGroups},
		// Review round 2 (#991): a verb before the negator, or an -ed
		// adjective after a determiner, does not make the preposition a barrier.
		{text: "It is clear that no records since the restart were lost.", only: lostGroups},
		{text: "There were no events since the restart lost.", only: lostGroups},
		{text: "Order-sync has confirmed no events after the restart were deleted.", only: delGroups},
		{text: "No unprocessed records since the restart were lost.", only: lostGroups},
		{text: "No committed offsets before the reset were lost.", only: lostGroups},
		{text: "No records since Tuesday's restart were lost.", only: lostGroups},
		{text: "Nothing until the backfill was deleted.", only: delGroups},
		// A scenario outside retention: source-column-dropped takes drop*.
		{text: "The amount column was not dropped; it was renamed to total_amount.", only: dropGroups},
		{text: "Nothing was dropped from public.orders.", only: dropGroups},

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
		// "reset" is a barrierVerb (review round 2): the past tense before
		// "before" opens a clause; the noun after a barrier does not count.
		{want: true, text: "Nobody reset order-sync before retention deleted the records.", only: delGroups},
		{want: true, text: "No one reset the group until the changes had expired.", only: delGroups},
		// Review round 1 (#991): wordings an earlier draft denied. A counter
		// reaches three words; a verb negator before another verb negates
		// that verb, not the loss word after it.
		{want: true, text: "Zero-lag consumer still lost data.", only: lostGroups},
		{want: true, text: "Consumer lag is zero yet records were lost.", only: lostGroups},
		{want: true, text: "Consumer lag is zero yet the changes were deleted by retention.", only: delGroups},
		{want: true, text: "With zero lag order-sync still lost the changes.", only: lostGroups},
		{want: true, text: "Without retention set records were deleted.", only: delGroups},
		{want: true, text: "Without a consumer running the records expired.", only: delGroups},
		{want: true, text: "Without a migration review the amount column was dropped from public.orders.", only: dropGroups},
		{want: true, text: "Records it never read expired.", only: delGroups},
		{want: true, text: "Data order-sync never read expired.", only: delGroups},
		{want: true, text: "The messages order-sync never consumed expired.", only: delGroups},
		{want: true, text: "Messages that weren't consumed expired.", only: delGroups},
		{want: true, text: "Messages that were not consumed expired.", only: delGroups},
		{want: true, text: "We can't restore deleted records from the topic.", only: delGroups},
		{want: true, text: "Lag reads zero because the records it never read expired.", only: delGroups},
		{want: true, text: "Migration 0042 dropped the amount column from public.orders.", only: dropGroups},
	}
	ran := make([]bool, len(cases))
	for _, lg := range lossGroups {
		sc := loadScenario(t, lg.path)
		g := groupOf(t, sc, lg.group)
		for n, c := range cases {
			if !contains(c.only, sc.Name) {
				continue
			}
			ran[n] = true
			if got := matchGroups(g, c.text); got != c.want {
				t.Errorf("%s group %d: matchGroups(%q) = %v, want %v", sc.Name, lg.group, c.text, got, c.want)
			}
		}
	}
	for n, c := range cases {
		if !ran[n] {
			t.Errorf("row %q matches no scenario of its filter %v", c.text, c.only)
		}
	}
}

// lostGroups are the scenarios whose loss group takes lost and loss*.
var lostGroups = []string{"topic-retention-data-loss", "consumer-offsets-out-of-range-retention"}

// delGroups are the scenarios whose loss group takes delet*, expir* and purg*.
var delGroups = []string{"consumer-offsets-out-of-range-retention", "rows-lost-after-retention"}

// dropGroups are the scenarios whose loss group takes drop*.
var dropGroups = []string{"source-column-dropped"}

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

// The barriers live in denied, so they reach layer naming too: a clause a
// conjunction opens names its layer, a temporal preposition opens none.
func TestDenialBarrierLayers(t *testing.T) {
	connect := layerTerms["kafka-connect"]
	cases := []struct {
		text string
		want bool
	}{
		{"The slot is not the cause because the connector failed.", true},
		{"It's not the registry since the connector crashed.", true},
		{"No consumer read the topic before the connector failed.", true},
		{"No errors since the connector restart.", false},
		{"There are no errors since the connector restart.", false},
		{"We saw no lag since the connector restart.", false},
		{"Not a connector problem.", false},
	}
	for _, c := range cases {
		if got := namesLayer(connect, c.text); got != c.want {
			t.Errorf("namesLayer(kafka-connect, %q) = %v, want %v", c.text, got, c.want)
		}
	}
}

// Known limits of the loss-word rule (evals/README.md): each row pins today's
// grade, which is wrong; a fix that grades a row right removes it here.
func TestNegatedLossKnownLimits(t *testing.T) {
	lost := [][]string{{"lost", "loss*"}}
	del := [][]string{{"delet*", "dropped"}}
	cases := []struct {
		groups [][]string
		text   string
		today  bool // the wrong grade the matcher gives
	}{
		{del, "Retention deleted nothing that order-sync had not read.", true},
		{del, "Retention deleted no records.", true},
		{lost, "Data loss: none.", true},
		{lost, "0 records were lost.", true},
		{lost, "I don't see any data loss.", true},
		{lost, "Not one record was lost.", true},
		{lost, "It is not true that records were lost.", true},
		{lost, "Data loss is unlikely.", true},
		{lost, "None of the 01:00-03:00 changes were lost.", true},
		{del, "The records nobody read were deleted by retention.", false},
		{del, "No events reach the warehouse since the slot was dropped.", false},
		{del, "Nothing produced after the restart was deleted.", true},
	}
	for _, c := range cases {
		if got := matchGroups(c.groups, c.text); got != c.today {
			t.Errorf("known limit graded right now, remove the row and its README line: matchGroups(%v, %q) = %v", c.groups, c.text, got)
		} else {
			t.Logf("known limit: matchGroups(%v, %q) = %v (wrong)", c.groups, c.text, got)
		}
	}
}
