package agent

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/tools"
)

// Hypothesis guard rule 2 (H1, docs/design/hypothesis-guard.md §B): a verdict
// that rules something out must cite a step that read it. In the
// connector-auth room the agent wrote "Ruled out: role/grants, publication,
// Connect config validation, long transactions" while citing only a connector
// status and a DDL read; the cause was publication ownership. F3 grounding
// (groundTerms) cannot catch that: it is text against text, and "publication"
// is backed by any cited step whose text has the word. This rule is claim
// against tool coverage: each ruled-out item maps to topics, each topic to the
// read tools that can answer it, and an item no cited step of those tools
// backs is refused (enforce) or logged (log).
//
// It is deterministic string work over the lower-cased text, per sentence;
// questions state nothing. An item that names no topic in ruleOutTopics is
// not checked: the table is the rule's scope, and unknown wording passes.

// ruleOutTopic is one thing a verdict can rule out and the reads that back it.
type ruleOutTopic struct {
	name string
	// re matches the topic's trigger words in an item.
	re *regexp.Regexp
	// tools back the topic; every name is a registered read tool
	// (internal/tools/**, checked by TestRuleOutToolsExist).
	tools []string
	// noForm topics are also claimed by "no <topic>" ("no long transactions"),
	// with at most one word between.
	noForm bool
	// generic topics count only when no specific topic matches the item:
	// "Connect config validation" is a config claim, not a task-state one.
	generic bool
}

// ruleOutTopics is the claim → backing tool table of the design (§B). The
// design's ownership "Tool gap" (pg_recent_ddl shows ALTER ... OWNER TO
// changes, not the current owner) is closed by pg_object_owners (H1).
var ruleOutTopics = []ruleOutTopic{
	{name: "long transactions", re: wordsRE(`long(?:[- ]running)? (?:transactions?|tx)`, `open transactions?`, `idle in transaction`, `(?:blocking|open|active) sessions?`, `xmin`),
		tools: []string{"pg_long_transactions", "source_long_transactions", "source_blocking_sessions"}, noForm: true},
	{name: "replication slot", re: wordsRE(`slots?`, `restart_lsn`, `retained wal`, `wal retention`),
		tools: []string{"pg_replication_slots", "pg_streaming_liveness"}},
	// pg_object_owners (H1) reads the publication's and the tables' owners
	// and the connector role's membership: it answers the publication,
	// role/grants and ownership claims of the connector-auth room.
	{name: "publication", re: wordsRE(`publications?`),
		tools: []string{"pg_publication_tables", "pg_connector_scope_gap", "pg_object_owners"}},
	{name: "role/grants", re: wordsRE(`grants?`, `privileges?`, `permissions?`, `roles?`),
		tools: []string{"source_role_grants", "pg_publication_tables", "pg_object_owners"}},
	{name: "ownership", re: wordsRE(`owners?`, `ownership`),
		tools: []string{"pg_recent_ddl", "pg_object_owners"}},
	{name: "connector config", re: wordsRE(`config\w*`, `validation`, `connector settings?`),
		tools: []string{"connect_config", "connect_validate_config"}},
	{name: "connector state", re: wordsRE(`connectors?`, `tasks?`, `connect`),
		tools: []string{"connect_status", "debezium_metrics"}, generic: true},
	{name: "ddl", re: wordsRE(`ddl`, `schema changes?`, `migrations?`, `alter\w*`),
		tools: []string{"pg_recent_ddl", "what_changed"}, noForm: true},
	{name: "schema registry", re: wordsRE(`schema registry`, `registry`, `subjects?`, `compatib\w*`),
		tools: []string{"schema_registry_versions", "schema_registry_diff", "schema_registry_subjects"}},
	// consumer_logs (H2) backs "consumer crashed", "consumer errors" and
	// "consumer not processing": the consumer's own log is where those show.
	{name: "consumer", re: wordsRE(`consumer\w*`, `lag`, `rebalanc\w*`),
		tools: []string{"kafka_consumer_lag", "kafka_list_groups", "consumer_logs"}},
	{name: "dlq", re: wordsRE(`dlq`, `dead[- ]letter\w*`),
		tools: []string{"dlq_peek"}, noForm: true},
	{name: "topic retention", re: wordsRE(`retention`),
		tools: []string{"kafka_topic_config", "kafka_topic_offsets"}, generic: true},
	{name: "replica identity", re: wordsRE(`replica identity`, `primary keys?`),
		tools: []string{"source_table_replication"}},
	{name: "source settings", re: wordsRE(`wal_level`, `max_slot_wal_keep_size`, `settings?`),
		tools: []string{"source_settings"}},
	{name: "broker", re: wordsRE(`brokers?`, `under-replicated`, `isr`),
		tools: []string{"kafka_cluster_health"}},
	{name: "warehouse", re: wordsRE(`warehouse`, `sink tables?`, `load errors?`),
		tools: []string{"warehouse_freshness", "warehouse_load_errors"}},
	{name: "network/tls", re: wordsRE(`network`, `dns`, `tls`, `certificates?`, `certs?`),
		tools: []string{"net_probe", "tls_cert_expiry"}},
	{name: "toast", re: wordsRE(`toast`),
		tools: []string{"pg_toast_risk"}},
}

// wordsRE matches any of the alternatives as whole words.
func wordsRE(alts ...string) *regexp.Regexp {
	return regexp.MustCompile(`\b(?:` + strings.Join(alts, "|") + `)\b`)
}

// RuledOutClaim is one item a text rules out that the guard's table checks.
type RuledOutClaim struct {
	// Item is the item as written, lower-cased, at most 80 bytes.
	Item string
	// Topics are the ruleOutTopics names it matched.
	Topics []string
	// Tools are the read tools any one of which backs it.
	Tools []string
}

// BackedBy reports whether a read of tool backs the claim.
func (c RuledOutClaim) BackedBy(tool string) bool { return slices.Contains(c.Tools, tool) }

// RuledOutClaims returns the checked rule-out claims of text, in text order,
// without duplicates. Exported for the evals (C6 max_unbacked_rule_outs).
func RuledOutClaims(text string) []RuledOutClaim {
	var out []RuledOutClaim
	seen := map[string]bool{}
	for _, item := range ruledOutItems(text) {
		c, ok := claimFor(item)
		if !ok || seen[c.Item] {
			continue
		}
		seen[c.Item] = true
		out = append(out, c)
	}
	return out
}

// UnbackedRuledOut returns the claims of text that no tool in read backs.
// read is the tools of the steps that count (cited, did not error, or
// answered not-found).
func UnbackedRuledOut(text string, read []string) []RuledOutClaim {
	var out []RuledOutClaim
	for _, c := range RuledOutClaims(text) {
		if !slices.ContainsFunc(read, c.BackedBy) {
			out = append(out, c)
		}
	}
	return out
}

// claimFor maps an item to its topics: the specific ones it names, else the
// generic ones. ok is false when it names none.
func claimFor(item string) (RuledOutClaim, bool) {
	c := RuledOutClaim{Item: cutBytes(item, maxUnbackedTermBytes)}
	var generic []ruleOutTopic
	add := func(t ruleOutTopic) {
		c.Topics = append(c.Topics, t.name)
		for _, tl := range t.tools {
			if !slices.Contains(c.Tools, tl) {
				c.Tools = append(c.Tools, tl)
			}
		}
	}
	for _, t := range ruleOutTopics {
		if !t.re.MatchString(item) {
			continue
		}
		if t.generic {
			generic = append(generic, t)
			continue
		}
		add(t)
	}
	if len(c.Topics) == 0 {
		for _, t := range generic {
			add(t)
		}
	}
	return c, len(c.Topics) > 0
}

var (
	// parenRE drops parenthesised asides (step ids, numbers) before parsing.
	parenRE = regexp.MustCompile(`\([^()]*\)`)
	// ruleOutLabelRE is the label form: "Ruled out: a, b", "Excluded: ...",
	// "Not the cause: ...", up to two words before the colon ("ruled out so far:").
	ruleOutLabelRE = regexp.MustCompile(`\b(?:ruled out|excluded|not the cause|not involved|eliminated)(?:\s+[a-z]+){0,2}\s*:`)
	// ruleOutVerbRE is the verb form: "ruled out the publication".
	ruleOutVerbRE = regexp.MustCompile(`\b(?:ruled out|rules out|rule out|ruling out|excluding|excluded|eliminated|eliminates|eliminating)\s+`)
	// ruleOutPredRE is the predicate form: "<subject> is not the cause",
	// "<subject> looks fine". The subject is the text before it.
	ruleOutPredRE = regexp.MustCompile(`\b(?:is|was|are|were|been|looks|look|looked|seems|seem|appears|appear|remains|remain|stays|stay)\s+(?:also\s+|now\s+|still\s+|all\s+|both\s+)?` +
		`(?:not the cause|not involved|not at fault|a red herring|ruled out|excluded|fine|healthy|unaffected|unrelated|ok|okay)\b`)
	// itemSplitRE separates the items of a list.
	itemSplitRE = regexp.MustCompile(`,|;|\band\b|\bor\b|\bnor\b`)
	// objectEndRE ends a verb form's object.
	objectEndRE = regexp.MustCompile(`,|;|\band\b|\bbut\b|\bbecause\b|\bsince\b|\bas\b|\bby\b|\bvia\b|\bwith\b|\bafter\b|\bfrom\b|\busing\b|\bon\b| — | - `)
	// subjectStartRE marks where a predicate's subject can start: after the
	// last of these, so "lag grows, but the slot is fine" claims the slot.
	subjectStartRE = regexp.MustCompile(`,\s*but\b|\bbut\b|\bwhile\b|\bwhereas\b|\balthough\b|\bthough\b|\bbecause\b|\bsince\b|\bso\b|:| — | - |\bwhich\b|\bthat\b`)
	// subjectVerbRE finds a verb inside a subject: "the consumer is stuck and
	// the slot" claims only what follows the verb's own clause.
	// segmentSplitRE cuts a sentence into the parts verb and predicate forms
	// are read in.
	segmentSplitRE = regexp.MustCompile(`;| — `)
	// listSepRE is the first list separator after a subject's verb.
	listSepRE     = regexp.MustCompile(`,|\band\b`)
	subjectVerbRE = regexp.MustCompile(`\b(?:is|are|was|were|has|have|had|shows?|showed|reads?|grows?|grew|fails?|failed)\b`)
)

// ruleOutHedges in a segment make it no claim: a plan, a condition, an open
// item, or another room's finding.
var ruleOutHedges = []string{" if ", " would ", " unless ", "still to check", "not checked", "unchecked",
	"past room", "past incident", "similar incident"}

// ruleOutNegators before a verb form cancel it ("cannot rule out", "has not
// been ruled out", "not yet ruled out").
var ruleOutNegators = []string{"not ", "n't ", "cannot", "never ", "unable", "yet ", "no one", "nobody"}

// ruledOutItems returns the raw items text rules out, lower-cased.
func ruledOutItems(text string) []string {
	low := strings.ToLower(strings.ReplaceAll(text, "`", ""))
	low = parenRE.ReplaceAllString(low, " ")
	var items []string
	for _, s := range ruleOutSentences(low) {
		if s.question {
			continue
		}
		body := s.text
		if loc := ruleOutLabelRE.FindStringIndex(body); loc != nil {
			if !negated(body[:loc[0]]) && !hedged(body[:loc[0]]) {
				for _, it := range itemSplitRE.Split(body[loc[1]:], -1) {
					// A part with a verb is a new statement, not an item:
					// "Ruled out: the slot; the cause is the consumer".
					if subjectVerbRE.MatchString(it) {
						break
					}
					items = appendItem(items, it)
				}
			}
			body = body[:loc[0]]
		}
		for _, seg := range segmentSplitRE.Split(body, -1) {
			if hedged(seg) {
				continue
			}
			items = append(items, verbItems(seg)...)
			items = append(items, predicateItems(seg)...)
			items = append(items, noFormItems(seg)...)
		}
	}
	return items
}

// verbItems returns the objects of the verb forms in seg.
func verbItems(seg string) []string {
	var out []string
	for _, loc := range ruleOutVerbRE.FindAllStringIndex(seg, -1) {
		before := strings.TrimSpace(seg[:loc[0]])
		// "to rule out X" is a plan; "is ruled out" / "been excluded" is the
		// predicate form, handled there.
		if negated(before) || lastWordIn(before, "to", "is", "was", "are", "were", "been", "be", "being") {
			continue
		}
		obj := seg[loc[1]:]
		if m := objectEndRE.FindStringIndex(obj); m != nil {
			obj = obj[:m[0]]
		}
		out = appendItem(out, obj)
	}
	return out
}

// predicateItems returns the subjects of the predicate forms in seg.
func predicateItems(seg string) []string {
	var out []string
	for _, loc := range ruleOutPredRE.FindAllStringIndex(seg, -1) {
		subj := seg[:loc[0]]
		if locs := subjectStartRE.FindAllStringIndex(subj, -1); len(locs) > 0 {
			subj = subj[locs[len(locs)-1][1]:]
		}
		words := strings.Fields(subj)
		for len(words) > 0 && slices.Contains([]string{"has", "have", "had"}, words[len(words)-1]) {
			words = words[:len(words)-1]
		}
		subj = strings.Join(words, " ")
		if negated(lastWords(subj, 2)) {
			continue
		}
		if locs := subjectVerbRE.FindAllStringIndex(subj, -1); len(locs) > 0 {
			rest := subj[locs[len(locs)-1][1]:]
			m := listSepRE.FindStringIndex(rest)
			if m == nil {
				continue // "the connector is running fine": a state, not a list
			}
			subj = rest[m[1]:]
		}
		for _, it := range itemSplitRE.Split(subj, -1) {
			out = appendItem(out, it)
		}
	}
	return out
}

// noFormItems returns the "no <topic>" claims in seg for the noForm topics.
// "no DDL yet" is not one: a pending answer is not a rule-out.
func noFormItems(seg string) []string {
	var out []string
	for _, loc := range noWordRE.FindAllStringIndex(seg, -1) {
		rest := seg[loc[1]:]
		words := strings.Fields(rest)
		if len(words) == 0 {
			continue
		}
		for _, t := range ruleOutTopics {
			if !t.noForm {
				continue
			}
			// The topic must start at the first or second word after "no".
			for skip := 0; skip <= 1 && skip < len(words); skip++ {
				cand := strings.Join(words[skip:], " ")
				m := t.re.FindStringIndex(cand)
				if m == nil || m[0] != 0 {
					continue
				}
				end := cand[m[1]:]
				if e := objectEndRE.FindStringIndex(end); e != nil {
					end = end[:e[0]]
				}
				if strings.Contains(" "+end+" ", " yet ") {
					break
				}
				out = appendItem(out, "no "+strings.Join(words[:skip], " ")+" "+cand[:m[1]])
				break
			}
		}
	}
	return out
}

// noWordRE is the "no" of the no-form.
var noWordRE = regexp.MustCompile(`\bno\s+`)

// appendItem cleans an item and appends it when anything is left.
func appendItem(items []string, it string) []string {
	it = strings.Join(strings.Fields(it), " ")
	it = strings.Trim(it, ` .,;:!"'`)
	for _, p := range []string{"the ", "a ", "an ", "any ", "also ", "both ", "either "} {
		it = strings.TrimPrefix(it, p)
	}
	if it == "" || strings.Contains(" "+it+" ", " yet ") || hedged(it) {
		return items
	}
	return append(items, it)
}

// hedged reports a segment that makes no claim (ruleOutHedges).
func hedged(s string) bool {
	s = " " + s + " "
	for _, h := range ruleOutHedges {
		if strings.Contains(s, h) {
			return true
		}
	}
	return false
}

// negated reports a negator in s (ruleOutNegators).
func negated(s string) bool {
	s = " " + s + " "
	for _, n := range ruleOutNegators {
		if strings.Contains(s, " "+n) || (n == "n't " && strings.Contains(s, n)) {
			return true
		}
	}
	return false
}

// lastWords returns the last n words of s.
func lastWords(s string, n int) string {
	w := strings.Fields(s)
	if len(w) > n {
		w = w[len(w)-n:]
	}
	return strings.Join(w, " ")
}

// lastWordIn reports whether the last word of s is one of words.
func lastWordIn(s string, words ...string) bool {
	return slices.Contains(words, lastWords(s, 1))
}

// ruleOutSentences cuts text at sentence ends as claimClauses does, but keeps
// ';' inside the sentence: a label list may use it between items.
func ruleOutSentences(text string) []claimClause {
	var out []claimClause
	start := 0
	add := func(end int, q bool) {
		if c := strings.TrimSpace(text[start:end]); c != "" {
			out = append(out, claimClause{text: c, question: q})
		}
		start = end + 1
	}
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '!', '\n':
			add(i, false)
		case '?':
			add(i, true)
		case '.':
			if i+1 == len(text) || text[i+1] == ' ' || text[i+1] == '\n' {
				add(i, false)
			}
		}
	}
	add(len(text), false)
	return out
}

// Rule 2 limits on the refusal text.
const (
	maxRuleOutItemsListed = 5
	maxRuleOutToolsListed = 3
)

// backingRead reports whether a step's fact backs a claim: a read of one of
// its tools that did not error, or that answered not-found (a missing
// publication is a finding about the publication).
func backingRead(f stepFact, c RuledOutClaim) bool {
	if !c.BackedBy(f.tool) {
		return false
	}
	if !f.isError {
		return true
	}
	rf, ok := parseFailureFact(f.tool, f.summary)
	return ok && rf.class == readFailureNotFound
}

// checkRuledOut is rule 2's check. counted are the steps that may back a
// claim (the cited ones, or every step for post_message); all is every step
// of the run, newest first, for the "cite it" hint (nil: no hint, the tool
// has no citations). It returns nil when every checked claim is backed.
func checkRuledOut(text string, counted, all []ledgerStep, offered func(string) bool) *guardFinding {
	var missing []RuledOutClaim
	for _, c := range RuledOutClaims(text) {
		if !slices.ContainsFunc(counted, func(s ledgerStep) bool { return backingRead(s.fact, c) }) {
			missing = append(missing, c)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	var parts, items, topics []string
	for i, c := range missing {
		items = append(items, c.Item)
		for _, t := range c.Topics {
			if !slices.Contains(topics, t) {
				topics = append(topics, t)
			}
		}
		if i >= maxRuleOutItemsListed {
			continue
		}
		hint := orList(offeredTools(c.Tools, offered))
		for _, s := range all {
			if backingRead(s.fact, c) {
				hint += "; step " + s.id + " of this run read it: cite it"
				break
			}
		}
		parts = append(parts, "`"+strings.ReplaceAll(c.Item, "`", "'")+"` ("+hint+")")
	}
	if len(missing) > maxRuleOutItemsListed {
		parts = append(parts, fmt.Sprintf("and %d more", len(missing)-maxRuleOutItemsListed))
	}
	what := "no cited step read"
	fix := `Cite the step, read it now, or write "not checked: <item>".`
	if all == nil {
		what = "no step of this run read"
		fix = `Read it now, or write "not checked: <item>".`
	}
	return &guardFinding{
		rule:     guardRuleRuledOut,
		refusal:  fmt.Errorf("%w: hypothesis guard (%s): you rule out things %s: %s. %s", tools.ErrBadInput, guardRuleRuledOut, what, strings.Join(parts, ", "), fix),
		attrs:    []any{"items", len(missing), "topics", topics},
		unbacked: items,
	}
}

// offeredTools keeps the tools the room offers, at most
// maxRuleOutToolsListed; all of them when it offers none (the hint still
// names what would answer).
func offeredTools(names []string, offered func(string) bool) []string {
	var out []string
	if offered != nil {
		for _, n := range names {
			if offered(n) {
				out = append(out, n)
			}
		}
	}
	if len(out) == 0 {
		out = names
	}
	if len(out) > maxRuleOutToolsListed {
		out = out[:maxRuleOutToolsListed]
	}
	return out
}

// orList renders "a", "a or b", "a, b or c".
func orList(xs []string) string {
	if len(xs) <= 1 {
		return strings.Join(xs, "")
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " or " + xs[len(xs)-1]
}
