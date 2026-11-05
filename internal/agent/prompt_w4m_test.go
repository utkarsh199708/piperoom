package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// w4mHealthy is rule 7's healthy-verdict line after W4m (KAN-508): an
// earlier fault stays in the verdict, cited, in past tense. Its 8 words
// replace the 8 of "put any earlier fault in the past tense".
const w4mHealthy = "   If every stage reads healthy, your closing message and hypothesis must say the pipeline is healthy now and there's nothing to fix; keep any earlier fault, cited, in past tense; keep any hypothesis naming a current failing component low; post nothing after."

// w4mSchemaHistory is the MySQL schema history bullet after ruling R21: the
// recovery is proposed after checking the stored position's schemas, not
// "only if" they did not change. Same word count.
const w4mSchemaHistory = "- The schema history topic needs one partition and unlimited retention (kafka_schema_history); if lost, propose snapshot.mode recovery via connect_update_connector_config after checking no captured table's schema changed since the stored position."

// w4mBatchSHA256 is the batch prompt's hash on W4m's base: W4m touches only
// the CDC method and the MySQL replacer, so it must not move.
const w4mBatchSHA256 = "f50450e04a2d6afcaf687d6e6b504b48a0958074fcc236296af60ce0d05d145e"

// w4mAdded is the text W4m adds, for the leakage check: the reworded part of
// each line, tool names left out (they are not answers).
var w4mAdded = []string{
	"keep any earlier fault, cited, in past tense",
	"after checking no captured table's schema changed",
}

// w4mPrompts are the four CDC prompts W4m moves: both sources, both modes.
func w4mPrompts() map[string]string {
	prompts := map[string]string{}
	for _, write := range []bool{true, false} {
		prompts["cdc, write "+boolWord(write)] = systemPromptForMode(events.PipelineKindCDC, RoomMode{Write: write})
		prompts["mysql, write "+boolWord(write)] = systemPromptForSource(events.PipelineKindCDC, events.SourceKindMySQL, RoomMode{Write: write})
	}
	return prompts
}

// TestSystemPromptRecoveryKeepsCitedCause pins W4m. Rule 7 (shared by the
// Postgres and MySQL prompts): a healthy verdict keeps any earlier fault,
// cited, in past tense, so a post-recovery hypothesis still names the cause
// and its step ids (consumer-skipped-range's said "Recovered: ... nothing
// more to fix" and dropped both). R21 (MySQL only): snapshot.mode recovery
// after a lost schema history topic is proposed after checking no captured
// table's schema changed since the stored position, so the agent reads
// instead of asking; the check stays. Word-neutral: 1352 words with write
// mode on, 1353 with it off, in both sources.
func TestSystemPromptRecoveryKeepsCitedCause(t *testing.T) {
	words := map[string]int{"cdc, write on": 1352, "cdc, write off": 1353, "mysql, write on": 1352, "mysql, write off": 1353}
	for name, p := range w4mPrompts() {
		if got := cdcMethodLine(t, p, "   If every stage reads healthy"); got != w4mHealthy {
			t.Errorf("%s: rule 7's healthy line =\n%s\nwant\n%s", name, got, w4mHealthy)
		}
		if n := strings.Count(p, "keep any earlier fault, cited, in past tense"); n != 1 {
			t.Errorf("%s: the cited-fault clause is stated %d times, want 1", name, n)
		}
		if strings.Contains(p, "put any earlier fault in the past tense") {
			t.Errorf("%s: the prompt still says \"put any earlier fault in the past tense\"", name)
		}
		if got := len(strings.Fields(p)); got != words[name] {
			t.Errorf("%s: %d words, want %d (W4m is word-neutral)", name, got, words[name])
		}
		mysql := strings.HasPrefix(name, "mysql")
		if mysql {
			if got := cdcFixesLine(t, p, "- The schema history topic"); got != w4mSchemaHistory {
				t.Errorf("%s: schema history bullet =\n%s\nwant\n%s", name, got, w4mSchemaHistory)
			}
		}
		if strings.Contains(p, "only if no captured table's schema changed") {
			t.Errorf("%s: the prompt still says \"only if no captured table's schema changed\"", name)
		}
		// R21 is MySQL's: the Postgres prompt has no schema history bullet.
		if got := strings.Contains(p, "after checking no captured table's schema changed"); got != mysql {
			t.Errorf("%s: carries R21's \"after checking\" = %v, want %v", name, got, mysql)
		}
		// Rule 7's other sentences keep their words.
		for _, keep := range []string{
			"7. Once cited evidence establishes the cause, stop reading; instead of asking unprompted, call propose_fix with the smallest listed action that fixes it, saying what it gives up, or say nothing needs fixing.",
			"   If no evidence names a cause, say in your closing message what you ruled out, what remains open and that no cause is established; keep any hypothesis low, propose nothing and wait.",
		} {
			if !strings.Contains(p, keep+"\n") {
				t.Errorf("%s: rule 7 lost %q", name, keep)
			}
		}
	}
	// The batch prompt keeps its bytes and its own wording.
	batch := systemPromptFor(events.PipelineKindBatch)
	sum := sha256.Sum256([]byte(batch))
	if h := hex.EncodeToString(sum[:]); h != w4mBatchSHA256 {
		t.Errorf("batch prompt sha256 = %s, want %s: W4m must not move it", h, w4mBatchSHA256)
	}
	if !strings.Contains(batch, "put any earlier fault in the past tense, and keep any hypothesis naming a current failing component low.") {
		t.Error("the batch prompt lost its own healthy-verdict wording")
	}
	for _, cdcOnly := range []string{"cited, in past tense", "after checking no captured"} {
		if strings.Contains(batch, cdcOnly) {
			t.Errorf("batch prompt carries the CDC wording %q", cdcOnly)
		}
	}
}

// w4mScenario is the part of an eval scenario the W4m leakage check reads.
type w4mScenario struct {
	Expect struct {
		RootCause  [][]string                `yaml:"root_cause"`
		FixTargets map[string]map[string]any `yaml:"fix_targets"`
	} `yaml:"expect"`
}

// w4mTargetValues flattens a fix target's values: strings, lists of strings,
// and nested maps (a config change's keys and values, such as snapshot.mode
// and recovery).
func w4mTargetValues(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []any:
		var out []string
		for _, e := range x {
			out = append(out, w4mTargetValues(e)...)
		}
		return out
	case map[string]any:
		var out []string
		for k, e := range x {
			out = append(out, k)
			out = append(out, w4mTargetValues(e)...)
		}
		return out
	}
	return nil
}

// TestSystemPromptLeaksNoW4mAnswer: W4m's words carry no root-cause term of
// its two scenarios, consumer-skipped-range (rule 7's target) and
// mysql-history-topic-deleted (R21's), and no part of their fix targets. The
// whole-prompt check is not run: the MySQL bullet W4m rewords already names
// the schema history topic and snapshot.mode recovery (the open item under
// R21), and W4m adds neither.
func TestSystemPromptLeaksNoW4mAnswer(t *testing.T) {
	added := promptTokenRe.ReplaceAllString(strings.ToLower(strings.Join(w4mAdded, "\n")), "")
	addedWords := " " + strings.Join(promptWordRe.FindAllString(added, -1), " ") + " "
	for name, p := range w4mPrompts() {
		for _, frag := range w4mAdded {
			if strings.HasPrefix(name, "cdc") && strings.HasPrefix(frag, "after checking") {
				continue // R21's bullet is MySQL's
			}
			if !strings.Contains(p, frag) {
				t.Errorf("%s: the prompt lacks W4m's %q, so the leakage check reads text it does not have", name, frag)
			}
		}
	}
	for _, name := range []string{"consumer-skipped-range", "mysql-history-topic-deleted"} {
		b, err := os.ReadFile(filepath.Join("..", "..", "evals", "scenarios", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var sc w4mScenario
		if err := yaml.Unmarshal(b, &sc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(sc.Expect.RootCause) == 0 || len(sc.Expect.FixTargets) == 0 {
			t.Fatalf("%s: no root-cause groups or fix targets read; the scenario format changed", name)
		}
		for _, group := range sc.Expect.RootCause {
			for _, term := range group {
				if promptHasTerm(addedWords, term) {
					t.Errorf("%s: W4m's words carry its root-cause term %q", name, term)
				}
			}
		}
		for action, target := range sc.Expect.FixTargets {
			for key, v := range target {
				for _, s := range w4mTargetValues(v) {
					s = strings.ToLower(s)
					if strings.Contains(added, s) || promptHasTerm(addedWords, s) {
						t.Errorf("%s: W4m's words name the %s %s target %q", name, action, key, s)
					}
				}
			}
		}
	}
}
