package agent

import (
	"encoding/json"
	"fmt"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// The restore guard (H4) refuses a proposal whose later step puts back a
// setting an earlier step changed while a step of another action sits between
// them. The executor runs a proposal's steps back to back, so the restore lands
// before the middle step's effect has settled: live in room
// r_01M5RWMGFA12MXCBT0TV766C16 (2026-10-25) the agent proposed
// compatibility NONE, restart task 0, compatibility BACKWARD on one subject;
// all three ran in the same second, BACKWARD was back before the restarted
// task registered its schema, and the task failed again with the same 409.
// The restore belongs in a separate proposal once recovery is verified. The
// executor cannot know which effects need time, so the check sits here, in
// what the agent may propose, before Proposals.Create.
//
// The rule, per pair of steps M < N:
//   - same action name;
//   - same setting target: the step's top-level params minus its value fields
//     (restoreValueKeys). A "changes" object (connector config, tuning) is one
//     setting per key, so the key is part of the target and two steps that
//     change different config keys of one connector never pair;
//   - a different value for that setting;
//   - at least one step strictly between M and N whose action differs.
//
// Adjacent steps (nothing between) and steps separated only by the same
// action are allowed: no other action's effect is waiting on the first change,
// so an overwrite there loses nothing. Actions without a value field (restarts,
// resumes, snapshots, schema_register, whose schema text is the target and
// not a setting) never pair.

// restoreValueKeys are the top-level param fields that carry the value a step
// sets, as opposed to naming what it sets: schema_set_compatibility "level",
// replica identity "mode", the one-key connector config "value", offset reset
// "to", and the per-key "changes" object handled in stepSettings.
var restoreValueKeys = map[string]bool{"level": true, "mode": true, "value": true, "to": true, "changes": true}

// stepSetting is one setting a step writes: target is the canonical JSON of
// what is set, value the canonical JSON of what it is set to.
type stepSetting struct {
	target string
	value  string
}

// stepSettings returns the settings a step's params write, or nil when the
// params have no value field (or are not a JSON object).
func stepSettings(params json.RawMessage) []stepSetting {
	var m map[string]any
	if err := json.Unmarshal(params, &m); err != nil || m == nil {
		return nil
	}
	target := map[string]any{}
	values := map[string]any{}
	for k, v := range m {
		if restoreValueKeys[k] {
			values[k] = v
		} else {
			target[k] = v
		}
	}
	if len(values) == 0 {
		return nil
	}
	changes, perKey := values["changes"].(map[string]any)
	if !perKey {
		return []stepSetting{{target: canonJSON(target), value: canonJSON(values)}}
	}
	delete(values, "changes")
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]stepSetting, 0, len(keys)+1)
	for _, k := range keys {
		t := map[string]any{"target": target, "changes_key": k}
		out = append(out, stepSetting{target: canonJSON(t), value: canonJSON(changes[k])})
	}
	if len(values) > 0 {
		out = append(out, stepSetting{target: canonJSON(target), value: canonJSON(values)})
	}
	return out
}

// canonJSON renders v with sorted object keys (encoding/json sorts map keys),
// so equal params compare equal whatever order the model wrote them in.
func canonJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// restoreBeforeSettle returns the error the model sees when a later step
// re-sets what an earlier step changed with another action between them, or
// nil. Steps are numbered from 1 in the message. The first offending pair by
// the later step, then the earlier, is reported.
func restoreBeforeSettle(steps []events.ProposalStep) error {
	settings := make([][]stepSetting, len(steps))
	for i, s := range steps {
		settings[i] = stepSettings(s.Params)
	}
	for n := 2; n < len(steps); n++ {
		if len(settings[n]) == 0 {
			continue
		}
		for m := 0; m < n-1; m++ {
			if steps[m].Action != steps[n].Action || !settingReversed(settings[m], settings[n]) {
				continue
			}
			for k := m + 1; k < n; k++ {
				if steps[k].Action != steps[m].Action {
					return fmt.Errorf("%w: step %d restores what step %d changed, but step %d between them depends on that change taking effect: send the restore as a separate proposal after you have verified recovery",
						tools.ErrBadInput, n+1, m+1, k+1)
				}
			}
		}
	}
	return nil
}

// settingReversed reports whether later writes a different value to a target
// earlier also wrote.
func settingReversed(earlier, later []stepSetting) bool {
	for _, a := range earlier {
		for _, b := range later {
			if a.target == b.target && a.value != b.value {
				return true
			}
		}
	}
	return false
}
