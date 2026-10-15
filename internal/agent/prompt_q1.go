package agent

import "strings"

// Guidance for tools that exist only in some deployments or runs. Each section
// is part of the system prompt only when the run really offers the tool, so the
// prompt never tells the model to call something that is not there (the wiring
// test of cmd/investigate holds the always-present prompt to that rule; these
// sections hold themselves to it by construction, and TestSystemPromptForNamesOnlyOfferedTools
// checks it).

const pageOwnerGuidance = `- page_owner asks the owners of one layer to look at the room. When no available action can fix the cause, or a proposal waits on a layer nobody in the room is watching, page that layer's owner once instead of only writing that a human is needed. Give one sentence a person can act on and name a waiting proposal by its title. Never page a layer twice: the room refuses it, and repeating it wears people out.`

const withdrawProposalGuidance = `- withdraw_proposal takes back one of your own open proposals. If new evidence makes an open proposal wrong or risky, or you want the approvers to hold, withdraw it with a one-sentence reason and propose again later when it is right. Do not ask people in prose to hold while the proposal stays open for approval.`

const connectWorkerLogGuidance = `- connect_worker_log reads the worker log lines of one connector. When Connect says a task is RUNNING but no data flows or the replication slot does not advance, read connect_worker_log before you propose a restart: a task that keeps retrying a failing read shows RUNNING while the log shows the error.`

// systemPromptFor is the system prompt of a run that offers the tools in tl: the
// fixed prompt plus one section for each optional tool the run has. Like
// systemPrompt it holds nothing that changes within a run, so the prompt cache
// stays warm across the run's requests.
func systemPromptFor(tl toolList) string {
	return systemPromptWith(func(name string) bool {
		_, ok := tl.byName[name]
		return ok
	})
}

// systemPromptWith is systemPromptFor over any "does the run offer this tool"
// test.
func systemPromptWith(has func(name string) bool) string {
	var extra []string
	if has("page_owner") {
		extra = append(extra, pageOwnerGuidance)
	}
	if has("withdraw_proposal") {
		extra = append(extra, withdrawProposalGuidance)
	}
	if has("connect_worker_log") {
		extra = append(extra, connectWorkerLogGuidance)
	}
	if len(extra) == 0 {
		return systemPrompt()
	}
	return systemPrompt() + "\n\nExtra tools in this room\n" + strings.Join(extra, "\n")
}
