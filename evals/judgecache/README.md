# Recorded judge verdicts (#454)

One JSON file per LLM judge verdict on an ambiguous corpus text, named
`sha256(JudgePromptVersion NUL scenario name NUL hypothesis text).json`
(`evals/judgecache.go`). `-judge auto` reads them before any live call; unit
tests and CI replays read only them, and a miss fails loudly.

- `ambiguous.json` is the manifest: every pinned corpus text the tri-state
  keyword pass marks ambiguous. `TestAmbiguousCorpusSet` keeps it current.
- Verdicts are written only by `evalrun -judge-fill`, a paid run that needs
  the owner's approval, and only for manifest entries.
- The repository is public: only synthetic eval and corpus texts belong here,
  never text from real rooms or learned scenarios.
- Bumping `JudgePromptVersion` (evals/judge.go) invalidates every entry; an
  entry whose scenario truth changed is stale and must be refilled.

Until the owner approves the first fill this directory holds no verdicts.
