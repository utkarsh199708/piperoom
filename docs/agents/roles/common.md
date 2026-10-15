# Common setup for every autonomous role

1. Repo: `utkarsh199708/piperoom`. If it is not checked out, attach it with `add_repo` and clone it.
2. Docs come from `main` once merged, else from the branch `claude/sharp-fermat-r6uh64`:
   `git fetch origin main claude/sharp-fermat-r6uh64` then
   `git show origin/main:docs/agents/<f> 2>/dev/null || git show origin/claude/sharp-fermat-r6uh64:docs/agents/<f>`.
   Read `AGENTS.md`, `docs/agents/org.md`, `docs/agents/routing.yaml` and your role file.
3. GitHub only through the GitHub MCP tools. Every comment or issue you write ends with the Claude Code footer.
4. State lives in the pinned issue titled **`Agent ops log`** (create it once, label `type:chore`). Each run reads the
   newest comment from its own role (it starts with `[<ROLE> STATE]` followed by a JSON block) and ends by posting a
   new one. Keep it under 40 lines.
5. Never merge, never push to `main`, never edit contract files, never touch a real customer system or secrets. When
   blocked on one of these, file a `sev:critical` `squad:leads` `lane:lead-arch` issue and stop.
6. Do not ask the human anything. Decide from the docs, record why in the ops log, and continue.
