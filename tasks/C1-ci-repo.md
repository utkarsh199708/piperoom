# C1 — CI and repo hygiene (Claude cloud agent, Sonnet)

Read `AGENTS.md` and `docs/collab.md` first. You own: `.github/**`, `README.md`, `docs/runbook.md`, `Makefile` (optional).

You run in a cloud sandbox: **no Docker dev stack**. Unit tests only.

## Goal
Every PR on GitHub gets the same checks the leads run by hand.

## Work
1. `.github/workflows/ci.yml` on `pull_request` and `push` to `main`:
   - Go job (`ubuntu-latest`, Go version from `go.mod`): `gofmt -l` must be empty, `go vet ./...`, `go build ./...`,
     `go test ./...` (unit only — integration tests need the stack, they are tagged and skipped).
   - Web job (`web/`, Node 24): `npm ci`, `npm run lint`, `npm run build`, `npx vitest run`.
     npm install scripts: esbuild needs its script — allow only esbuild (document how, no `--ignore-scripts=false` blanket).
   - Cache Go modules and npm. Pin every action to a full commit SHA with a version comment.
   - `permissions: contents: read` at the top. No secrets used.
2. `README.md`: what Piperoom is (2 short paragraphs), repo map, how to run the dev stack (`go run ./cmd/devctl reset`),
   how to run tests, how the team works (link `AGENTS.md`, `docs/collab.md`).
3. `docs/runbook.md`: start/stop stack, the 4 failure scenarios (link `dev/scenarios.md`), stack lock usage
   (`devctl lock --owner <tag>`), common errors (Docker not running, port in use, goose on PATH).

## Done when
A PR shows both CI jobs green on GitHub; a deliberately broken commit (in a throwaway branch) turns them red.
Report the run URLs in the PR description.
