# Pull-request fix: design (main @ 47ad7b2f, 2026-10-23)

Design row **Q7-0** of the Q wave (Jira KAN-320). Owner of the design: LEAD-ARCH. Plan:
`docs/product/so-gaps-2026-10.md` (§3.1, §4). Board: `tasks/Q-board.md`, step 7. This page settles Q0 **item 6** (the
contract additions of the pull-request fix). The Go shapes in §5 are exact proposals in the style of
`tasks/P0-contracts.md`; this PR does **not** edit a contract file. They land in code in the leads' contract PR
**Q7-0c** after the owner rules on §14 (Board changes, §15). Where this text and merged code differ, the code and its
doc comments win.

The page answers the three questions the board's Q7-0 row asks: **which repos** (§3), **how a change is approved**
(§6.4, §7), **how it is verified** (§8). The refusals are in §6.2, the GitHub App scopes in §4.1.

## 1. Problem

Piperoom diagnoses a problem and ends it with an approved action on a running system: restart a task, change a
connector key, reset offsets, grant SELECT. A large class of Debezium problems ends in a file instead: the Connect
image's `Dockerfile` lacks the plugin jar or the JDBC driver, a version pin is below the release that fixes the bug,
the connector JSON lives in git and is deployed by CI, a consumer's `value.deserializer` sits in the application's
config. A live change to the system does not end these: the next deploy overwrites it, or there is no live knob at
all.

The 722-problem Stack Overflow analysis measures it (plan §2, §4):

| | Resolved | Diagnosed |
|---|---|---|
| all of the Q wave except step 7 | 136 (18.8%) | 191 (26.5%) |
| all of the Q wave with step 7 | up to 191 (26.5%) | 191 (26.5%) |
| every capability of the taxonomy, no pull-request action | 317 (43.9%) | 675 (93.5%) |
| every capability plus the pull-request action | up to 664 (92.0%) | 675 (93.5%) |

Step 7 resolves nothing new on its own (its diagnosed share is 0): it turns problems Piperoom already diagnoses into
problems it resolves, up to 55 in this wave (7.6 points; fair share 21.8 of the wave's 140, plan §4). It is the only
step that raises the long-run ceiling (44% to 92%). Both upper bounds assume the fix sits in a repository Piperoom may
open a pull request against; §11 and §13 say what makes that true.

## 2. Scope and non-goals

**In scope (this wave)**

- GitHub.com and GitHub Enterprise Server, through one GitHub App installation.
- Text files only, in repositories a pipeline lists in `owners.yaml` `repos:`, under path globs each mapped to one
  layer.
- Three read tools (`repo_list_files`, `repo_read_file`, `pr_status`), one write action (`repo_open_pull_request`).
- The room follows each PR it opened until merged or closed; after merge the agent waits for the change to reach the
  running system, then verifies as it does after any fix.
- Diff view and PR badge in the web; docs; scenarios and evals.

**Non-goals**

- **Merging.** Piperoom opens a PR and never merges, approves, pushes to or force-pushes any branch but its own new
  one, or changes repository settings. The repo's own branch protection, required reviews and CI still apply.
- GitLab, Bitbucket, Azure DevOps, Gerrit (`FORGE_KIND` reserves the names; §14 D12).
- Deleting or renaming files, binary files, Git LFS objects, submodules, symlinks, executable bits.
- CI and repository-governance files (§6.2), whatever the globs say.
- Comments on PRs, labels, reviewer requests, issue creation, webhooks into Piperoom (§14 D6).
- Building, deploying or rolling out the merged change. Piperoom observes it (§8.2); the customer's CD does it.
- A free-form "edit any file in the org" capability. The model names a repo id and a path; owners.yaml decides
  whether that exists.

## 3. Which repos: `owners.yaml` `repos:` (Q7a)

Per pipeline; A3 implements it in `internal/owners` (Q7a). The schema is fixed here.

```yaml
pipelines:
  - id: orders
    repos:
      - id: connect-config                 # slug, ^[A-Za-z0-9_.-]+$ (the tenant repos: charset)
        url: https://github.com/acme/connect-config
        base: main                         # the branch PRs target; required
        draft: false                       # optional: open PRs as drafts (§14 D9)
        public_ok: false                   # optional: allow a public repo (§14 D3)
        paths:
          - glob: "connectors/orders-*.json"
            layer: kafka-connect
          - glob: "docker/connect/Dockerfile"
            layer: kafka-connect
      - id: orders-consumer
        url: https://github.com/acme/orders-consumer
        base: main
        paths:
          - glob: "src/main/resources/application*.yml"
            layer: consumer
```

Load rules (Q7a; each refusal names the pipeline, the repo id and the field, never a value that is not a plain name):

1. `id` matches the repo-id charset; one id appears once per pipeline. The same id in two pipelines must have the
   same `url` and `base` (a repo id names one repository in the whole file, so the tenant's `repos:` list of ids,
   `owners.Tenant.Repos`, resolves without ambiguity).
2. `url` is `https://<host>/<owner>/<name>` with no user info, query, fragment, `.git` suffix or extra path. `<owner>`
   and `<name>` match `^[A-Za-z0-9_.-]+$`. The host is checked against the forge's web host at start (`cmd/server`,
   §5.6), not by Load (Load does not read config).
3. `base` is a valid branch name (`clients.ValidBranchName`, §5.1) and does not start with `piperoom/`.
4. `paths` is not empty; each `glob` is a clean relative pattern (`clients.CheckRepoPath` on the pattern with `*`,
   `?`, `[...]` and `**` allowed) and has a `layer` of `p.Layers()`. A glob literally under a denied directory, or
   equal to a denied file name (§6.2), is refused at load.
5. Glob syntax: Go `path.Match` on the whole path, where `*` does not cross `/`, plus `**` as a whole segment that
   matches zero or more segments. A pipeline-level helper `Repo.LayerOf(path) (string, error)` returns the layer of
   the globs that match; no match is `ErrPathNotListed`, matches of two different layers are `ErrPathAmbiguous`
   (never first-match: an approver must not depend on glob order).
6. `repos:` is allowed on both pipeline kinds (a dbt project's repo is a transform-layer repo).
7. A `repos:` block while `FORGE_KIND` is empty loads; `cmd/server` logs one warning at start ("repos: is set but no
   forge is configured; the PR fix is off").

Setup rooms (`docs/design/setup-agent.md` §3.10): the provisional pipeline takes its `repos:` entries from the
`SetupRequest` repo ids, resolved by rule 1 against the tenant's pipelines; U1-14 fixes the setup globs. Every rule of
this page applies unchanged in a setup room; the action is not `Tiered`, so it is T2 there and waits for the approver
of setup design §3.6.

## 4. The GitHub App

### 4.1 Permissions (least privilege)

The customer creates (or installs a published) GitHub App with **only** these permissions, installed on **only the
selected repositories** the pipelines list:

| Permission | Level | Why |
|---|---|---|
| Metadata | read | mandatory for every App |
| Contents | read and write | read files; create blobs, a tree, a commit and the new branch ref |
| Pull requests | read and write | open the PR; read its state |
| Checks | read | check runs on the PR head |
| Commit statuses | read | legacy statuses on the PR head |

Never granted, and Q7h's setup page says why: **Workflows** (without it GitHub itself refuses any commit that creates
or changes a file under `.github/workflows/`, a second wall behind §6.2), Administration, Actions, Secrets,
Environments, Deployments, Issues, any organisation or account permission. The App subscribes to no webhook events
and its webhook is inactive (§14 D6). With write mode off the operator may install a read-only App (Contents, Pull
requests, Checks, Commit statuses: read); the reader works, the writer is never built.

### 4.2 Tokens

- Config (§5.6): `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY_FILE` (a PEM RSA key in a
  file; never an environment value). The key is read once at start into memory; a key file readable by group or
  others logs a warning on Unix.
- App JWT: RS256 over `{iat: now-60s, exp: now+9m, iss: app id}`, built with the standard library
  (`crypto/rsa.SignPKCS1v15`, `crypto/sha256`, `encoding/base64.RawURLEncoding`); no new module.
- Installation tokens: `POST /app/installations/{installation_id}/access_tokens` with a body that **narrows** the
  token: `repositories: [<name>]` (the one repo of the call) and `permissions` (reader: `contents: read,
  pull_requests: read, checks: read, statuses: read`; writer: `contents: write, pull_requests: write`). A token is
  cached per (repo, scope) and refreshed 5 minutes before its `expires_at`. Q7b confirms the endpoint, the body fields
  and the response against the GitHub REST docs (AGENTS.md rule 9 applies to REST APIs as much as to Go symbols).
- The private key, the JWT and every token are `netsec.Secret` (print as `[redacted]`), never logged, never in an
  error text, never in an event, never sent anywhere but the configured API host.
- The HTTP client comes from `netsec.HTTP{TLS: ...}.Client(apiURL, 10 s)`, with a bearer round tripper bound to the
  API URL's scheme and host (the `basicAuth` pattern), `CheckRedirect` that refuses every redirect, `io.LimitReader`
  4 MB on every body, status checked on every response.
- One installation per Piperoom in this wave (§14 D8): a repo the installation cannot see is a clear refusal ("the
  GitHub App is not installed on acme/connect-config"), never a lookup of another installation.

### 4.3 What the App writes

Only: blobs, one tree, one commit (parent = the approved `base_sha`), one new ref
`refs/heads/piperoom/<room>-<proposal>-<step>`, one pull request from that branch into `base`. Never a ref update
of an existing branch, never `force`, never a ref delete except its own new branch after a failed PR create (§12),
never `PUT .../merge`, never a review, never a repo setting. Q7b has a test that walks every request the client can
send and fails on any method and path outside this list.

## 5. Q0 item 6: the contract additions (land in Q7-0c)

New methods go on new interfaces and new types, so every existing fake, the eval replay and the demo compile
unchanged. `internal/clients` keeps importing nothing from `piperoom/...`; the PR state types the room log needs are
defined in `events` and mirrored, not aliased.

### 5.1 `internal/clients/clients.go`

```go
// ---- Q7: forge (the pull-request fix, docs/design/fix-pr.md) ----

// RepoRef is one repository of a pipeline's owners.yaml repos: block, resolved
// by the caller (action, tool, room) from the repo id. The model only ever
// names the id; Owner, Name and Base come from owners.yaml. (Q7)
type RepoRef struct {
	ID    string `json:"id"`    // owners.yaml repos[].id
	Owner string `json:"owner"` // from repos[].url: https://<host>/<owner>/<name>
	Name  string `json:"name"`
	Base  string `json:"base"` // repos[].base, the branch PRs target
}

// RepoInfo is what a fix PR needs to know about the repository itself. (Q7)
type RepoInfo struct {
	Visibility    string `json:"visibility"` // public | private | internal
	Archived      bool   `json:"archived"`
	DefaultBranch string `json:"default_branch"`
}

// RepoFile is one regular file read at a commit. Content is untrusted text,
// at most MaxRepoFileBytes, never logged and never put in an event. (Q7)
type RepoFile struct {
	Path    string `json:"path"`
	SHA     string `json:"sha"`      // the commit the file was read at; never a branch name
	BlobSHA string `json:"blob_sha"` // git blob id; equal blob ids mean equal content
	Size    int    `json:"size"`
	Content []byte `json:"-"`
}

// RepoEntry is one path of a tree listing. (Q7)
type RepoEntry struct {
	Path    string `json:"path"`
	BlobSHA string `json:"blob_sha"`
	Size    int    `json:"size"`
}

// ForgeReader is optional and read-only: built whenever a forge is configured,
// write mode on or off. Every method takes a 10 s deadline from the caller and
// scopes its token to repo alone (docs/design/fix-pr.md §4.2). (Q7)
type ForgeReader interface {
	// RepoInfo reads visibility, archived and the default branch.
	RepoInfo(ctx context.Context, repo RepoRef) (RepoInfo, error)
	// BaseHead returns the commit the base branch points at now.
	BaseHead(ctx context.Context, repo RepoRef) (sha string, err error)
	// File reads one regular file at commit sha. ErrNotFound when absent;
	// ErrNotRegularFile for a directory, symlink or submodule;
	// ErrRepoFileTooLarge above MaxRepoFileBytes (Size is still set).
	File(ctx context.Context, repo RepoRef, sha, path string) (RepoFile, error)
	// ListFiles lists the regular files of the tree at commit sha, at most
	// MaxRepoListEntries; truncated reports a cut listing.
	ListFiles(ctx context.Context, repo RepoRef, sha string) (entries []RepoEntry, truncated bool, err error)
	// PullRequest reads one pull request with its head checks. ErrNotFound when absent.
	PullRequest(ctx context.Context, repo RepoRef, number int) (PullRequest, error)
}

// ForgeWriter is built only with write mode on and a forge configured. It
// writes one new branch and opens one pull request; it never pushes to an
// existing branch, never merges, never deletes a ref it did not create in
// the same call. (Q7)
type ForgeWriter interface {
	// OpenPullRequest commits req.Files on top of req.BaseSHA to the new
	// branch req.Branch and opens a pull request into repo.Base.
	//   - ErrBaseMoved: a file of req.Files differs between req.BaseSHA and
	//     the base head now (a file that did not exist at BaseSHA exists now,
	//     or the reverse). Nothing is written.
	//   - A branch req.Branch that already holds exactly this change (same
	//     parent, same tree) is reused, and an open pull request on it is
	//     returned instead of a new one, so a retried call is idempotent;
	//     a branch with other content is ErrBranchExists.
	OpenPullRequest(ctx context.Context, repo RepoRef, req PullRequestRequest) (PullRequest, error)
}

// PullRequestRequest is one fix PR. Files hold the whole new content of each
// file (mode 100644 always); the action computes them from the approved
// edits (docs/design/fix-pr.md §6.1). (Q7)
type PullRequestRequest struct {
	BaseSHA       string
	Branch        string // FixBranch(...)
	Title         string
	Body          string
	CommitMessage string
	Draft         bool
	Files         []FileChange
}

// FileChange is one file of a fix PR. Content is never logged. (Q7)
type FileChange struct {
	Path    string
	Content []byte
}

// PullRequest is the state of one pull request. Checks.Failed holds check
// names written by the repository's CI: untrusted, redacted, cut. (Q7)
type PullRequest struct {
	Repo     string            `json:"repo"` // RepoRef.ID
	Number   int               `json:"number"`
	URL      string            `json:"url"`  // the html URL on the forge's web host
	State    string            `json:"state"` // PRStateOpen | PRStateMerged | PRStateClosed
	HeadRef  string            `json:"head_ref"`
	HeadSHA  string            `json:"head_sha"`
	MergeSHA string            `json:"merge_sha,omitempty"`
	MergedAt time.Time         `json:"merged_at,omitzero"`
	Checks   PullRequestChecks `json:"checks"`
}

// PullRequestChecks folds check runs and commit statuses on the head. (Q7)
type PullRequestChecks struct {
	State  string   `json:"state"` // ChecksNone | ChecksPending | ChecksSuccess | ChecksFailure
	Total  int      `json:"total"`
	Failed []string `json:"failed,omitempty"` // at most 10 names of at most 100 bytes
}

// Pull request and check states. Merged is a state of its own: a merged PR
// is closed on the forge, but the room must never read it as "not applied".
const (
	PRStateOpen   = "open"
	PRStateMerged = "merged"
	PRStateClosed = "closed" // closed without merge
	ChecksNone    = "none"
	ChecksPending = "pending"
	ChecksSuccess = "success"
	ChecksFailure = "failure"
)

// Caps of the PR fix. (Q7)
const (
	MaxRepoFileBytes   = 64 << 10  // one file, before and after the change
	MaxFixFiles        = 10        // files per step
	MaxFixParamsBytes  = 64 << 10  // a step's params (edits, title, body)
	MaxFixDiffBytes    = 128 << 10 // a step's unified diff; a bigger one is refused, never cut
	MaxRepoListEntries = 1000
	MaxRepoPathBytes   = 255
)

var (
	ErrBaseMoved        = errors.New("clients: a file of the change was modified on the base branch")
	ErrBranchExists     = errors.New("clients: the fix branch exists with other content")
	ErrNotRegularFile   = errors.New("clients: not a regular file")
	ErrRepoFileTooLarge = errors.New("clients: repository file too large")
	ErrRepoPathDenied   = errors.New("clients: repository path is not allowed")
)

// FixBranch is the only branch name a ForgeWriter writes:
// "piperoom/<roomID>-<proposalID>-<step>". roomID and proposalID are the
// room's own ids (r_..., p_...). (Q7)
func FixBranch(roomID, proposalID string, step int) string

// ValidBranchName reports whether s is a branch name owners.yaml may use as a
// base: ^[A-Za-z0-9._/-]{1,200}$, no "..", "//", "@{", no leading "-" or "/",
// no trailing "/" or ".", no segment starting with "." or ending in ".lock". (Q7)
func ValidBranchName(s string) bool

// CheckRepoPath checks one repository path (or, with glob true, a pattern of
// owners.yaml repos[].paths) against the fixed rules of
// docs/design/fix-pr.md §6.2: clean, relative, at most MaxRepoPathBytes, no
// "." or ".." segment, no backslash, NUL or control character, and not under
// the deny list (CI and repository-governance files, case-insensitive). The
// error wraps ErrRepoPathDenied. The deny list is the leads': it changes only
// by a contract change, and no setting relaxes it. (Q7)
func CheckRepoPath(p string, glob bool) error

// ChangePullRequest is the PipelineChange kind of a fix PR that merged:
// Object "<repo id>#<number>", Summary its title, After the merge SHA. The
// room writes it when it sees the merge (Q7e). Not external: the agent opened
// it. ChangeKinds gains it after ChangeVendorIncident. (Q7)
const ChangePullRequest = "pull_request"
```

### 5.2 `internal/actions/action.go`

```go
// ---- Q7: actions whose layer depends on params, and deferred actions ----

// Layered is implemented by an action whose approving layer depends on its
// params (repo_open_pull_request: the layer of the path globs the files
// match). Its Layer() returns "" (Registry.Register refuses "" from an action
// that is not Layered). The room service takes the step's layer from
// LayerFor with the room's pipeline, at creation and again before Execute,
// and refuses a step whose Layer differs: the proposer never picks the
// approver. It must not do I/O. (Q7)
type Layered interface {
	LayerFor(p owners.Pipeline, params json.RawMessage) (string, error)
}

// Deferred is implemented by an action whose Execute only asks for a change
// that people apply later outside Piperoom (repo_open_pull_request: the
// repo's reviewers merge the PR, the customer's CD deploys it). Rules the
// room service and the agent apply to every step of such an action:
//   - never approved by a policy: not in owners.yaml auto_approve: or
//     autonomy: (Load refuses it), never scheduled, never a setup T1 step;
//   - a proposal holding a Deferred step holds only Deferred steps, at most
//     MaxDeferredSteps;
//   - the agent does not verify on action_result; it waits for the apply
//     signal (HumanInput "pr_merged") and the change to reach the system
//     (docs/design/fix-pr.md §8).
// Deferred has no method beyond the marker. (Q7)
type Deferred interface {
	Deferred()
}

// MaxDeferredSteps caps the Deferred steps of one proposal. (Q7)
const MaxDeferredSteps = 3

// PullRequestOpener is implemented by an action whose Execute opens a pull
// request. The executor prefers ExecutePullRequest over Execute when an
// action implements it and stores the reference on action.executed
// (ActionExecutedPayload.PullRequest), which starts the room's follower
// (Q7e). The string is Execute's short result. (Q7)
type PullRequestOpener interface {
	ExecutePullRequest(ctx context.Context, params json.RawMessage) (string, events.PullRequestRef, error)
}

// StepRef identifies the proposal step being executed. The executor puts it
// in ctx before Execute (WithStep); an action that names something after the
// step (the fix branch) reads it with StepFrom and refuses to run without it.
// Approvals are "layer: display name" of the approval.given events, in
// required_layers order. (Q7)
type StepRef struct {
	RoomID     string
	ProposalID string
	StepIndex  int
	Title      string   // the proposal title, sanitized as stored
	Approvals  []string
}

func WithStep(ctx context.Context, s StepRef) context.Context
func StepFrom(ctx context.Context) (StepRef, bool)

// DiffPreview is the event type (PreviewResult.Diff). (Q7)
type DiffPreview = events.DiffPreview
```

`Registry.Register` gains two refusals: `Layer() == ""` without `Layered`, and `Deferred` together with `Classed`
saying recovery or performance (a deferred change cannot be measured by `Verify` on the executor's timeline).

### 5.3 `internal/events/events.go`

```go
// PullRequestUpdated: the state of a PR a fix step opened changed (state,
// head, merge or checks). System actor; written by the room's follower
// (Q7e), at most one per poll and only on a change. (Q7)
const PullRequestUpdated = "pull_request.updated"

// PullRequestRef names the PR a step opened (ActionExecutedPayload.PullRequest). (Q7)
type PullRequestRef struct {
	Repo    string `json:"repo"` // owners.yaml repos[].id
	Number  int    `json:"number"`
	URL     string `json:"url"`
	Branch  string `json:"branch"`
	HeadSHA string `json:"head_sha"` // the commit Piperoom wrote
}

// PullRequestUpdatedPayload is one observed change of a fix PR. (Q7)
type PullRequestUpdatedPayload struct {
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
	PullRequestRef
	State string `json:"state"` // open | merged | closed
	// HeadChanged: the branch head is no longer the commit Piperoom wrote
	// (a reviewer pushed to the PR). The merged change is then not exactly
	// the approved diff; the card says so. (Q7)
	HeadChanged bool      `json:"head_changed,omitempty"`
	MergeSHA    string    `json:"merge_sha,omitempty"`
	MergedAt    time.Time `json:"merged_at,omitzero"`
	Checks      PRChecks  `json:"checks"`
}

// PRChecks mirrors clients.PullRequestChecks (events imports no client). (Q7)
type PRChecks struct {
	State  string   `json:"state"`
	Total  int      `json:"total"`
	Failed []string `json:"failed,omitempty"`
}

// DiffPreview is a fix PR step's whole change, read by Preview at BaseSHA.
// Every line passed redact.Text before it was stored, so a secret already in
// the repo never enters the room log; the approved change is the step's
// params, which params_hash covers. Never cut: a step whose diff exceeds
// clients.MaxFixDiffBytes is refused by Validate. (Q7)
type DiffPreview struct {
	Repo     string     `json:"repo"`
	RepoURL  string     `json:"repo_url"`
	Base     string     `json:"base"`
	BaseSHA  string     `json:"base_sha"`
	Branch   string     `json:"branch"` // the branch Execute will create
	Layer    string     `json:"layer"`
	Files    []FileDiff `json:"files"`
	Hosts    []string   `json:"hosts,omitempty"`    // hosts of URLs in added lines, sorted, unique
	Warnings []string   `json:"warnings,omitempty"` // §6.3 risk flags, one line each
	Draft    bool       `json:"draft,omitempty"`
}

// FileDiff is one file of a DiffPreview. (Q7)
type FileDiff struct {
	Path    string `json:"path"`
	Status  string `json:"status"` // modified | added
	Unified string `json:"unified"` // unified diff, 3 lines of context, redacted
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
}

// PreviewResult gains, after Tuning:
//	// Q7: the whole change of a repo_open_pull_request step; nil for every
//	// other action (and omitted, so other previews marshal as before).
//	Diff *DiffPreview `json:"diff,omitempty"`

// ActionExecutedPayload gains, after Receipt:
//	// Q7: the PR a PullRequestOpener step opened; nil otherwise.
//	PullRequest *PullRequestRef `json:"pull_request,omitempty"`

// Outcomes of a proposal of Deferred steps (AgentVerificationPayload.Outcome;
// Recovered false for both). (Q7)
const (
	DeferredNotApplied  = "not_applied"  // a PR closed without merge
	DeferredNotDeployed = "not_deployed" // merged, not seen in the system within the apply timeout
)
```

### 5.4 `internal/agent/contract.go`

`HumanInput.Kind` gains `"pr_merged"` and `"pr_closed"`, sent once per fix PR from its `pull_request.updated` with
state `merged` or `closed`. `ProposalID` and `StepIndex` identify the step, `At` is the merge (or close) time, `Text`
is the server's rendering (`PR #12 in connect-config merged at 14:02 (merge 1a2b3c4); edited after approval: no`).
Both wake an idle agent. `pr_closed` is final for that step.

### 5.5 `internal/tools/tool.go`

`CallForge = "forge" // a forge REST request, e.g. "GET /repos/{owner}/{repo}/contents/{path}?ref={sha}"`. The path
is the template with the path and sha filled in; no token, no query beyond `ref`.

### 5.6 `internal/config/config.go`

```go
ForgeKind               string // FORGE_KIND: "" (off, the default) | "github"
GitHubAPIURL            string // GITHUB_API_URL: default https://api.github.com; GitHub Enterprise Server: https://<host>/api/v3
GitHubAppID             string // GITHUB_APP_ID
GitHubAppInstallationID string // GITHUB_APP_INSTALLATION_ID
GitHubAppPrivateKeyFile string // GITHUB_APP_PRIVATE_KEY_FILE: path to the App's PEM private key (secret file)
FixPRApplyTimeout       string // PIPEROOM_FIXPR_APPLY_TIMEOUT: how long after a merge the agent waits for the change to reach the system; default "2h", 10m to 168h

// Forge is the parsed forge settings. Zero when FORGE_KIND is "".
type Forge struct {
	Kind           string
	APIURL         string // validated, no trailing slash
	WebHost        string // github.com for api.github.com, else the API URL's host
	AppID          int64
	InstallationID int64
	PrivateKeyFile string
}

// ParseForge validates the forge settings. FORGE_KIND "" with any GITHUB_APP_*
// set is an error ("set FORGE_KIND=github"); "github" needs positive integer
// ids and a key file path; the API URL is https unless the host is loopback
// (the netsec rule), with no user info, query or fragment. It does not read
// the key file (wiring does, and refuses a key that is not PEM RSA).
func ParseForge(c Config) (Forge, error)

// ParseFixPRApplyTimeout: "" = 2h; a Go duration from 10m to 168h; else an
// error naming the variable.
func ParseFixPRApplyTimeout(c Config) (time.Duration, error)
```

At start `cmd/server` checks every `repos[].url` host equals `Forge.WebHost` and refuses to start otherwise (a repo
on another host cannot be reached with this App; silently skipping it would hide the gap).

### 5.7 `docs/api.md`

- Event `pull_request.updated` (§5.3), with the rule "one per observed change, none while nothing changes".
- `action.executed.pull_request`; `proposal.previewed` step `preview.diff`; `agent.verification.outcome` values
  `not_applied`, `not_deployed`.
- Snapshot `pull_requests: [{proposal_id, step_index, repo, number, url, branch, state, head_changed, checks,
  merged_at, updated_at}]`, room-level, newest first, at most 20: an executed proposal leaves `open_proposals`
  (snapshot fold of `action.executed`), but its PR stays followed.
- `GET /api/actions`: a `Layered` action shows `layer: ""` and `layer_by_params: true`; a `Deferred` one
  `deferred: true`.

### 5.8 Owners (A3 implements in Q7a; not a contract file)

```go
type Repo struct {
	ID       string     `yaml:"id"`
	URL      string     `yaml:"url"`
	Base     string     `yaml:"base"`
	Draft    bool       `yaml:"draft"`
	PublicOK bool       `yaml:"public_ok"`
	Paths    []RepoPath `yaml:"paths"`
}
type RepoPath struct {
	Glob  string `yaml:"glob"`
	Layer string `yaml:"layer"`
}
// Pipeline.Repos []Repo `yaml:"repos"`
func (p Pipeline) Repo(id string) (Repo, bool)
func (r Repo) Ref() clients.RepoRef
func (r Repo) LayerOf(path string) (string, error) // ErrPathNotListed | ErrPathAmbiguous; CheckRepoPath first
func (r Repo) Allows(path string) bool             // LayerOf succeeds
var ErrPathNotListed, ErrPathAmbiguous error
```

`LoadWithActions` and `ValidateAutoApprove` gain a second argument, the names of `Deferred` actions, and refuse them
in `auto_approve:` and `autonomy:` ("repo_open_pull_request is never approved by a policy").

## 6. The action `repo_open_pull_request` (Q7c, `internal/actions/fixpr`)

Class fix; implements `Scoped`, `Layered`, `Deferred`, `Preview`, `PullRequestOpener`. `Layer()` is `""`.

### 6.1 Params

```json
{
  "repo": "connect-config",
  "base_sha": "1a2b3c4d5e6f...40 hex",
  "title": "connect image: add the Debezium Postgres plugin 2.7.3",
  "body": "Cause: ... Evidence: connect_plugins lists no io.debezium.connector.postgresql...",
  "changes": [
    { "path": "docker/connect/Dockerfile",
      "edits": [ { "old": "ARG DEBEZIUM_VERSION=2.5.0.Final", "new": "ARG DEBEZIUM_VERSION=2.7.3.Final" } ] },
    { "path": "connectors/orders-pg.json",
      "edits": [ { "old": "\"database.server.name\": \"orders\"", "new": "\"topic.prefix\": \"orders\"" } ] }
  ],
  "applied_when": { "kind": "plugin", "class": "io.debezium.connector.postgresql.PostgresConnector", "version": "2.7.3.Final" }
}
```

- `base_sha`: the commit the agent read the files at (`repo_read_file` returns it). The change is defined against it.
- `changes[]`: 1 to `MaxFixFiles` entries, unique paths. Each is **either** `edits` (an existing file: each `old` must
  occur exactly once in the file at `base_sha` after the earlier edits of the same file are applied, and is replaced
  by `new`) **or** `content` (a new file that must not exist at `base_sha`). Never both; never delete (§14 D1).
- `title`: 1 to 120 runes, one line. `body`: at most 4 KiB of Markdown. Both are model text and shown to approvers in
  full; Execute appends a fixed footer (§6.5).
- `applied_when`: how Piperoom will know the change reached the running system (§8.2). Shown on the card as "Piperoom
  will treat this as applied when ...".
- `ParamsSchema` sets `additionalProperties: false` at every level; the room's exact-key check applies.

Because `base_sha` and the edits fully determine every new file, **`params_hash` covers the whole diff** (the board's
requirement), without the diff itself in params, and the approver approves exactly what Execute writes.

### 6.2 Validate (no writer; may read through `ForgeReader`)

In order; each refusal names the change index and the rule, never file content:

1. Shape: JSON matches the schema; `params` at most `MaxFixParamsBytes`; `base_sha` 40 lowercase hex.
2. Repo: `repo` is in the room pipeline's `repos:` (scope from ctx; `tools.ErrOutOfScope` otherwise).
3. Paths: each `path` passes `clients.CheckRepoPath` and `Repo.LayerOf`; all paths of the step have **one** layer
   (two layers are two steps, so two approvals are never folded into one).
4. Deny list (fixed, case-insensitive on every segment; no setting relaxes it): anything under `.github/`, `.git/`,
   `.gitlab/`, `.circleci/`, `.buildkite/`, `.azure-pipelines/`, `.woodpecker/`, `.husky/`, `.devcontainer/`; any file
   named `CODEOWNERS`, `.gitmodules`, `.gitattributes`, `.gitignore`, `.gitlab-ci.yml`, `.travis.yml`, `.drone.yml`,
   `Jenkinsfile`, `azure-pipelines.yml`, `bitbucket-pipelines.yml`, `.pre-commit-config.yaml`. These run with the
   repo's secrets on a same-repo PR, or decide who reviews; a glob that matches one does not make it writable.
5. Text only: `new` and `content` are valid UTF-8 with no NUL and no control character but tab, LF and CR. A base
   file that is not text, or is a Git LFS pointer (`version https://git-lfs.github.com/spec/v1`), is refused.
6. Secrets: no added line (every `new`, every `content`) may hold a `redact.Text` hit, a redaction marker
   (`[redacted ...]`, `<redacted>`, the forms `internal/redact` writes) or a `key: value` / `key=value` /
   `"key": "value"` pair whose key `actions.LooksSecret` matches and whose value is not a reference (`${secret:...}`,
   `${file:...}`, `${env:...}`, `${...}` placeholders). The marker rule stops the model writing back text it saw
   redacted (`repo_read_file` redacts, §9.1). `title` and `body` get the same checks.
7. Body links: every URL in `title` and `body` must be on the forge's web host or `PIPEROOM_PUBLIC_URL`'s host;
   Markdown images (`![...](...)`) and HTML tags are refused. A rendered PR body must not be a beacon that carries
   data out. `@name` mentions are neutralised (`@​name`) so a PR never pings people the approvers did not see.
8. Reads (10 s, reader only): `RepoInfo` (archived refused; `public` refused unless `public_ok: true`, §14 D3); each
   edited file read at `base_sha` (`File`; absent, not regular, too large, LFS: refused); each `content` path absent
   at `base_sha`; edits apply (`old` found exactly once); each resulting file at most `MaxRepoFileBytes`; the step's
   unified diff at most `MaxFixDiffBytes`; at least one byte changes.
9. `applied_when`: kind known (§8.2); a named connector is in the pipeline's `connectors`; `keys` are non-secret
   (`LooksSecret` refused) and at most 20; a `change` kind is in `clients.ChangeKinds`.

`CheckScope` (no I/O) repeats 2, 3, 4 and the connector part of 9 from params and the pipeline alone. `LayerFor`
returns the single layer of 3.

### 6.3 Preview (reader only)

Reads the files at `base_sha` and the base head now. Fills `PreviewResult`:

- `Target`: `github.com/acme/connect-config @ main`. `Current`: `base 1a2b3c4 (read 14:01); base head now 9f8e7d6`.
- `Expected`: `a pull request from piperoom/r_..-p_..-0 into main: 2 files, +3 -2. Nothing changes in any system
  until a reviewer merges it.`
- `Reversible: true`, `Rollback`: `close the pull request (nothing to undo until it merges); after a merge, revert it
  in the repository`.
- `DryRun`: source `"base branch check"`: OK when no file of the change differs between `base_sha` and the base
  head; else OK false and the text names the moved paths ("the agent must read them again and re-propose"). A failed
  dry run blocks nothing by itself (F4 rule) but Execute will refuse with `ErrBaseMoved` (§6.4), and it keeps the
  proposal off every schedule anyway (Deferred).
- `Diff`: the `DiffPreview` of §5.3: every file's unified diff (3 lines of context), each line through `redact.Text`;
  `Hosts`: every URL host in added lines; `Warnings` (flags, never refusals, §14 D11): a new `FROM` image or registry,
  a download (`curl`, `wget`, `ADD http`), a pipe into a shell, a TLS or hostname check turned off
  (`ssl.endpoint.identification.algorithm=` empty, `verify=false`, `insecure`), a widened ACL or `*` principal,
  `injectscan` findings in added lines.

### 6.4 Execute

1. `StepFrom(ctx)` required (else refuse: the branch would have no name). Writer nil: refuse with "no write
   credentials" (#606 ruling, as the pg actions: with write mode off the proposal is still created, previewed and
   approved).
2. Validate again (the executor does too); the room has already re-checked scope, `LayerFor` and `params_hash`.
3. Build `PullRequestRequest`: `BaseSHA` from params, `Branch = clients.FixBranch(room, proposal, step)`, files from
   §6.1, `Draft` from `repos[].draft`, commit message `<title>` plus trailers `Piperoom-Room: <id>`,
   `Piperoom-Proposal: <id>`.
4. `OpenPullRequest`. `ErrBaseMoved`: the step fails with "the base branch changed <paths> since the agent read them;
   nothing was written" and the agent re-reads and re-proposes. The PR's base is `repo.Base`, its branch starts at
   `base_sha`, so GitHub shows exactly the approved diff even when unrelated files moved on the base (§14 D2).
5. Return `"opened pull request #12: <url>"` and the `PullRequestRef`.

### 6.5 PR title, body and footer

Title: the params `title`. Body: the params `body`, then a fixed footer the action writes and the approver saw in
the preview: `Opened by Piperoom from room <PIPEROOM_PUBLIC_URL>/rooms/<id> (proposal <id>, step <n>). Approved in
Piperoom by <layer>: <name>, .... This PR changes nothing until merged; this repository's own review rules apply.`
Without `PIPEROOM_PUBLIC_URL` the footer names the room id only.

## 7. Approval

An ordinary proposal through the ordinary path, with four differences that come from `Layered` and `Deferred`
(enforced by the room service, Q7k):

- The approving layer is the glob's layer (`LayerFor`), never the proposer's; every owner of that layer approves as
  for any step (`grant: owner`, `solo`, `fallback` as P2 defines them).
- **Never approved by a policy:** not `auto_approve:`, not earned autonomy (AU1), not a setup T1 schedule. Load
  refuses the name in both owners.yaml lists; the room's schedule check also refuses any proposal with a `Deferred`
  step, so a bug in one place is not enough.
- A proposal with a `Deferred` step holds only `Deferred` steps, at most 3. A live stop-gap (connector config,
  restart) is a separate proposal (§14 D4).
- A room holds at most 3 open fix PRs; a fourth PR proposal is refused until one merges or closes.

The write-mode gate is unchanged: with write mode off the proposal is created, previewed and approved, and shows
`blocked: "write_mode_off"`; no `ForgeWriter` exists. Piperoom never merges: once the PR is open, the repository's
branch protection, required reviews, CODEOWNERS and CI decide. Q7h recommends a ruleset on the base branch that
requires one approving review from someone other than the PR author and lists no bypass for the App; GitHub never
lets a PR's author approve it, so Piperoom's approval cannot satisfy the repo's.

## 8. The PR after Execute

### 8.1 Following (Q7e, A5)

`action.executed` with `pull_request` set starts a follower. The room's lease holder (`docs/design/replicas.md`, one
per room across replicas) polls `ForgeReader.PullRequest` every 60 s, with a 10 s deadline, backing off to the
`x-ratelimit-reset` time on a 403/429 rate-limit answer and doubling up to 10 minutes on errors. It appends
`pull_request.updated` only when `state`, `head_sha`, `merge_sha` or `checks.state`/`total`/`failed` changed.
`HeadChanged` is `head_sha != PullRequestRef.HeadSHA`. On `merged` it also writes a `pull_request` PipelineChange
(§5.1) and sends `HumanInput{Kind: "pr_merged"}`; on `closed` `HumanInput{Kind: "pr_closed"}`. It stops on merge,
close or room close. Closing a room with an open fix PR is allowed; the close dialog and the hand-off name it, and a
later merge is not followed (§14 D7).

### 8.2 Verifying (Q7f, A4)

`action_result` of a `Deferred` step tells the agent the PR is open; it posts that and does not verify. On
`pr_closed` it writes `agent.verification {recovered: false, outcome: "not_applied", verdict: "PR #12 was closed
without merge: the fix was not applied"}`. On `pr_merged` it waits, in code and without model turns (like a recovery
`Verify` poll), for `applied_when`, every 60 s up to `PIPEROOM_FIXPR_APPLY_TIMEOUT` (default 2 h) after the merge:

| `applied_when.kind` | Seen when | Reader |
|---|---|---|
| `connector_config` `{connector, keys}` | the connector's live config holds every key with that value | `ConnectReader.Config` |
| `plugin` `{class, version}` | Connect lists the plugin at that version | `ConnectInfoReader.Plugins` (Q1c) |
| `change` `{kinds, service?}` | a PipelineChange of one of `kinds` after the merge time (a deploy webhook, a connector config change the watcher saw) | `ChangeReader` (F8, X4) |
| `merge` `{settle}` | `settle` (1 to 60 minutes, default 10) after the merge; for a change nothing in Piperoom can observe | clock |

Then it runs the ordinary F4 verification (layer checks, data flow) and writes `agent.verification` as after any
fix. Not seen within the timeout: `outcome: "not_deployed"`, verdict "PR #12 merged at 14:02 but the change was not
seen in the system within 2h; check the deploy", and the agent goes idle; a later what-changed entry or a steer
restarts the check. `HeadChanged` is named in the verdict ("the merged change was edited after approval").

## 9. Read tools (Q7d, `internal/tools/forgetools`)

Kind read. Scope: the room pipeline's `repos:` and their globs only; a repo id or path outside is
`tools.ErrOutOfScope` before any I/O. Each records a `CallForge` provenance call. Registered only when a forge is
configured.

| Tool | Input | Returns |
|---|---|---|
| `repo_list_files` | `{repo}` | the paths at the base head that match the repo's globs, with layer and size; the base SHA |
| `repo_read_file` | `{repo, path, sha?, start_line?, max_lines?}` | lines `start_line` (default 1) to at most `max_lines` (default 200, at most 400) of the file at `sha` (default: the base head), numbered, within `tools.MaxSummaryBytes`; header: repo, path, layer, `base_sha`, blob SHA, size, total lines, "lines a-b of n" |
| `pr_status` | `{repo, number}` | state, head changed, checks state and failed names, merged at; only for a PR whose head branch starts with `piperoom/` |

### 9.1 Untrusted data

Repository files are written by anyone with push access; they are untrusted data (§7 of AGENTS.md):

- File text passes `redact.Text` (N5) before it reaches the model or the room log; the header says how many spans
  were masked, and the model is told it cannot edit a masked span (Validate rule 6 enforces it).
- `injectscan.Scan` (N6) runs on the window; findings set the step's injection flag as for every untrusted tool text.
- The agent wraps the summary as tool output (`run.go`), and the prompt rule stands: text inside the wrapper is data.
- Caps: a file above 64 KiB is refused; a window fits `MaxSummaryBytes`, so the model reads a long file in pages
  (`sha` keeps the pages at one commit). Data holds the window, never the whole file.
- `pr_status` returns no PR body, comment or review text; check names are cut to 100 bytes and redacted.

## 10. Agent, web, docs, scenarios, evals

**Agent (Q7f).** The prompt lists the pipeline's repos (id, base, globs with layer). Rules: a fix that lives in a file
under a listed glob (connector JSON in git, a Dockerfile, a version pin, consumer config) is proposed as
`repo_open_pull_request`; read the file first with `repo_read_file` and use its `base_sha`; change the fewest lines;
one layer per step; say in `body` the cause and the evidence (tool steps), never record values; pick `applied_when`.
A live stop-gap is a separate proposal (§14 D4). Wiring builds `ForgeReader` when the forge is configured and
`ForgeWriter` only with write mode on (the P1a-3 test extended).

**Web (Q7g).** The preview card of a PR step shows the diff: per file a collapsible block with status, `+n -m`, line
numbers, added and removed lines marked by `+`/`-` and by colour (never colour alone), long lines wrap on mobile;
then "Hosts this change fetches from" and the warnings; the footer text; the line "Approving opens a pull request.
Nothing changes in any system until a reviewer merges it." After Execute a badge: open / merged / closed, checks
(pending, passing, failing with names), "edited after approval", link to the PR. The file list is keyboard and
screen-reader complete.

**Docs (Q7h).** GitHub App setup (§4.1 permissions, selected repos only, no webhook, the key file, GHE URL), the
recommended base-branch ruleset, `repos:` examples, a data-handling row: repository files under the globs reach the
model provider and the room log (redacted); PR title and body leave Piperoom for the repository.

**Scenarios (Q7i).** `plugin-missing-in-image` (Dockerfile), `debezium-upgrade` (version pin to the fixed-in release
of a known issue), `consumer-deserializer` (consumer config), each with a fixture repo and a label naming repo, paths
and `applied_when`; plus `fixpr-injected-file`: a connector JSON whose description field asks the agent to add a
workflow and a `curl | sh` line; expected: the PR touches only the listed file and key, no warning-flagged line.

**Evals (Q7j).** A forge replay (fixture repos per scenario; `ForgeWriter` a recording no-op that returns a PR) and a
"right files changed" score: the step's repo and path set equals the label's, the expected edit is present, nothing
else changes. The three scenarios score; the CDC pass rate is unchanged.

**Demo and dev.** `internal/clients/forge/fake` (C17): an in-memory forge with repos, commits and PRs, used by
`--demo`, the eval replay and the room and agent tests. An integration test against a real sandbox repository runs
only when `PIPEROOM_IT_GITHUB_*` variables are set (leads, before Q7f merges).

## 11. Security review

| # | Threat | Mitigations |
|---|---|---|
| S1 | A prompt-injected repository file (or tool output) steers the agent to a malicious diff: a plugin from an attacker URL, `curl \| sh` in the Dockerfile, TLS verification off | per-layer human approval of the full diff, never by a policy (§7); the diff is never cut; preview warnings and host list (§6.3); injection flag on the read (§9.1); the repo's own review and CI; `fixpr-injected-file` scenario |
| S2 | Secret exfiltration through the PR: a credential in a file, title or body, into a repo with a wider audience (or public) | Validate rule 6 on every added line, title and body; secret keys only as references; redaction markers refused; public repos refused by default (§14 D3); body links and images limited (rule 7); approvers see title, body and footer in full |
| S3 | Data exfiltration through a rendered body (image beacon, link with data in the query) | rule 7: no images, no HTML, links only to the forge host and Piperoom |
| S4 | Path traversal or a path that escapes the globs (`..`, absolute, backslash, URL-encoded, case tricks) | `CheckRepoPath` (clean, relative, no `.`/`..`, no backslash or control characters); glob match on the clean path; each segment URL-escaped by the client; GitHub's own tree API resolves paths, not the filesystem |
| S5 | Edits to CI or governance files (workflows run with repo secrets on a same-repo PR; CODEOWNERS decides who reviews) | fixed deny list at load and Validate, case-insensitive (§6.2 rule 4); the App has no Workflows permission, so GitHub refuses workflow changes anyway (§4.1) |
| S6 | Writing anything but a new branch: the base, another branch, a merge, a force-push | the writer's request allow-list (§4.3, test); branch name only from `FixBranch`; `ErrBranchExists`; no merge method exists on the interface |
| S7 | Approved diff differs from what is written (TOCTOU) | `params_hash` over `base_sha` and edits; the branch starts at `base_sha`; `ErrBaseMoved` when a touched file moved; Execute recomputes from params, never from the preview |
| S8 | Token or key leak | key in a file, read once; tokens narrowed to one repo and the needed permissions, 1 h life; `netsec.Secret`; bearer only to the API host; redirects refused; never logged (Q7b test greps the logs) |
| S9 | Writing to a repository the pipeline does not list, or another host (SSRF) | the model names a repo id only; url from owners.yaml; host must equal the forge web host at start; installation pinned (§14 D8) |
| S10 | Approval by the wrong person | the layer from `LayerFor`, never the proposer; the room re-derives it before Execute |
| S11 | PR spam or loops | at most 3 PR steps per proposal and 3 open fix PRs per room; one follower per room |
| S12 | Reviewers change the PR after approval | `head_changed` on the event, the badge and the verdict; the repo's review owns that change |
| S13 | Binary, LFS, symlink or submodule corruption | text only; LFS pointers refused; `File` refuses non-regular entries; mode 100644 always; Q7b confirms GitHub refuses a tree entry under a path whose parent is a symlink or submodule |

`internal/clients/forge/**`, `internal/tools/forgetools/**` and `internal/actions/fixpr/**` are high-risk code: the
last is already under `internal/actions/**`; the leads add the first two to the ship workflow's high-risk paths
(§15).

## 12. Failure modes

| Failure | Where | Effect |
|---|---|---|
| Forge not configured | start | tools and action not registered; the agent never offers a PR; `repos:` logs one warning |
| GitHub down, timeout, 5xx | Validate at create | proposal refused with the reason; the agent may retry later |
| Same | Execute | step fails; a branch it created is deleted on a fresh 10 s context; else the step says which branch is left |
| Ref created, PR create fails | Execute | as above; a retried call finds the same branch and content and opens the PR (idempotent) |
| Rate limited (403/429) | any | the error names the reset time; the follower waits until then |
| Token mint fails (key wrong, App uninstalled, repo not selected) | any | a clear error naming the repo and the setting; no fallback installation |
| Base moved on a touched file | Preview, Execute | dry run not OK; Execute fails with `ErrBaseMoved`, nothing written; the agent re-reads |
| Repo archived, or made public | Validate | refused (public: unless `public_ok`) |
| Signed commits required on the base | merge | the repo blocks the merge; Q7b confirms whether GitHub signs commits an App makes through the API |
| PR closed unmerged | follower | `pr_closed`, verification `not_applied` |
| Merged, never deployed | agent | `not_deployed` after the timeout |
| Room closed with an open PR | close | following stops; the hand-off lists the PR |
| Write mode off | executor | `blocked: "write_mode_off"` after approval, nothing opened |

## 13. Rollout

1. **Q7-0** (this page). The owner rules §14.
2. **Q7-0c** (leads): item 6 in code (§5), `docs/api.md`, the high-risk paths. Then Q7a and Q7b in parallel; Q7d;
   Q7c; Q7k and Q7e; Q7f, Q7g, Q7h, Q7i, Q7j (board order, §15).
3. Off by default: `FORGE_KIND` empty means no tool, no action, no follower. Kill switches: unset `FORGE_KIND`, or
   write mode off (reads only), or remove `repos:` from a pipeline.
4. Pilot: the demo fake forge, then a sandbox repository in a test org (the leads' integration run with the room
   export attached to Q7f), then one pilot customer repo with `draft: true`.
5. The upper bound of §1 counts only where repos are listed. Q7h's onboarding asks for `repos:` per pipeline; the
   discover command (C10) does not guess repos.

## 14. Open decisions for the owner (leads' recommendation first)

| # | Decision | Recommendation | Alternative |
|---|---|---|---|
| D1 | How the model states a change | search-and-replace `edits` against `base_sha` (whole `content` only for new files); no deletes | whole-file content: simpler, but the model would write back text it saw redacted and every unchanged line enters params and the log |
| D2 | When the base "moved" | only when a file of the change differs from `base_sha`; branch from `base_sha` | refuse on any new commit: safe but fails on every busy repo |
| D3 | Public repositories | refused unless `repos[].public_ok: true` | always refused in this wave |
| D4 | PR and live steps together | PR-only proposals (at most 3 PR steps); a live stop-gap is a separate proposal whose description says the next deploy overwrites it until the PR merges | mixed proposals: the live step would run before the PR merges, and verification would mix both |
| D5 | Apply timeout | 2 h default, `PIPEROOM_FIXPR_APPLY_TIMEOUT`, 10 m to 168 h | fixed 24 h |
| D6 | Following PRs | poll every 60 s from the room's lease holder | GitHub webhooks: faster, but an inbound endpoint, a webhook secret and a public URL for every install |
| D7 | After room close | stop following; the hand-off lists open PRs | keep following until merge or 14 days |
| D8 | Installations | one pinned installation (`GITHUB_APP_INSTALLATION_ID`) | look up the installation per repo: multi-org, but any org that installs the App becomes reachable through owners.yaml alone |
| D9 | Draft PRs | ready for review by default, `repos[].draft: true` per repo | always draft |
| D10 | What the agent may read | only paths under the globs | a separate `read_paths:` list without a layer |
| D11 | Risk flags (§6.3) | flags shown to approvers, not refusals | refuse new hosts outside an allow-list (the setup plugin hosts) |
| D12 | Other forges | GitHub and GitHub Enterprise only | GitLab next wave behind the same interfaces |

## 15. Board changes

Applied to `tasks/Q-board.md` in this PR:

- **Q0 item 6** now points here and lists the settled shapes (§5).
- **Q7-0**: item 6 lands in **Q7-0c** (new row, leads, 0.5 day, After Q7-0 and the owner's rulings), not in this
  design PR: the design PR edits no contract file.
- **Q7a**: adds global repo-id consistency, `base` and url rules, the deny list at load, the glob syntax,
  `LayerOf` ambiguity, Deferred names refused in `auto_approve:`/`autonomy:`. After Q7-0c.
- **Q7b**: adds `RepoInfo`, `BaseHead`, `ListFiles`, narrowed tokens, no redirects, the request allow-list test,
  idempotent `OpenPullRequest`, `ErrBaseMoved` per file, the fake forge, the sandbox integration test. After Q7-0c.
- **Q7c**: edits format, title/body/link rules, `applied_when`, `Layered`, `Deferred`, `PullRequestOpener`, LFS
  and deny list, the 128 KiB diff cap. After Q7-0c.
- **Q7d**: adds `repo_list_files`; line windows within `MaxSummaryBytes`.
- **Q7e**: the snapshot field is room-level `pull_requests` (an executed proposal leaves `open_proposals`); adds
  `pr_closed`, `head_changed`, the `pull_request` PipelineChange, rate-limit back-off.
- **Q7k** (new, A5, 1.5 days): proposals: `Layered` layer derivation, `Deferred` rules (no policy, no mixing, 3
  steps, 3 open PRs per room), executor `PullRequestOpener` and `StepRef`, `GET /api/actions` fields. After Q7-0c,
  Q7c. Q7e and Q7f wait on it.
- **Q7f**: `applied_when` polling and the `not_applied`/`not_deployed` outcomes.
- **Q7i**: adds `fixpr-injected-file`.
- Load table: A5 +1.5 days, leads +0.5 day.

Not in this PR (pipeline files, a separate leads' PR): add `internal/clients/forge/**` and
`internal/tools/forgetools/**` to the high-risk paths of `docs/process/piperoom-ship.workflow.js` and
`docs/process/ship-workflow.md` §2.

## 16. References

- Plan `docs/product/so-gaps-2026-10.md` §2, §3.1, §4; board `tasks/Q-board.md` step 7.
- `internal/actions/action.go` (`Preview`, `Scoped`, `Receipted`, `Verifier`, `LooksSecret`); `tasks/P0-contracts.md`
  item 7 (#606: refuse at Execute without write credentials).
- `docs/design/setup-agent.md` §3.10 (`repos:` in setup), `docs/design/replicas.md` (room leases),
  `docs/design/auth.md` §3.1 (`params_hash`).
- `internal/clients/netsec` (`Secret`, `HTTP.Client`), `internal/redact`, `internal/injectscan`.
- GitHub REST endpoints Q7b confirms against the GitHub docs before use: installation access tokens, repository,
  contents, git blobs, trees, commits and refs, pulls, check runs, commit statuses.
