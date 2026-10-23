# Fix pull requests: the GitHub App

For the engineer who lets Piperoom end an incident with a **pull request** instead of a live change: the fix that
lives in a file (a connector JSON deployed by CI, the Connect image's `Dockerfile`, a version pin, a consumer's
config). Piperoom opens a new branch and a pull request from it, and nothing else. A person reviews and merges it
under your repository's own rules.

Written against the design `docs/design/fix-pr.md` (Q7-0, Jira KAN-320) on the integration branch at `e61d90ec`.
Each claim names the design section or the vendor page it comes from. If the design or the code changes, update
the claim and its citation in the same PR. Where this page and merged code differ, the code wins (design header).

## Status on this release: partly built, not wired

| Piece | State | Where |
|---|---|---|
| GitHub client, App tokens (Q7b) | **built, not wired**: `internal/clients/forge`; no server builds it until Q7-0c's settings and the wiring row land | `tasks/Q-board.md` row Q7b |
| Tools `repo_list_files`, `repo_read_file`, `pr_status` (Q7d) | **built, not registered** (`internal/tools/forgetools`) | Q-board row Q7d |
| Action `repo_open_pull_request` (Q7c) | **not in the repo** (`internal/actions/fixpr`) | Q-board row Q7c |
| `owners.yaml` `repos:` (Q7a) | **in the loader** (`internal/owners/repos.go`): a `repos:` block loads and is checked; nothing reads it until the forge is wired | Q-board row Q7a; design §3, §5.8 |
| `FORGE_KIND`, `GITHUB_API_URL`, `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY_FILE`, `PIPEROOM_FIXPR_APPLY_TIMEOUT` | **proposed only**: the contract PR Q7-0c adds them to `internal/config/config.go`; a server on this commit does not read them | design §5.6; `grep GITHUB internal/config/config.go` finds nothing |

Until the settings and wiring land, set none of the settings below on a pilot server; a `repos:` block loads but has no effect yet.
Create the App and the ruleset now, so the PR fix can be turned on with one restart. The names below are the
design's; if Q7-0c renames one, its PR updates this page.

## 1. What Piperoom will and will never do

**Will** (design §4.3, §6.4):

- create blobs, one tree and one commit whose parent is the commit the agent read the files at (`base_sha`);
- create one **new** branch, `piperoom/<room>-<proposal>-<step>`;
- open one pull request from that branch into the repo's `base` branch, as a draft when `draft: true`;
- read files under the listed globs, the base branch head, the PR's state and its checks and statuses;
- delete its own new branch, and only that, when the PR create fails after the branch was made (design §12).

**Will never** (design §2 "Non-goals", §4.3):

- merge, approve, review or comment on a pull request; request reviewers; add labels;
- push to, update, force-push or delete any existing branch, the base branch included;
- change repository settings, branch protection, rulesets, secrets, Actions or webhooks;
- touch CI or governance files, whatever the globs say: anything under `.github/`, `.git/`, `.gitlab/`,
  `.circleci/`, `.buildkite/`, `.azure-pipelines/`, `.woodpecker/`, `.husky/`, `.devcontainer/`, and files named
  `CODEOWNERS`, `.gitmodules`, `.gitattributes`, `.gitignore`, `.gitlab-ci.yml`, `.travis.yml`, `.drone.yml`,
  `Jenkinsfile`, `azure-pipelines.yml`, `bitbucket-pipelines.yml`, `.pre-commit-config.yaml` (design §6.2 rule 4;
  refused at load and at Validate, case-insensitive, and no setting relaxes it);
- delete or rename files, or write binary files, Git LFS objects, submodules, symlinks or executable bits (design
  §2, §6.2 rule 5);
- build or deploy the merged change: your CD does that; Piperoom only observes it (design §8.2).

A PR proposal is approved by every owner of the layer its paths map to, never by `auto_approve:`, earned autonomy or
a setup schedule (design §7). With `PIPEROOM_WRITE_MODE` off the proposal is still made, previewed and approved, then
shows `blocked: "write_mode_off"` and nothing is opened (design §7, §12).

## 2. Create the GitHub App

Create the App under the **organisation** that owns the repositories (Settings, Developer settings, GitHub Apps,
New GitHub App). GitHub's steps: [Registering a GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app).

| Field | Value | Why |
|---|---|---|
| Name | for example `piperoom-<your-org>` | it is the author shown on every PR |
| Homepage URL | your Piperoom URL (`PIPEROOM_PUBLIC_URL`) or any internal page | required by the form |
| Callback URL, "Request user authorization (OAuth) during installation", Device flow | leave empty / off | Piperoom acts as the App installation, never as a user (design §4.2) |
| Webhook | **Active: off**; no URL, no secret, no events | Piperoom polls the PR every 60 s and needs no inbound endpoint (design §4.1, §8.1, §14 D6) |
| Where can this GitHub App be installed? | **Only on this account** | one pinned installation per Piperoom (design §4.2, §14 D8) |

### 2.1 Permissions (least privilege)

Repository permissions only (design §4.1). Leave every other repository permission at **No access** and every
organisation and account permission at **No access**.

| Repository permission | Level | Why |
|---|---|---|
| Metadata | Read-only | mandatory for every App (GitHub sets it) |
| Contents | Read and write | read files; create blobs, a tree, a commit and the new branch ref |
| Pull requests | Read and write | open the PR; read its state |
| Checks | Read-only | check runs on the PR head |
| Commit statuses | Read-only | legacy statuses on the PR head |

**Never grant** (design §4.1):

- **Workflows.** Without it GitHub itself refuses a commit that creates or changes a file under
  `.github/workflows/`. That is a second wall behind Piperoom's own deny list: a bug or a prompt-injected diff that
  slipped past Validate still cannot change a workflow, which would run with the repository's secrets
  (design §6.2 rule 4, §11 S5).
- Administration, Actions, Secrets, Environments, Deployments, Issues, and any organisation or account permission.

With write mode off you may install a **read-only** App instead (Contents, Pull requests, Checks, Commit statuses:
Read-only). The read tools then work, and the writer is never built (design §4.1).

On top of the App's permissions, every installation token Piperoom mints is narrowed to the **one repository** of
the call and to the permissions that call needs: the reader asks for `contents`, `pull_requests`, `checks` and
`statuses` read, the writer for `contents` and `pull_requests` write (design §4.2). Tokens live one hour and are
refreshed 5 minutes before they expire (design §4.2, §11 S8). The token request is GitHub's
[create an installation access token](https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app);
the design leaves confirming its body fields to Q7b (design §4.2).

GitHub's own reference of what each permission allows:
[Permissions required for GitHub Apps](https://docs.github.com/en/rest/authentication/permissions-required-for-github-apps).

### 2.2 Install it on selected repositories only

Install the App on your organisation and choose **Only select repositories**: exactly the repositories the
pipelines list in `repos:` (design §4.1). Steps:
[Installing your own GitHub App](https://docs.github.com/en/apps/using-github-apps/installing-your-own-github-app).

A repository listed in `owners.yaml` that the installation cannot see is a clear refusal naming the repo ("the
GitHub App is not installed on acme/connect-config"); Piperoom never looks up another installation (design §4.2,
§12). Add a repository to the installation before you add it to `repos:`.

### 2.3 The ids

- **App ID**: on the App's settings page (General, "App ID"). It goes in `GITHUB_APP_ID` (design §5.6).
- **Installation ID**: the number at the end of the installation's settings URL,
  `https://github.com/organizations/<org>/settings/installations/<installation id>`. It goes in
  `GITHUB_APP_INSTALLATION_ID` (design §5.6). Both must be positive integers, or the start fails (design §5.6,
  `ParseForge`).

### 2.4 The private key

Generate a private key on the App's settings page (Private keys, Generate a private key). GitHub downloads a PEM
file and keeps only its public half:
[Managing private keys for GitHub Apps](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps).

- Keep the PEM in your **secret store** (Vault, a Kubernetes Secret, a Docker secret, your cloud's secret manager)
  and mount it into the Piperoom host or container as a **file**. Point `GITHUB_APP_PRIVATE_KEY_FILE` at that file.
  The key is never an environment value (design §4.2, §5.6).
- Make the file readable by the Piperoom user only (`chmod 600`). On Unix a key file readable by group or others
  logs a warning at start (design §4.2).
- **Never put the key, the App id or a token in `owners.yaml`.** The `repos:` schema has no field for them
  (design §5.8): `owners.yaml` names repositories, never credentials. The forge settings are process-wide, one
  installation per Piperoom (design §14 D8).
- Piperoom reads the key once at start into memory. The key, the App JWT and every token print as `[redacted]`,
  are never logged, never in an error text or an event, and are sent only to the configured API host (design §4.2).
  The wiring refuses a key that is not a PEM RSA key (design §5.6).
- Rotate: generate a second key, swap the file, restart Piperoom, then delete the old key on GitHub. Revoke
  quickly: delete the key, or suspend or uninstall the App. Every token mint then fails with an error naming the
  repo and the setting (design §12).

## 3. Settings (proposed)

Proposed for `internal/config/config.go` by design §5.6 (contract PR Q7-0c). Not read on this commit.

| Variable (proposed) | Value | Notes |
|---|---|---|
| `FORGE_KIND` | `github` | empty (the default) turns the whole PR fix off: no tool, no action, no follower (design §13). Any `GITHUB_APP_*` set while `FORGE_KIND` is empty stops the start ("set FORGE_KIND=github") (design §5.6) |
| `GITHUB_API_URL` | default `https://api.github.com`; GitHub Enterprise Server: `https://<host>/api/v3` | https unless the host is loopback; no user info, query or fragment (design §5.6) |
| `GITHUB_APP_ID` | the App ID | positive integer |
| `GITHUB_APP_INSTALLATION_ID` | the installation ID | positive integer; pinned (design §14 D8) |
| `GITHUB_APP_PRIVATE_KEY_FILE` | path to the PEM file | a secret file; never the key itself |
| `PIPEROOM_FIXPR_APPLY_TIMEOUT` | default `2h`; `10m` to `168h` | how long after a merge the agent waits to see the change in the running system before it reports `not_deployed` (design §5.6, §8.2) |

Kill switches: unset `FORGE_KIND`; or turn write mode off (reads only); or remove `repos:` from a pipeline
(design §13).

Network: the Piperoom host needs outbound HTTPS to the API host only (`api.github.com`, or your GitHub Enterprise
Server). The HTTP client refuses every redirect and caps every body at 4 MB (design §4.2).

## 4. `owners.yaml`: `repos:` per pipeline

Proposed schema (design §3; the loader is Q7a). Each pipeline lists the repositories a fix may change, and each
path glob maps to **one** layer, whose owners approve a change under it.

```yaml
pipelines:
  - id: orders
    repos:
      - id: connect-config                 # slug, ^[A-Za-z0-9_.-]+$
        url: https://github.com/acme/connect-config
        base: main                         # the branch PRs target; required
        draft: true                        # optional: open PRs as drafts (recommended for the first pilot)
        public_ok: false                   # optional: a public repo is refused unless true
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

Load rules (design §3):

- `url` is `https://<host>/<owner>/<name>`: no user info, query, fragment, `.git` suffix or extra path. At start
  every `url` host must equal the forge's web host (`github.com` for `api.github.com`, else the API URL's host), or
  the server refuses to start (design §5.6).
- `base` is a valid branch name and does not start with `piperoom/`.
- The same `id` in two pipelines must have the same `url` and `base`.
- `paths` is not empty; each `layer` is one of the pipeline's layers. A glob under a denied directory or equal to a
  denied file name (section 1) is refused.
- Glob syntax: Go `path.Match` on the whole path (`*` does not cross `/`), plus `**` as a whole segment matching
  zero or more segments. A path matched by globs of two different layers is refused as ambiguous, never
  first-match.
- The agent may read only paths under the globs (design §14 D10). Keep the globs to the files a fix should change.
- `repos:` while `FORGE_KIND` is empty loads, with one warning at start (design §3 rule 7).

The discover command does not guess repositories; list them by hand per pipeline (design §13 step 5).

## 5. The recommended base-branch ruleset

Piperoom's approval is not the repository's approval. Once the PR is open, your branch protection, required
reviews, CODEOWNERS and CI decide (design §7). Make that explicit with a ruleset on each listed repository's `base`
branch ([About rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets);
[Available rules for rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)):

| Rule | Setting | Why |
|---|---|---|
| Target | the `base` branch named in `repos:` (for example `main`) | the branch Piperoom's PRs merge into |
| Require a pull request before merging | on, **1** required approval (or your usual number) | a human from your team approves the change in GitHub too; GitHub never lets a PR's author approve it, so the App's PR cannot satisfy this itself (design §7) |
| Dismiss stale approvals when new commits are pushed | on | a commit added after review needs a new review; Piperoom also flags `head_changed` in the room (design §8.1, §11 S12) |
| Require review from Code Owners | on, if you use CODEOWNERS | Piperoom can never edit `CODEOWNERS` (section 1) |
| Require status checks to pass | your CI checks | the room shows the same checks on its PR badge (design §8.1) |
| Block force pushes; restrict deletions | on | Piperoom never force-pushes or deletes the base; this keeps anyone else from doing it either |
| Bypass list | **empty for the App**: do not add the Piperoom App, or any role it could use, to the bypass list | a bypass would let the App's own token merge or push past review (design §7) |

The App's Contents write permission is what lets it create its new branch. The ruleset on the base branch is what
keeps that permission from reaching `main`. If signed commits are required on the base, check that GitHub accepts
the App's API commits before the pilot: the design leaves this open for Q7b to confirm (design §12).

## 6. GitHub Enterprise Server

- Set `GITHUB_API_URL=https://<your-ghes-host>/api/v3`. The web host Piperoom compares with each `repos[].url`
  host is then the API URL's host, so `url:` entries use `https://<your-ghes-host>/<owner>/<name>` (design §5.6).
- Create and install the App on your GitHub Enterprise Server instance with the same permissions (section 2).
  The menu paths and the permission names may differ by GHES version; check your version's
  [GitHub Enterprise Server docs](https://docs.github.com/en/enterprise-server@latest/apps/creating-github-apps/registering-a-github-app/registering-a-github-app).
  Rulesets need a GHES version that has them; otherwise use classic branch protection with the same rules.
- A private CA for the GHES host is handled by the client's TLS settings (`netsec.HTTP{TLS: ...}`, design §4.2);
  how the CA file is named in config is not settled in the design (it names no `GITHUB_*` CA variable).
- One installation per Piperoom applies on GHES too: repositories on `github.com` and on a GHES host cannot be
  served by one Piperoom (design §5.6: a repo on another host stops the start).

## 7. What leaves Piperoom and what reaches the model

The data-handling rows are in [data-handling.md](data-handling.md) (sections 1 and 2). In short (design §6.5,
§9.1, §10):

- **Repository files under the globs reach the model provider and the room log**, after secret redaction
  (`redact.Text`) and an injection scan, wrapped as untrusted data, in windows of at most 400 lines; a file above
  64 KiB is refused. Files outside the globs are never read.
- **The PR title, body and diff leave Piperoom for the repository**, where everyone with read access to it sees
  them. Title and body are model text that approvers see in full, plus a fixed footer naming the room, the
  proposal and the approvers. Every added line, the title and the body are refused if they hold a secret
  (Validate rule 6). Links only to the forge host and `PIPEROOM_PUBLIC_URL`, no images, no HTML, `@` mentions
  neutralised (Validate rule 7).
- **Public repositories are refused** unless `public_ok: true` (design §14 D3): a PR there is public.

## Review checklist

- [ ] App permissions: Metadata, Contents (rw), Pull requests (rw), Checks (r), Commit statuses (r); nothing else;
  **no Workflows**.
- [ ] Webhook inactive, no events; installable only on this account.
- [ ] Installed on **selected** repositories only, the same list as `repos:`.
- [ ] Private key in the secret store, mounted as a file, mode 600; not in `owners.yaml`, not in an environment
  value, not in git.
- [ ] Ruleset on each base branch: PR required with 1 approval, stale approvals dismissed, status checks, no force
  push, **no bypass for the App**.
- [ ] `draft: true` on the first pilot repository (design §13 step 4).
- [ ] Repositories under the globs hold nothing you may not send to the model provider.
