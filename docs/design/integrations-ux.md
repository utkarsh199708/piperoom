# Integrations UX for enterprise customers (rev 1)

Companion to `docs/design/integrations.md`, which covers the server side: kinds, storage, the no-restart swap and
the API. This doc covers the product: who sets Piperoom up at an enterprise, where it hurts today, and the screens
and rules that take the pain away. Screen mockups: https://claude.ai/artifact/2FMVtuQEuNoSzozFFfmgNi (private until shared).

## 1. Why enterprise setup is a headache

At a 2,000-person company, the person who wants Piperoom is not the person who holds the keys.

| Person | Wants | Holds |
|---|---|---|
| **Platform / data engineer** (the setup owner) | Piperoom working this week | nothing they can grant themselves |
| **DBA** | no risk to the production database | the Postgres role and grants |
| **Kafka / streaming admin** | no load on the cluster | the Kafka ACLs, Connect and Registry logins |
| **Cloud admin** (AWS, GCP, Azure) | least privilege, everything in IaC | IAM roles, network paths |
| **Security / GRC** | to know what data leaves, and who approved it | the sign-off, the vendor review |
| **Slack / IT admin** | no unknown bots | the app install |
| **On-call engineers** | just to use it | nothing |

So the real cost is not filling forms. It is **chasing four people over two weeks**, with tickets that say "please
create a read-only role", followed by back-and-forth about what "read-only" means. Every design rule below aims at
that.

The five headaches, in order of cost:

1. **Chasing credential owners.** The setup owner cannot finish alone.
2. **Network.** Piperoom cannot reach a private database, and the error says "timeout", not "ask for this firewall
   rule".
3. **Security review.** Nobody can say exactly what Piperoom reads and stores.
4. **Opaque failures.** "Permission denied" without saying which grant is missing, or who can add it.
5. **Config drift.** Settings typed into a UI that the infra team's Terraform does not know about.

## 2. Design principles

1. **Start from the stack, not from a list of apps.** First question: "What does your pipeline run on?" Pick
   *AWS (RDS + MSK)*, *Confluent Cloud + Snowflake*, *Self-managed Kafka*, and so on. That picks the right cards and
   their order. Nobody browses 20 logos to guess which six they need.
2. **One cloud login discovers many apps.** One AWS role finds the RDS instances, MSK clusters, MSK Connect
   connectors and Bedrock. One Confluent Cloud API key finds the cluster, Schema Registry and managed connectors.
   **Never ask for anything Piperoom can find out.** The person confirms what was found; they do not type it.
3. **Every card can be handed off.** "Ask someone to connect this" sends the DBA a link to fill **only that card**,
   with the exact grant script for them to run. The setup owner sees live status: *waiting on Priya (DBA), sent
   2 days ago, reminded today*.
4. **Every failure says what to do and who can do it.** Not "permission denied", but: *"`piperoom_ro` cannot read
   `pg_replication_slots`. Fix: `GRANT pg_monitor TO piperoom_ro;` — [Copy] [Send to DBA]"*.
5. **Read-only by default, and proven.** Each card shows a green *Verified read-only* badge, backed by checks
   (`docs/design/integrations.md` §4.2). Turning on write fixes is a separate, later step, one layer at a time, done
   by that layer's owner.
6. **Show what we read before asking for access.** Each card lists "Piperoom will read…" and "Piperoom never
   reads…" before anything is connected. Security can review it on one page and export it as a PDF.
7. **UI and code are the same config.** Everything set in the UI can be exported as Terraform or YAML, and a
   connection created from code shows *Managed by Terraform* and is locked in the UI. Infra teams adopt tools they
   can put in git.
8. **Secrets stay in their vault.** The first option is a reference to the customer's secret manager (AWS Secrets
   Manager, HashiCorp Vault, Azure Key Vault, GCP Secret Manager). Pasting a password is the fallback, not the
   default.
9. **Try before connecting.** A sample pipeline with a scripted incident (the existing `--demo` mode) lets people see
   a room on the first click. That is what convinces the DBA to create the role.
10. **Staging first, then copy to production.** Environments are first-class, and *Copy setup from staging* reuses
    every card, asking only for the production hostnames and secrets.

## 3. The setup journey

```
Sign in with SSO ─► Pick your stack ─► Connect (do it / hand off) ─► Pick pipelines ─► Who owns what ─► First room
     1 min              1 click          parallel, per card            discovered         from SSO groups     test alert
```

Target: **first real room in under 30 minutes of the setup owner's own time**, however long the hand-offs take.

### 3.1 Sign in and workspace

- SSO (OIDC/SAML) from the first screen. SCIM later for user lists. The first person in becomes the workspace
  admin.
- Pick a deployment shape (§5): *Hosted* or *In your network*. For *In your network*, a one-line Helm or Docker
  command with a pre-filled join token.

### 3.2 Pick your stack

Big tiles, one click each. Each tile pre-selects the cards and the discovery method:

| Tile | Cards it selects | Discovery |
|---|---|---|
| AWS: RDS + MSK + MSK Connect | AWS account (1 role) → RDS, MSK, MSK Connect, Bedrock | AWS role finds them |
| Confluent Cloud | Confluent Cloud (1 API key) → Kafka, Schema Registry, connectors; Postgres source | Confluent API |
| Self-managed (Strimzi, Debezium on Connect) | Postgres, Kafka, Connect REST, Schema Registry | Connect REST lists connectors |
| GCP / Azure | (later) | |
| Something else | the full catalog | manual |

The warehouse (Snowflake, Redshift, BigQuery later, Postgres), alerts and Slack are added on every path.

### 3.3 The setup checklist (home of setup)

One page with one row per card in the chosen stack, like a payments-company onboarding checklist:

- Status per row: *Not started*, *Waiting on <person>*, *Testing*, *Connected · verified read-only*, *Needs
  attention*.
- Each row has two buttons: **Connect** (do it yourself) and **Ask someone**.
- Progress bar, and *"You can open a room once Kafka Connect and Postgres are connected"*. Cards that are not
  needed for a first room (warehouse, Slack) are marked *optional for now*.
- Rows run in parallel. The setup owner can connect Kafka while the DBA works on Postgres.

### 3.4 Connect drawer (one card)

A right-side drawer, so the checklist stays in view. Three tabs:

1. **Use a secret manager** (default when one is connected): pick the secret from a list; Piperoom reads it at run
   time.
2. **Enter credentials**: the form from the card schema. Host fields accept a pasted connection string and split it
   up. Secret fields are write-only.
3. **Ask someone**: name or email (from SSO), a message, and a preview of what they will get.

Below the form, two things always shown:

- **Setup script**: the exact SQL, CLI or IaC for the person who creates the account, filled with the tables found
  so far. [Copy] [Download] [Send].
- **What Piperoom reads**: the list from `docs/pilot/data-handling.md` for this card.

**Test connection** runs checks live, one line each, in order: *reach host → TLS → login → version → each needed
read → cannot write*. Each failing line expands to *why*, *the exact fix*, and *[Send fix to …]*. Save is allowed
when nothing is red. Yellow lines (for example "statement_timeout not set") are saved as follow-ups on the health
page.

### 3.5 The hand-off page (what the DBA sees)

- Arrives as an email or a Slack DM: *"Utkarsh asked you to connect the orders database to Piperoom (read-only).
  Takes about 5 minutes."*
- Opens with SSO. The person sees **only this card**: what it is for, what will be read, the script to run, and the
  form. They cannot see rooms or other cards.
- The link is single-use, expires in 7 days, and is scoped to one card. Its use is audited.
- When they save, the setup owner is notified and the checklist row turns green.
- If they say no, they can reply with a reason, which shows on the row.

### 3.6 Pick pipelines

Once Connect (or the cloud account) is connected, discovery runs and shows **found pipelines as cards**: connector
name, source tables, topics, consumer groups, sink tables, health right now. Tick the ones to watch. Edit only what
discovery could not know (a title, the sink table). This replaces writing `owners.yaml` by hand.

### 3.7 Who owns what

For each layer of each pipeline, pick an **SSO group** (recommended) or people. Default suggestions come from the
group names (`dba-oncall` → postgres). Approvers come from the same picker. This writes the `owners:` blocks from
`docs/design/auth.md` §2.3.

### 3.8 First room

**Send a test alert** opens a room on the chosen pipeline, and the agent runs its read-only checks. That proves
every connection end to end. The last screen shows the webhook snippet for their alert tool.

## 4. After setup: running it for months

Enterprise pain does not end at setup. The **Integrations health** page covers day 2:

- One row per connection: status, last successful check, *verified read-only* as of when, who connected it, and the
  source (UI, Terraform, environment).
- Background re-test every 6 hours. A connection that starts failing posts in the admin channel and the room
  timeline before an incident needs it.
- **Expiring credentials:** API keys and certificates with an expiry date are warned about 14 days ahead, with a
  *Rotate* button that re-runs the hand-off flow to the same owner.
- **Drift:** if a check finds new write grants on a read-only role, the card turns yellow: "This role can now
  INSERT into public.orders".
- **Change log:** every change (who, what field names, when), exportable as CSV or to their SIEM.

## 5. Enterprise requirements, and where each lives

| Need | How Piperoom meets it | Where |
|---|---|---|
| Data stays in our network | **In your network** deployment (Helm, Terraform, Docker; `deploy/`) is the default for enterprise. Hosted is the option for teams that accept it | §3.1 |
| Hosted, but our databases are private | **Piperoom Link**: a small relay in their network that dials **out** to Piperoom over TLS. No inbound firewall rule. Per-card choice: *direct* or *through Link* | new, phase 3 |
| Firewall allow-list | The network check shows the source IPs to allow, or offers Link | §3.4 |
| SSO, SCIM, roles | OIDC today (`docs/design/auth.md`). SAML and SCIM later. New **admin** and **integration manager** roles | `integrations.md` §7 |
| Four-eyes on production changes | Optional: a change to a production connection needs a second admin's approval, the same pattern as fix approvals | phase 2 |
| Secrets in our vault | Secret-manager references (§2.8) | phase 2 |
| Our own encryption key | BYOK: `PIPEROOM_SECRET_KEY` self-hosted; a KMS key ARN when hosted | `integrations.md` §4.3 |
| Config as code | Terraform provider `piperoom_integration`, `piperoom_pipeline`; YAML export and import; *Managed by code* lock | phase 3 |
| Separate customers | Hosted: one workspace per customer, separate data and keys, no cross-workspace search. Self-hosted: one install per customer | §6 |
| Security review | A generated "Data access summary" PDF per workspace: what each connection reads, the model provider, retention, sub-processors | phase 2 |
| Audit | Change log export, room export (`GET /api/rooms/{id}/export`) | today + §4 |
| Staging vs production | Environments with *Copy from staging* | phase 2 |

## 6. Many enterprise customers: one product, two shapes

Different customers will want different things. Build one UI and one API, and support two deployment shapes:

| | **In your network** (self-hosted) | **Hosted** (multi-tenant) |
|---|---|---|
| Who it suits | banks, healthcare, anyone with a "no SaaS on prod data" rule | mid-size teams that want speed |
| Reaching databases | direct, same network | Piperoom Link relay |
| Slack, AWS | one-time "create app / launch stack" step per install (`integrations.md` §6) | true one-click: one published Slack app, one AWS account to trust |
| Updates | customer pulls a release | continuous |
| Customer separation | separate installs | workspaces: tenant id on every row, per-tenant key, per-tenant model budget |

**Decision (2026-10-14): both shapes from the start.** Customers may pick either, so neither is "later":

- One codebase and one UI. The hosted service is the same server with a workspace id on every row (integrations,
  rooms, sessions, audit), a per-workspace encryption key and a per-workspace model budget. Self-hosted is one
  workspace. Every API is workspace-scoped from the first PR, so there is no rewrite later.
- **Piperoom Link moves to phase 2.** Hosted customers with private databases cannot start without it.
- AWS, Azure and Google connections in hosted mode use cross-account trust (role with external ID, federated
  identity, service account), never long-lived keys.
- Secret managers work in both shapes (`docs/design/secret-providers.md` §7), including a strict hosted mode where
  credentials never leave the customer network.

## 7. What "easy" is measured by

| Metric | Target |
|---|---|
| Setup owner's own time to the first real room | < 30 min |
| Calendar time to the first real room | < 3 days (the hand-offs) |
| Setups finished without a support call | > 80% |
| Hand-off links completed within 48 h | > 70% |
| Connections failing a background re-test that were found by Piperoom before an incident | > 90% |

Instrument each step of §3 (started, finished, failed with which check) so the slowest step is visible per
customer.

## 8. Build order

| Phase | Ships | Why first |
|---|---|---|
| 1 | Checklist, catalog, connect drawer with live checks and fix text, setup scripts, *Verified read-only*, sample room, test alert; cards: Postgres, Kafka, Confluent Cloud, Connect REST, Schema Registry, Snowflake, Anthropic, alert snippets, Slack | removes headaches 2, 4 and most of 3; the server side is `integrations.md` PRs 1–5 |
| 2 | **Ask someone** hand-off, stack tiles with discovery, pick pipelines, owners from SSO groups, health page, secret managers tier 1 (AWS Secrets Manager, SSM, Vault, Azure Key Vault, Google, Kubernetes, mounted files), environments, data access summary PDF, four-eyes, **hosted workspaces and Piperoom Link** | removes headache 1, the biggest one, and opens hosted |
| 3 | AWS one-role discovery, Terraform provider and YAML export, secret managers tier 2, strict hosted mode (Link runs the reads), SAML and SCIM | removes headache 5 |

## 9. Decisions (2026-10-14)

1. **Deployment: both shapes**, chosen by each customer (§6). Piperoom Link is phase 2.
2. **Secret managers: all of them**, through built-in providers, a universal mounted-file adapter and a small provider
   interface. Design: `docs/design/secret-providers.md`.
3. **Hand-off links need the customer's SSO.** A contractor DBA is added to the customer's identity provider (most
   support guest accounts) before the link works. Reasons: the customer stays in control of who touches their
   systems, every action is tied to a real identity, and a forwarded link is useless to anyone else. The hand-off
   page says this plainly when an outsider opens it: "Ask Utkarsh to add you to Acme's sign-in".
