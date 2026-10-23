# Piperoom landing page: final design spec

Status: **final, build from this file only.** Inputs: `research.md`, `facts.md` (main @ `2d4895c0`), and the three
directions. Every product string below was checked against facts.md, and the UI strings `Sent to agent`, `Decline`,
`Shift 1 summary`, `Checked against the room log`, `Approving exactly this` and `Terms the cited steps do not show`,
plus the event types used in §3.7, were checked in `origin/main` (`web/src`, `internal/events/events.go`).
If a sentence is not in this file, it does not go on the page.

---

## 0. Decision

### 0.1 Scores (1–10)

| Criterion | REPLAY (film) | Drawn & Approved (blueprint) | NIGHT SHIFT (story) |
|---|---|---|---|
| 1. Wow on first screen vs. Renderforest richness | **9**: the hero *is* a playing film with a cast, camera moves and a premiere | 5: a P&ID drawing with people on it is still, at heart, a diagram, the exact complaint | **10**: four lit windows in four skies is the richest first screen |
| 2. Explains the product in 30 s to a data-platform lead | **9**: H1 + lead + the first 30 s of film show break → room → read-only steps | 8: strong lead and drawing, but the story waits below the fold | 7: charm first; the 3 a.m. framing delays what the product does |
| 3. Credibility for a serious infra buyer | 8: real app UI, real strings, real tool output | **10**: proof file paths, parts list, drafting rigour | 5: the cat, sticker buttons, five chairs and a recast demo read as a consumer app |
| 4. Feasible to a very high standard in one HTML file | 7: one deterministic world of HTML mocks; ambitious but bounded | **8**: symbols + sheets; the cqw stage is the only hard part | 4: ~20 bespoke illustrated scenes, a walk cycle, two 2560-px worlds; will not fit 250 KB at quality |
| 5. Both themes and phone width | 8: designed matinee/screening-room pair; 4:5 portrait rig | 8: light table vs. drafting film; portrait plates | 7: two full sky sets to tune; heavy scenes at 390 px |
| 6. Avoids AI-design clichés | 6: dark + border beam + glow sits close to the Linear clone | 7: blueprint grids are common, P&ID conventions are fresh | 6: sparkle agent icon, marigold sticker buttons, cosy-illustration trend |
| **Total / 60** | **47** | 46 | 39 |

**Winner: REPLAY.** It is the only direction that delivers the thing the owner asked for (a watchable, character-led
explainer with Renderforest-level richness) without giving up product truth: every frame of the film is the real
room UI with real strings. Its weak spots (cliché risk from dark-mode glow, a slightly generic display face, nothing
to *do* outside the film, thin proof) are exactly what the runners-up do best, so they are grafted in below.

### 0.2 Grafts (each named; everything else from the runners-up is rejected)

| # | Graft | From | Where it lands | Why |
|---|---|---|---|---|
| G1 | **Archivo, wide** as the display face (replaces Bricolage Grotesque) | blueprint | all headings | engineering voice, less trendy, title-card feel at 112% width |
| G2 | **Proof lines**: each trust clause names the file on `main` that enforces it | blueprint | §3.6 Trust | credibility for the infra buyer |
| G3 | **Two shelves**: 43 read tools (open shelf) and 15 write actions (locked shelf) | blueprint parts list | §3.3 and §3.4 | shows "read-only" as an inventory, not a promise |
| G4 | **Click-to-approve climax** with a title block (`REV A` → `REV B`) and a real browser-computed fingerprint | blueprint + story | §3.4 | the visitor does the trust moment with their own hand |
| G5 | **Interactive suggestion row** (`Send to agent` / `Decline` → `Sent to agent`) | story | §3.3 | makes "one driver, many voices" tangible |
| G6 | **Interactive hand-off** (`Accept` moves the baton) and **two windows, two local times** (dusk for Arjun, morning for Meera) | story + blueprint | §3.5 and film chapter 6 | shift change across time zones, with people at the centre |
| G7 | **State light**: the rim light on each figure takes the room's state colour | story | illustration system, film | the viewer reads the incident state from people's faces |
| G8 | **Arjun uses a manual wheelchair** (push handles, wheel rim), drawn as everyday kit | story | cast | real variety beyond skin tone |
| G9 | **Pilot ladder**: Install → Shadow → Rooms → Write mode, each with its real setting | blueprint | §3.8 | honest about defaults (`log`, `off`) and how a pilot grows |
| G10 | **"Not supported today"** line under integrations | blueprint | §3.7 | pre-empts the wrong-stack lead |
| G11 | **Honesty plates** in the film: `watcher mode: rooms` and `write mode: on (demo)` | blueprint | film ch 1 and ch 5 | defaults are `log` and `off` |
| G12 | **One cursor on screen at a time**, each owned by the POV person | blueprint | film | the product has no shared cursors |

Rejected on purpose: the cat, five chairs, marigold sticker buttons, the sparkle agent icon, recasting Kabir as
driver, the floor-plan "room." device, the P&ID hero plate, the tool ticker, the shadow "metrics card" with dashes,
hover parallax on stills, the nav "reel" as section tracker, animated grain.

---

## 1. Thesis (for the builder's head, not for the page)

The page is a screening room. The hero is a 76-second film of one real Piperoom room (the `schema-change` incident)
with a cast of four owners and a read-only agent. Every section below is one scene of that film shot closer, and
each one lets the visitor do the scene's key action themselves: send a suggestion, approve a fix, accept a hand-off.
The **Lamp** (champagne) lights what is playing or what you hold; the **Tally** (broadcast red) marks only the break.

---

## 2. Tokens

### 2.1 Page colours (dark-first)

Bare `:root` = dark values + `color-scheme: dark`. Light values go under
`@media (prefers-color-scheme: light) { :root:not([data-theme="dark"]) { …; color-scheme: light } }` and again,
identically, under `:root[data-theme="light"] { … }`. `body { background: var(--bg); color: var(--ink); }`.

| Token | Job | Dark (primary) | Light |
|---|---|---|---|
| `--bg` | page ground | `#090A0E` | `#F3F4F7` |
| `--surface` | cards, nav, transport bar, form | `#111319` | `#FFFFFF` |
| `--surface-2` | chips, inputs, insets | `#181B23` | `#ECEEF2` |
| `--line` | hairlines, borders | `#262A35` | `#D9DCE3` |
| `--ink` | primary text | `#EDEDF1` | `#111318` |
| `--ink-2` | secondary text | `#A4A7B4` | `#4A4F5D` |
| `--ink-3` | meta, fine print | `#7C8090` | `#646979` |
| `--lamp` | accent A: playhead, focus ring, driver ring, active chapter, underline | `#E8D3A2` | `#7E5E22` |
| `--tally` | accent B: the break, LIVE dot, refused write; max once per screen | `#FF4F3F` | `#C42A1A` |
| `--stage` | film ground; dark in both themes | `#050609` | `#0F1116` |
| `--cta-bg` / `--cta-ink` | primary button | `#E8D3A2` / `#15120B` | `#14161C` / `#F7F2E6` |
| `--nav-bg` | sticky nav + safe-area band | `rgb(9 10 14 / .80)` | `rgb(243 244 247 / .84)` |
| `--lamp-glow` | projector beam, light spill | `rgb(232 211 162 / .20)` | `rgb(126 94 34 / .10)` |
| `--tally-glow` | halo of the LIVE dot | `rgb(255 79 63 / .28)` | `rgb(196 42 26 / .16)` |
| `--scrim` | poster dim | `rgb(5 6 9 / .58)` | `rgb(15 17 22 / .52)` |
| `--caption-bg` / `--caption-ink` | captions | `rgb(5 6 9 / .80)` / `#F4F4F6` | `rgb(15 17 22 / .82)` / `#F4F4F6` |
| `--ok` | success text on the page (Copied, recovered) | `#4FD48F` | `#157A49` |
| `--fig-line` | outline of drawn people and props on the page | `#07080B` | `#1A1C22` |
| `--shadow-1` | raised surfaces | `inset 0 1px 0 rgb(255 255 255/.05), 0 1px 2px rgb(0 0 0/.5)` | `0 1px 2px rgb(16 24 40/.06), inset 0 1px 0 rgb(255 255 255/.8)` |
| `--shadow-frame` | player frame, hero mock | `0 0 0 1px var(--line), 0 40px 90px -30px rgb(0 0 0/.8), 0 -20px 120px -40px var(--lamp-glow)` | `0 0 0 1px var(--line), 0 1px 2px rgb(16 24 40/.08), 0 30px 70px -30px rgb(16 24 40/.38)` |
| `--shadow-card` | section cards | `inset 0 1px 0 rgb(255 255 255/.05), 0 0 0 1px var(--line)` | `0 1px 2px rgb(16 24 40/.06), 0 12px 32px -12px rgb(16 24 40/.18)` |

At ≤ 720px, `--shadow-frame` and `--shadow-card` use the same values with blur halved and alpha × 0.7 (declare in a
`@media (max-width: 720px)` block inside each theme block).

Contrast (WCAG, computed by the film direction and rechecked for changed pairs): `--ink-2` ≥ 8:1 on `--bg` both
themes; `--ink-3` ≥ 4.9:1 on `--bg`; `--lamp` 13:1 dark / 5.4:1 light; `--tally` 6.1:1 / 5.2:1; `--cta-ink` on
`--cta-bg` ≥ 12:1; `--ok` ≥ 4.9:1 on `--surface` both themes. `--lamp` (dark) is never used as text on light, and
vice versa: every use goes through the token.

### 2.2 Product-UI tokens (every mock)

Section mocks use the page theme. **Inside `.stage` the dark column is re-declared in both themes** (the film is
always graded in the app's dark default).

| Token | Dark (`web/src/index.css`) | Light |
|---|---|---|
| `--ui-bg` / `--ui-card` | `#0a0a0b` / `#121214` | `#ffffff` / `#ffffff` |
| `--ui-fg` | `#ececf0` | `#18181b` |
| `--ui-muted` / `--ui-accent` | `#19191d` / `#1f1f25` | `#f4f4f5` / `#ececf0` |
| `--ui-border` | `#222228` | `#e4e4e7` |
| `--ui-muted-fg` | `#9d9da8` | `#71717a` |
| `--ui-primary` / `--ui-primary-fg` / `--ui-ring` | `#516edf` / `#8ea3ff` / `#5b7cfa` | `#4d6beb` / `#3f5bd6` / `#4d6beb` |
| `--ui-agent` / `--ui-agent-fg` / `--ui-agent-subtle` | `#8b5cf6` / `#b7a0ff` / `rgb(139 92 246/.14)` | `#7c4ddc` / `#6a3fcc` / `rgb(124 77 220/.10)` |
| `--ui-success` / `--ui-success-solid` / `--ui-success-fg` | `#2ea36f` / `#26865b` / `#4fd48f` | `#1f9d5f` / `#1a8752` / `#157a49` |
| `--ui-warning` / `--ui-warning-fg` | `#d9a21b` / `#f0c04a` | `#a87600` / `#8a6100` |
| `--ui-danger` / `--ui-danger-fg` | `#d34247` / `#ff7074` | `#c8343a` / `#b42a30` |
| `--ui-info` | `#3b9eff` | `#1f6fd1` |
| `--ui-shadow` | `0 1px 2px rgb(0 0 0/.4), 0 8px 24px rgb(0 0 0/.4)` | `0 1px 2px rgb(16 24 40/.06), 0 12px 32px -12px rgb(16 24 40/.18)` |

Layer colours (dark / light): `--l-pg #7cb8ff / #185eb7` · `--l-connect #f0a35c / #964c08` ·
`--l-sr #5ad3c0 / #0c6c5e` · `--l-kafka #f28fb4 / #a93060` · `--l-consumer #b9d96b / #4c690b` ·
`--l-wh #9fb3ff / #4356bd` · `--l-orch #4dd6e0 / #0d7f88` · `--l-transform #d98bd4 / #9b3d95`.

### 2.3 Illustration tokens (declared once in bare `:root`; identical in both themes, because people and skies do not change colour)

| Token | Value | Use |
|---|---|---|
| `--skin-arjun` / `--skin-priya` / `--skin-kabir` / `--skin-meera` | `#B07650` / `#E3B48D` / `#5E3A27` / `#8C5A3A` | faces, hands |
| `--hair-dark` / `--hair-brown` / `--hair-grey` | `#16141A` / `#3A2A22` / `#C9C5BF` | hair |
| `--metal` / `--tyre` | `#8E93A3` / `#1C1E24` | Arjun's wheelchair |
| `--sky-dusk-1` / `--sky-dusk-2` | `#2E2363` / `#A65A7A` | Arjun's window, chapter 6 and §3.5 |
| `--sky-dawn-1` / `--sky-dawn-2` | `#D9A066` / `#EED8AE` | Meera's window, chapter 6 and §3.5 |
| `--sun` | `#F5B53A` | Meera's low sun |
| `--state-alert` / `--state-agent` / `--state-need` / `--state-ok` / `--state-idle` | `#FF7074` / `#B7A0FF` / `#F0C04A` / `#4FD48F` / `#E8D3A2` | state light (G7) |

Clothing uses the person's layer colour token (so it re-themes with the layer token): Arjun `--l-connect`, Priya
`--l-sr`, Kabir `--l-consumer`, Meera `--l-wh`. Shade tone for any fill:
`color-mix(in oklab, <fill> 84%, black)`.

### 2.4 Fonts (Google Fonts only)

```html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@100..125,500..800&family=Geist:wght@400..600&family=Geist+Mono:wght@400..600&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist+Mono:wght@400..600&text=%E2%86%92%E2%86%B5%E2%8C%98">
```

The second request adds `→ ↵ ⌘` to Geist Mono (its latin subset lacks U+2192). Never put `→` in Archivo or Geist;
arrows in display or body text are inline SVG (12px, stroke 1.5).

| Role | Family | Settings | Fallback stack |
|---|---|---|---|
| Display | **Archivo** | 700 (H1), 680 (H2), 640 (H3/names); `font-stretch: 112%` ≥ 720px, `104%` below | `"Archivo", "Helvetica Neue", Arial, system-ui, sans-serif` |
| Body + UI | **Geist** | 400 body, 500 labels/buttons, 600 mock titles | `"Geist", ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif` |
| Mono | **Geist Mono** | 400 data, 500 slates; `font-variant-numeric: tabular-nums` everywhere | `"Geist Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace` |

### 2.5 Type scale

| Step | Spec |
|---|---|
| `display-xl` (H1) | Archivo 700, `clamp(40px, 6vw + 8px, 92px)` / 0.95, tracking −0.03em |
| `display-l` (H2) | Archivo 680, `clamp(30px, 3.4vw + 12px, 56px)` / 1.02, tracking −0.02em, `text-wrap: balance`, max 22ch |
| `display-m` (H3, lower-third names) | Archivo 640, `clamp(20px, 1vw + 16px, 26px)` / 1.15, tracking −0.01em |
| `lead` | Geist 400, `clamp(17px, 0.4vw + 15px, 20px)` / 1.5, `--ink-2`, max 60ch |
| `body` | Geist 400, 16px / 1.6, max 66ch, `text-wrap: pretty` |
| `small` | Geist 400, 14px / 1.5 |
| `slate` | Geist Mono 500, 12px / 1.3, uppercase, tracking .08em, `--ink-3` |
| `data` | Geist Mono 400, 13px / 1.55 |
| `caption` | Geist 500, `clamp(15px, 1.4vw, 19px)` / 1.35, max 2 lines |
| mock UI | Geist 12/16, 13/18, 14/20, 16/22, 20/26; Geist Mono 12/16 for tool calls |

### 2.6 Radius, spacing, layout

- Radius: player frame 18 · page cards 14 · stills 12 · controls/buttons 10 · inputs 8 · pills 999 · mocks keep the
  app's 8 / 6 / 4.
- Spacing scale: 4, 8, 12, 16, 24, 32, 48, 64, 96, 128. Section `padding-block: clamp(72px, 9vw, 128px)`.
- Container `.wrap { max-width: 1200px; margin-inline: auto; }`; the player may reach 1240px. 12 columns, gap 24px
  (16px below 720px). Breakpoints: 600 (player rig), 720, 1024.
- **Outer gutter** on one wrapper only: `.page { padding-left: max(16px, env(safe-area-inset-left, 0px));
  padding-right: max(16px, env(safe-area-inset-right, 0px)); }`. Vertical spacing via `padding-block` only. Every grid
  or flex child holding text gets `min-width: 0`; mono strings in mocks get `overflow-wrap: anywhere`.
- `html { overflow-x: clip; }` is a safety net, not a fix: 390px must not overflow without it.

### 2.7 Motion tokens

`--ease-cam: cubic-bezier(.65,0,.35,1)` (camera, baton) · `--ease-out: cubic-bezier(.2,.7,.2,1)` (entrances) ·
`--ease-pop: cubic-bezier(.3,1.4,.5,1)` (checks, avatars). Durations: 120 press · 200 state · 320 reveal · 600 heal/cite
· 900–1200 camera.

---

## 3. Page, top to bottom

File order: `<title>Piperoom</title>` → `<meta name="claude:viewport-fit-sides" content="cover">` → the 4 font tags →
`<style>` → markup → `<script>`. Markup starts with one hidden sprite
`<svg width="0" height="0" aria-hidden="true" style="position:absolute">` holding every `<symbol>` (avatars, half-body
figures, icons, lens, padlock, brand mark), then `<div class="page">` with nav, `<main>`, footer.

Every content section opens with a **slate row**: `0N — NAME` (`slate`), a 1px `--line` rule filling the width, and a
**timecode button** (`<button class="tc">Watch 0:12–0:38</button>`, Geist Mono 12px, `--lamp` text, play glyph). It
seeks the film to the start time, scrolls the player into view (`behavior: smooth`, or `auto` when reduced),
moves focus to the play button and plays (reduced motion: seeks and shows that cue's key frame, paused).

Section ids: `#top`, `#film`, `#how`, `#investigation`, `#approval`, `#handoff`, `#trust`, `#integrations`, `#pilot`,
`#apply`.

### 3.1 Sticky nav

- `<header class="nav">`, height 60px, `position: sticky; top: env(safe-area-inset-top, 0px); z-index: 50;
  background: var(--nav-bg); backdrop-filter: blur(14px) saturate(140%)`. A 1px `--line` bottom border is added
  after 8px of scroll (passive listener toggles `.is-scrolled`).
- `body::before { content: ""; position: fixed; inset: 0 0 auto 0; height: env(safe-area-inset-top, 0px);
  background: var(--nav-bg); z-index: 51; }`.
- Left: brand mark (22px rounded square radius 6, fill `--ink`, pulse path `M4 12h5l2-5 3 10 2-5h4` in `--bg`,
  viewBox 24, stroke 1.75, round caps) + wordmark **Piperoom** (Archivo 700, 112%, 18px).
- Centre-right (≥ 1024px): links **Film** (`#film`) · **How it works** (`#how`) · **Trust** (`#trust`) · **Pilot**
  (`#pilot`). Geist 500 14px; hover underline in `--lamp` grows from the left (200ms).
- Right: **Resume film · 0:34** pill (hidden; shown only when the film was auto-paused off-screen; click scrolls back
  and resumes) · theme toggle `<button id="theme-toggle" aria-label="Switch to light theme">` (sun/moon SVG; label
  flips) · primary button **Join the pilot** (`#apply`).
- Under 720px: links hide; brand, toggle and **Join the pilot** stay.
- Under the nav bar: a 2px **film progress line** (aria-hidden), six segments with 3px gaps, played part `--lamp`,
  rest `--line`. It mirrors `t` while the film plays or is paused mid-way, and is empty at 0.

### 3.2 Hero = the film (`#top`, player `#film`)

**Layout ≥ 1024px.** Row 1: slate + H1 in columns 1–8; lead, buttons and status line in columns 9–12, bottom-aligned
to the H1's last line. Row 2: the player, full container width (max 1240px). Row 3: chapter list. Row 4: credits
line + transcript disclosure. Hero height is set by content (never `100vh`). Below 1024px everything stacks in source
order: slate, H1, lead, buttons, status line, player, chapters, credits.

**Copy (final):**
- Slate, with a 6px `--tally` dot that pulses once on load: `NOW SHOWING · SCHEMA CHANGE · 1:16`
- H1 (two lines on desktop): **Claude investigates.** / **Your team decides.** The final full stop is set in
  `--tally`.
- Lead: "Piperoom is a self-hosted incident room for Postgres, Debezium and Kafka pipelines. When one breaks, a room
  opens and Claude investigates with read-only tools while your team watches and steers. A fix runs only after every
  owner it touches approves it."
- Primary button: **Play the film** (play glyph + mono chip `1:16`). It restarts the film at 0:00 with captions on and
  scrolls the player into view.
- Secondary button (ghost, 1px `--line`, `--ink` text): **Join the pilot** (`#apply`).
- Status line (`slate`): `PRE-1.0 · LOOKING FOR FIVE DESIGN-PARTNER TEAMS`
- Credits line (`small`, `--ink-2`), under the chapter list: "With Arjun, on call · Priya, schema-registry owner ·
  Kabir, consumer owner · Meera, next shift · and Claude, read-only." Right-aligned on desktop, below on phones:
  "Dramatised from Piperoom's schema-change demo. Times are illustrative."
- `<details id="transcript">` summary **Read the transcript**: all 21 cues as paragraphs `0:00 · text`, grouped under
  the six chapter titles. Open by default under reduced motion.

**Visual:** a projector-beam cone above the player (an absolutely placed element: `conic-gradient` wedge in
`--lamp-glow`, `filter: blur(40px)`, height 240px, `pointer-events: none`), and a soft floor reflection under the
frame (a 60%-wide ellipse of `--lamp-glow`, blur 50px). In light theme the beam becomes a warm spill (same element,
the token changes). Player spec in §4.

### 3.3 01 — How a room works (`#how`)

- Slate: `01 — HOW A ROOM WORKS` · button `Watch 0:00–1:16`
- H2: **One room, from the first alert to the hand-off.**
- Lead: "Every room is an append-only log of events. Anyone who joins late sees it replay, then follows along live."
- Four steps, each a still + number + H3 + body:
  1. **A room opens itself.** "A built-in watcher spots a failed connector, an inactive replication slot or growing
     WAL and opens a room. Alerts from Alertmanager, Grafana, PagerDuty, Datadog or any signed webhook open one too."
     Footnote (`small`, `--ink-3`): "The watcher only writes log lines until you switch it to rooms."
  2. **Claude investigates. Everyone watches.** "Claude works through read-only tool calls you can follow step by
     step. Anyone can comment. One driver steers, and everyone else's ideas arrive as suggestions the driver can send
     on or decline."
  3. **Owners approve the exact fix.** "Claude can only propose. Each step names the layer it touches, and the fix
     runs only after every owner of those layers approves that exact proposal."
  4. **Hand off without losing the thread.** "At shift change Claude writes what broke, why, what's done, what's ruled
     out, what's still open and the risks. The driver role moves when the next person accepts."
- **Visual: the filmstrip band.** The four stills sit on a `--surface` band (inside the gutter, radius 14) with
  sprocket holes along its top and bottom edges: `radial-gradient(circle, var(--bg) 3px, transparent 3.5px)` repeated
  every 18px in a 12px strip. This is the only filmstrip motif on the page. Stills are 4:3, radius 12, built from the
  same component CSS as the film (dark UI tokens in both themes, so they read as frames from the film):
  1. Arjun half-body (wheelchair push handle visible), rim-lit `--state-alert`; an alert toast
     `connector_down · sev2 · orders` with a `--tally` dot and `watcher ✓`. Tag `CH 1 · 0:00`.
  2. A step stream: `s_1 connect_status`, `s_2 schema_registry_versions`, `s_3 schema_registry_diff`, and Kabir's
     comment bubble pinned on s_1; the lens glyph at top. Tag `CH 2 · 0:12`.
  3. A mini proposal card with two owner rings: Priya's closed with a check, Arjun's dashed; padlock on the run
     button. Tag `CH 4 · 0:38`.
  4. The hand-off card between Arjun's and Meera's avatars; the `--lamp` driver ring mid-arc. Tag `CH 6 · 1:04`.
  Each still carries a visible button **Play from 0:12** (bottom-right, always visible, not hover-only) with the
  timecode button behaviour.
- Layout: 4 columns ≥ 1100px; 2×2 from 600px; 1 column below. Stills are `role="img"` with an `aria-label` that
  states their content.
- Motion: none.

### 3.4 02 — The investigation (`#investigation`)

- Slate: `02 — THE INVESTIGATION` · button `Watch 0:12–0:38`
- H2: **Watch the reasoning, one tool call at a time.**
- Lead: "Claude works through read-only tools you can follow step by step: what it called, with what input, and what
  came back. The hypothesis cites the steps it rests on, and any term the cited steps don't show is marked."
- Three lines, each with a 16px SVG glyph (lens, bracket, people):
  - "43 read-only tools across Kafka Connect, Schema Registry, Kafka, Postgres, your warehouse and batch jobs."
  - "Alerts, logs and record values reach the model only inside an untrusted wrapper. Suspected instructions are
    flagged, not followed."
  - "Anyone can comment on a step. The driver steers; the driver's lease lasts 30 minutes and renews whenever they
    act."
- Button (ghost, small): **Replay the stream** (`id="iv-replay"`).

**Visual, desktop:** copy + the open shelf in columns 1–5; the investigation card in 6–12. Phone: copy, card, shelf.

- **Investigation card** (product mock, page-theme UI tokens, radius 8, `--ui-shadow`):
  - Header row: lens glyph in a 24px `--ui-agent-subtle` square + **Investigation** + status pill
    `Investigating · step 6` (agent dot).
  - **First look** row (collapsed, one line): badge `First look` · label `triage, unverified` · text "Task 0 of
    pg-orders-connector failed on a Schema Registry error." (marked illustrative in the build checklist).
  - **Step rail**: numbered nodes (18px circles, 1px `--ui-border`, check glyph when done) joined by a 1px line. Each
    step = title (Geist 500 14px) → tool call (Geist Mono 12px, `--ui-agent-fg`) with an argument chip → result in an
    **untrusted block**: 1px dashed `--ui-border` over a 45° hatch (`repeating-linear-gradient(45deg,
    var(--ui-muted) 0 2px, transparent 2px 6px)`), a mono tab on its top edge `tool output · untrusted`.
    - `s_1` Check the Debezium connector → `connect_status` `connector=pg-orders-connector` →
      `Task 0: FAILED … incompatible with an earlier schema for subject "pg.public.orders-value" … error code: 409`
    - `s_2` Inspect the Schema Registry subject → `schema_registry_versions` `subject=pg.public.orders-value` →
      `versions [1] … compatibility BACKWARD`
    - `s_3` Diff the attempted schema against v1 → `schema_registry_diff` → `amount: int → bytes(decimal 12,2)`
    - `s_4` Check consumer lag → `kafka_consumer_lag` `group=order-sync` → `order-sync: Stable, 1 members, total lag 0`
      + tag `ruled out · consumer`
    - `s_5` Check the DDL log → `pg_recent_ddl` →
      `ALTER TABLE public.orders: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)`
    - `s_6` Check what changed before the alert → `what_changed` → `DDL 90 s before the alert · connector config change · schema version`
  - **Thread on s_1** (right margin on desktop, under s_1 on phone): Kabir's 24px avatar + bubble "Wrong service? I'd
    expect Connect to be down, not the schema." Reply with the lens avatar, violet edge: "Connect is up; task 0 is
    failed on a Schema Registry compatibility error."
  - **Suggestion row (G5, interactive):** heading **Suggestions**; row: Kabir avatar · "Check the connector config first, not the topic." · buttons
    **Send to agent** (`id="iv-send"`, primary) and **Decline** (`id="iv-decline"`, ghost). Note under it (`small`):
    "You are Arjun, the driver. Kabir can't steer, so his idea waits for you."
    - Send to agent → row status `Sent to agent`; then an agent message types in under the rail (18 ms/char):
      "Connector config is unchanged; the failure is the value schema subject." Live region announces
      "Sent to agent."
    - Decline → row status `Declined`; live region "Declined." (`Declined` is page copy; listed in §8.)
    - A text button **Reset** (`id="iv-reset"`) restores the row.
  - **Hypothesis card**: `Hypothesis` · badge `high confidence` · confidence path `low 09:45 → high 09:47` (two-point
    mini line from `--ui-muted-fg` to `--ui-agent`) · text "A schema change to orders.amount broke the
    pg-orders-connector on a Schema Registry incompatibility; no new rows reach the warehouse." · `based on` chips
    `s_1 s_3 s_5 s_6` (buttons; hover/focus plays the app's `cite` tint on that step: `--ui-agent-subtle` fading over
    600ms) · line `Terms the cited steps do not show: none` · buttons **Confirm** / **Dispute…** rendered `disabled`
    with `title="Preview only"`. No percentages.
- **The open shelf (G3):** label `READ SHELF · 43 TOOLS · KIND read`. All 43 names as mono chips (Geist Mono 12px,
  `--surface-2`, 1px `--line`, radius 4), grouped under slate labels with a 6px layer dot: Connect (4) · Schema
  Registry (3) · Kafka (12) · Postgres source (8) · Warehouse (3) · Cross-pipeline (6) · Batch (7). Names verbatim from
  facts.md §4. The six used here plus `warehouse_freshness` get a 2px `--lamp` underline. Chips wrap; never scroll.
  Under it (`small`): "Write actions are not on this shelf. They sit in a separate registry and run only from an
  approved proposal."
- **Motion:** at rest everything is visible. First time the card is ≥ 40% in view (motion allowed): one emphasis
  sweep, 1.2 s per step: the rail line fills `--lamp` node to node, each node's check pops, the matching shelf chip
  brightens for 600ms. **Replay the stream** does a true re-stream on request: steps collapse to s_1 and type in
  (titles appear, tool names type at 20 ms/char, results fade in), ~8 s, ending in the rest state. Reduced motion: no
  sweep; Replay highlights each step for 600ms in turn without typing or movement.

### 3.5 03 — The approval (`#approval`)

- Slate: `03 — THE APPROVAL` · button `Watch 0:38–1:04`
- H2: **Nothing runs until every owner says yes.**
- Lead: "Claude can only propose. Each step names the layer it touches, and every owner of those layers approves that
  exact proposal. Each approval carries a SHA-256 fingerprint of the proposal, so an edited proposal no longer matches.
  After the fix runs, Claude re-reads the touched layers and checks that new rows arrive."
- Try-it line (`slate`, `--lamp`): `TRY IT: APPROVE AS ARJUN, OR CHANGE ONE PARAMETER FIRST.`

**Layout:** desktop: the proposal sheet in columns 1–7, the owners column in 8–12. Phone: owners (compact rows), then
the sheet.

- **Proposal sheet** (product mock, page theme, radius 8, `--ui-shadow`, `id="ap-card"`):
  - Decision bar (top strip, `--ui-warning` tint): text per state (table below).
  - `Proposals` heading; title **Set subject compatibility to NONE and restart the connector task**.
  - `Step 1` · `schema_set_compatibility` · layer chip `schema-registry` (`--l-sr` dot) · rows
    `subject  pg.public.orders-value` / `level  NONE` · note "Allow the new decimal schema to register under the same
    subject."
  - `Step 2` · `connect_restart_task` · layer chip `kafka-connect` (`--l-connect` dot) · rows
    `connector  pg-orders-connector` / `task  0` · note "Restart Debezium task 0 so it picks up the new schema."
  - `Rollback: set compatibility back to BACKWARD` · `Required layers: schema-registry · kafka-connect`
  - **Fingerprint** row: label `Fingerprint`, under it `Approving exactly this`; hash in Geist Mono 16px grouped the
    app's way (first 4, next 4, `…`, last 4): **`205a 8899 … f485`**; hint (`small`): "if the steps change, this
    fingerprint changes and your approval is void".
  - **Title block** (G4) along the bottom edge: 1px `--ui-border` cells, Geist Mono labels 10px uppercase, values 13px:
    `PROPOSED BY Claude (agent)` | `SCHEMA-REGISTRY Priya · approved 09:49` | `KAFKA-CONNECT Arjun · pending` |
    `FINGERPRINT 205a…f485` | `REV A`. Cells stack 2-up below 720px.
  - Two-segment progress bar: teal (`--l-sr`) segment, amber (`--l-connect`) segment; label `1 of 2 approved`.
  - Run button with SVG padlock: **Approve and run** (state-driven).
  - **Executor log** (Geist Mono 13px, `--ui-muted`): empty state `Runs after the last approval.`
  - Mini **pipeline lane** (`Pipeline, source to sink`): Source `orders` (Priya) · Connector `pg-orders-connector`
    (Arjun, **failed**) · Schema subject `pg.public.orders-value` (Priya, **degraded**) · Topic `pg.public.orders`
    (Arjun) · Consumer `order-sync` (Kabir, healthy) · Sink `analytics.orders` (Meera, **degraded**). Wraps 3×2 on
    phones.
- **Owners column:** Priya and Arjun as half-body figures (§5) side by side, each above a 72px owner ring and a label
  `schema-registry · Priya` / `kafka-connect · Arjun`. Ring states: pending = dashed (`stroke-dasharray: 4 4`) in the
  layer colour, turning one revolution per 8 s; approved = solid ring that closes clockwise + 14px check badge.
- **Controls** (real `<button>`s):
  - `ap-approve`: **Approve and run as Arjun** (write mode on) / **Approve as Arjun (will not run)** (write mode off).
  - `ap-level`: segmented control `level: NONE · FULL` (`role="radiogroup"`, two `role="radio"` buttons) labelled
    "Change one parameter".
  - `ap-write`: `role="switch"`, `aria-checked="true"`, label **Write mode** with sub-label "on for this demo · ships
    off".
  - `ap-undo`: **Undo change** (visible only in state V).
  - `ap-reset`: **Reset** (text button).
- **Locked shelf (G3)** under the sheet: label `WRITE ACTIONS · 15 · RUN ONLY FROM AN APPROVED PROPOSAL`, padlock glyph,
  chips grouped by layer: kafka-connect `connect_restart_task` `connect_restart_connector` `connect_resume_connector`
  `connect_pause_connector` `connect_update_connector_config` `connect_snapshot_table` · schema-registry
  `schema_set_compatibility` `schema_register` · kafka `dlq_replay` `kafka_replay_range` · consumer
  `kafka_reset_consumer_offsets` · postgres `pg_drop_replication_slot` `pg_add_publication_table` `pg_snapshot_signal`
  `pg_resnapshot_rows`. Chips have a dashed border (locked look). Note (`small`): "Write mode is off by default.
  Proposals and approvals work, but nothing runs until you turn it on."

**State machine** (`id="ap-live"` polite live region announces each status once):

| State | Decision bar | Status pill | Agent pill | Title block / other |
|---|---|---|---|---|
| **R (rest, first paint, no-JS)** | `Waiting for kafka-connect · 1 of 2 approved` | `Needs approval · 4m` | `Agent waiting · 1 of 2 approvals` | Priya approved, Arjun pending, `REV A`, `1 of 2 approved`, padlock closed |
| **V (level → FULL)** | `Needs your approval · schema-registry · kafka-connect` | `Needs approval · 4m` | `Agent waiting for approval` | row `level FULL` gets a `--tally` revision triangle `B`; `REV B`; hash scrambles 400ms then settles to **`9bd1 c62c … 27f1`**; Priya's cell struck through with `void · fingerprint changed`; ring unwinds; bar `0 of 2 approved`; `ap-approve` disabled; note: "Priya approved 205a…f485. The proposal is now 9bd1…27f1, so her approval no longer counts: the server answers 409 params_mismatch." |
| **A-on (approve, write on)** | `Running step 1 of 2` → `Fix ran · 2 of 2 steps` → `Verified · recovered` | `Executing fix · 1 of 2` → `Fix ran · verifying` → `Fix ran · recovered` | `Executing fix` → `Agent working` | climax (§6.2); executor lines `✓ pg.public.orders-value compatibility BACKWARD → NONE`, `✓ restarted pg-orders-connector task 0 (RUNNING)`; lane heals; verification message: "Verification passed: pg-orders-connector task 0 is RUNNING and new rows reached analytics.orders. pg.public.orders-value compatibility is still NONE; restore BACKWARD with a separate fix."; follow-up chip `Restore BACKWARD compatibility · waiting for schema-registry` |
| **A-off (approve, write off)** | `Approved · waiting for write mode` | unchanged | unchanged | both rings closed, `2 of 2 approved`, padlock stays closed, log stays empty; note "Write mode is off, so this fix is approved and will not run." |

- **Fingerprint mechanics:** canonical JSON = keys sorted at every level, no whitespace, arrays in order, over
  `{required_layers, rollback, steps:[{action, layer, params}], title}`. Compute with
  `crypto.subtle.digest('SHA-256', new TextEncoder().encode(json))` on load, assert it equals the constant, and use
  the constants if `crypto.subtle` is missing. Constants (verified with Python `hashlib`):
  - `NONE` → `205a8899f2dacab0a2af2a9d33b652cd9ba714015d1cce8818d74d988866f485`
  - `FULL` → `9bd1c62c39aa1caa58408624df0b2782c9e30cf8303608c39e1af0cc05e227f1`
  - The title stays "…to NONE…" in state V on purpose: only the parameter changed.
  - Fine print under the sheet: "Fingerprint computed in your browser over the steps shown. Piperoom's own also covers
    the room, the proposal id and the risk, in RFC 8785 form."
- The film shows the same `205a 8899 … f485`, so film and section agree.

### 3.6 04 — The hand-off (`#handoff`)

- Slate: `04 — THE HAND-OFF` · button `Watch 1:04–1:16`
- H2: **Shift change without the scroll-back.**
- Lead: "When the driver hands off, Claude writes the summary from the room log: what broke, why, what's done, what's
  ruled out, what's still open and the risks. It is checked against the log before anyone accepts, and the driver
  role moves only when the next person does."
- Small line with a keycap `H`: "Hand-offs never start on their own. The driver presses H and picks who takes over.
  The summary stays pinned in the room for everyone who joins later."
- **Visual (G6): "Two windows, one baton."** A 21:9 scene card (radius 14, `--shadow-frame`; phones: stacked
  Arjun panel 4:3, card, Meera panel 4:3). Left: Arjun at his desk in his wheelchair, a window behind him with the dusk
  sky (`--sky-dusk-1 → --sky-dusk-2`), desk lamp on (`--lamp-glow` cone), tag `Arjun · local 18:00`. Right: Meera at her
  desk, window with the dawn sky and a low `--sun` disc, a long sunlight parallelogram across the desk, tag
  `Meera · local 07:00`. Mono plate bottom-centre: `room clock 10:00 UTC · times illustrative`.
- **Hand-off card** (product mock, centred over the seam; on phones between the panels): header **Shift 1 summary**
  + `from Arjun to Meera`, then:
  - **What broke** Debezium task 0 failed after orders.amount was altered to numeric(12,2).
  - **Why** New schema is incompatible under BACKWARD compatibility for subject pg.public.orders-value.
  - **Done** Set compatibility to NONE · Restarted task 0 · Warehouse caught up
  - **Ruled out** Kafka brokers · Consumer group config
  - **Still open** Consider pinning decimal.handling.mode · Restore BACKWARD (waiting for schema-registry)
  - **Risks** BACKWARD compatibility is off until re-enabled
  - Check line with a check glyph: `Checked against the room log · no issues`
  - Button row labelled `You are Meera`: **Accept** (`id="ho-accept"`, `--ui-success-solid`) and **Not yet**
    (`id="ho-later"`, ghost).
- **The baton:** a 2px `--lamp` ring around Arjun's avatar (32px, at his panel's top) and a dashed SVG arc
  (`id="ho-arc"`) over the card to Meera's avatar.
- **Interaction:** Accept → the baton ring travels the arc (`offset-path` from the arc's `d`, 900ms `--ease-cam`),
  Meera's avatar gains the ring, tags become `Meera · driving` and `Arjun · watching`, Arjun's lamp fades to 30%, the
  buttons are replaced by the line "Meera is driving." (live region). Not yet → line "The room stays with Arjun until
  you accept." A text button **Reset** (`id="ho-reset"`) restores the rest state.
- Rest state (first paint, no-JS): baton on Arjun, card complete, both buttons enabled.
- Reduced motion: the baton jumps; the lamp switches without a fade.

### 3.7 05 — Trust (`#trust`)

- Slate: `05 — TRUST`
- H2: **It can read. It can't write.**
- Lead: "Piperoom is built so Claude can be in the room without being able to touch the pipeline. Each guarantee
  names the file on main that enforces it."
- **Layout (G2):** a bento of five panels, 12 columns: 7+5 / 5+7 / 12; stacked on phones. Each panel: `T1`–`T5`
  (`slate`), H3, body, a visual, and a **proof line** (Geist Mono 12px, `--ink-3`, prefix `Proof:`). Panels differ in
  anatomy; no left accent bars.
  1. **T1 · Read-only by construction** (7 cols). "Every investigation tool sits in a read-only registry that refuses
     anything else. Client interfaces are split into readers and writers, and writes live only in a separate set of
     actions. Source Postgres reads run in a READ ONLY transaction that is rolled back. Every read and every action is
     checked against the room's own pipeline." Visual: a registry log, three mono rows:
     `connect_status · read · accepted` / `schema_registry_diff · read · accepted` /
     `connect_restart_task · write · refused (ErrNotReadOnly)` (third row struck through in `--tally`), then a ribbon
     `BEGIN READ ONLY … ROLLBACK`. Proof: `internal/tools/tool.go · internal/clients/postgres · internal/tools/scope.go`
  2. **T2 · Approval per layer, bound to the exact plan** (5 cols). "Each step names its layer, and the approver must
     own that layer. The approval carries a SHA-256 fingerprint of the proposal: without it the server answers 400,
     with a different one 409 params_mismatch. Each write action has a preview and a rollback." Visual: three 28px
     owner rings (postgres blue, connect amber, schema-registry teal) around a padlock. Proof:
     `internal/room/params_hash.go · internal/room/service_proposals.go · internal/actions/preview.go`
  3. **T3 · Secrets masked, untrusted text kept as data** (5 cols). "Secrets are masked in tool summaries, error traces
     and record values before they reach the prompt, the room log, Slack or an export. Redaction is partial: names and
     timestamps still appear, and redacted tool summaries do reach your Claude endpoint. Alerts, logs and record values
     reach the model only inside an untrusted wrapper." Visual: `postgres://app:••••••••@db:5432/orders` that resolves
     to `postgres://app:[redacted 16]@db:5432/orders` (600ms scramble, once when first in view, again on hover/focus;
     rest state shows the redacted form). Proof: `internal/redact/redact.go · internal/agent/prompt.go`
  4. **T4 · An audit trail you can export** (7 cols). "Every room is an append-only event log; events are never
     edited. Export a room as JSON with an X-Piperoom-Export-SHA256 integrity header, or start from the post-mortem
     draft. Export is limited to approvers and auditors." Visual: an export ticket: file glyph `room-orders.json`,
     mono event lines `41 approval.given` · `42 approval.given` · `43 action.executed` · `44 action.executed` ·
     `45 agent.verification` · `51 handoff.created` · `52 handoff.accepted` · `58 room.closed`, then
     `SHA-256 of the lines above: ea39 bebe … f5a5` (the real hash of those eight lines, each ending in `\n`:
     `ea39bebe5dcd4dad4190f604d3ccf42f67a45921bfac564bd2afcfdcd082f5a5`) and a chip **Post-mortem draft**. Caption
     (`small`): "Event types are real; sequence numbers are illustrative." Proof:
     `internal/events/events.go · internal/room/service_export.go`
  5. **T5 · Runs where your pipeline runs** (12 cols, strip). "Self-hosted: a Docker image, Docker Compose, a Helm chart
     or Terraform for GCP. Sign-in with OIDC, with member, viewer and auditor roles. Credentials are set per pipeline,
     from environment variables or files. Claude runs through the Anthropic API, AWS Bedrock or Google Vertex AI, with
     token and cost budgets per room and per pipeline." Visual: mono deploy chips `Dockerfile` ·
     `docker-compose.pilot.yml` · `helm/piperoom` · `terraform/gcp`, a hairline, then `Piperoom` + SVG arrow +
     `your Claude endpoint`. Proof: `deploy/ · internal/httpapi/auth_oidc.go · internal/config/config.go`
- Line under the bento (`small`): "Slack is outbound only: one thread per room. Nothing typed in Slack reaches a room,
  and approvals happen only in Piperoom."
- Motion: each panel's visual plays once when first ≥ 40% in view, starting from its readable rest state (the strike
  redraws, rings close, scramble resolves). Reduced motion: none.

### 3.8 06 — Works with (`#integrations`)

- Slate: `06 — WORKS WITH`
- H2 (`display-m` size): **Works with the pipeline you already run.**
- Groups (slate label + pill chips, Geist 500 13px, 1px `--line`, 6px dot in the layer colour or `--ink-3`):
  - `CDC PATH` PostgreSQL · Debezium on Kafka Connect · Kafka · Confluent Schema Registry · Postgres warehouse ·
    TLS and SASL
  - `WAREHOUSES` Snowflake · BigQuery · Databricks SQL · ClickHouse
  - `BATCH ROOMS` Apache Airflow 2.x · dbt
  - `ALERTS IN` Alertmanager · Grafana · PagerDuty · Datadog · signed webhook
  - `OUT` Slack, outbound, one thread per room
  - `CLAUDE VIA` Anthropic API · AWS Bedrock · Google Vertex AI
- Note (G10, `small`, `--ink-2`): "Not supported today: MySQL, MongoDB, Oracle or SQL Server sources, two-way Slack,
  Microsoft Teams, or a hosted plan. Product names belong to their owners."
- Motion: on first view one `--lamp` pulse runs along a hairline under the CDC row and stops, turning `--tally` under
  **Confluent Schema Registry** for 800ms, then fades. Reduced motion: none.

### 3.9 07 — Shadow mode (`#pilot`)

- Slate: `07 — SHADOW MODE`
- H2: **Try it silently first.**
- Lead: "Switch on shadow for one pipeline. Your team runs incidents exactly as today, and nobody sees Claude's work
  until the room closes."
- Three panels, left to right, joined by a 1px rail (stacked on phones):
  1. **Your team works the incident.** "The room opens as usual. People investigate, steer and close it." Visual: a
     small room window, four avatars, pill `Investigating · step 3`.
  2. **Claude works in the dark.** "On the same alert, Claude investigates with read tools only, proposes nothing and
     writes to a separate shadow log." Visual: the same window behind frosted glass (`backdrop-filter: blur(6px)` over
     dimmed step lines; fallback: 70% opaque `--surface`), pill `Shadow · agent works silently`, seal glyph
     `sealed until close`.
  3. **Label first, then grade.** "After the room closes, someone on the pipeline names the layer and the cause,
     blind. Then they open Claude's answer and grade it: right layer or not, and root cause yes, partial or no."
     Visual: a static grade card: `layer_correct` `yes · no` and `root_cause` `yes · partial · no` chips, label
     `schema-registry`, terms `ALTER`, `orders.amount`.
- One line (`small`): "Grades roll up in GET /api/shadow/metrics on your own server. Piperoom publishes no benchmark
  numbers."
- **Pilot ladder (G9)**, title **How a pilot grows, if you want it to**: four landings rising left to right (SVG
  staircase, 1px `--line`, landings in `--surface`; phones: vertical list with a step line), each with its setting in
  Geist Mono and one line:
  1. **Install** `docker compose · helm · terraform` "Run it on your own infrastructure. Sign in through your OIDC
     provider."
  2. **Shadow** `shadow: {enabled: true}` "Claude works silently on one pipeline. You grade every answer."
  3. **Rooms** `PIPEROOM_WATCH_MODE=rooms` "The watcher opens rooms and your team works them with Claude. By default the
     watcher only writes log lines."
  4. **Write mode** `PIPEROOM_WRITE_MODE=on` "Approved fixes run. It stays off until you turn it on."
- Motion: none.

### 3.10 08 — Design partners (`#apply`)

- Slate: `08 — DESIGN PARTNERS`
- H2 (centred): **Five teams. One pipeline each. Shadow mode first.**
- Lead (centred): "Piperoom is pre-1.0 and has no customers yet. We're looking for the first five teams to run it. If
  you run Postgres, Debezium and Kafka in production and you're the one who gets paged when the warehouse goes stale,
  we'd like to hear from you."
- Above the H2: the four cast avatars (40px) in a row, Meera wearing the driver ring, caption (`small`, `--ink-3`):
  "Arjun, Priya, Kabir and Meera are Piperoom's demo users."
- Panel: `--surface`, radius 14, `--shadow-card`, a soft `--lamp-glow` light leak in the top-right corner. Desktop:
  "What it involves" (cols 1–5) + form (cols 6–12). Phone: one column.
- **What it involves** (check glyphs):
  - "You host it: Docker Compose, a Helm chart or Terraform for GCP."
  - "You create read-only credentials for one pipeline."
  - "Write mode stays off, so no fix can run."
  - "After each closed room, someone labels the cause and grades Claude's answer."
  - "Redacted tool summaries go to the Claude endpoint you choose: Anthropic API, AWS Bedrock or Google Vertex AI."
- **Form** `<form id="pilot-form" novalidate>`; labels above fields; 44px controls; submit handled in JS with
  `preventDefault()`; nothing is sent anywhere.

| id | Label | Control |
|---|---|---|
| `pf-name` | Your name | text, required, `autocomplete="name"` |
| `pf-role` | Your role | text, placeholder "Data platform engineer" |
| `pf-company` | Team or company | text, required, `autocomplete="organization"` |
| `pf-email` | Work email | email, required, `autocomplete="email"` |
| `pf-stack-pg`, `pf-stack-debezium`, `pf-stack-connect`, `pf-stack-sr`, `pf-stack-kafka` | Your CDC path (`fieldset` + `legend`) | checkboxes: PostgreSQL source · Debezium · Kafka Connect · Schema Registry · Kafka |
| `pf-warehouse` | Warehouse | select: Postgres · Snowflake · BigQuery · Databricks SQL · ClickHouse · Other |
| `pf-pipelines` | CDC pipelines in production | select: 1 · 2–5 · 6–20 · More than 20 |
| `pf-deploy` | Where you'd run it | select: Docker Compose · Kubernetes (Helm) · GCP (Terraform) · Not sure yet |
| `pf-model` | Claude endpoint | select: Anthropic API · AWS Bedrock · Google Vertex AI · Not sure yet |
| `pf-incident` | Your last pipeline incident, in a sentence or two | textarea, 3 rows, `maxlength="600"` |

  - Errors inline under the field (`--tally`), `aria-invalid="true"`, `aria-describedby`: "Add your name" / "Add a work
    email" / "Add your team or company"; focus moves to the first invalid field.
  - Submit: **Prepare my application** (`--cta-bg`). Under it (`small`): "This page doesn't send anything. You'll get a
    summary to copy and send."
- **Result panel** (replaces the form in place, form kept in the DOM with values; `role="status"`; focus moves to its
  heading, `tabindex="-1"`):
  - Heading: **Your application is ready to send.**
  - `<textarea id="pf-summary" readonly rows="12">` (Geist Mono 13px):
    ```
    Piperoom design-partner application
    Name: …
    Role: …
    Team or company: …
    Work email: …
    CDC path: <checked items, comma-separated, or "not given">
    Warehouse: …
    CDC pipelines in production: …
    Would run it on: …
    Claude endpoint: …
    Last pipeline incident: <text or "not given">
    ```
  - **Copy application** (`id="pf-copy"`): `navigator.clipboard.writeText` inside the click handler; success → label
    "Copied" (`--ok`, check glyph) for 2 s; rejection or no clipboard → `pf-summary.select()` and show "Selected. Press
    Ctrl+C, or ⌘C on a Mac, to copy."
  - **Edit answers** (`id="pf-edit"`) restores the filled form.
  - Send line (`id="pf-sendto"`): if `PILOT_CONTACT !== ""`: "Send it to the Piperoom team at **{PILOT_CONTACT}**."
    (selectable text + a small **Copy address** button `id="pf-copy-addr"`, same clipboard/fallback rule; no mailto).
    If empty: "Send it to the Piperoom team. We'll share the address with you."
  - Banned words anywhere on the page: "submitted", "sent" (as form status), "received", "we'll be in touch".
- **FAQ** (four `<details>` under the panel):
  - **Can Claude change my pipeline?** "Not by itself. Its tools are read-only. A fix runs only after every owner of the
    layers it touches approves that exact proposal, and only when write mode is on. It is off by default."
  - **Where does my data go?** "Piperoom runs in your environment. Redacted tool summaries go to the Claude endpoint you
    configure. If you connect Slack, it gets one outbound thread per room."
  - **Which systems does it read?** "PostgreSQL sources through Debezium on Kafka Connect, Kafka, Confluent Schema
    Registry, and Postgres, Snowflake, BigQuery, Databricks SQL or ClickHouse warehouses. Batch rooms read Airflow 2.x
    and dbt."
  - **Is it open source?** "No. The source is published for viewing only, and all rights are reserved."

### 3.11 Footer

- Brand mark + **Piperoom** + "The incident room for CDC pipelines."
- "Pre-1.0 · Self-hosted · Claude through the Anthropic API, AWS Bedrock or Google Vertex AI."
- "The film is dramatised from Piperoom's schema-change demo. Arjun, Priya, Kabir and Meera are demo users; times are
  illustrative."
- Links: Film · How it works · Trust · Pilot · Join the pilot · second theme toggle (`id="theme-toggle-2"`).
- "Product names belong to their owners and are named only as integrations. © 2026 Piperoom. All rights reserved."

---

## 4. The explainer film

### 4.1 Player anatomy

```html
<section id="film" class="player" data-explainer aria-label="Piperoom explainer film, 1 minute 16 seconds">
  <div class="frame">                         <!-- radius 18, --shadow-frame, position:relative -->
    <div class="stage" tabindex="0" role="group" aria-roledescription="video"
         aria-label="Film stage. Space plays or pauses.">
      <div class="set set-prologue" aria-hidden="true">…</div>
      <div class="world" aria-hidden="true">…</div>   <!-- the room, §4.3 -->
      <div class="set set-handoff" aria-hidden="true">…</div>
      <div class="ov" aria-hidden="true"><!-- cursor, lower third, chapter bug, POV chip, room clock, plates --></div>
      <button class="poster" id="fp-poster" aria-label="Play the film, 1 minute 16 seconds">…</button>
      <div class="endcard" id="fp-end" hidden>…</div>
    </div>
    <p class="cc" id="fp-cc" aria-live="polite"></p>
    <div class="transport">…</div>
  </div>
  <ol class="chapters">…6 × <li><button data-chapter="1">…</button></li>…</ol>
</section>
```

- **Stage aspect:** 16:9 when the player is ≥ 600px wide (container query on `.player`); **4:5 below 600px** with
  the portrait rig (§4.4). Background `--stage`. A static grain layer on the stage only (inline SVG `feTurbulence`
  baseFrequency .9, 2 octaves, as a `data:` URI, opacity .05, `mix-blend-mode: overlay`).
- **Captions `#fp-cc`:** ≥ 600px: absolutely positioned over the stage, bottom 64px from the frame bottom (the
  transport is 48px), centred, max-width 80%, `--caption-bg`, radius 6, padding 6px 12px, `caption` type, max 2 lines.
  Below 600px: in normal flow between stage and transport, min-height 2 lines, so captions never cover the picture.
  CC off adds `.cc-hidden` (visually hidden via clip, still in the a11y tree; the live region keeps updating).
- **Transport** (48px, `--surface`, always visible, never auto-hidden): play/pause `<button id="fp-play"
  aria-label="Play film">` (label flips to "Pause film") · time `0:34 / 1:16` (Geist Mono, tabular) · scrubber
  `<div id="fp-seek" role="slider" tabindex="0" aria-label="Seek" aria-valuemin="0" aria-valuemax="76"
  aria-valuenow="34" aria-valuetext="0:34, chapter 3, Everyone watches, one drives">` drawn as 6 segments with 3px gaps
  (played `--lamp`, rest `--line`, 6px tall, 16px thumb); hover/drag tooltip `0:34 · Everyone watches, one drives` ·
  `<button id="fp-cc-btn" aria-pressed="true">CC</button>` · `<button id="fp-speed">1×</button>` (cycles 1× ↔ 1.5×;
  hidden below 480px) · `<button id="fp-fs" aria-label="Full screen">` (hidden unless `document.fullscreenEnabled`;
  calls `frame.requestFullscreen()`).
- **Chapter list** under the frame: `<ol>` of 6 buttons: `01` (mono) · title · `0:00` · a 2px progress fill;
  `aria-current="step"` on the active one, `--lamp` outline. Desktop 6 across; 600–1023px 3×2; phone 2×3.
- **Keyboard** (keydown on `.player`; ignored when the target is a text field): Space or K play/pause (if the target is
  a `<button>`, let Space activate it), ←/→ −5/+5 s, J/L −10/+10 s, Home/End, 1–6 jump to chapter start, C captions,
  F full screen. The slider also takes ←/→ (±5 s), PageUp/PageDown (±10 s), Home/End.
- **Poster** (`#fp-poster`; shown before first play, and the reduced-motion rest state): the world rendered at
  **t = 47.5** (proposal + fingerprint + decision bar "Needs your approval · schema-registry · kafka-connect"), tilted
  (`perspective: 1400px; rotateX(5deg) rotateY(-7deg) scale(.9)`), `--scrim` over it; centre a 72px round `--cta-bg`
  play disc with one soft ring pulse on load; bottom-left title block: **Schema change, 09:41** (Archivo 700, 112%,
  `clamp(22px, 3cqw, 36px)`, `--caption-ink`) / "A Piperoom incident in six chapters · 1:16"; bottom-right: the four
  cast avatars + the lens avatar; small line "Dramatised from the demo · times illustrative". Below 600px the cast row
  is hidden and the title block shrinks to one line.
- **End card** (`#fp-end`, at t ≥ 76): over a `--scrim`: **That was one room.** / "Run Piperoom on one of your
  pipelines in shadow mode: Claude investigates silently, and you grade it." Buttons **Join the pilot** (`--cta-bg`,
  scrolls to `#apply`) and **Replay** (restarts at 0).

### 4.2 Behaviour

- **Autoplay:** after the premiere (§6.1), only if motion is allowed, the tab is visible and the stage is ≥ 50% in view
  at load. Plays once; no loop. Auto-pauses when < 35% visible and shows the nav pill **Resume film · m:ss**; it
  resumes on return only if it was auto-paused (never a user pause). Hidden tab pauses too.
- **Play the film** (hero button) and **Replay** restart at 0:00, captions on, and scroll the player into view.
- **Reduced motion:** never autoplays. The poster is the rest state. Play advances the clock normally (time, scrubber,
  captions move), but the picture shows the **key frame `k` of the active cue** (21 designed stills), swapped instantly
  at each cue boundary: no tweens, typing, camera moves, beams or cursor travel. Chapter buttons and seeking show the
  key frame of the cue landed in. The transcript is open by default.

### 4.3 The room world

Two layouts of the same DOM, switched by `data-layout` on `.world` (set from the player width, re-rendered on resize):

- **Landscape, 1280 × 720 logical px:**
  - Header `#w-header` (y 0–88): title `orders: connector pg-orders-connector task 0 FAILED 3m` · badge `SEV2` ·
    chip `alert · watcher` + verified check · status pill · agent pill · buttons **Hand off** (keycap H),
    **Close room** (keycap C), **Invite** · mono facts line `orders · opened 09:44:30` · avatar stack right, driver
    ringed, label `Arjun · driving`.
  - Decision bar `#w-bar` (y 88–124).
  - Left column `#w-col` (x 24–856, y 136–640): **Pipeline lane** `#w-lane` ("Pipeline, source to sink", six nodes
    Source · Connector · Schema subject · Topic · Consumer · Sink; each a pill with status dot, layer colour, owner
    initials) at its top, then a clipped feed `#w-feed` whose inner `#w-feed-in` is translated by the `feed` track:
    **First look** `#w-first`, **What changed** `#w-changed` (Before/After), steps `#w-s1`…`#w-s6` (thread on s_1
    `#w-thread`), agent messages, **Hypothesis** `#w-hypo`, **Proposals** `#w-prop` (with `#w-print` fingerprint and
    `#w-owners` owner row + audit rows), executor log `#w-exec`, verification message, follow-up card `#w-follow`.
  - Right rail `#w-rail` (x 872–1256, y 136–640): **People** (four rows: avatar, name, role, presence dot) ·
    **Suggestions** `#w-suggest` · **Proposals** summary · **Activity** `#w-activity` (illustrative times:
    `09:44 room opened by watcher`, `09:45 Kabir commented on s_1`, `09:46 Arjun sent Kabir's suggestion`,
    `09:49 Priya approved · schema-registry`, `09:49 Arjun approved · kafka-connect`, `09:50 fix ran`,
    `10:00 Arjun handed off to Meera`, `10:03 Priya approved restore`, `10:05 Meera closed the room`).
  - Composer `#w-composer` (y 652–704): driver view "Steer the agent…" + **Steer** + "You are driving · Lease ends in
    28m — any action renews"; non-driver view "Suggest a first step…" + **Suggest**.
- **Portrait, 400 px wide, height by content (≈ 1120):** one column in this order: header (wrapping: title on two
  lines, badges, pills; avatars on their own row), decision bar, lane (3×2), feed (same inner content, translated by
  the same `feed` track), composer, then **Suggestions**, **People**, **Activity**. Feed viewport height 520.
- **Measurement:** at init and on resize, cache each anchor's box in world coordinates from the `offsetLeft/Top`
  chain (unaffected by the world transform). Anchors inside `#w-feed-in` add the current `feed` translation at render
  time. No layout reads inside `render()`.
- **Feed track** `feed`: keys name an item (`[t, '#w-s3']`); the translation puts that item's top 16px below the feed
  top, clamped to the content height; eased with `--ease-cam` over 0.8 s between keys.

### 4.4 Camera

- **Base scale:** landscape `b = stageW / 1280`; portrait `b = stageW / 400`. A shot `{a, z, pz, fy}` scales the world
  by `b × z` (portrait `b × pz`) and translates so the anchor's centre (vertical point at fraction `fy` of the anchor
  box, default .5) sits at the stage centre, clamped so the visible window stays inside the world bounds. `a: 'world'`
  means the whole world (z 1).
- Camera keys are `[t, shotName, dur]`; between keys the transform interpolates (scale and translate) with
  `--ease-cam` over `dur`, then holds.

```js
const SHOTS = {
  WIDE:     { a: 'world',       z: 1.0,  pz: 1.0  },
  HEADER:   { a: '#w-header',   z: 1.6,  pz: 1.3  },
  LANE:     { a: '#w-lane',     z: 1.7,  pz: 1.25 },
  STEPS:    { a: '#w-feed', fy: .45,  z: 1.45, pz: 1.0 },
  THREAD:   { a: '#w-thread',   z: 1.8,  pz: 1.25 },
  COMPOSER: { a: '#w-composer', z: 1.7,  pz: 1.2  },
  RAIL:     { a: '#w-suggest',  z: 1.6,  pz: 1.15 },
  HYPO:     { a: '#w-hypo',     z: 1.6,  pz: 1.1  },
  PROPOSAL: { a: '#w-prop',     z: 1.5,  pz: 1.0  },
  PRINT:    { a: '#w-print',    z: 2.2,  pz: 1.4  },
  OWNERS:   { a: '#w-owners',   z: 1.9,  pz: 1.3  },
  EXEC:     { a: '#w-exec',     z: 1.6,  pz: 1.1  },
};
```

Minimum rendered text: at 1180px stage width, 12px mock text renders ≥ 11px at z 1; at a 358px portrait stage,
`b = .895`, so 12px renders ≥ 10.7px at pz 1. Never use pz < 1.

### 4.5 Data (paste verbatim)

```js
const PILOT_CONTACT = "";   // the first line of the script
const DURATION = 76;
const CHAPTERS = [
  { n: 1, id: "opens",    title: "A room opens",                 start: 0,  end: 12 },
  { n: 2, id: "reads",    title: "Claude investigates",          start: 12, end: 26 },
  { n: 3, id: "steers",   title: "Everyone watches, one drives", start: 26, end: 38 },
  { n: 4, id: "proposes", title: "A fix is proposed",            start: 38, end: 50 },
  { n: 5, id: "approves", title: "Every owner approves",         start: 50, end: 64 },
  { n: 6, id: "handoff",  title: "Shift change",                 start: 64, end: 76 },
];
// s/e = cue start/end; k = key frame for reduced motion (all of that cue's motion complete)
const CUES = [
  { s: 0.4,  e: 3.6,  k: 3.3,  text: "09:41. A migration changes orders.amount to numeric(12,2)." },
  { s: 3.6,  e: 6.8,  k: 6.5,  text: "Schema Registry rejects the new schema with a 409. Debezium task 0 fails." },
  { s: 6.8,  e: 9.6,  k: 9.3,  text: "The connector still reads RUNNING. New rows stop reaching the warehouse." },
  { s: 9.6,  e: 12.0, k: 11.8, text: "The watcher sees the failed task and opens a room. Arjun is on call." },
  { s: 12.0, e: 15.2, k: 15.0, text: "A first look arrives, marked unverified. Then Claude starts the real investigation." },
  { s: 15.2, e: 18.8, k: 18.6, text: "Every step is a read-only tool call: connector status, schema versions, a diff." },
  { s: 18.8, e: 22.4, k: 22.2, text: "Consumer lag is zero, so the consumer is ruled out. The DDL log shows the ALTER." },
  { s: 22.4, e: 26.0, k: 25.8, text: "Tool output stays data: instructions hidden in it are flagged, not followed." },
  { s: 26.0, e: 29.6, k: 29.4, text: "Anyone can watch the room live. Kabir questions step 1, right on the step." },
  { s: 29.6, e: 33.2, k: 33.0, text: "Only the driver steers. Kabir's idea becomes a suggestion, not a command." },
  { s: 33.2, e: 38.0, k: 37.8, text: "Arjun drives, so he sends it on. Claude checks: the connector config is unchanged." },
  { s: 38.0, e: 41.6, k: 41.4, text: "Root cause: the ALTER on orders.amount breaks BACKWARD compatibility." },
  { s: 41.6, e: 45.6, k: 45.4, text: "The hypothesis cites the steps it rests on. Then Claude writes a fix it cannot run." },
  { s: 45.6, e: 50.0, k: 49.8, text: "Two steps, each tagged with its layer, a rollback, and a fingerprint of the plan." },
  { s: 50.0, e: 53.4, k: 53.2, text: "Priya owns the Schema Registry. She approves her step, bound to that fingerprint." },
  { s: 53.4, e: 56.8, k: 56.6, text: "Arjun approves for Kafka Connect. With both owners in, the fix can run." },
  { s: 56.8, e: 60.4, k: 60.2, text: "Compatibility goes to NONE and task 0 restarts. Then Claude checks the recovery." },
  { s: 60.4, e: 64.0, k: 63.8, text: "New rows reach the warehouse. A follow-up to restore BACKWARD waits for Priya." },
  { s: 64.0, e: 67.6, k: 67.4, text: "Shift change. Arjun hands off, and Claude writes the summary from the room log." },
  { s: 67.6, e: 71.2, k: 71.0, text: "What broke, why, what's done, what's ruled out, what's still open, and the risks." },
  { s: 71.2, e: 76.0, k: 75.8, text: "Meera accepts and drives. Priya approves the restore, and the room closes verified." },
];
const FP = { NONE: "205a8899f2dacab0a2af2a9d33b652cd9ba714015d1cce8818d74d988866f485",
             FULL: "9bd1c62c39aa1caa58408624df0b2782c9e30cf8303608c39e1af0cc05e227f1" };
const shortFp = (h) => `${h.slice(0, 4)} ${h.slice(4, 8)} … ${h.slice(-4)}`;
```

Room clock keys (illustrative, linear between keys, shown `HH:MM:SS` in the top-right chip with the label
`room clock · illustrative`): 0 → `09:41:07` · 10.6 → `09:44:30` · 38 → `09:47:00` · 50 → `09:49:00` · 60.4 →
`09:51:00` · 64 → `10:00:00` · 76 → `10:05:00`.

POV keys (chip `Viewing as <name>`, top-right above the clock): 0 Arjun · 26 Kabir · 33.2 Arjun · 50 Priya · 53.4 Arjun
· 70.6 Meera. Only the POV person's cursor is ever on screen (G12).

State light keys (G7; rim colour of every figure on stage): 0 `--state-idle` · 4.4 `--state-alert` · 12 `--state-agent`
· 48.4 `--state-need` · 60.4 `--state-ok` · 64 `--state-idle` · 74.6 `--state-ok`.

### 4.6 Storyboard (76 s)

Every chapter starts with a 0.9 s chapter bug in the top-left (`0N · TITLE`, Geist Mono 12px, a `--lamp` underline
draws under it), then the bug stays at 60% opacity. Typing is 40 ms/char with a blinking caret unless stated.
Lower thirds: 3px layer-colour bar wipes in, then name (Archivo 640 18px, uppercase) + role line (Geist Mono 11px),
on a `--ui-card` plate at 92%; in from the left 240ms, out 200ms.

#### Chapter 1 · A room opens · 0:00–0:12 (set: prologue)

Prologue set: dark stage with a faint 24px dot grid; three app-styled fragments at three depths
(`perspective: 1200px`, translateZ −120 / 0 / +80). Its camera trucks between fragments (x translate) and dollies (scale).

| t | Camera | On screen | Caption |
|---|---|---|---|
| 0.0–0.4 | fade up from black | room clock `09:41:07`; bug `01 · A ROOM OPENS` | — |
| 0.4–3.4 | fragment A, scale 1.0 → 1.06 | **Fragment A "app migration"** (mono card) types `ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2);`, then `ALTER TABLE`, then `INSERT 0 1` twice | cue 1 |
| 3.4–4.4 | truck to fragment B (1.0 s) | **Fragment B "Schema Registry"**: `POST /subjects/pg.public.orders-value/versions` | cue 2 |
| 4.4–6.6 | hold, scale 1.04 | stamps in: `409 Conflict · MISSING_UNION_BRANCH`; rows `compatibility BACKWARD`, `amount: int → bytes(decimal 12,2)` (the diff row gets a `--tally` underline) | cue 2 |
| 6.6–7.4 | truck to fragment C | **Fragment C "Kafka Connect"**: `pg-orders-connector` with a green `RUNNING` chip | cue 3 |
| 7.4–9.2 | rack focus: `RUNNING` blurs to 3px while `task 0  FAILED` sharpens and blinks red twice | rows type: `slot debezium_orders active=false · retained WAL growing`; `analytics.orders · last new row 0m → 3m ago` | cue 3 |
| 9.2–10.6 | pull back, scale 0.9; fragments dim | alert chip centre: `connector_down · sev2 · orders` + `watcher ✓` with a `--tally` halo; plate bottom-right (G11): `watcher mode: rooms · times illustrative`; **Arjun** enters from the right edge, half-body in his wheelchair, rim-lit alert red, head turning to the chip | cue 4 |
| 10.6–11.4 | **circle wipe** (`clip-path: circle()` from the chip centre) | the world at WIDE: header, `SEV2`, `alert · watcher ✓`, status `Investigating · step 1`, agent pill `Agent working`; lane: Connector `failed`, Schema subject `degraded`, Source `degraded`, Sink `degraded`, Topic and Consumer `unknown` | cue 4 |
| 11.4–12.0 | WIDE | avatars pop into the header 80ms apart: Arjun (ringed, `Arjun · driving`), Priya, Kabir, Meera; lower third **ARJUN** `ON CALL · DRIVING · OWNS KAFKA-CONNECT, KAFKA` (to 13.2) | cue 4 |

#### Chapter 2 · Claude investigates · 0:12–0:26 (set: world)

| t | Camera | On screen | Caption |
|---|---|---|---|
| 12.0–13.2 | WIDE → STEPS (1.2 s) | **First look** card slides in: `First look · triage, unverified · 2 reads` — "Task 0 of pg-orders-connector failed on a Schema Registry error." | cue 5 |
| 13.4–16.0 | STEPS | lower third: lens glyph + **CLAUDE** `READ-ONLY AGENT`; `s_1` "Check the Debezium connector" → `connect_status` `connector=pg-orders-connector`, spinner → check; result streams line by line in the `tool output · untrusted` block | cue 5 → 6 |
| 16.0–19.2 | STEPS, feed → `#w-s2` | `s_2` `schema_registry_versions` → `versions [1] … compatibility BACKWARD`; `s_3` `schema_registry_diff` → `amount: int → bytes(decimal 12,2)`; status `Investigating · step 3` | cue 6 |
| 19.2–22.4 | STEPS, feed → `#w-s4` | `s_4` `kafka_consumer_lag` → `order-sync: Stable, 1 members, total lag 0`; lane Consumer → `healthy` with the app's `heal` tint (600ms) and a `ruled out` tag; `s_5` `pg_recent_ddl` → `ALTER TABLE public.orders: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)` | cue 7 |
| 22.4–24.6 | STEPS, feed → `#w-s6` | `s_6` `what_changed` → **What changed** fills Before/After: `09:41 · DDL ALTER TABLE orders … numeric(12,2) · 90 s before the alert`, `schema version`; s_1's untrusted block gets a brief hatched-border highlight | cue 8 |
| 24.6–26.0 | STEPS → LANE (0.9 s) | lane: Source `degraded` (Priya), Connector `failed` (Arjun), Schema subject `degraded` (Priya), Consumer `healthy` (Kabir), Sink `degraded` (Meera) | cue 8 |

#### Chapter 3 · Everyone watches, one drives · 0:26–0:38

| t | Camera | On screen | Caption |
|---|---|---|---|
| 26.0–27.0 | LANE → THREAD (1.0 s), feed → `#w-s1`; POV `Viewing as Kabir` | — | cue 9 |
| 27.0–29.4 | THREAD | Kabir's cursor (lime, label `Kabir`) glides to s_1's comment icon, click ripple; bubble types "Wrong service? I'd expect Connect to be down, not the schema."; lower third **KABIR** `WATCHING · OWNS THE CONSUMER` | cue 9 |
| 29.4–30.6 | THREAD | agent reply in the thread (violet edge): "Connect is up; task 0 is failed on a Schema Registry compatibility error." | cue 10 |
| 30.6–32.4 | THREAD → COMPOSER (0.9 s) | composer shows "Suggest a first step…" + **Suggest**; types "Check the connector config first, not the topic."; cursor clicks **Suggest** | cue 10 |
| 32.4–33.2 | COMPOSER → RAIL (0.8 s); cursor fades out | the suggestion lands in **Suggestions**: `Kabir · Check the connector config first, not the topic.` with **Send to agent** and **Decline** | cue 10 |
| 33.2–35.4 | RAIL; POV `Viewing as Arjun` | Arjun's cursor (amber) fades in, clicks **Send to agent** (ripple); row status `Sent to agent`; Activity adds `09:46 Arjun sent Kabir's suggestion`; composer reads "You are driving · Lease ends in 28m — any action renews" | cue 11 |
| 35.4–38.0 | RAIL → STEPS (1.0 s) | agent message: "Connector config is unchanged; the failure is the value schema subject." | cue 11 |

#### Chapter 4 · A fix is proposed · 0:38–0:50

| t | Camera | On screen | Caption |
|---|---|---|---|
| 38.0–39.1 | STEPS → HYPO (1.1 s) | **match cut**: chips `s_1 s_3 s_5 s_6` lift out of the stream and fly into the new **Hypothesis** card's `based on` row | cue 12 |
| 39.1–41.6 | HYPO | `Hypothesis` · `high confidence` · path draws `low 09:45 → high 09:47` · text types (25 ms/char) "A schema change to orders.amount broke the pg-orders-connector on a Schema Registry incompatibility; no new rows reach the warehouse." · each cited chip flashes `cite` in turn | cue 12 |
| 41.6–42.7 | HYPO → PROPOSAL (1.1 s) | agent message: "Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value. I've written a fix proposal." | cue 13 |
| 42.7–45.6 | PROPOSAL | **Proposals** card builds: title types "Set subject compatibility to NONE and restart the connector task"; `Step 1 · schema_set_compatibility · schema-registry` + rows; `Step 2 · connect_restart_task · kafka-connect` + rows; `Rollback: set compatibility back to BACKWARD` | cue 13 |
| 45.6–46.5 | PROPOSAL → PRINT (0.9 s) | — | cue 14 |
| 46.5–48.4 | PRINT | **Fingerprint**: hex scrambles 500ms, settles left to right into `205a 8899 … f485`; label `Approving exactly this` | cue 14 |
| 48.4–50.0 | PRINT → PROPOSAL (1.0 s) | decision bar "Needs your approval · schema-registry · kafka-connect"; agent pill `Agent waiting for approval`; agent message "The fix proposal is waiting for the schema-registry and kafka-connect owners." | cue 14 |

#### Chapter 5 · Every owner approves · 0:50–1:04

| t | Camera | On screen | Caption |
|---|---|---|---|
| 50.0–51.0 | → OWNERS (1.0 s); POV `Viewing as Priya` | decision bar "Needs your approval · schema-registry"; owner row: `schema-registry · Priya` (dashed), `kafka-connect · Arjun` (dashed); lower third **PRIYA** `OWNS POSTGRES, SCHEMA-REGISTRY` | cue 15 |
| 51.0–53.4 | OWNERS | Priya's teal cursor clicks **Approve**; her ring closes clockwise (700ms), check pops; header agent pill `Agent waiting · 1 of 2 approvals`; bar "Waiting for kafka-connect · 1 of 2 approved"; audit row `09:49 Priya approved · schema-registry · 205a…f485` | cue 15 |
| 53.4–55.0 | OWNERS; POV `Viewing as Arjun` | Arjun's amber cursor clicks **Approve and run**; his ring closes; audit row `09:49 Arjun approved · kafka-connect · 205a…f485`; a bracket joins the two fingerprints, mono label `same fingerprint` | cue 16 |
| 55.0–56.8 | OWNERS, scale 1.9 → 1.7 | padlock shackle lifts 4px and rotates −20°; **border beam** runs once round the proposal card (1.4 s, `--lamp`); header plate (G11) `write mode: on (demo)` appears and stays to 64.0 | cue 16 |
| 56.8–57.8 | → EXEC (1.0 s) | status `Executing fix · 1 of 2`; bar "Running step 1 of 2"; line `✓ pg.public.orders-value compatibility BACKWARD → NONE` | cue 17 |
| 57.8–60.4 | EXEC | `✓ restarted pg-orders-connector task 0 (RUNNING)`; status `Fix ran · verifying`; three verify checks tick: `connect_status · task 0 RUNNING`, `schema_registry_versions · compatibility NONE`, `warehouse_freshness · new rows in analytics.orders` | cue 17 |
| 60.4–62.6 | → LANE (0.9 s) | lane heals left to right (`heal`, 150ms apart): every node `healthy`; status `Fix ran · recovered` | cue 18 |
| 62.6–64.0 | LANE → WIDE (1.2 s) | room message: "Verification passed: pg-orders-connector task 0 is RUNNING and new rows reached analytics.orders. pg.public.orders-value compatibility is still NONE; restore BACKWARD with a separate fix." A follow-up card slides into Proposals: `Restore BACKWARD compatibility · waiting for schema-registry` | cue 18 |

#### Chapter 6 · Shift change · 1:04–1:16 (sets: world → handoff → world)

| t | Camera | On screen | Caption |
|---|---|---|---|
| 64.0–64.9 | WIDE → HEADER (0.9 s) | — | cue 19 |
| 64.9–66.0 | HEADER | Arjun's cursor presses **Hand off** (keycap `H` depresses), picks Meera; status "Writing hand-off…" with the app's `shimmer` | cue 19 |
| 66.0–66.7 | **dissolve** to the hand-off set: the world blurs 6px and dims while the hand-off card grows from the header to centre | hand-off set (§3.6 composition at stage scale): Arjun left under a dusk window, lamp on, tag `Arjun · local 18:00`; Meera right under a dawn window, tag `Meera · local 07:00`; lower third **MEERA** `NEXT SHIFT · OWNS THE WAREHOUSE` | cue 19 |
| 66.7–70.6 | hold, set scale 1.0 → 1.04 | card header `Shift 1 summary · from Arjun to Meera`; sections appear 0.6 s apart: **What broke · Why · Done · Ruled out · Still open · Risks** (text from §3.6); then `Checked against the room log · no issues` with a check pop | cue 20 |
| 70.6–72.2 | hold; POV `Viewing as Meera` | Meera's periwinkle cursor presses **Accept** (ripple at 71.0); the `--lamp` driver ring travels the arc from Arjun to Meera (71.0–72.2); Arjun's lamp fades to 30%; tags `Meera · driving`, `Arjun · watching` | cue 21 |
| 72.2–73.0 | **dissolve** back to the world, WIDE | header avatars: `Meera · driving`; Arjun unringed | cue 21 |
| 73.0–74.6 | WIDE | Priya's ring closes on the follow-up `Restore BACKWARD compatibility`; line `✓ compatibility BACKWARD`; resolution line in the header: "ALTER on orders.amount rejected by Schema Registry (BACKWARD); set NONE, restarted task 0, data flowing, BACKWARD restored." | cue 21 |
| 74.6–76.0 | WIDE, scale 1.0 → 0.96 | status `Resolved · verified` with `heal` tint; **Post-mortem draft** and **Export JSON** buttons get one `--lamp` outline flash; clock `10:05`; 300ms fade to the end card at 75.7 | cue 21 |

### 4.7 Engine

- One clock `t` (seconds). Playing: `t += min(dt, 0.1) × speed` per `requestAnimationFrame`; at `t ≥ 76` clamp, pause,
  show the end card. Paused: `t` changes only by seek, keys, chapters or `__explainerSeek`.
- `render(t)` is pure: every visible property derives from `t`. Data-driven tracks:
  `{ el, prop, keys: [[t, value, ease?], …] }` where `prop ∈ x | y | scale | rot | opacity | type | dash | clip |
  state | text | path | blur`. `type` reveals `data-text.slice(0, n)`; `state` sets `data-state` (lamps, pills, rings,
  nodes); `text` swaps strings (pills, bars, clock); `path` sets `offset-distance` %. Values hold before the first and
  after the last key. Eases `lin | out | cam | pop`, evaluated in JS with a small cubic-bezier solver.
- Per frame only `transform`, `opacity`, `filter`, `clip-path`, `offset-distance`, `data-state` and `textContent`
  change; text is written only when it differs.
- Sets outside their window get `visibility: hidden`: prologue 0–11.4, world 10.6–66.7 and 72.2–76, handoff 66.0–73.0.
- People life (only while playing): blink when `((t × 1000 + offsetPerPerson) % 4300) < 110` (eyes `scaleY(.1)`),
  head tilt ±1.5° sine at 0.15 Hz.
- Captions: the cue containing `t` is written into `#fp-cc` only when it changes; empty between cues.
- UI sync per frame: time label, slider `aria-valuenow` / `aria-valuetext` (`"0:34, chapter 3, Everyone watches, one
  drives"`), chapter `aria-current` + progress fill, nav progress line.
- Pause the rAF loop when the stage is off-screen (IntersectionObserver) or `document.hidden`.
- **QA hooks (required):**

```js
window.__explainerDuration = DURATION;          // 76
window.__explainerSeek = (seconds) => {          // exact frame, paused, poster hidden, in every motion mode
  pause(); hidePoster(); hideEndIfBefore(76);
  t = Math.max(0, Math.min(DURATION, Number(seconds) || 0));
  render(t, { exact: true }); syncUI(t);
};
```

---

## 5. Illustration system

### 5.1 People

- **Cast** (the product's demo users; owners from `dev/owners.yaml`): **Arjun** (driver; kafka-connect, kafka),
  **Priya** (postgres, schema-registry), **Kabir** (consumer; watcher), **Meera** (warehouse; next shift). A person's
  colour is their story layer, used for clothes, cursor, ring and lower-third bar: Arjun `--l-connect`, Priya `--l-sr`,
  Kabir `--l-consumer`, Meera `--l-wh`.
- **Line language:** flat fills, one 1.5px outline in `--fig-line` (inside `.stage` always `#07080B`), one shade tone
  per fill (`color-mix(in oklab, <fill> 84%, black)`) on neck, ears and the side away from the light. In dark contexts
  (the stage, dark page theme) a 1.5px **rim light** on the screen-facing side in the current state colour (G7) at 70%;
  in the light page theme no rim, a soft ground-shadow ellipse instead.
- **Faces:** two 1.5px dot eyes, a short bent-stroke nose, an optional 5px neutral mouth line at 70%. No smiles, no
  teeth, no blush, no stock poses. Warmth comes from context: names, roles, cursors, comments, local time.

| | Skin | Hair | Distinguishing | Clothes |
|---|---|---|---|---|
| Arjun | `--skin-arjun` | short crop, side part, trimmed full beard, `--hair-dark` | broad jaw; **manual wheelchair** (push handles behind the shoulders, wheel rim and tyre at the bottom edge, `--metal` / `--tyre`) | amber hoodie with drawstrings |
| Priya | `--skin-priya` | long, centre part, falling behind the shoulders, `--hair-brown` | round glasses | teal collared shirt |
| Kabir | `--skin-kabir` | tight curls (7 circles), `--hair-dark` | over-ear headphones in lime | lime crew-neck |
| Meera | `--skin-meera` | grey chin-length bob, `--hair-grey` | small hoop earrings in `--lamp`; two fine smile-line ticks (older) | periwinkle cardigan over a dark top |

- **Head avatar** `<symbol id="av-<who>" viewBox="0 0 64 64">`, used at 20–72px. Arjun's (the template):

```svg
<symbol id="av-arjun" viewBox="0 0 64 64">
  <path d="M8 64c1-12 11-19 24-19s23 7 24 19z" fill="var(--l-connect)"/>
  <path d="M27 38h10v9c-3 2-7 2-10 0z" fill="color-mix(in oklab, var(--skin-arjun) 84%, black)"/>
  <ellipse cx="19.5" cy="28.5" rx="2.4" ry="3.4" fill="var(--skin-arjun)"/>
  <ellipse cx="44.5" cy="28.5" rx="2.4" ry="3.4" fill="var(--skin-arjun)"/>
  <ellipse cx="32" cy="27" rx="12.5" ry="14.5" fill="var(--skin-arjun)" stroke="var(--fig-line)" stroke-width="1.5"/>
  <path d="M19.5 25c-.5-9 5.5-14.5 12.5-14.5 8 0 13 5 12.8 13.5-2.6-3.6-7.4-6-13.3-6-4.6 0-8.8 2.4-12 7z" fill="var(--hair-dark)"/>
  <path d="M20.5 30.5c.6 8.2 5.6 12.5 11.5 12.5s10.9-4.3 11.5-12.5c-2.2 3.8-6.3 5.6-11.5 5.6s-9.3-1.8-11.5-5.6z" fill="var(--hair-dark)"/>
  <circle class="eye" cx="27.2" cy="27.5" r="1.5" fill="var(--fig-line)"/>
  <circle class="eye" cx="36.8" cy="27.5" r="1.5" fill="var(--fig-line)"/>
  <path d="M32 28.5v4.2h1.6" fill="none" stroke="var(--fig-line)" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>
  <path class="rim" d="M41 17c3 3 4.5 6.5 4.5 10.5" fill="none" stroke="var(--rim, transparent)" stroke-width="1.5" stroke-linecap="round"/>
</symbol>
```

  If `color-mix()` inside an SVG `fill` attribute is not supported by the target browser, set it via a CSS rule on a
  class instead (`.shade-arjun { fill: color-mix(…) }`); never a literal.
  Variants change only hair and accessories: Priya adds back hair behind the head
  (`M18 30c-1-12 5-19 14-19s15 7 14 19l2 20H16z`), a centre-part fringe and glasses (two 9×6.4 rounded rects, rx 2.6,
  stroke 1.3, bridge 1.2); Kabir: 7 circles r 4–4.6 over the crown + headphone band
  (`M17 30c0-12 6.5-19.5 15-19.5S47 18 47 30`, stroke 3, `--l-consumer`) and two 6×10 cups (rx 2.5); Meera: a bob
  framing the face to the jaw, earrings (r 1.6, stroke 1.2), two 3px ticks at the outer eye corners.
- **Avatar rings:** 2px `--surface` gap + 2px ring. Driver = `--lamp` on the page, `--ui-primary` inside product
  mocks (the app's driver ring). Owner pending = dashed `4 4` in the layer colour; approved = solid + 14px check badge.
- **Half-body figure** `<symbol id="fig-<who>" viewBox="0 0 200 220">`, used at 120–260px: the head via `<use>` at
  2.2×, torso `M40 220c2-42 26-66 60-66s58 24 60 66z` in the layer colour, two forearms resting on a laptop (base
  `M58 176h84l10 36H48z`, lid back `M64 120h72l-4 56H68z` in `--surface-2`, 1px `--line` edge, no logo); a radial
  `--lamp-glow` screen spill on the face. Arjun's figure adds the wheelchair: two push-handle strokes rising behind the
  shoulders (`M44 150v-26h-6` and mirror, stroke 3, `--metal`) and a wheel arc at the bottom-left
  (`M30 220a46 46 0 0 1 40-44`, stroke 6 `--tyre`, inner stroke 2 `--metal`). Two poses per person, swapped, never
  morphed: *watching* (both hands on the desk) and *acting* (one hand forward to the trackpad).
- **Props:** desk lamp (base ellipse, two-segment arm, cone shade; light = a blurred `--lamp-glow` cone whose opacity
  is the "lamp on" value); window (frame 10px in `--surface`, 1.5px outline, one mullion, sky gradient from the sky
  tokens via `<linearGradient>` stops styled by CSS); sun disc; mug. 3–5 shapes each, same outline.
- **Claude** is never a person, robot, face, sparkle or logo: a 28px rounded square in `--ui-agent-subtle` with a 1.5px
  `--ui-agent` stroke containing a **lens** (circle r 5 + 4px handle). Label `Claude · read-only`.
- **Cursors:** 16px SVG arrow in the person's colour, 1.5px `--fig-line` outline, name pill in their colour with
  `#0B0C10` text; click = a ring scaling 0.6 → 1.6 while fading (350ms). One cursor visible at a time (G12).
- **Diversity check** on every group of four: skin from light-medium to deep, four hair silhouettes, ages (Meera is
  grey and older), accessories (glasses, headphones, earrings, beard), one wheelchair user. Never one face recoloured.

### 5.2 Product UI mocks

- HTML/CSS, never SVG text. Tokens `--ui-*` and layer colours only; radius 8 cards / 6 controls / 4 chips; 1px
  `--ui-border`; `--ui-shadow`; Geist 12/13/14/16/20; Geist Mono for tool names, params, hashes, times.
- Icons: inline SVG in Lucide style (viewBox 24, stroke 1.75, round caps): check, x, lock, unlock, lens, message, user,
  play, pause, captions, maximize, copy, sun, moon, arrow-right. No icon fonts, no emoji.
- Status semantics: node `healthy` `--ui-success`, `degraded` `--ui-warning`, `failed` `--ui-danger`, `unknown`
  `--ui-muted-fg`; agent always violet; layer chips in layer colours; `Approve` buttons `--ui-success-solid`; `Steer`
  `--ui-primary`.
- Only real strings (facts.md §2 and §5, plus those verified in §0) except the illustrative ones listed in §8.
- No cost figures, no durations in ms, no confidence percentages.

---

## 6. Motion plan

### 6.1 Premiere ("Lights down", page load, 2.4 s, once)

Everything is readable from first paint (nav, H1, lead, buttons, poster). The premiere only adds light:

1. 0–900ms: projector beam opacity 0 → 1; the frame's top inner highlight brightens; the H1 tally full stop pulses once
   (`--tally-glow` 0 → 1 → 0).
2. 400–1800ms: one `--lamp` border beam traces the player frame once (`@property --beam { syntax: '<angle>';
   inherits: false; initial-value: 0deg }`, a `conic-gradient(from var(--beam), transparent 0 80%, var(--lamp) 90%,
   transparent)` ring on `::before`, masked to 1px with `mask-composite: exclude`).
3. 900–1500ms: the poster's play ring pulses once; the scrim fades out (600ms) and the tilt flattens to 0° as the film
   starts at 0:00.

Skipped under reduced motion, when the stage is < 50% visible at load, or when the tab is hidden; in those cases the
poster stays.

### 6.2 Approval climax (§3.5, user-triggered, ≈ 4 s)

| ms from click | What happens |
|---|---|
| 0 | button press `translateY(1px)`; Arjun's ring turns solid and closes clockwise (`stroke-dashoffset`, 320ms); check pops (`--ease-pop`, 200ms); title-block cell `KAFKA-CONNECT Arjun · approved 09:49` |
| 200 | amber segment fills (240ms); label `2 of 2 approved` |
| 380 | padlock shackle lifts 4px and rotates −20° (240ms); button becomes `Approved` with a check |
| 560 | **one border beam** round the proposal sheet (900ms, `--lamp`); in light theme a 1px bronze line with no glow |
| 700 | bar `Running step 1 of 2`; pill `Executing fix · 1 of 2`; executor line 1 types (300ms) |
| 1300 | line 2; bar `Fix ran · 2 of 2 steps`; pill `Fix ran · verifying` |
| 1900 | lane heals left to right (`heal`, 150ms apart) |
| 2800 | pill `Fix ran · recovered`; bar `Verified · recovered`; verification message fades in; follow-up chip fades in |

Reduced motion: end state in one frame.

### 6.3 Other motion (each with one job, once)

- Investigation: one emphasis sweep on first view; Replay on request; `cite` tint on chip hover/focus; agent message
  typing after Send to agent.
- Approval: level change scrambles the hash 400ms and unwinds Priya's ring 500ms; owner rings' slow dashed spin while
  pending (stops when the section is off-screen).
- Hand-off: baton travel 900ms on Accept; Arjun's lamp fades 600ms.
- Trust: each visual once on first view; T3 scramble again on hover/focus.
- Integrations: one pulse that stops at Schema Registry.
- Micro: buttons lift 1px on hover, `scale(.98)` on press (120ms); nav underline 200ms; theme toggle sun ↔ moon (rotate
  90°, 240ms) and a 200ms colour transition on `background-color, color, border-color` only; Copy → Copied for 2 s;
  chapter progress fill.

### 6.4 Rules

- Never animate content from `opacity: 0` on scroll; in-view triggers replay emphasis from a visible rest state.
- Loops (pending-ring spin, people blinks, poster ring) run only while their element is on screen and, for the film,
  only while playing. No parallax, no scroll-jacking, no marquee, no continuous beams.
- Reduced motion (`matchMedia('(prefers-reduced-motion: reduce)')`, read at each use and listened to): no premiere, no
  autoplay, no loops, no beams, no typing, no blur, no smooth scroll; interactive demos still change state, instantly.
  CSS mirror: `@media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: .001ms
  !important; animation-iteration-count: 1 !important; transition-duration: .001ms !important; scroll-behavior: auto
  !important; } }`.

---

## 7. Light and dark

- **Dark (primary): the screening room.** Blue-black `#090A0E`; surfaces step up in lightness, each with a 5% white
  top highlight; the champagne Lamp is the only warm light (beam, playhead, driver ring, CTA). Tally red at most once
  per screen. Figures read as cut-outs with a state-coloured rim. Section mocks use the app's dark tokens.
- **Light: the matinee.** Cool porcelain `#F3F4F7`, pure-white cards with real two-layer shadows; glows become shadows
  plus a faint warm tint; Lamp turns bronze `#7E5E22` for text and strokes; the CTA becomes an ink button with
  warm-white text; Tally deepens to `#C42A1A`. **The film stays dark** (`--stage #0F1116`, dark UI tokens inside),
  framed by a crisp shadow like a lit screen in a bright room. The "How it works" stills also stay dark (they are film
  frames). Section mocks (investigation, approval, hand-off, shadow) switch to the app's light tokens, so the page
  shows the app in both themes. Page figures get ink outlines and a ground shadow, no rim.
- Theme mechanics: both toggles set `document.documentElement.dataset.theme` to `light` or `dark` and store it in
  `localStorage` (`piperoom-theme`) inside try/catch. The first statements of the script (right after `PILOT_CONTACT`)
  read the stored value inside try/catch and set `data-theme`; with no stored value the OS preference applies through
  the media query. The toggle's `aria-label` names the theme it will switch to.

---

## 8. Build checklist

**Contract**
- [ ] File starts `<title>Piperoom</title>`, then `<meta name="claude:viewport-fit-sides" content="cover">`, then the
      four font tags, `<style>`, markup, `<script>`. No `<!doctype>`, `<html>`, `<head>`, `<body>`.
- [ ] No external scripts, images, video, iframes, Lottie, or `fetch()`. Stylesheets only from fonts.googleapis.com.
- [ ] First script line: `const PILOT_CONTACT = "";`.
- [ ] Every colour is a custom property; bare `:root` complete (dark); light block under the media query **and** under
      `:root[data-theme="light"]`; `body` background from `--bg`. Grep the CSS for `#` and `rgb(` outside token blocks:
      zero hits except inside the stage's re-declared dark UI block and the sprite's `data:` grain.
- [ ] Gutter on `.page` only with the two `max()` paddings; vertical spacing via `padding-block`; sticky nav uses
      `top: env(safe-area-inset-top, 0px)` and the `body::before` band.
- [ ] Hero height from content; no `100vh`; nothing waits at `opacity: 0` for an observer.
- [ ] No `alert`/`confirm`/`prompt`, `window.print`, `window.open`, mailto or tel.
- [ ] Every `localStorage` access in try/catch; page works without it.
- [ ] Size ≤ 230 KB target, 250 KB hard (CSS ≈ 40, sprite ≈ 25, world + sets ≈ 55, sections ≈ 65, JS ≈ 45). Avatars and
      figures are `<symbol>`s reused with `<use>`; path data to one decimal.

**Film**
- [ ] `[data-explainer]` on `#film`; `window.__explainerDuration === 76`; `__explainerSeek(s)` pauses, hides the
      poster and renders the exact frame in every motion mode.
- [ ] Seek frames each look composed and legible at 1440px and 390px:
      `0, 3.3, 7.8, 11.8, 15.0, 22.4, 29.4, 33.0, 37.8, 41.4, 47.5, 53.2, 55.8, 61.0, 67.4, 71.6, 75.9`.
- [ ] Keyboard: Space/K, ←/→, J/L, Home/End, 1–6, C, F; slider arrows; chapter buttons; visible focus on all.
- [ ] Captions in `#fp-cc` with `aria-live="polite"`, one write per cue; CC off keeps the live region.
- [ ] Reduced motion: no autoplay, poster at rest, Play steps through cue key frames without tweening, transcript open.
- [ ] Autoplay pauses off-screen and on hidden tab; Resume pill only for auto-pauses.
- [ ] Portrait rig below 600px: 4:5 stage, portrait world layout, captions below the stage, no text under 10px.

**Sections**
- [ ] Investigation: Send to agent / Decline / Reset work; `cite` highlight on chips; Replay; 43 read chips wrap.
- [ ] Approval: rest state R at first paint without JS; level change → V with `9bd1 c62c … 27f1` and the 409 note;
      Undo; write switch relabels the button; approve → climax (on) or A-off; Reset; fingerprint recomputed in the
      browser equals `FP.NONE` / `FP.FULL`.
- [ ] Hand-off: Accept / Not yet / Reset; baton via `offset-path`; reduced motion jumps.
- [ ] Form: required-field errors and focus; summary text exactly as specified; Copy with clipboard and select-fallback;
      Edit answers keeps values; send line follows `PILOT_CONTACT` ("We'll share the address with you." when empty).

**Layout and theme**
- [ ] 390px: `document.documentElement.scrollWidth === clientWidth` in both themes; H1 wraps by word
      (`Claude / investigates. / Your team / decides.`); chips and shelves wrap; title block cells stack.
- [ ] Both themes at 1440px and 390px look designed: light sections use light UI mocks; film and stills stay dark.
- [ ] Contrast ≥ 4.5:1 for text and ≥ 3:1 for UI strokes in both themes.

**Accessibility**
- [ ] All controls are `<button>` (links only navigate); every form control has a stable id and `<label for>`;
      `fieldset`/`legend` for checkbox groups; `role="switch"` + `aria-checked` on write mode; radiogroup on level.
- [ ] Decorative SVG `aria-hidden="true"`; meaningful SVG (owner rings, padlock state) has `<title>`; static mocks and
      stills are `role="img"` with an aria-label; the world is `aria-hidden` and the transcript carries the story.
- [ ] Every element closed; every attribute double-quoted.

**Honesty sweep (search the final file)**
- [ ] No logos, testimonials, "trusted by", customer or user counts, MTTR or accuracy numbers, cost figures, compliance
      badges or awards. Search for: `trusted`, `customers` (only "has no customers yet" allowed), `SOC`, `ISO`, `HIPAA`,
      `%` in copy, `faster`, `self-healing`, `auto-remediation`, `autonomous`, `submitted`, `we'll be in touch`.
- [ ] Watcher described as needing rooms mode; write mode as off by default wherever a fix runs; Slack outbound only;
      hand-off started by the driver and accepted by the next person; redaction partial; "no data loss" never said;
      not open source said once (FAQ); Claude only.
- [ ] Every clock time sits under an "illustrative" label (credits line, poster, film clock chip, hand-off plate, T4
      caption).
- [ ] Illustrative strings the product does not print, and only these: the First look verdict text; `Declined`;
      `void · fingerprint changed`; `Arjun · watching`; "Meera is driving."; the local times and skies; "Restore BACKWARD
      compatibility" as a loose title; the title-block labels; the page's fingerprints and export hash (each labelled
      in fine print); the activity-feed times; `room clock 10:00 UTC`.
- [ ] Vendor names appear as text only; no logo or logo-like drawing (no elephant, no Kafka mark, no flake, no
      Anthropic mark).
