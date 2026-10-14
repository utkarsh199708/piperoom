# F7 — A lane that fits, a trace that leads with the verdict, a mobile that decides (A6; model: Sonnet 5.5 — layout and pure helpers; Opus 5.5 only if F7c's tab refactor of `RoomScreen` grows past 400 lines)

Read `AGENTS.md`, `docs/collab.md`, `docs/product/vision-2026-10.md` (§3 D3, §4 F7) and
`docs/design/ui-v2/project/Room-Investigating.dc.html` (lane, rail), `Components.dc.html` (PipelineLane, Sheet).
You own `web/**` only. Design on top of merged #146 (TracePanel placeholders, header `Export` button,
`handoffBusy`), #135 (`RoomHeader` verified marks), #125 (`Jump to latest` button at `fixed bottom-36 right-4 …
xl:right-[324px]`, rail `SuggestionsPanel`/`NotifyToggle`), #126 (`ShadowPanel` replaces the composer on closed
shadow rooms) and F1b (`DecisionBar` with a `placement` prop, `--composer-h`).

## Goal
The symptom is the one thing the lane must show, and today the failing sink is off-screen with no cue. The lane
fits the viewport, the worst node is in view on first paint, the trace says where the record stopped before you
read a single stage, and below 1280 px the room still leads with the decision.

## Evidence
- Lane overflow: `scrollWidth` 1196 vs `clientWidth` 852 at 1440 px, only 3 of 5 nodes visible, the failed sink
  hidden (`oncall-walkthrough.md` journey 4 and §3 item 2, screens `04-room-no-driver` vs `A1-lane-scrolled-right`);
  `PipelineMap.tsx:154-160` `overflow-x-auto` with no affordance; nodes `min-h-[92px] min-w-[200px]` (`:48`), DLQ node
  always drawn (`:171`); the lane spends ~190 px of height before the first step (`ux-critique.md` item 11).
- Trace overflow: 1080 px of stages in a 718 px dialog, the `stopped here` stage hidden (`oncall` journey 12,
  screens `10b` vs `A3`); `TracePanel.tsx:85-90` `overflow-x-auto`, centred `Dialog` (`:52-53`, board says Sheet);
  button text `Trace one record` (`PipelineMap.tsx:148`, board: `Trace a record`).
- Mobile: header stack ≈ 280 px (`screens-ux/41`), key caps shown on touch (`ui/kbd.tsx`), `Take control` 26 px
  (`RoomList.tsx:654` `h-[26px]`), the approval two screens down (`ux-critique.md` items 1, 12); the rail stacks under
  the column below `xl` (`RoomScreen.tsx:221-236`), so People/Activity push the composer and proposals out of reach.

## Scope — what to build

### F7a — The lane (M)
1. **Compact strip.** `PipelineMap.tsx`: `Node` gets `compact: boolean`. Compact = `h-14` (56 px) `min-w-[168px]`
   `px-3 py-1.5`, two rows: `{KIND_LABEL} · <Badge layer>` (row 1, `text-xs`) and `<name mono truncate> <StatusDot>
   {word} <first fact mono>` (row 2). Expanded = today's 92 px card. The lane heading gets a ghost `h-7` button
   `Expand` / `Compact` (`aria-pressed`) and `Enter`/`Space` on a focused node toggles the whole lane; the choice is
   kept in `sessionStorage` `piperoom.lane.expanded.<pipeline_id>` (default compact). `EDGE`/`EDGE_STALLED`
   `mt-[38px]` becomes `mt-[27px]` in compact (the middle of 56 px) — compute the class from `compact`.
2. **DLQ node only when it matters.** A `dlq_topic` node is drawn only when `facts.records` parses to > 0 or its
   `health !== 'ok'` (`internal/topology/dlq.go:122,136` sets `records`); otherwise the heading shows a muted
   `DLQ empty` chip. Edge drawing skips hidden nodes.
3. **Worst node in view.** `lib/lane.ts` (new): `worstNode(nodes): TopologyNode | null` (rank `fail` > `warn` >
   `unknown` > `ok`, first by lane order on a tie). On first topology paint and whenever the worst node's id
   changes, scroll the **lane scroller only** (never `scrollIntoView`, which also moves the page):
   `scroller.scrollLeft = node.offsetLeft - (scroller.clientWidth - node.offsetWidth) / 2`, `behavior` `auto` under
   reduced motion, else `smooth`. Keep a `data-worst` attribute on that node for tests.
4. **Edge fades and `+N →`.** Wrap the scroller in `relative`; two `pointer-events-none absolute inset-y-0 w-8`
   overlays (`bg-gradient-to-r from-background to-transparent` / `bg-gradient-to-l`) shown when `scrollLeft > 0`
   and when `scrollLeft + clientWidth < scrollWidth - 1` (state from a `scroll` listener + `ResizeObserver`,
   `useLaneOverflow(ref)` in `lib/lane.ts`'s sibling hook `hooks/useLaneOverflow.ts`). A pill button on the right
   fade `+{N} →` (`N` = nodes whose `offsetLeft + offsetWidth > scrollLeft + clientWidth`), `aria-label="Show {N}
   more nodes"`, click scrolls by `0.8 * clientWidth`; mirror `← +N` on the left. The group keeps `tabIndex=0`
   so `←/→` scroll natively.
5. **Click filters the stream.** Each node becomes a `<button aria-pressed>` (`title="owner: …"` kept). `PipelineMap`
   gets `selectedLayer?: string` and `onSelectLayer?: (layer: string | null) => void`. `RoomScreen` holds
   `layerFilter` and passes `filterLayer` to `Timeline`, which shows steps where
   `step.tool_calls.some(t => toolLayer(t.tool) === layer)` (`lib/toolLayers.ts`). Above the list:
   `Showing {k} {k===1?'step':'steps'} on <Badge layer> · [Clear]` (`role="status"`); zero matches: `No steps on
   {layer} yet · [Clear]`. Esc with the filter on and no dialog open clears it (add to `room/useRoomShortcuts.ts`
   as `clearFilter?`). The filter never hides the live step (`working && last`): it is appended with a muted
   `(live)` tag.
6. Rename the button to `Trace a record` (`PipelineMap.tsx:148`); icon `size-3.5` like the other `h-7` ghosts.

### F7b — The trace (S)
7. `lib/trace.ts` (new): `traceVerdict(trace: TraceState): { tone: 'fail' | 'ok' | 'warn' | 'muted'; text: string }`:
   first `stopped_here` stage → `Stopped at {layer} {name} — {k}={v}` with the stage's first fact (omit the dash
   when no facts), tone fail; no stop and the last stage `seen` → `Reached {name} — every stage saw key {key}`, ok;
   no stage `seen` → `Not seen anywhere — no stage holds key {key}`, warn; otherwise (`unknown` stages) →
   `Partly traced — {n} stages could not be read`, muted. Rendered in `TracePanel.tsx` **above** the stages as
   `<p role="status" class="text-sm font-medium">` with a `StatusDot`, then the `table · key` line.
8. Stages wrap: replace `flex … overflow-x-auto` (`TracePanel.tsx:85-90`) with
   `grid grid-cols-[repeat(auto-fill,minmax(160px,1fr))] gap-2`; the arrow between stages goes, each card gets a
   `{i+1}` mono index (`aria-label="Stage {i+1} of {n}"`) so order survives wrapping; the `stopped_here` card keeps the
   only coloured border and gets `data-worst`.
9. Sheet, not centred dialog: `DialogContent className="left-auto right-0 top-0 h-full max-h-none w-full max-w-md
   translate-x-0 translate-y-0 rounded-none border-l data-[state=open]:slide-in-from-right data-[state=closed]:slide-out-to-right
   data-[state=open]:zoom-in-100 data-[state=closed]:zoom-out-100 overflow-y-auto"` (tailwindcss-animate is already a
   dependency). Esc, X and focus restore unchanged. Below `md` the sheet is full width.

### F7c — Responsive: tabs, bottom sheet, touch (M)
10. **Tabs below 1280.** `RoomScreen.tsx`: `useMediaQuery('(min-width: 1280px)')` (new `hooks/useMediaQuery.ts`,
    `matchMedia` absent → true, as `AppShell.useIsDesktop`). Wide: today's two-column layout. Narrow: a `Tabs`
    (`ui/tabs.tsx` line variant, `role=tablist`, sticky under the decision bar) with `Investigation` · `Fix {open}`
    · `People {n}` · `Activity`; panels: Investigation = lane + hypothesis + timeline; Fix = `ProposalsPanel` +
    `ProposalsSummary`; People = `PresencePanel`; Activity = `SuggestionsPanel` + `NotifyToggle` + `ActivityFeed`.
    Tab ids `tab-investigation` etc.; the chosen tab is kept in `sessionStorage` `piperoom.tab.<roomId>`; `View steps ↓`
    from the decision bar switches to `Fix` first, then scrolls. The composer and the decision bar are outside the
    tabs and always visible. Panels stay mounted (`forceMount` + `hidden`) so the comment boxes keep their text.
11. **Bottom sheet below 768.** `DecisionBar` `placement="sheet"` (F1b exported the prop): a `fixed inset-x-0
    bottom-0 z-30 border-t bg-card shadow-lg px-4 pt-3 pb-[max(env(safe-area-inset-bottom),12px)]` panel (no focus
    trap, no overlay), one text row and two full-width `h-11` buttons `[Reject] [Approve {layer}]`; `waiting`/
    `executing`/`done` render as a one-row strip at the same place. The composer collapses to one line above it
    (F1b idle height) and `--composer-h` includes the sheet (`SteerBox` and the sheet both write their heights;
    `main` uses the sum via a second var `--sheet-h`). The #125 `Jump to latest` button moves up by both.
12. **Header actions collapse.** `room/RoomHeader.tsx`: below `md` the `Hand off`, `Close room`, `Export` (#146) and
    invite buttons move into a `DropdownMenu` behind a `⋯` icon button (`aria-label="More actions"`); items keep
    `aria-keyshortcuts` and the keys still work (`useRoomShortcuts` is independent of the buttons). The title row
    becomes `title · SEV · status`, the facts line hides the room id below `sm`.
13. **Touch.** `tailwind.config.ts`: `addVariant('coarse', '@media (pointer: coarse)')` and `addVariant('fine',
    '@media (pointer: fine)')`. `index.css`: `@media (pointer: coarse) { kbd[data-slot='kbd'] { display: none } }` with
    an opt-in utility `.kbd-visible kbd { display: inline-flex }` for the keyboard cheat-sheet (later). Apply
    `coarse:min-h-11` to: `RoomList.tsx:654` Take control (`h-[26px]` → `h-[26px] coarse:h-11`), `NoDriverBanner`
    button, `HandoffAcceptBanner` buttons, `LayerRow` Approve/Reject (`size="sm"` → `coarse:h-11 coarse:px-4`),
    `SteerBox` ghost buttons, `DoneStep` rows (`min-h-9` → `coarse:min-h-11`), `ActivityFeed` `Show all events`,
    `ProposalsSummary` cards, lane nodes (56 px already ≥ 44), tab triggers (`h-9` → `coarse:h-11`).

## Out of scope
Flow-rate on edges and healed-node flash (F4), evidence renderers (F3), a vertical lane on phones (keep the
horizontal strip; 390 px shows ~2 compact nodes with fades), keyboard cheat-sheet overlay, pipelines page.

## Contract
none.

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `web/src/lib/lane.ts`, `lib/trace.ts`, `hooks/useLaneOverflow.ts`, `hooks/useMediaQuery.ts` | A6 | new |
| `web/src/components/PipelineMap.tsx`, `TracePanel.tsx`, `Timeline.tsx`, `RoomScreen.tsx`, `RoomList.tsx`, `NoDriverBanner.tsx`, `HandoffDialog.tsx`, `ProposalsPanel.tsx`, `SteerBox.tsx`, `ActivityFeed.tsx`, `components/room/{RoomHeader,DecisionBar,useRoomShortcuts,ProposalsSummary}.tsx`, `components/ui/kbd.tsx`, `tailwind.config.ts`, `index.css` | A6 | modified |

## Dependencies and merge order
F7a and F7b need nothing and can start now. F7c needs **F1b merged** (`DecisionBar`, `--composer-h`, idle
composer) and should follow F5a (it moves `SuggestionsPanel` into a tab). Three PRs; F7c may split into F7c-1
(tabs + header `⋯`) and F7c-2 (sheet + touch) if over 600 lines. F4 later restyles the lane edges; keep the
`EDGE*` constants exported.

## Design details
- `worstNode` runs on the *rendered* list (after the DLQ rule) so it never targets a hidden node. The scroll runs in
  a `useLayoutEffect` keyed on `worst?.id` and `compact`.
- Overflow state is derived, never stored in React state on every scroll tick: throttle with
  `requestAnimationFrame` in `useLaneOverflow`.
- The layer filter is UI state only (no URL, no storage); it resets on `roomId` change (F10a keys `RoomScreen`).
- Untrusted text: node names, facts, stage names and keys are text nodes; `traceVerdict` clips facts to 64 chars.
- a11y: fades are `aria-hidden`; the `+N →` pill is the accessible control; nodes are buttons with `aria-pressed`;
  verdict `role="status"`; tabs follow the Radix roving tabindex; the sheet is not modal (no trap), its buttons
  are reachable in DOM order right after the composer. Light/dark: gradients use `from-background`.
- Motion: lane scroll and sheet slide 150–240 ms, `motion-safe:` only; reduced motion → instant.
- Failure modes: `ResizeObserver` missing (old jsdom) → fades off; `sessionStorage` throws → defaults; topology
  `null` → skeleton strips (compact height); `trace.stages` empty → verdict `Not seen anywhere`.

## Done when
- [ ] At 1440×900 the schema-change fixture lane shows the failed sink in view on first paint, right fade with
      `+2 →` when clipped, and the heading `Expand` toggles to 92 px cards.
- [ ] Clicking the `kafka-connect` node shows `Showing 2 steps on kafka-connect · Clear`; Esc clears.
- [ ] The DLQ node is absent while `records=0`, present when the fixture sets `records=3`.
- [ ] Trace result starts with `Stopped at consumer order-sync — committed=48212` and all stages are visible
      without horizontal scrolling at 718 px; the panel slides in from the right.
- [ ] At 1024 px the rail is four tabs; at 390 px a proposal that needs me shows a bottom sheet with
      `[Reject] [Approve kafka-connect]` and no key caps anywhere; every row button is ≥ 44 px tall under
      `pointer: coarse`; no horizontal page scroll.
- [ ] `npm run build && npm run lint && npx vitest run` green; no new dependency.

## Tests (Vitest)
- `lib/lane.test.ts`: `worstNode` ranking and tie order; DLQ visibility rule.
- `components/PipelineMap.test.tsx`: compact by default and `sessionStorage` restore; `data-worst` on the failed
  node; `+N →` label with a mocked `clientWidth`/`scrollWidth`; node click calls `onSelectLayer`, `aria-pressed`.
- `components/Timeline.test.tsx`: `filterLayer` shows only matching steps, live step kept with `(live)`, status line copy.
- `lib/trace.test.ts`: four verdict branches; fact clipping.
- `components/TracePanel.test.tsx`: verdict first in DOM order; stage index labels; `Trace a record` heading.
- `components/RoomScreen.layout.test.tsx`: `matchMedia` mocked to 1024 → tablist with four tabs, panels mounted
  hidden; 390 → sheet placement; `View steps` switches to `Fix`.
- `components/room/RoomHeader.test.tsx` (new): below `md` the actions are in `More actions`.
- `tailwind`/`kbd`: a `kbd.test.tsx` that renders `<Kbd>` and asserts the `data-slot` the CSS targets.

## Effort
F7a M · F7b S · F7c M (split as above if needed).
