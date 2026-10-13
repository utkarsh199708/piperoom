# Piperoom UI v2 — design (approved by the owner 2026-10-13)

Linear/Vercel look, dark first with a light theme, built with **shadcn/ui + Tailwind + Radix + Lucide**.
Source: the owner's Claude Design canvas (private). These files are a copy for coders.

Open any `project/*.dc.html` in a browser to read the markup and CSS (the `{{theme}}` placeholder is
`dark` or `light`). They are static references, not code to ship.

| File | What it shows |
|---|---|
| `Main.dc.html` | Rooms home: sidebar, ⌘K search, stat strip, incident table, filters |
| `Room-Investigating.dc.html` | Room while the agent works (Kabir drives, you suggest): header, pipeline lane, hypothesis, step stream, composer, right rail |
| `Room-Approval.dc.html` | Approval moment: "Needs your approval · kafka-connect", steps, params diff, fingerprint, approval matrix |
| `Command-Menu.dc.html` | ⌘K command menu over the room |
| `Sign-In.dc.html` | SSO + dev user picker |
| `Room-NoDriver.dc.html` | No driver banner + Take control |
| `Room-Viewer.dc.html` | Viewer (read-only) |
| `Solo-Confirm.dc.html` | Solo mode typed confirm |
| `Stale-409.dc.html` | Proposal changed (409 params_mismatch) |
| `Room-Investigating-Light.dc.html` | Light theme |
| `Rooms-Empty.dc.html`, `Room-Loading.dc.html` | Empty and loading states |
| `Foundations.dc.html` | Color tokens (dark + light), type (Geist / Geist Mono), spacing, radius, elevation |
| `Components.dc.html` | Component anatomy mapped to shadcn/ui |
| `Handoff.dc.html` | Tokens → shadcn CSS variables + `tailwind.config`, components → shadcn/ui |

Rules that do not change: every feature and role rule in the current app stays (viewer sees no write
controls; solo confirm typed by hand; params_hash sent from what is on screen; 409 stale flow; login error
pages). Contract: `docs/api.md`. Old spec with all states: `reviews/ui-refresh.md` is not in git — use the
current components' tests as the behaviour contract.
