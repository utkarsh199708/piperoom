# piperoom.ai landing page

The public website for Piperoom: one static page with a 76-second animated explainer of the
`schema-change` incident, served from Cloudflare Workers (static assets, no Worker code).

| Path | What |
|---|---|
| `public/index.html` | The built page. This is what gets deployed. Do not edit it by hand. |
| `src/*.html` | The editable sources: `a-head` (tokens and CSS), `a2-css`, `b-sprite` (SVG symbols), `c-hero` (nav, hero, film player), `d-sections`, `e-script` (all JS, including the film's `CHAPTERS`/`CUES`). |
| `src/build.mjs` | Builds `public/index.html` from the sources. |
| `src/shoot.mjs` | Screenshots at desktop and phone widths, light and dark, plus film frames (needs Playwright + Chromium). |
| `design/` | The design spec and the product fact sheet every claim on the page was checked against. |
| `wrangler.jsonc` | Cloudflare config: serves `public/` on `piperoom.ai` and `www.piperoom.ai`. |

## Change the page

```sh
# edit site/src/*.html, then:
node site/src/build.mjs
node site/src/shoot.mjs site/public/index.html /tmp/shots   # optional visual check
```

Keep the page under 250 KB, and keep every product claim inside `design/facts.md`: no customers,
metrics or badges the product does not have.

## Deploy

Needs the `piperoom.ai` zone in the Cloudflare account, and either the Cloudflare connector or a
`CLOUDFLARE_API_TOKEN` with Workers Scripts edit and DNS edit on that zone.

```sh
cd site && npx wrangler deploy
```

## Before going public

- The pilot form has no destination yet: `PILOT_CONTACT` at the top of the script in
  `src/e-script.html` is empty, so the form tells applicants "We'll share the address with you."
  Set it to the address applications should go to (for example a `pilot@piperoom.ai` route set up
  with Cloudflare Email Routing), rebuild and redeploy.
