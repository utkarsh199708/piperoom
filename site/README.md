# piperoom.ai landing page

`public/index.html` is the landing page (from the claude.ai artifact
https://claude.ai/artifact/EW85VXKRoEQe13v9rMKDAc, with the claude.ai frame runtime removed).
It is served by a Cloudflare Worker with static assets, configured in `wrangler.jsonc`.

## Deploy

One-time setup in Cloudflare:

1. Add `piperoom.ai` as a zone in your Cloudflare account (nameservers pointed at Cloudflare).
2. Create an API token with **Workers Scripts: Edit**, **Workers Routes: Edit** and **Zone: DNS: Edit**
   on the `piperoom.ai` zone, plus **Account Settings: Read**.
3. In GitHub → Settings → Secrets → Actions, add `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.

After that, every push to `main` that touches `site/**` deploys (workflow `.github/workflows/site.yml`);
it can also be run by hand from the Actions tab.

From a machine instead:

```
cd site
npx wrangler@4.148.0 login
npx wrangler@4.148.0 deploy
```

Preview locally: `npx wrangler@4.148.0 dev` (serves on http://localhost:8787).

## The pilot form

The page sends nothing: the form builds a summary to copy. To show a contact address under it,
set `PILOT_CONTACT` at the top of the page's script in `public/index.html`.
