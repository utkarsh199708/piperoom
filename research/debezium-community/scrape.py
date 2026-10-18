#!/usr/bin/env python3
"""Pull every message of the Debezium Zulip streams through the official Zulip API.

Auth: anonymous for web-public streams; otherwise set ZULIP_EMAIL and ZULIP_API_KEY
(your own account, from Zulip > Personal settings > Account & privacy > API key).
The key is read from the environment and never printed or written.

Output: messages.jsonl, one message per line, sender replaced by a salted hash.
Usage: python3 -I scrape.py [--site URL] [--streams dev,community-general] [--out DIR]
       (no --streams = every stream the caller can see)
"""
import argparse, base64, hashlib, json, os, sys, time, urllib.parse, urllib.request, urllib.error

def call(site, path, params, auth):
    url = f"{site}/api/v1/{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "piperoom-research/0.1"})
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = float(e.headers.get("Retry-After") or 5 * (attempt + 1))
                print(f"rate limited, sleeping {wait:.0f}s", file=sys.stderr)
                time.sleep(wait)
                continue
            body = e.read(500).decode("utf-8", "replace")
            raise SystemExit(f"{path}: HTTP {e.code}: {body}")
        except urllib.error.URLError as e:
            if attempt == 5:
                raise SystemExit(f"{path}: {e}")
            time.sleep(2 ** attempt)
    raise SystemExit(f"{path}: gave up after retries")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", default="https://debezium.zulipchat.com")
    ap.add_argument("--streams", default="")
    ap.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--pause", type=float, default=0.5)
    a = ap.parse_args()

    email, key = os.environ.get("ZULIP_EMAIL"), os.environ.get("ZULIP_API_KEY")
    auth = f"{email}:{key}" if email and key else None
    salt = os.urandom(16)  # per run: hashes group a sender's messages, not identify them

    if a.streams:
        streams = [s.strip() for s in a.streams.split(",") if s.strip()]
    else:
        res = call(a.site, "streams", {"include_web_public": "true"}, auth)
        streams = sorted(s["name"] for s in res["streams"])
    print(f"streams: {len(streams)}: {', '.join(streams)}", file=sys.stderr)

    out = os.path.join(a.out, "messages.jsonl")
    total = 0
    with open(out, "w") as f:
        for name in streams:
            anchor, n = "oldest", 0
            while True:
                res = call(a.site, "messages", {
                    "anchor": anchor, "include_anchor": "false" if anchor != "oldest" else "true",
                    "num_before": 0, "num_after": a.batch, "apply_markdown": "false",
                    "narrow": json.dumps([{"operator": "channel", "operand": name}]),
                }, auth)
                msgs = res.get("messages", [])
                for m in msgs:
                    f.write(json.dumps({
                        "id": m["id"], "stream": name, "topic": m.get("subject", ""),
                        "ts": m["timestamp"],
                        "sender": hashlib.sha256(salt + str(m["sender_id"]).encode()).hexdigest()[:12],
                        "content": m["content"],
                    }) + "\n")
                n += len(msgs)
                if res.get("found_newest") or not msgs:
                    break
                anchor = msgs[-1]["id"]
                time.sleep(a.pause)
            print(f"{name}: {n} messages", file=sys.stderr)
            total += n
    print(f"total {total} messages -> {out}", file=sys.stderr)

if __name__ == "__main__":
    main()
