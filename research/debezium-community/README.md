# Debezium community problems vs Piperoom

Scripts that collect public Debezium problem threads and score them against what Piperoom can
diagnose and fix. Standard-library Python 3 only. Not part of the product; nothing here is built or shipped.

## Run (from this folder)

Zulip, no key needed if the streams are public:

    python scrape.py --out .
    python threads.py

If that stops with `HTTP 401`, the streams need a login: set `ZULIP_EMAIL` and `ZULIP_API_KEY`
(Zulip > Personal settings > Account & privacy > API key) and run `scrape.py` again.

Stack Overflow, no key and no account:

    python so_scrape.py --out .

Both write `threads.jsonl`. Label it with `rubric.md`, then `python tally.py labels.jsonl`.

## Do not commit the data

`messages.jsonl`, `threads.jsonl` and `labels.jsonl` hold other people's posts. They are ignored by
the `.gitignore` in this folder. Share them privately (for example Google Drive), never in this
public repo.
