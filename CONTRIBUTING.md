# Contributing to Piperoom

Thanks for helping. Bug reports, fixes, new failure scenarios and docs are all welcome.

## Before you start

- For a bug, open an issue with the steps to reproduce it, what you expected, and what happened.
- For a larger change, open an issue first so we can agree on the approach before you write it.
- To report a security problem, follow [`SECURITY.md`](SECURITY.md) and do not open a public issue.

## Making a change

1. Fork the repo and branch from `main`.
2. Keep each pull request to one change. Under about 600 changed lines is easiest to review, not
   counting generated files and lockfiles.
3. Add or update tests for the behaviour you changed.
4. Run the checks below, then open the pull request and explain what changed and why.

## Checks

| When | Command |
|------|---------|
| While working | `make test` |
| Before opening a pull request | `make ci` |

`make ci` runs the Go checks, the eval validation, and the web lint, build and tests. To run the local
stack, see [Quickstart](README.md#quickstart).

## Repo conventions

Most of Piperoom is written by AI coding agents, and [`AGENTS.md`](AGENTS.md) holds the rules they
follow: folder ownership, contract files, and coding and test standards. Those rules apply to human
contributors too. Changes to a contract file listed there need extra review, so raise them in an issue
first.

## Licensing

Piperoom is licensed under [Apache-2.0](LICENSE). Unless you say otherwise, anything you submit for
inclusion is licensed under the same terms, as section 5 of the license describes.

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
