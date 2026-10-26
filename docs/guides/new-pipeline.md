# Set up a new pipeline with the setup runner

> [!NOTE]
> Preview. The setup-mode agent is landing in the U wave (Jira epic KAN-5). The setup
> room, the typed operations the runner executes and the approval model are described in
> [`docs/design/setup-agent.md`](../design/setup-agent.md).

Start with the setup runner. `piperoom-setup` is the one binary that runs the setup agent on your
machine: with `--standalone` it starts Piperoom in-process for one person (in-memory rooms, dev login
on 127.0.0.1, write mode and setup on), prints the URL to open, and stays up as the pipeline's
watcher once the pipeline is live.

```sh
mkdir -p orders                 # --project must name an existing directory
piperoom-setup --standalone --project ./orders
```

`--project` is the directory the run writes to: `owners.yaml` (a bootstrap file on a first start, the
live pipelines after setup), `setup-log.jsonl` (every room event, one JSON object per line) and
`.piperoom/` (the secret store). Use an empty directory for a new pipeline. Later,
`piperoom-setup --standalone --watch --project ./orders` restarts the server whenever the setup room
writes `owners.yaml` (`--watch` needs `--standalone`: the standalone server is the one that restarts).
Set `ANTHROPIC_API_KEY` for the agent and have Docker running (Docker Desktop in Linux-container
mode) for a Compose target.

## Getting the binary

The binary comes from this checkout (`go build -o bin/piperoom-setup ./cmd/setup`) or from the pilot
image, which ships it as `/piperoom-setup` for runner mode:

```sh
read -rs PIPEROOM_SETUP_CODE && export PIPEROOM_SETUP_CODE   # paste the pairing code
docker run --rm -it -e PIPEROOM_SETUP_CODE --mount type=volume,source=piperoom-orders,destination=/work \
  --entrypoint /piperoom-setup piperoom:pilot \
  --server <origin> --project /work
```

The pairing code is single-use; passing it through the environment keeps it out of shell history,
`ps` and `docker inspect`. The named volume keeps `owners.yaml`, `setup-log.jsonl` and the secret
store after the container exits; a tmpfs mount or no mount loses them.

`--standalone` is for your own machine, because it insists on a loopback address, and the image sets
`PIPEROOM_HTTP_ADDR=0.0.0.0`, so it refuses there by design.

## What the runner may do

Commands run through the runner, never on the server: it executes only typed operations from an allow-listed
catalogue, where your Docker, `kubectl` and cloud credentials already are. The server never holds them. Every
artifact that executes (compose files, unit files, manifests) is templated or validated, never free text, and
the runner refuses any operation above read-only (T1 and up) without an approved proposal, like a fix in an
incident room. The full rules are in
[`AGENTS.md`](../../AGENTS.md) (U wave) and [`docs/design/setup-agent.md`](../design/setup-agent.md).
