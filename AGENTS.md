# AGENTS.md — Working with Rally

Guidance for agents operating Rally (`esrally`), the macrobenchmarking framework for
Elasticsearch. It sets up clusters, runs benchmarks (races), records results, and compares them.

The `docs/*.rst` files and the [user guide](https://esrally.readthedocs.io/) are the source
of truth — prefer reading them or running `esrally <subcommand> --help` over guessing flags.

## Vocabulary

- **race** — one benchmark execution, identified by a unique `race-id` (UUID).
- **track** — a workload + dataset (e.g. `geonames`, `pmc`, `nyc_taxis`).
- **challenge** — a named schedule of tasks within a track.
- **task** — a scheduled step within a challenge that runs one operation; unique per invocation, so the same operation scheduled multiple times yields distinct tasks.
- **operation** — the action a task runs (e.g. a bulk index or a search query), defined in the track.
- **car** — an Elasticsearch node configuration. Only relevant when Rally provisions the cluster.
- **pipeline** — what happens *around* the benchmark (provisioning/teardown), not the benchmark itself.
- **telemetry device** — an optional probe that records extra metrics during a race.
- **metrics store** — where Rally persists results: in-memory by default, or a dedicated Elasticsearch cluster.

## Prerequisites & hard rules

- Rally runs on Unix (Linux/macOS). Needs Python 3.10–3.13, git 1.9+.
- **Never run Rally as root** — Elasticsearch refuses to start with root privileges.
- **Never benchmark a production or production-like cluster.** Rally creates and mutates
  indices, and any competing traffic invalidates results. Use a dedicated, quiet environment.
- Config lives in `~/.rally/rally.ini`; cached data/artifacts under `~/.rally/benchmarks/`.
- Do not invent flags. Verify against `docs/command_line_reference.rst` or `--help`.

## Skills

Task workflows live in `skills/`:

- `skills/running-benchmarks/` — launch a race against a cluster (pipelines, target hosts,
  TLS/API-key auth, tags, test mode) and interpret the summary report.
- `skills/accessing-benchmark-results/` — list past races, get a race's overall results,
  chart a metric across runs, compare races, and check convergence from an external metrics store.

## Where to look next

- Run benchmarks: `docs/race.rst`, `docs/pipelines.rst`
- Recipes (existing cluster, Elastic Cloud, multi-cluster, distributed load): `docs/recipes.rst`
- Serverless: `docs/serverless.rst`
- Config & metrics store: `docs/configuration.rst`, `docs/metrics.rst`
- Report interpretation: `docs/summary_report.rst`
- Comparing races: `docs/tournament.rst`
- Full CLI reference: `docs/command_line_reference.rst`
- Authoring tracks: `docs/adding_tracks.rst`, `docs/track.rst`
