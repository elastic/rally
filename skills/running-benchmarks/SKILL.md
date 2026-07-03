---
name: running-benchmarks
description: >-
  Run Rally benchmarks (races) against Elasticsearch — an existing/external
  cluster or a Rally-provisioned distribution — and read the summary report.
  Use when launching a race, benchmarking a cluster, choosing a pipeline,
  track, or challenge, setting target hosts or TLS/API-key auth, tagging runs,
  saving reports, or interpreting throughput, latency, and service_time results.
---

# Running Rally benchmarks

Launch a race against Elasticsearch and interpret its summary report. Rally can run
benchmarks several ways (see [Choose a pipeline](#choose-a-pipeline)).

For **meaningful results**, prefer an existing/external cluster you control: running Rally
locally co-locates the load driver and the system under test on one host, where they can
perturb each other's measurements. Local deployment is a good fit when that's not a
concern — smoke testing, fast iterative local development, or demos.

## Safety

- **Never run Rally as root** — Elasticsearch refuses to start with root privileges.
- **Strongly avoid benchmarking production.** Rally mutates cluster state (creating,
  writing to, and deleting indices), and competing traffic can perturb results. Prefer a
  dedicated, quiet environment.

## Choose a pipeline

Rally infers the pipeline from your flags.

| Pipeline | Use when | Selected by |
|---|---|---|
| `benchmark-only` | **Primary path.** Cluster is already running and you provisioned it. | `--pipeline=benchmark-only --target-hosts=...` |
| `from-distribution` | Rally downloads and runs Elasticsearch locally. Smoke/demo only. | `--distribution-version=X` |
| `from-sources` | Build Elasticsearch from a git revision (CI/dev). | `--revision=...` |

`benchmark-only` trade-off (`docs/pipelines.rst`): because Rally did not provision the
cluster, results are not easily reproducible and Rally cannot gather host-level metrics
(CPU, GC, disk I/O, index size). Treat those numbers as directional.

## Launch a race

1. Discover tracks, then inspect one:

   ```bash
   esrally list tracks
   esrally info --track=pmc
   ```

2. Run against the cluster:

   ```bash
   esrally race --track=pmc --pipeline=benchmark-only \
     --target-hosts=10.5.5.10:9200,10.5.5.11:9200
   ```

3. Secured cluster — TLS + basic auth via `--client-options`:

   ```bash
   esrally race --track=pmc --pipeline=benchmark-only --target-hosts=host:9243 \
     --client-options="use_ssl:true,verify_certs:true,basic_auth_user:'elastic',basic_auth_password:'changeme'"
   ```

   API-key auth (including Elastic Cloud / Serverless — see `docs/serverless.rst`):

   ```bash
   esrally race --track=geonames --pipeline=benchmark-only --target-hosts=${ES_HOST}:443 \
     --client-options="use_ssl:true,api_key:${ES_API_KEY}" --on-error=abort
   ```

4. Record the `race-id` so you can find the results later. Rally assigns a random UUID by
   default; pass your own with `--race-id` to know it up front:

   ```bash
   RACE_ID=$(uuidgen)
   esrally race --track=pmc --pipeline=benchmark-only --target-hosts=host:9200 --race-id="$RACE_ID"
   echo "$RACE_ID"   # note this for `esrally compare` / metrics-store queries
   ```

   If Rally generated the id, recover the most recent one afterwards with `esrally list races`.
   (Optionally add `--user-tags="key:value"` for human-friendly filtering — not required.)
5. Save the report: `--report-file=~/benchmarks/result.md` (add `--report-format=csv` for CSV).
6. Fast setup/smoke check: add `--test-mode` to ingest a tiny slice instead of the full corpus.

Multi-cluster A/B (benchmark-only only; telemetry disabled in this mode): add `--multi-cluster`
with JSON-format `--target-hosts`/`--client-options`. See `docs/recipes.rst`.

Races are long-running (often 20+ minutes) and download large corpora on the first run for a
track. Treat a race as a long job, not a quick command.

## Validate correctness before trusting numbers

Rally does **not** abort on query errors by default — it folds them into the **error rate**
in the summary, which silently skews results.

- Confirm operations return the hits you expect. A mapping mismatch (e.g. `text` vs `keyword`)
  yields 0 hits with no error.
- While validating, use `--on-error=abort` and/or add track-level `assertions` and run with
  `--enable-assertions`.
- A non-zero error rate on a task means those results are suspect. See `docs/recipes.rst`.

## Read the summary report

- **throughput** — operations per second (higher is better).
- **latency** — includes the time a request waits in the queue before Rally sends it.
- **service_time** — request→response only, excludes wait (what most load-test tools
  incorrectly call "latency").
- **processing_time** — includes Rally's client-side overhead; a large gap vs `service_time`
  points to a client-side bottleneck.
- Metrics are reported **per task**; "Cumulative … of primary shards" is not wall-clock time.
- Check **error rate** first as a validity gate.

See `docs/summary_report.rst` and `docs/metrics.rst`.

## References

- Running & pipelines: `docs/race.rst`, `docs/pipelines.rst`
- Recipes (existing cluster, Elastic Cloud, multi-cluster): `docs/recipes.rst`
- Serverless: `docs/serverless.rst`
- Report & metric definitions: `docs/summary_report.rst`, `docs/metrics.rst`
- All flags (do not invent flags; verify here or via `esrally <subcommand> --help`): `docs/command_line_reference.rst`
- Comparing and charting results across past races: the `accessing-benchmark-results` skill
