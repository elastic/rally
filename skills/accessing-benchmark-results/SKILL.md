---
name: accessing-benchmark-results
description: >-
  Retrieve Rally benchmark results from an external Elasticsearch metrics store.
  Use to list past races, get a single race's overall (per-task) results, chart
  a metric's trend across multiple runs, compare two races, or check whether a
  run converged — e.g. "show me recent geonames races", "what's the service_time
  trend for nyc_taxis over the last 30 days?", "compare these two race-ids".
  Applies when datastore.type = elasticsearch is set in ~/.rally/rally.ini.
---

# Accessing Rally benchmark results

Rally can persist every race to a dedicated Elasticsearch **metrics store** (set
`datastore.type = elasticsearch` in the `[reporting]` section of `~/.rally/rally.ini`;
see `docs/configuration.rst`). This skill covers reading that data back — overall results,
trends across runs, comparisons, and convergence checks.

The metrics store is a separate cluster from the one under test. Point queries at the
metrics store host from `[reporting]`, never at the benchmark target.

**Check for an existing report first.** If the race was launched via the
`running-benchmarks` skill with `--report-file` (e.g. `~/benchmarks/result.md`), the
summary report for that single race is already saved on disk — read that file instead of
re-querying. Use this skill's queries for anything the saved report doesn't cover: trends
across runs, comparisons, convergence, or ad-hoc aggregations.

## When to use

- Browsing past races for a track or time range.
- Getting one race's overall per-task results (throughput, latency, service_time).
- Charting how a metric moved across runs (regression / improvement detection).
- Comparing two specific races to quantify a change.
- Checking whether a run's metrics stabilized (converged) over its duration.

## Where the data lives

By default (`datastore.use_data_streams = true`) Rally writes to three data streams;
`rally-races-v1`, `rally-results-v1`, and `rally-metrics-v1` (detailed below). If you set
`datastore.use_data_streams = false`, Rally writes to monthly indices instead
(`rally-races-YYYY-MM`, `rally-results-YYYY-MM`, `rally-metrics-YYYY-MM`). Querying with a
wildcard (`rally-races-*`, `rally-results-*`, `rally-metrics-*`) covers both layouts.

| Stream / index | One doc per | Contains |
|---|---|---|
| `rally-races-v1` | race | race metadata: track, challenge, car, timestamps, user tags |
| `rally-results-v1` | task+metric | aggregated results — the summary-report numbers |
| `rally-metrics-v1` | sample | raw time-series samples + node/GC/segment metrics |

## Key fields (`docs/metrics.rst`)

`race-id` (UUID grouping a run), `race-timestamp`, `@timestamp` (epoch ms per sample),
`relative-time` (ms since race start), `track`, `challenge`, `car`, `environment`,
`sample-type` (`normal` = measured samples, `warmup` = warmup-phase samples), `name` (metric name, e.g.
`service_time`), `value`, `unit`, `task`, `operation`, `operation-type`, and `meta.*`
(host/CPU/OS info, `source_revision`, `distribution_version`, and user tags stored as
`meta.tag_<key>`).

Metric direction: for `latency`, `service_time`, `processing_time`, and GC time, **lower
is better**; for `throughput`, **higher is better**.

## Prefer the built-in commands

Two operations need no query — Rally reads the configured datastore directly:

```bash
# List past races (filter by track and/or user tags); note the race-ids.
esrally list races --track=pmc --user-tags="intention:baseline-8.x"

# Quantify the difference between two races.
esrally compare --baseline=<race-id> --contender=<race-id>
```

See `docs/tournament.rst`. Reach for direct queries below when you need trends,
aggregations, or fields these commands don't expose.

## Direct queries

Run these against the **metrics store** cluster with any Elasticsearch client, `curl`, or
Kibana Dev Tools. Build complex request bodies programmatically (e.g. a Python dict +
`json.dumps`) rather than hand-writing nested heredocs.

### List recent races for a track

```json
GET rally-races-*/_search
{ "size": 20, "sort": [{"race-timestamp": "desc"}],
  "query": {"bool": {"filter": [{"term": {"track": "pmc"}}]}} }
```

### Get one race's overall results (per task)

Task and operation names are track-specific — discover them with `esrally info --track=<track>`.
Omit the `task` filter to return every task.

```json
GET rally-results-*/_search
{ "size": 200,
  "query": {"bool": {"filter": [
    {"term": {"race-id": "<uuid>"}},
    {"term": {"task": "<task>"}}
  ]}} }
```

### Trend of a metric across runs

One row per race; expand `size`/range as needed.

```json
GET rally-metrics-*/_search
{ "size": 0,
  "query": {"bool": {"filter": [
    {"term": {"track": "geonames"}},
    {"term": {"task": "<task>"}},
    {"term": {"name": "service_time"}},
    {"term": {"sample-type": "normal"}},
    {"range": {"@timestamp": {"gte": "now-30d"}}}
  ]}},
  "aggs": {"by_race": {
    "terms": {"field": "race-id", "size": 100, "order": {"ts": "asc"}},
    "aggs": {
      "ts": {"min": {"field": "@timestamp"}},
      "pcts": {"percentiles": {"field": "value", "percents": [50, 90, 99]}}
    }}} }
```

### Compare two races by query

When you want more than `esrally compare` shows, pull each race's per-task stats and diff
them client-side. For latency/service_time a positive delta is a regression; for throughput
a negative delta is a regression.

```json
GET rally-metrics-*/_search
{ "size": 0,
  "query": {"bool": {"filter": [
    {"terms": {"race-id": ["<baseline-uuid>", "<contender-uuid>"]}},
    {"term": {"name": "service_time"}},
    {"term": {"sample-type": "normal"}}
  ]}},
  "aggs": {"by_race": {"terms": {"field": "race-id", "size": 2},
    "aggs": {"by_task": {"terms": {"field": "task", "size": 100},
      "aggs": {"pcts": {"percentiles": {"field": "value", "percents": [50, 90, 99]}}}}}}} }
```

### Convergence / stability check

Use this to check whether a race reached steady state before you trust its numbers. The
query splits one task's `service_time` samples into up to ~60 time buckets and reports p50/p90/p99
for each. If those percentiles level off toward the end of the run, the workload converged and
the results are stable; if they keep drifting, the numbers are unreliable — rerun with a longer
warmup or more iterations.

```json
GET rally-metrics-*/_search
{ "size": 0,
  "query": {"bool": {"filter": [
    {"term": {"race-id": "<uuid>"}},
    {"term": {"name": "service_time"}},
    {"term": {"task": "<task>"}}
  ]}},
  "aggs": {"over_time": {
    "auto_date_histogram": {"field": "@timestamp", "buckets": 60},
    "aggs": {"pcts": {"percentiles": {"field": "value", "percents": [50, 90, 99]}}}
  }} }
```

### Filter by a user tag

Tags passed via `--user-tags` are stored under `meta.tag_<key>`. Add to any `filter`:

```json
{"term": {"meta.tag_intention": "baseline-8.x"}}
```

## Notes

- Exclude warmup with `{"term": {"sample-type": "normal"}}` for reported metrics — unless
  you specifically want to study cold-start behaviour, in which case keep the `warmup` samples.
- `task` is unique per invocation of an operation; `operation` may repeat. `task`/`operation`
  are only set on `latency`/`throughput`/`service_time` samples.
- Node-level metrics (GC, segments, store size) are only present when Rally provisioned the
  cluster or the `node-stats` telemetry device was enabled (`docs/telemetry.rst`).
