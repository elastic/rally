# Track race integration tests

This directory holds integration tests that run **Rally `race` inside Docker** (via `esrally.utils.compose`) against an Elasticsearch node from the bundled Compose stack. They exercise many **official / default Rally tracks** across multiple Elasticsearch versions.

## Contents

- **`race_test.py`** — defines `TrackCase` entries and `test_race_with_track`, parametrized over tracks and over Elasticsearch versions. Module-scoped fixtures build the Rally image and configure Compose; each test starts `es01` with the requested `ES_VERSION`, runs `rally race` toward `es01:9200`, and tears Elasticsearch down afterward. Version list and timeouts are driven by `conftest.py` (see below).
- **`conftest.py`** — registers pytest options, parametrizes the `elasticsearch` fixture indirectly, optionally filters tests by track name, and stashes per-race timeout after collection.
- **`helpers.py`** — small pure functions for parsing CLI/env (shared with unit tests).
- **`TRACK_RACE_EXECUTION_FINDINGS.md`** — notes from Docker IT runs (failures, timeouts, environment). Some `TrackCase` rows set `skip_reason_by_es_version` (map of ES version string to reason) so known-broken `(track, ES version)` pairs are skipped with an explicit message until fixed.

## Prerequisites

Same as the rest of the `it/` suite: Docker and Docker Compose available, with the daemon running. Other integration checks run from [`it/conftest.py`](../conftest.py) (for example `docker ps` / `docker compose version`).

## How to run

- Full integration test run (includes these tests): `make it`
- Only this folder: `uv run -- pytest -s it/tracks/` or `make it_tracks` (see `Makefile`; extra pytest args via `ARGS=...`)

## Environment variables

| Variable | Purpose |
| -------- | ------- |
| `IT_TRACKS_NO_SKIP` | If truthy (`1`, `true`, `yes`, `on`, case-insensitive), do **not** skip tests that would be skipped only because of `TrackCase.skip_reason_by_es_version`. Does **not** affect `@pytest.mark.skip` or other skips. |
| `IT_TRACKS_ES_VERSIONS` | Comma-separated ES image tags for `es01` (e.g. `8.19.13,9.2.7`). Default when unset: `8.19.13` and `9.2.7`. |
| `IT_TRACKS_TIMEOUT_MINUTES` | Total wall-clock **minutes** budget for **all** selected `test_race_with_track` runs before dividing by `N` (default `120`). Overridden by `--it-tracks-total-timeout-minutes` when that flag is passed. |
| `IT_TRACKS_NAME` | Comma-separated [`fnmatch`](https://docs.python.org/3/library/fnmatch.html) patterns matched against **`TrackCase.track_name`** only. If set, tests whose `track_name` matches **no** pattern are **deselected** (removed from the run and from `N`). Patterns are **OR**-ed: `geo*,elastic/*` keeps a case if either pattern matches. |

## Pytest CLI flags (same semantics as env, where applicable)

| Flag | Notes |
| ---- | ----- |
| `--it-tracks-no-skip` | Same as truthy `IT_TRACKS_NO_SKIP`. **Enabled if the flag is set *or* the env is truthy.** |
| `--it-tracks-es-versions` | Comma-separated list; overrides `IT_TRACKS_ES_VERSIONS` when non-empty. |
| `--it-tracks-total-timeout-minutes` | Integer; overrides `IT_TRACKS_TIMEOUT_MINUTES` when passed. |
| `--it-tracks-name` | Non-empty value overrides `IT_TRACKS_NAME` for the name filter. |

**Precedence:** for timeout and name filter, **non-empty CLI wins**, else env, else defaults (no name filter; timeout default 120 minutes).

## Per-race timeout and `N`

After collection (including `-k` / keyword deselection and the track-name filter), let **`N`** be the number of collected **`test_race_with_track`** items that **actually run** `rally race` for timeout purposes:

- With **`IT_TRACKS_NO_SKIP` / `--it-tracks-no-skip` off** (default): exclude items that would **`pytest.skip`** because `TrackCase.skip_reason_by_es_version` has an entry for the parametrized Elasticsearch version (same condition as in `race_test.py`: `.get(version) is not None`).
- With **no-skip on**: every collected race item counts toward **`N`**.

Then each race subprocess uses:

**`race_timeout_s = (total_timeout_minutes × 60) / max(1, N)`**

So the total budget is split across races that are expected to execute the subprocess, not across parametrized rows that will skip immediately. If **no** items count (`N == 0`), `max(1, N)` avoids division by zero (the full budget is stashed but unused).

If a race hits that subprocess timeout without another failure, the test treats it as success (see `race_test.py`).

## Track-name filter and pytest reporting

Filtering uses `fnmatch` on **`track_name`** (e.g. `elastic/*` matches `elastic/security`, not arbitrary node id text). Deselected items are reported via pytest’s **`pytest_deselected`** hook, so collect-only output looks like **`8/76 tests collected (68 deselected)`** rather than implying only eight tests exist in the tree.

## Rally container cleanup

`compose.run_service(..., remove=True)` (used by `rally_race`) wraps `docker compose run` in **`try` / `finally`**. After the run (including on **`subprocess` timeout**), it runs **`docker compose kill`** on the Rally service, then **`docker compose ps -a -q`** and **`docker rm -f`** on those IDs.

**Why not `compose rm --stop`?** Docker Compose’s **`rm`** and **`stop`** commands **ignore one-off containers** created by **`docker compose run`** (internal `oneOffExclude` filter), so they never matched `…-rally-run-…` containers. **`compose kill`** includes one-offs, so orphaned run containers are actually stopped and removed when the test driver times out or exits before `run --rm` runs. Teardown is best-effort (`check=False` / logged warnings). The persistent `es01` service is still torn down with **`remove_service`** in the `elasticsearch` fixture only.

## Examples

```bash
# Only geonames-like tracks (geonames, geoshape, …) across default ES versions
uv run -- pytest it/tracks/ --it-tracks-name='geo*'

# Single ES version from env
IT_TRACKS_ES_VERSIONS=8.19.13 uv run -- pytest -s it/tracks/

# Force-run cases that would skip for known Docker issues
IT_TRACKS_NO_SKIP=1 uv run -- pytest -s it/tracks/

# Shorter total budget (per-race timeout is total_min*60/N)
uv run -- pytest it/tracks/ --it-tracks-total-timeout-minutes=60
```
