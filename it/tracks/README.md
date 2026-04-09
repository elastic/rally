# Track race integration tests

This directory holds integration tests that run **Rally `race` inside Docker** (via `esrally.utils.compose`) against an Elasticsearch node from the bundled Compose stack. They exercise many **official / default Rally tracks** across multiple Elasticsearch versions.

## Contents

- **`race_test.py`** — defines `TrackCase` entries and `test_race_with_track`, parametrized over tracks and over Elasticsearch versions. Module-scoped fixtures build the Rally image and configure Compose; each test starts `es01` with the requested `ES_VERSION`, runs `rally race` toward `es01:9200`, and tears Elasticsearch down afterward. Version list and timeouts are driven by `conftest.py` (see below).
- **`conftest.py`** — registers pytest options, parametrizes the `elasticsearch` fixture indirectly, optionally filters tests by track name, and stashes per-race timeout after collection.
- **`helpers.py`** — small pure functions for parsing CLI/env (shared with unit tests).
- **`TRACK_RACE_EXECUTION_FINDINGS.md`** — notes from Docker IT runs (failures, timeouts, environment). Some `TrackCase` rows set `skip_reason_by_es_version` (map of ES version string to reason) so known-broken `(track, ES version)` pairs are skipped with an explicit message until fixed.

## Prerequisites

You need Docker and Docker Compose available, with the daemon running.

When you run the broader **`it/`** tree (for example **`make it`**), pytest loads [`it/conftest.py`](../conftest.py), which checks prerequisites (for example `docker ps` / `docker compose version`) before tests run.

**Track-only runs** (`pytest it/tracks/`, **`make it_tracks`**) use [`pytest.ini`](pytest.ini) in this directory, which sets `confcutdir` here so **parent `it/conftest.py` is not loaded**. Those checks do **not** run automatically; you still need a working Docker/Compose environment.

## How to run

- **Other `it/` integration tests (excludes track races):** `make it` passes `--ignore=it/tracks` (see the `it` target in the root `Makefile`).
- **This directory:** `make it_tracks` (builds the Compose images via `it_tracks_image`, then runs pytest with `-s` and `LOG_CI_LEVEL` logging) or `uv run -- pytest -s it/tracks/`. Extra pytest args: `make it_tracks ARGS='…'`.

Direct `pytest it/tracks/` relies on **`pytest_sessionstart`** / the **`build_rally`** fixture to build the Rally image when needed; **`make it_tracks`** always builds first via `docker compose --file it/tracks/compose.yaml build`, which matches a typical CI-style workflow.

### Parallel by default (pytest-xdist)

[`pytest.ini`](pytest.ini) enables **`-n auto`** and **`--dist loadgroup`** by default. The **`pytest_xdist_auto_num_workers`** hook in `conftest.py` (see pytest-xdist docs) sets the worker count to **the number of configured Elasticsearch versions** (from `--it-tracks-es-versions`, else `IT_TRACKS_ES_VERSIONS`, else defaults)—not host CPU count. Each **`test_race_with_track`** node sets its own **`COMPOSE_PROJECT_NAME`** (a Docker-safe encoding of the pytest **nodeid**) so stacks and host log directories do not clash across parametrized tests. Rally’s **`run_compose`** passes **`docker compose --project-name …`** when that variable is set. Elasticsearch and Rally log files are bind-mounted under **`logs/`** in the repo (gitignored), in a subdirectory layout that mirrors the nodeid (see **Host logs** below). The Rally service in [`compose.yaml`](compose.yaml) uses a fixed **`image: rally-it-tracks-rally:it`** next to **`build:`** so every project references the same local image tag. The xdist **controller** runs **`docker compose build rally` once** in **`pytest_sessionstart`**; workers still invoke the module **`build_rally`** fixture, which is a quick no-op when the image is already built. Race tests are marked with **`pytest.mark.xdist_group`** per ES version **only when** the xdist worker count is **at most** the number of configured ES versions, so **loadgroup** can keep one version lane per worker without capping parallelism. If you pass **`-n N`** with **N greater than that version count**, those marks are omitted so **all N workers** can run races in parallel. **`conftest.py` registers `pytest_collection_modifyitems` with `tryfirst=True`** so those marks are present before xdist rewrites nodeids on workers; otherwise grouping would not apply.

**Opt out of parallelism** (single process, one Docker stack, easier debugging): pass **`-n 0`** (or set distribution off in a way your pytest version supports, e.g. overriding `--dist` if needed).

**Explicit worker count:** e.g. **`-n 4`** uses four workers. With **more workers than ES versions**, per-version **xdist** grouping is disabled automatically so extra workers are not idle; with **at most as many workers as versions**, grouping keeps one version lane per worker where possible. That decision uses **`PYTEST_XDIST_WORKER_COUNT`** on worker processes (pytest-xdist sets it; worker configs clear ``-n``).

**Note:** pytest-xdist’s environment variable **`PYTEST_XDIST_AUTO_NUM_WORKERS`** is **not** consulted for `-n auto` in this tree, because the custom hook runs first and wins (firstresult hook).

With multiple workers, the per-race subprocess timeout is **`(total_timeout_minutes × 60) / max(1, N) × num_workers`** (see below): the same global **`N`** as in serial mode, scaled by the xdist worker count.

## Environment variables

| Variable | Purpose |
| -------- | ------- |
| `IT_TRACKS_NO_SKIP` | If truthy (`1`, `true`, `yes`, `on`, case-insensitive), do **not** skip tests that would be skipped only because of `TrackCase.skip_reason_by_es_version`. Does **not** affect `@pytest.mark.skip` or other skips. |
| `IT_TRACKS_ES_VERSIONS` | Comma-separated ES image tags for `es01` (e.g. `8.19.14,9.3.3`). Default when unset: `8.19.14` and `9.3.3`. |
| `IT_TRACKS_TIMEOUT_MINUTES` | Total wall-clock **minutes** budget for **all** selected `test_race_with_track` runs before dividing by `N` (default `120`). Overridden by `--it-tracks-total-timeout-minutes` when that flag is passed. |
| `IT_TRACKS_NAME` | Comma-separated [`fnmatch`](https://docs.python.org/3/library/fnmatch.html) patterns matched against **`TrackCase.track_name`** only. If set, tests whose `track_name` matches **no** pattern are **deselected** (removed from the run and from `N`). Patterns are **OR**-ed: `geo*,elastic/*` keeps a case if either pattern matches. |
| `IT_TRACKS_LOG_ROOT` | Optional absolute path: base directory for host-mounted logs (default: **`logs/`** under the Rally repo root). Each test writes under a subdirectory derived from its pytest nodeid. |
| `IT_TRACKS_HOST_LOG_DIR` | Set automatically per test (absolute path to the directory containing `es01/` and `rally/` log mounts). Override only for advanced debugging. Before the first test, defaults to `logs/_compose_default` so `docker compose` can parse the file. |

## Host logs (`logs/`)

Integration runs create **`logs/<path derived from nodeid>/`**: the pytest **`::`** after the file becomes a **`/`** segment so paths work in Compose volume specs. When the nodeid file part is only a basename (common with this tree’s **`pytest.ini`** confcutdir), logs are stored under **`logs/it/tracks/<module>/…`** (e.g. `logs/it/tracks/race_test.py/test_race_with_track[es_8.19.14-geonames]/es01` and `…/rally`). Longer nodeids that already include `it/tracks/…` behave the same. Elasticsearch uses **`ES_LOG_STYLE=file`**; Rally’s `~/.rally/logs` is bind-mounted there. The directory **`logs/`** is gitignored. **`COMPOSE_PROJECT_NAME`** is a **Docker-safe** slug derived from the same nodeid (raw pytest nodeids usually contain `/` and `:` and are invalid for `docker compose -p`).

On **`race`** failure, Rally’s **combined stdout+stderr** from `docker compose run` is logged at **ERROR**; pytest is configured (see [`pytest.ini`](pytest.ini)) to capture **INFO** and above so failed tests show useful context in the report.

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

- **Serial** (no xdist, or equivalently a single worker):  
  **`race_timeout_s = (total_timeout_minutes × 60) / max(1, N)`**
- **Parallel** (pytest-xdist with **`-n num_workers`**):  
  **`race_timeout_s = (total_timeout_minutes × 60) / max(1, N) × num_workers`**

The parallel formula uses the **same global `N`** as serial (computed on the controller and sent to workers) so the divisor is not accidentally recomputed from a worker-local subset. So the total budget is split across races that are expected to execute the subprocess, not across parametrized rows that will skip immediately; the **× num_workers** term matches the agreed policy when multiple races run at once. If **no** items count (`N == 0`), `max(1, N)` avoids division by zero (the full budget is stashed but unused).

If a race hits that subprocess timeout without another failure, the test treats it as success (see `race_test.py`).

## Track-name filter and pytest reporting

Filtering uses `fnmatch` on **`track_name`** (e.g. `elastic/*` matches `elastic/security`, not arbitrary node id text). Deselected items are reported via pytest’s **`pytest_deselected`** hook, so collect-only output looks like **`8/76 tests collected (68 deselected)`** rather than implying only eight tests exist in the tree.

## Rally container cleanup

`compose.rally_race` runs Rally via `compose.run_rally`, which passes **`name="rally"`** into `compose.run_service` so **`docker compose run`** uses **`--name rally`**. That keeps `containers.log` and Docker Desktop prefixes short (instead of a long one-off name derived from **`COMPOSE_PROJECT_NAME`**). Override with **`rally_race(..., name="…")`** when you need a unique **`docker compose run --name`** (for example multiple concurrent races on the same daemon, e.g. pytest-xdist with more workers than ES-version lanes).

`compose.run_service(..., remove=True)` (used by `rally_race`) wraps `docker compose run` in **`try` / `finally`**. After the run (including on **`subprocess` timeout**), it runs **`docker compose kill`** on the Rally service, then **`docker compose ps -a -q`** and **`docker rm -f`** on those IDs.

**Why not `compose rm --stop`?** Docker Compose’s **`rm`** and **`stop`** commands **ignore one-off containers** created by **`docker compose run`** (internal `oneOffExclude` filter), so they never matched `…-rally-run-…` containers. **`compose kill`** includes one-offs, so orphaned run containers are actually stopped and removed when the test driver times out or exits before `run --rm` runs. Teardown is best-effort (`check=False` / logged warnings). The persistent `es01` service is still torn down with **`remove_service`** in the `elasticsearch` fixture only.

## Session teardown (Compose project)

`pytest_sessionfinish` in **`conftest.py`** calls **`compose.teardown_project`** as a safety net when the session ends (including **Ctrl+C**), using the **`COMPOSE_PROJECT_NAME`** left in that process’s environment (typically the **last** test’s project on that worker). The **`elasticsearch`** fixture also runs **`teardown_project`** after each test so per-test compose stacks are torn down promptly. Under **pytest-xdist**, **each worker** runs **`teardown_project`** in **`pytest_sessionfinish`**; the **controller** skips it while workers are active. In **serial** mode (no xdist, or **`-n 0`**), the main process runs **`teardown_project`** once at session end.

## Examples

```bash
# Only geonames-like tracks (geonames, geoshape, …) across default ES versions
uv run -- pytest it/tracks/ --it-tracks-name='geo*'

# Single ES version from env
IT_TRACKS_ES_VERSIONS=8.19.14 uv run -- pytest -s it/tracks/

# Force-run cases that would skip for known Docker issues
IT_TRACKS_NO_SKIP=1 uv run -- pytest -s it/tracks/

# Shorter total budget (per-race timeout is total_min*60/N, or × num_workers with xdist)
uv run -- pytest it/tracks/ --it-tracks-total-timeout-minutes=60

# Serial run (disable default xdist)
uv run -- pytest -s it/tracks/ -n 0
```
