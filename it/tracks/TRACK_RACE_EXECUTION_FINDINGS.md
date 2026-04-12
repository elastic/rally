# Track race integration test — execution findings

This note records **issues observed** while running the Docker-based track races (`test_race` in [`race_test.py`](race_test.py), often shown in logs as `it/tracks_test.py` depending on layout). Each item is tied to **track name** and **Elasticsearch distribution version** (`ES_VERSION` / `es_version_*` in pytest ids).

**Elasticsearch versions under test** (defaults: [`helpers.DEFAULT_IT_TRACKS_ES_VERSIONS`](helpers.py), used by `race_test.py`): **8.19.14**, **9.3.3**. Older captures in this file reference **8.19.13** / **9.2.7**.

**Per-race time budget**: derived from `IT_TRACKS_TIMEOUT_MINUTES` (default `120`) split across all track cases and both ES versions (~**94 s** per case unless overridden). Hitting this limit without another error is treated as **pytest success** by design.

### Recent focused run (completed, 2026-04-10)

Command: `make it_tracks IT_TRACKS_NAME='msmarco-passage-ranking,msmarco-v2-vector,search/mteb/dbpedia'` (default ES **8.19.14** / **9.3.3**; pytest-xdist **2** workers, `LoadGroupScheduling`).

| Metric | Value |
|--------|--------|
| **Outcome** | **6 passed**, **0 failed** |
| **Wall time** | **1238.58 s** (~**20 m 39 s**) |
| **Shell exit code** | **0** |

| Track | 8.19.14 | 9.3.3 |
|-------|---------|--------|
| `search/mteb/dbpedia` | **PASSED** | **PASSED** |
| `msmarco-v2-vector` | **PASSED** | **PASSED** |
| `msmarco-passage-ranking` | **PASSED** | **PASSED** |

**Context:** Current [`it/tracks/Dockerfile`](Dockerfile) (**`CFLAGS` / `CXXFLAGS`** for **`pytrec_eval`**, venv **`pytrec_eval==0.5`** + **`numpy`**) and [`race_test.py`](race_test.py) without Docker-IT skip rules on these tracks. **`IT_SKIP_XFAIL`** did **not** need to be set to false for that run. Under heavier host memory pressure, **`msmarco-v2-vector`** has been seen to fail with Docker exit **137** (SIGKILL) when two races run in parallel; this run succeeded with the same worker count.

### Final pytest run (completed, historical full matrix)

Command: `make test ARGS='it/tracks_test.py -o log_cli=true --log-cli-level=DEBUG'` (pytest node ids may show `it/tracks_test.py` while the test module lives under [`it/tracks/race_test.py`](race_test.py)).

| Metric | Value |
|--------|--------|
| **Outcome** | **14 failed**, **62 passed** |
| **Wall time** | **4102.88 s** (~**1 h 8 m**) |
| **Shell exit code** | **2** (`last_exit_code` in terminal metadata) |

---

## Pytest FAILED — full matrix (14) — historical

Each row is one failing parametrized case (`es_version_*` × track case) from the **full** matrix capture below. The **recent focused run** (default **8.19.14** / **9.3.3**, three tracks above) shows **`search/mteb/dbpedia`** and both MSMARCO tracks **passing** with the current IT image and `race_test` configuration.

| Track | ES version | Category | See |
|-------|------------|----------|-----|
| `has_privileges` | 8.19.13 | Elasticsearch settings mismatch | §7 |
| `has_privileges_bystander` | 8.19.13 | Elasticsearch settings mismatch | §7 |
| `has_privileges_bystander` | 9.2.7 | Elasticsearch settings mismatch | §7 |
| `wiki_en_cohere_vector_int8` | 8.19.13 | Rally exit 64 (root cause not in saved stdout) | §8 |
| `wiki_en_cohere_vector_int8` | 9.2.7 | Rally exit 64 (~6 s; same as above) | §8 |
| `search/mteb/dbpedia` | 8.19.13 | Track load and/or dependencies | §1, §2 |
| `search/mteb/dbpedia` | 9.2.7 | Pip `pytrec_eval` / `numpy` (observed in logs) | §1 |
| `joins` | 8.19.13 | Missing `track.json` / incomplete checkout | §2 |
| `esql` | 8.19.13 | Rally exit 64 (details not in captured excerpt) | §8 |
| `msmarco-v2-vector` | 8.19.13 | Pip `pytrec_eval` / `numpy` | §1 |
| `msmarco-v2-vector` | 9.2.7 | Pip `pytrec_eval` / `numpy` | §1 |
| `big5` | 8.19.13 | Rally exit 64 (**not** a timeout—would be PASSED) | §8 |
| `msmarco-passage-ranking` | 8.19.13 | Pip `pytrec_eval` / `numpy` | §1 |
| `msmarco-passage-ranking` | 9.2.7 | Pip `pytrec_eval` / `numpy` | §1 |

---

## Other observations (pytest PASSED but noteworthy)

| Topic | Track(s) / version(s) | Notes |
|-------|----------------------|--------|
| **MSMARCO + dbpedia** focused IT | **8.19.14**, **9.3.3** | **2026-04-10:** all six parametrizations **PASSED** in ~**21 min** (see **Recent focused run** above). |
| Per-case **timeout** accepted as success | e.g. `sql`, `joins`, `big5` (**9.2.7**), `openai_vector`, `k8s_metrics`, `tsdb_k8s_queries`, `so_vector` | See §3; **`big5` @ 8.19.13** **FAILED** instead—different failure mode. |
| **`geopointshape`** `geoGrid_aggs_*` **100%** error rate | 8.19.13, 9.2.7 | §4 |
| **ELSER** / ML license | `elser-ingest-speedtest` @ 9.2.7 | §5 |
| **Cluster not clean** warnings | Many tracks (esp. after heavy races) | §6 |

---

## 1. Python track dependency install failure (`pytrec_eval`, `numpy`)

**Tracks:** `msmarco-passage-ranking` (**8.19.13**, **9.2.7**); `msmarco-v2-vector` (**8.19.13**, **9.2.7**); `search/mteb/dbpedia` (**9.2.7**).

**Symptom:** Before the race starts, Rally logs `Installing track dependencies [pytrec_eval==0.5, numpy]` then:

- `pip install ... --target /rally/.rally/libs` returns **non-zero**.
- `esrally.exceptions.SystemSetupError` referencing **`/rally/.rally/logs/dependency.log`**.
- Docker/Rally exit **64** → pytest **FAILED**.

**Environment (from logs):** Rally container **Python 3.13.7**, **linux-aarch64**, Wolfi-based dev image.

**Likely causes:** No usable wheel for **`pytrec_eval==0.5`** on this platform/Python; source build may fail (toolchain/headers) or be incompatible with 3.13. Full pip output is in the Docker volume at `.rally/logs/dependency.log` (see [`compose.yaml`](../../esrally/utils/resources/compose.yaml) `rally_data` volume).

---

## 2. Track repository / layout errors (`track.json` missing)

**Tracks:** `search/mteb/dbpedia` (**8.19.13**); `joins` (**8.19.13**) — observed in an earlier run on the same style of invocation.

**Symptom:** `TrackSyntaxError` / `jinja2.exceptions.TemplateNotFound: 'track.json'` under paths such as:

- `.../benchmarks/tracks/default/search/mteb/dbpedia`
- `.../benchmarks/tracks/default/joins`

**Note:** The same logical track can fail differently on another run (e.g. **dbpedia** failing on **dependency install** on **9.2.7** once the track tree is present but pip fails). Treat **track path** and **dependency** failures as separate classes.

---

## 3. Per-case timeout (race not finished; pytest still passes)

**Tracks (examples from long runs):** `sql` (**8.19.13** and often **9.2.7**); `big5` (**9.2.7** only in observed timeouts—**`big5` @ 8.19.13** **FAILED** in the completed run); `openai_vector` (**9.2.7**); `k8s_metrics` (**9.2.7**); `tsdb_k8s_queries` (**9.2.7**); `so_vector` (**9.2.7**); `joins` (**9.2.7**).

**Symptom:** `compose` terminates the rally subprocess after **`RACE_TIMEOUT_S`**; log: `Race timeout: no errors until now and we take it as a success.` → **PASSED**.

**Impact:** No complete benchmark for that case; for **`joins`**, timeout hit during **multi-GB corpus download**, so the run is dominated by I/O, not indexing.

**Mitigation (product/test design):** Raise `IT_TRACKS_TIMEOUT_MINUTES`, narrow parametrization, or mark heavy tracks as optional/slow.

---

## 4. Full operation error rate on subset of `geopointshape` tasks

**Tracks:** `geopointshape` (**8.19.13** and **9.2.7**).

**Symptom:** Rally summary shows **0%** error for most geo tasks and **100%** error for:

- `geoGrid_aggs_geohash`
- `geoGrid_aggs_geotile`
- `geoGrid_aggs_geohex`

Non-aggregation `geoGrid_geohash` / `geoGrid_geotile` / `geoGrid_geohex` stayed at **0%**. Rally emitted warnings about 100% error rate and missing throughput for those ops; overall race still **`SUCCESS`** and pytest **PASSED**.

**Likely area:** Track operation definitions vs Elasticsearch behavior (e.g. aggregation on mapped field types, API changes). Confirm with ES response bodies in Rally logs.

---

## 5. ELSER track vs basic license (ML)

**Track:** `elser-ingest-speedtest` (**9.2.7**).

**Symptom:** Logs include:

- `AuthorizationException(403, 'security_exception', 'current license is non-compliant for [ml]')`
- `NotFoundError(404, 'resource_not_found_exception', 'Could not find trained model [.elser_model_2]')`

Compose stack uses **basic** license and **ML disabled** in image config; the track still advanced through scheduled steps and finished with a metric table (pytest **PASSED** in the captured run).

**Impact:** ELSER-related steps are **not** representative of a licensed ML deployment; metrics may reflect degraded or no-op behavior.

---

## 6. Cross-cutting: “cluster not in a defined clean state”

**Affects:** Many races after prior cluster activity (e.g. **`elser-ingest-speedtest`**, **`so`**, **`so_vector`**, **`geopointshape`**, **`noaa`**) on **9.2.7** in the same session.

**Symptom:** Rally warnings such as:

- `merges_total_time ... not in a defined clean state`
- `indexing_total_time ...`
- `refresh_total_time ...`
- `flush_total_time ...` (seen e.g. before **`noaa` @ 9.2.7**)

**Impact:** Reported index-time metrics for subsequent challenges may be **misleading** relative to a cold cluster.

---

## 7. `_has_privileges` tracks vs default Compose Elasticsearch

**Tracks:** `has_privileges` (**8.19.13**); `has_privileges_bystander` (**8.19.13**, **9.2.7**).

**Symptom:** Rally exits **64** within a few seconds.

**Cause (from track metadata in logs):** These tracks are meant to exercise Netty / `_has_privileges` behavior and **require** the target cluster to set **`http.netty.worker_count:1`**. The bundled [`compose.yaml`](../../esrally/utils/resources/compose.yaml) Elasticsearch service does **not** set this, so the race is **not valid** in the default IT environment.

**Mitigation:** Add the required `ES_JAVA_OPTS` / `elasticsearch.yml` override to the `es01` service for these cases only, skip the tracks in IT, or run them against a manually configured cluster.

---

## 8. Rally exit 64 — failures needing Rally stdout / `dependency.log`

**Tracks:** `wiki_en_cohere_vector_int8` (**8.19.13**, **9.2.7**); `esql` (**8.19.13**); `big5` (**8.19.13**).

**Symptom:** Pytest reports `CalledProcessError` / Rally **exit 64** without a captured `[ERROR] Cannot race` block in the terminal excerpt (or with stderr not decoded in the failure report). **`wiki_en_cohere_vector_int8`** runs **without** `--test-mode` (`test_mode=False` in the track case) and failed in ~**6 s** in the captured **9.2.7** run—consistent with **early setup** (track load, dependency, or cluster check), not a full download.

**Next step:** Inspect **`/rally/.rally/logs/`** inside the Rally container or re-run with Docker logs captured for the `resources-rally-run-*` container.

---

## 9. Infrastructure / environment (not track-specific)

| Topic | Detail |
|-------|--------|
| **Logs location** | Rally data under Docker named volume `rally_data` → `/rally/.rally` (including `logs/dependency.log`). Not in the git tree. |
| **Image** | IT races: [`it/tracks/Dockerfile`](Dockerfile) (track-dep build flags + venv **`pytrec_eval` / `numpy`**). Upstream dev image: [`docker/Dockerfiles/dev/Dockerfile`](../../docker/Dockerfiles/dev/Dockerfile). |
| **Command** | Typical local invocation: `make test ARGS='it/tracks/... -o log_cli=true --log-cli-level=DEBUG'` (or equivalent `pytest` on `it/tracks/`). |

### Expected failures in `race_test.py` (`ExpectCommandFailure`)

Known-broken track/version combinations are recorded in [`race_test.py`](race_test.py) by setting `TrackCase.expect_failure` to `helpers.ExpectCommandFailure(...)` (`returncode`, `stdout`, `reason`, optional `es_version_prefix` defaulting to `""`) when the parametrized ES version matches the prefix; other tracks keep the default `expect_failure` of `None` in `race_test.py`. Default CI/local runs use **skip-xfail on**, so those nodes `pytest.skip` before `rally race`. With **skip-xfail off** (`IT_SKIP_XFAIL=0` or `--it-skip-xfail`), the race runs and a matching subprocess failure becomes `pytest.xfail`. If a `stdout` pattern is too weak or the return code wrong, run **serially** (`pytest it/tracks/ … -n 0`) with skip-xfail off, read the real exit code and logs, then tighten the spec (see [`README.md`](README.md)).

---

## Revision history

- **Initial capture:** Aggregates Cursor terminal transcripts for `make test ARGS='it/tracks_test.py …'` / `it/tracks/` races, including multiple ES versions and both **hard failures** and **soft** warnings/timeouts. Reuse this file when triaging CI or local Docker IT failures.
- **Update (mid-run):** Partial log: extra **9.2.7** failures on MSMARCO tracks (pip), timeouts on **`big5` / `openai_vector` / `k8s_metrics`**, etc.
- **Update (run completed):** Final **`pytest`** result **14 failed, 62 passed** in **~4103 s**; full **FAILED** matrix added; new **§7** for **`has_privileges*`** (Netty `worker_count`) and **§8** for other exit-64 cases (**`wiki_en_cohere_vector_int8`**, **`esql`**, **`big5` @ 8.19.13**).
- **Update (2026-04-10):** After terminal **`make it_tracks`** with `IT_TRACKS_NAME='msmarco-passage-ranking,msmarco-v2-vector,search/mteb/dbpedia'` finished (**6 passed**, **~1239 s**, exit **0**), added **Recent focused run** table and aligned default ES wording with [`helpers.py`](helpers.py) (**8.19.14** / **9.3.3**).
- **Update (2026-04-10):** Documented **`ExpectCommandFailure`** / **`expect_failure`** (replace prior skip-only list); serial discovery workflow for `returncode` / `stdout` when tightening specs.
