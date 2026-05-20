# Track race “expected failure” run report (2026-04-13)

## Command

From the Rally repo root, with **skip-xfail disabled** so `TrackCase.expect_failure` cases **run** `rally race` and matching subprocess failures become **xfail** (not `pytest.skip`):

```bash
uv run -- pytest it/tracks/ -n 4 --dist loadgroup --it-skip-xfail \
  --it-tracks-name='elastic/logs,has_privileges*,wiki_en_cohere_vector_int8,joins,esql,big5' \
  -v --tb=short
```

- **Workers:** 4 (`-n 4`).
- **Filter:** Only tracks that define `expect_failure` in `race_test.py` (plus `has_privileges` on 9.x, which has no policy for that version).
- **Full console log:** `/tmp/it_tracks_expected_failure_run.log` (very large; includes full Rally stdout).

## Pytest outcome

| Metric   | Count |
| -------- | ----- |
| **xfailed** | 7 |
| **passed**  | 5 |
| **failed**  | 2 |
| **Wall time** | ~54m 51s (`3290.99s`) |

**Short summary line (from pytest):** `2 failed, 5 passed, 7 xfailed`.

## Per-node results

| Node | Result | Notes |
| ---- | ------ | ----- |
| `test_race[es_8.19.14-elastic_logs]` | **XFAIL** | Matches `ExpectCommandFailure` (exit 64, stdout substring). |
| `test_race[es_9.3.3-elastic_logs]` | **XFAIL** | Same. |
| `test_race[es_8.19.14-has_privileges_bystander]` | **XFAIL** | `http.netty.worker_count` / exit 64. |
| `test_race[es_9.3.3-has_privileges_bystander]` | **XFAIL** | Same. |
| `test_race[es_8.19.14-wiki_en_cohere_vector_int8]` | **XFAIL** | `FAILURE (took` / exit 64. |
| `test_race[es_9.3.3-wiki_en_cohere_vector_int8]` | **XFAIL** | Same. |
| `test_race[es_8.19.14-big5]` | **XFAIL** | Policy applies only to `8.*`; matches. |
| `test_race[es_8.19.14-has_privileges]` | **PASSED** | Policy is `8.*`-only; race **succeeded** (no `CalledProcessError`). Docstring in `race_test.py` notes 8.x failure has been seen but 9.3.3 passes—here 8.19.14 also passed. |
| `test_race[es_9.3.3-has_privileges]` | **PASSED** | No `expect_failure` for 9.x; success is expected. |
| `test_race[es_8.19.14-esql]` | **PASSED** | Policy is `8.*`-only; race **succeeded** (differs from some historical Docker IT notes where exit 64 was seen). |
| `test_race[es_9.3.3-esql]` | **PASSED** | Expected. |
| `test_race[es_9.3.3-joins]` | **PASSED** | No policy for 9.x; success expected. |
| `test_race[es_8.19.14-joins]` | **FAILED** | See below (host/container disk, not `track.json`). |
| `test_race[es_9.3.3-big5]` | **FAILED** | See below (disk; **no** `expect_failure` on 9.x, so this is a hard failure). |

## Does this match what is “expected”?

- **XFAIL rows (7):** Yes. Each ended with exit code **64** and stdout containing the configured substring (`validate-package-template-installation`, `http.netty.worker_count`, `FAILURE (took`, or the big5 pattern on 8.x).
- **PASSED rows where a policy exists only for 8.x but the race succeeded:** `has_privileges` and `esql` on **8.19.14** are **consistent with the test harness**: `pytest.xfail` is only applied when `rally race` raises `CalledProcessError` with a **matching** return code and substring. A **successful** race is always reported as **passed**, even if comments/docs mention intermittent failures elsewhere.
- **The two FAILED tests:** These are **not** failures of the `ExpectCommandFailure` machinery. They are **environmental**: the Rally container hit **ENOSPC** while handling large track artifacts.

## Root cause of each command failure (log analysis)

Both failures share the same underlying error in decoded Rally output:

```text
[ERROR] Cannot race. Error in task executor
	[Errno 28] No space left on device
```

### `test_race[es_8.19.14-joins]`

- **Exit code:** 64 (as with many Rally failures).
- **Why xfail did not trigger:** `ExpectCommandFailure` for `joins` requires substring **`track.json`**. The actual failure was **disk full** after large downloads/decompression (e.g. join track data up to ~40.8GB decompressed per log line); **`track.json` does not appear** in this failure output.
- **Rally message tail:** `FAILURE (took 825 seconds)` after the ENOSPC error.

### `test_race[es_9.3.3-big5]`

- **Exit code:** 64.
- **Why this is a hard FAIL:** `big5`’s `expect_failure` only applies to **`es_version_prefix="8."`**. On **9.3.3** there is **no** xfail translation—any `CalledProcessError` fails the test.
- **Same root cause:** `[Errno 28] No space left on device` during a large **big5** download (~7.5GB track data still progressing in the log).
- **Rally message tail:** `FAILURE (took 760 seconds)`.

### Host-mounted logs

No separate review of `logs/<nodeid>/rally` or `es01` files was required: the **pytest captured log** already contains the decisive Rally stderr/stdout with ENOSPC. If you reproduce locally, check the same tree under `IT_TRACKS_LOG_ROOT` (default repo `logs/`).

## Recommendations

1. **Before re-running joins / big5 (especially in parallel):** free Docker disk space (`docker system df`, prune unused images/volumes), and ensure the Docker VM / disk image has enough space for multi-GB benchmarks.
2. **If ENOSPC persists:** run fewer workers or serialize heavy tracks (`-n 0` or a narrower `--it-tracks-name`) so multiple large downloads do not fill the graph driver at once.
3. **Optional spec tightening:** If ENOSPC should be treated as a known IT flake, that would require an explicit policy decision (today it is not encoded in `ExpectCommandFailure`).

## Environment notes

- Run recreated `.venv` via `uv run` at session start.
- Platform from log: **darwin**, Python **3.12.2**, pytest **8.4.2**, xdist **3.8.0**.
