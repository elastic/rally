---
name: developing-rally
description: >-
  Work on Rally's own codebase, not running benchmarks with it. Use when
  setting up the dev environment, running Rally's tests or linters, navigating
  its source, debugging Rally's own code, or making changes to Rally itself.
---

# Developing Rally

For working *on* Rally, not just running it. The docs under [References](#references) are the
source of truth — prefer them over guessing.

## Setup

Development uses [`uv`](https://docs.astral.sh/uv/) and needs `jq`. Rally runs on Linux/macOS
only (no Windows) and targets Python 3.10–3.13. From a clone:

```
make install            # create .venv and editable-install with dev extras
source .venv/bin/activate
esrally --help          # runs your working-tree code (editable install)
```

`make install` editable-installs the workspace, so `esrally` / `esrallyd` (or `uv run esrally …`)
run your working-tree code directly. Avoid the `./rally` / `./rallyd` wrappers while developing:
they auto-update from git (via `run.sh`) and only run on a clean `master`, otherwise erroring
`There are uncommitted changes …` — pass `--skip-update` if you must use them.

## Everyday make targets

- `make lint` / `make format` — run/apply pre-commit linters (black, isort, …).
- `make test` — unit tests. Scope with `make test ARGS=tests/foo_test.py`; `make test-all`
  runs across Python 3.10–3.13.
- `make it` — integration tests (slow, CI-oriented). `make benchmark` runs the micro-benchmarks.
- `make docs` / `make serve-docs` — build/serve the `docs/` site.

Before committing run `make lint test` (unit tests + lint; integration tests are too slow for
the inner loop). Optionally `make install-pre-commit` to run lint automatically on each commit.

## Testing & conventions

- Unit tests live in `tests/` mirroring `esrally/`, named `*_test.py`; integration tests in
  `it/`, micro-benchmarks in `benchmarks/`.
- `asyncio_mode = strict`, so async tests need an explicit `@pytest.mark.asyncio`.
- `xfail_strict = true`, so an `xfail` test that unexpectedly passes fails the suite.
- Type hints are **not** used broadly — they're added opportunistically per module, with the
  strictly-checked modules listed under `[tool.mypy].overrides` in `pyproject.toml`. Match the
  surrounding code rather than typing everything.
- **Keep the load-driver hot path tight.** Code in `driver/` runners, schedulers, and parameter
  sources runs per operation during measurement, so extra overhead (logging, allocations,
  blocking I/O, locks) perturbs the throughput/latency/service_time numbers Rally exists to
  report. Do expensive work outside the measured loop.
- For a **big or risky hot-path change**, consider quantifying its overhead with a
  [`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/) micro-benchmark under
  `benchmarks/` — coverage there is sparse, so treat this as a tool to reach for, not a standing
  requirement. Take the `benchmark` fixture, call `benchmark(fn)`, decorate with
  `@pytest.mark.benchmark(group="...", warmup="on", disable_gc=True)`, and compare against a
  baseline variant. See `benchmarks/driver/runner_test.py`.

## Source organization (`esrally/`)

- `rally.py` — CLI entry point: argument parsing and subcommand dispatch.
- `racecontrol.py` — orchestrates a race end-to-end via the pipelines registry.
- `mechanic/` — provisions, launches, and tears down Elasticsearch (only for `from-*` pipelines).
- `driver/` — generates load: schedules tasks and runs `runner.py` operations against the cluster.
- `track/` — loads and parses tracks/challenges; `tracker/` generates a track from an existing cluster.
- `metrics.py` — the metrics store (in-memory or Elasticsearch); `reporter.py` — the summary report.
- `client/` — Elasticsearch client factory; `telemetry.py` — telemetry devices; `config.py` / `types.py` — config.

## Actor system

Rally is a distributed system built on [Thespian](https://github.com/kquick/Thespian); even a
single-machine race runs as actors. Three *logical* roles (`docs/rally_daemon.rst`) that, by
default, all run on the one machine where you invoke `esrally`:

- **benchmark coordinator** — drives the whole race and shows results.
- **load driver** — interprets and runs the track.
- **provisioner** — configures and starts Elasticsearch.

`--load-driver-hosts` and `--target-hosts` split the load driver and provisioner onto other
machines; multi-machine runs use the `esrallyd` daemon. `esrally/actor.py` holds the shared
`RallyActor` base and the bootstrap logic; `docs/architecture/actor_system.md` walks through the actors
and their message flow with sequence diagrams. If actor-system startup fails (common on a VPN),
set `THESPIAN_BASE_IPADDR` to a routable address.

## Debugging

- Rally logs to `~/.rally/logs/rally.log` (human-readable) and `rally.json` (structured);
  profiling output goes to `profile.log`. Console output is only a summary — the full stack
  trace of a failure is usually in the log file, not on stdout.
- Each log line carries the emitting actor's address (`%(actorAddress)s`), so you can trace a
  failure across the actor system. The actor framework's own internal log is separate:
  `~/.rally/logs/actor-system-internal.log` (path via `THESPLOG_FILE`) — check it when actors
  fail to start or communicate, and set `THESPLOG_THRESHOLD` (default `INFO`) to `DEBUG` for
  more detail on the actor system itself.
- Raise the log level to see more: edit `~/.rally/logging.json` (created on first run from
  `esrally/resources/logging.json`) and set `"level": "DEBUG"` on the `root` logger for
  everything, or add a per-module entry under `loggers` to scope it (matching the existing
  entries, which just set a level and inherit the root handlers), e.g.:

  ```json
  "loggers": {
    "esrally.driver": {"level": "DEBUG"}
  }
  ```

  Then re-run. Revert to `INFO` when done — `DEBUG` is verbose.

## Reproduce a failing integration test

Integration tests in `it/` drive real races, so a failure usually reflects a real bug.
To debug any IT failure:

- Re-run the single failing test in isolation: `make it ARGS="it/<file>.py -k <pattern>"`.
- Reproduce outside the test harness by running the equivalent race yourself — fast, against a
  throwaway local cluster — then read the logs (see [Debugging](#debugging), raising the level
  to `DEBUG` as needed):

  ```bash
  esrally race --distribution-version=9.4.0 --track=geonames --test-mode --on-error=abort
  ```

- If the failure is in the actor system, add `THESPLOG_THRESHOLD=DEBUG` and inspect a running
  system with the Thespian shell: `python3 -m thespian.shell` (see `docs/rally_daemon.rst`).

## References

- Developer setup & key components: `docs/developing.rst`
- Contribution workflow (PRs, license headers, CLA): `CONTRIBUTING.md`
- Distributed setup & the actor-system roles: `docs/rally_daemon.rst`
- Actor message flow (sequence diagrams): `docs/architecture/actor_system.md`
- Thespian actor framework: https://github.com/kquick/Thespian
- All flags (do not invent flags; verify here or via `esrally <subcommand> --help`): `docs/command_line_reference.rst`
- Running benchmarks against a cluster: the `running-benchmarks` skill
