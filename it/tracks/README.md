# Track race integration tests

This directory holds integration tests that run **Rally `race` inside Docker** (via `esrally.utils.compose`) against an Elasticsearch node from the bundled Compose stack. They exercise many **official / default Rally tracks** across multiple Elasticsearch versions.

## Contents

- **`race_test.py`** — defines `TrackCase` entries and `test_race_with_track`, parametrized over tracks and over `ES_VERSIONS` in that module. Module-scoped fixtures build the Rally image and configure Compose; each test starts `es01` with the requested `ES_VERSION`, runs `rally race` toward `es01:9200`, and tears Elasticsearch down afterward.

## Prerequisites

Same as the rest of the `it/` suite: Docker and Docker Compose available, with the daemon running. Other integration checks run from [`it/conftest.py`](../conftest.py) (for example `docker ps` / `docker compose version`).

## How to run

- Full integration test run (includes these tests): `make it`
- Only this folder: `uv run -- pytest -s it/tracks/` or `make test ARGS=it/tracks/`

## Tuning

- **`IT_TRACKS_TIMEOUT_MINUTES`** — optional environment variable (default `120`). The total budget is split across all track cases and Elasticsearch versions to set a per-race timeout in seconds. If a race hits that timeout without another failure, the test treats it as success. Increase this value if legitimate runs need more time.
