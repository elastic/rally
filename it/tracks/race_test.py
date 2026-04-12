# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Docker track race integration tests for Rally.

See ``it/tracks/README.md`` for how to run, timeouts, and pytest options.
``conftest.py`` in this directory configures pytest isolation from ``it/conftest.py``.
"""

from __future__ import annotations

import dataclasses
import logging
import subprocess
import time
from collections.abc import Generator

import pytest

from esrally import config
from esrally.utils import compose
from esrally.utils.cases import cases
from it.tracks import helpers

LOG = logging.getLogger(__name__)

# Default ES versions (overridden by conftest / IT_TRACKS_ES_VERSIONS / --it-tracks-es-versions).
ES_VERSIONS = list(helpers.DEFAULT_IT_TRACKS_ES_VERSIONS)


@dataclasses.dataclass
class TrackCase:
    """One Rally track exercised by ``test_race``."""

    track_name: str
    description: str
    test_mode: bool = True
    challenge: str | None = None

    # ``None``: no skip / no xfail translation around ``rally race``. Use
    # ``helpers.ExpectCommandFailure(...)`` for known subprocess failures; the test wraps the race with
    # ``helpers.expect_command_failure`` (see ``helpers.it_skip_xfail_applies`` and
    # ``it/tracks/README.md``).
    expect_failure: helpers.ExpectCommandFailure | None = None


@pytest.fixture(scope="module", autouse=True)
def compose_config() -> Generator[compose.ComposeConfig]:
    """Initialize Rally global config from Docker Compose settings for this module; clear it on teardown."""
    cfg = compose.ComposeConfig()
    config.init_config(cfg=cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="module", autouse=True)
def build_rally(compose_config: compose.ComposeConfig) -> None:
    """Build the Rally Docker image once per module (after ``compose_config`` is ready)."""
    compose.build_image("rally")


@pytest.fixture
def elasticsearch_version(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[str]:
    """Indirect parametrization from ``it/tracks/conftest.py`` (ES version string in ``request.param``)."""
    # Per-test host log tree (mirrors nodeid); compose bind-mounts this into es01 / rally containers.
    host_log = helpers.host_log_dir_for_nodeid(helpers.log_root(), request.node.nodeid)
    host_log.mkdir(parents=True, exist_ok=True)
    helpers.prepare_compose_bind_mount_dirs(host_log)
    # Isolate this parametrized node from other tests/workers (compose project + log path).
    monkeypatch.setenv("IT_TRACKS_HOST_LOG_DIR", str(host_log))
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", helpers.compose_project_name_for_nodeid(request.node.nodeid))
    # compose.yaml uses ES_VERSION to pick the Elasticsearch image tag.
    monkeypatch.setenv("ES_VERSION", request.param)
    # Drop any leftover es01 from a prior run in the same project before starting fresh.
    compose.remove_service("es01", force=True, volumes=True)
    try:
        compose.start_elasticsearch("es01")
        yield request.param
    finally:
        # Stop es01 and remove the compose project so the next test gets a clean stack.
        compose.remove_service("es01", force=True, volumes=True)
        compose.teardown_project(cfg=compose.ComposeConfig())


@cases(
    geonames=TrackCase(
        track_name="geonames",
        description="POIs from Geonames",
    ),
    percolator=TrackCase(
        track_name="percolator",
        description="Percolator benchmark based on AOL queries",
    ),
    github_archive=TrackCase(
        track_name="github_archive",
        description="GitHub timeline from gharchive.org",
    ),
    http_logs=TrackCase(
        track_name="http_logs",
        description="HTTP server log data",
    ),
    wikipedia=TrackCase(
        track_name="wikipedia",
        description="Benchmark for search with Wikipedia data",
    ),
    geoshape=TrackCase(
        track_name="geoshape",
        description="Shapes from PlanetOSM",
    ),
    elastic_apm=TrackCase(
        track_name="elastic/apm",
        description="Elastic APM benchmark for Rally",
    ),
    elastic_security=TrackCase(
        track_name="elastic/security",
        description="Track for simulating Elastic Security workloads",
    ),
    elastic_logs=TrackCase(
        track_name="elastic/logs",
        description="Track for simulating logging workloads",
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="validate-package-template-installation",
            reason=(
                "Cannot run task [validate-package-template-installation]: Index templates missing for packages: "
                "['apache', 'kafka', 'mysql', 'nginx', 'postgresql', 'redis', 'system']"
            ),
        ),
    ),
    elastic_endpoint=TrackCase(
        track_name="elastic/endpoint",
        description="Endpoint track",
    ),
    tsdb=TrackCase(
        track_name="tsdb",
        description="metricbeat information for elastic-app k8s cluster",
    ),
    metricbeat=TrackCase(
        track_name="metricbeat",
        description="Metricbeat data",
    ),
    has_privileges=TrackCase(
        track_name="has_privileges",
        description="Benchmarks _has_privileges API with index and Kibana application privileges",
        test_mode=False,
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="http.netty.worker_count",
            reason=(
                "Fails in Docker IT against ES 8.x (passes on 9.3.3 in observed runs); " "see it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            es_version_prefix="8.",
        ),
    ),
    has_privileges_bystander=TrackCase(
        track_name="has_privileges_bystander",
        description=(
            "Demonstrates Netty event-loop head-of-line blocking caused by expensive _has_privileges requests. "
            "Requires http.netty.worker_count:1 on the target cluster."
        ),
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="http.netty.worker_count",
            reason=(
                "Track requires http.netty.worker_count:1 on Elasticsearch; bundled compose es01 does not set it. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        ),
    ),
    geopoint=TrackCase(
        track_name="geopoint",
        description="Point coordinates from PlanetOSM",
    ),
    nyc_taxis=TrackCase(
        track_name="nyc_taxis",
        description="Taxi rides in New York in 2015",
    ),
    wiki_en_cohere_vector_int8=TrackCase(
        track_name="wiki_en_cohere_vector_int8",
        description="Benchmark for vector search using Cohere embed-multilingual-v3 int8 embeddings on English Wikipedia",
        test_mode=False,
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="FAILURE (took",
            reason=(
                "Rally exits 64 during early setup in Docker IT (root cause not in captured logs). "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        ),
    ),
    tsdb_k8s_queries=TrackCase(
        track_name="tsdb_k8s_queries",
        description="metricbeat information for elastic-app k8s cluster",
    ),
    elser_ingest_speedtest=TrackCase(
        track_name="elser-ingest-speedtest",
        description="Benchmark weighted terms queries on ELSER tokens on the MS MARCO passage dataset",
    ),
    search_mteb_dbpedia=TrackCase(
        track_name="search/mteb/dbpedia",
        description="Benchmark text search relevance with different configurations",
    ),
    geopointshape=TrackCase(
        track_name="geopointshape",
        description="Point coordinates from PlanetOSM indexed as geoshapes",
    ),
    so=TrackCase(
        track_name="so",
        description="Indexing benchmark using up to questions and answers from StackOverflow",
    ),
    so_vector=TrackCase(
        track_name="so_vector",
        description="Benchmark for vector search with StackOverflow data",
    ),
    joins=TrackCase(
        track_name="joins",
        description="Indexes for JOIN tests",
        test_mode=False,
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="track.json",
            reason=(
                "Track load failed (track.json missing/incomplete) against ES 8.x in Docker IT. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            es_version_prefix="8.",
        ),
    ),
    random_vector=TrackCase(
        track_name="random_vector",
        description="Benchmarking filtered search on random vectors",
    ),
    dense_vector=TrackCase(
        track_name="dense_vector",
        description="Benchmark for dense vector indexing and search",
    ),
    cohere_vector=TrackCase(
        track_name="cohere_vector",
        description="Benchmark for vector search with Cohere Wikipedia data",
    ),
    eql=TrackCase(
        track_name="eql",
        description="EQL benchmarks based on endgame index of SIEM demo cluster",
    ),
    esql=TrackCase(
        track_name="esql",
        description="Benchmarks for Elasticsearch SQL (ESQL) queries",
        test_mode=False,
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="FAILURE (took",
            reason="Rally exits 64 in Docker IT against ES 8.x. See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md",
            es_version_prefix="8.",
        ),
    ),
    nested=TrackCase(
        track_name="nested",
        description="StackOverflow Q&A stored as nested docs",
    ),
    noaa=TrackCase(
        track_name="noaa",
        description="Global daily weather measurements from NOAA",
    ),
    msmarco_v2_vector=TrackCase(
        track_name="msmarco-v2-vector",
        description="Benchmark for vector search with msmarco-v2 passage data",
    ),
    big5=TrackCase(
        track_name="big5",
        description="Benchmark for the Big5 workload",
        test_mode=False,
        expect_failure=helpers.ExpectCommandFailure(
            returncode=64,
            stdout="FAILURE (took",
            reason=(
                "Rally exits 64 against ES 8.x in Docker IT (not a subprocess timeout). " "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            es_version_prefix="8.",
        ),
    ),
    openai_vector=TrackCase(
        track_name="openai_vector",
        description="Benchmark for vector search using the OpenAI text-embedding-ada-002 model",
    ),
    k8s_metrics=TrackCase(
        track_name="k8s_metrics",
        description="Index refresh benchmarks with a Kubernetes pod metrics dataset",
    ),
    pmc=TrackCase(
        track_name="pmc",
        description="Full text benchmark with academic papers from PMC",
    ),
    msmarco_passage_ranking=TrackCase(
        track_name="msmarco-passage-ranking",
        description="Benchmark bm25, semantic and hybrid search on the MS MARCO passage dataset",
    ),
    sql=TrackCase(
        track_name="sql",
        description="SQL query performance based on NOAA Weather data",
        test_mode=False,
    ),
)
def test_race(
    case: TrackCase,
    elasticsearch_version: str,
    race_timeout_s: float,
    request: pytest.FixtureRequest,
) -> None:
    """Integration test: run Rally in Docker against a compose-managed Elasticsearch node.

    For each ``TrackCase`` (and each configured Elasticsearch version), starts ``es01`` with
    ``ES_VERSION`` set, then runs ``rally race`` targeting ``es01:9200``. ``TrackCase.test_mode``
    controls ``--test-mode`` (off for tracks that do not support it). ``TrackCase.challenge``,
    when set, is passed as ``--challenge``. ``TrackCase.expect_failure`` (``None`` or
    ``helpers.ExpectCommandFailure(...)``) is applied via
    ``helpers.expect_command_failure``:
    ``pytest.skip`` before the race when skip-xfail is on (default), else ``pytest.xfail`` on a matching
    ``CalledProcessError`` when skip-xfail is off.
    Per-race timeout is ``race_timeout_s`` (total minutes from CLI/env divided by ``N``; ``N`` omits
    version-skip rows when skip-xfail applies; see ``it/tracks/README.md``).
    Subprocess timeout is treated as success; Rally one-off containers are torn down in ``run_service``
    when ``remove`` is ``True`` (default).
    """
    LOG.info("Testing track name: %s (%s)", case.track_name, case.description)
    LOG.info("Testing timeout: %s seconds", race_timeout_s)
    LOG.info("Testing with elasticsearch version: %s", elasticsearch_version)

    start_time = time.time()
    try:
        with helpers.expect_command_failure(case.expect_failure, elasticsearch_version, request.config):
            compose.rally_race(
                track_name=case.track_name,
                test_mode=case.test_mode,
                target_hosts=["es01:9200"],
                challenge=case.challenge,
                timeout=race_timeout_s,
            )
    except subprocess.TimeoutExpired:
        LOG.warning("Race timeout: no errors until now and we take it as a success.")
    finally:
        end_time = time.time()
        LOG.info(
            "Race terminated after %d seconds for track [%s] and elasticsearch version [%s]",
            int(end_time - start_time),
            case.track_name,
            elasticsearch_version,
        )
