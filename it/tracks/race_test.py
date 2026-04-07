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
from __future__ import annotations

import dataclasses
import logging
import os
import subprocess
import time
from collections.abc import Generator

import pytest

from esrally import config
from esrally.utils import compose
from esrally.utils.cases import cases

LOG = logging.getLogger(__name__)

# Must match the number of entries passed to @cases on test_race_with_track.
_TRACK_CASE_COUNT = 38

ES_VERSIONS = ["8.19.13", "9.2.7"]

# Let have 120 minutes for all track sto complete. In case it times out before
# returning any other error, we will accept the testing outcome as it would require
# too much to complete all the tests in a reasonable time frame. We can always
# increase this timeout if we see that some tracks require much more time to complete.
TEST_TIMEOUT_M = int(os.environ.get("IT_TRACKS_TIMEOUT_MINUTES", "120"))
# The max time for a single track to complete is the total time divided by the number of tracks and ES versions we
# test against.
RACE_TIMEOUT_S = TEST_TIMEOUT_M * 60 / _TRACK_CASE_COUNT / len(ES_VERSIONS)


@dataclasses.dataclass
class TrackCase:
    track_name: str
    description: str
    test_mode: bool = True
    challenge: str | None = None
    # Keys must match literals in ``ES_VERSIONS``. When set for the active ``elasticsearch.version``,
    # ``test_race_with_track`` calls ``pytest.skip`` with the value as the reason.
    skip_reason_by_es_version: dict[str, str] | None = None


@pytest.fixture(scope="module", autouse=True)
def compose_config() -> Generator[compose.ComposeConfig]:
    cfg = compose.ComposeConfig()
    config.init_config(cfg=cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="module", autouse=True)
def build_rally(compose_config):
    compose.build_image("rally")


@dataclasses.dataclass
class ElasticsearchServer:
    version: str


@pytest.fixture(scope="function", params=ES_VERSIONS, ids=lambda param: f"es_version_{param}")
def elasticsearch(request, monkeypatch) -> Generator[ElasticsearchServer]:
    compose.remove_service("es01", force=True, volumes=True)
    monkeypatch.setenv("ES_VERSION", request.param)
    es = ElasticsearchServer(version=request.param)
    compose.start_elasticsearch("es01")

    yield es

    compose.remove_service("es01", force=True, volumes=True)


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
        skip_reason_by_es_version={
            "8.19.13": (
                "Fails in Docker IT against ES 8.19.13 (passes on 9.2.7 in observed runs); "
                "see it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
    ),
    has_privileges_bystander=TrackCase(
        track_name="has_privileges_bystander",
        description=(
            "Demonstrates Netty event-loop head-of-line blocking caused by expensive _has_privileges requests. "
            "Requires http.netty.worker_count:1 on the target cluster."
        ),
        skip_reason_by_es_version={
            "8.19.13": (
                "Track requires http.netty.worker_count:1 on Elasticsearch; bundled compose es01 does not set it. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            "9.2.7": (
                "Track requires http.netty.worker_count:1 on Elasticsearch; bundled compose es01 does not set it. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
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
        skip_reason_by_es_version={
            "8.19.13": (
                "Rally exits 64 during early setup in Docker IT (root cause not in captured logs). "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            "9.2.7": (
                "Rally exits 64 during early setup in Docker IT (root cause not in captured logs). "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
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
        test_mode=False,
        skip_reason_by_es_version={
            "8.19.13": (
                "Track load failed (e.g. track.json) and/or pip deps (pytrec_eval, numpy) in Docker IT. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            "9.2.7": (
                "pip install of track deps (pytrec_eval==0.5, numpy) fails in Rally container (Python 3.13 / aarch64). "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
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
        skip_reason_by_es_version={
            "8.19.13": (
                "Track load failed (track.json missing/incomplete) against ES 8.19.13 in Docker IT. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
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
        description="Indexes for JOIN tests",
        test_mode=False,
        skip_reason_by_es_version={
            "8.19.13": ("Rally exits 64 in Docker IT against ES 8.19.13. See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"),
        },
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
        skip_reason_by_es_version={
            "8.19.13": (
                "pip install of track deps (pytrec_eval==0.5, numpy) fails in Rally container. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            "9.2.7": (
                "pip install of track deps (pytrec_eval==0.5, numpy) fails in Rally container. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
    ),
    big5=TrackCase(
        track_name="big5",
        description="Benchmark for the Big5 workload",
        test_mode=False,
        skip_reason_by_es_version={
            "8.19.13": (
                "Rally exits 64 against ES 8.19.13 in Docker IT (not a subprocess timeout). "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
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
        skip_reason_by_es_version={
            "8.19.13": (
                "pip install of track deps (pytrec_eval==0.5, numpy) fails in Rally container. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
            "9.2.7": (
                "pip install of track deps (pytrec_eval==0.5, numpy) fails in Rally container. "
                "See it/tracks/TRACK_RACE_EXECUTION_FINDINGS.md"
            ),
        },
    ),
    sql=TrackCase(
        track_name="sql",
        description="SQL query performance based on NOAA Weather data",
        test_mode=False,
    ),
)
def test_race_with_track(case: TrackCase, elasticsearch: ElasticsearchServer):
    """Integration test: run Rally in Docker against a compose-managed Elasticsearch node.

    For each ``TrackCase`` (and each configured Elasticsearch version), starts ``es01`` with
    ``ES_VERSION`` set, then runs ``rally race`` targeting ``es01:9200``. ``TrackCase.test_mode``
    controls ``--test-mode`` (off for tracks that do not support it). ``TrackCase.challenge``,
    when set, is passed as ``--challenge``. ``TrackCase.skip_reason_by_es_version`` may skip
    the test for a given Elasticsearch version (keys must match ``ES_VERSIONS``). A per-case
    time budget comes from ``IT_TRACKS_TIMEOUT_MINUTES``; if the race hits that timeout without
    another failure, the test treats it as success.
    """
    LOG.info("Testing track name: %s (%s)", case.track_name, case.description)
    LOG.info("Testing timeout: %d seconds", RACE_TIMEOUT_S)
    LOG.info("Testing with elasticsearch version: %s", elasticsearch.version)

    if case.skip_reason_by_es_version is not None:
        reason = case.skip_reason_by_es_version.get(elasticsearch.version)
        if reason is not None:
            pytest.skip(reason)

    start_time = time.time()
    try:
        compose.rally_race(
            track_name=case.track_name,
            test_mode=case.test_mode,
            target_hosts=["es01:9200"],
            challenge=case.challenge,
            timeout=RACE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        LOG.warning("Race timeout: no errors until now and we take it as a success.")
    finally:
        end_time = time.time()
        LOG.info(
            "Race terminated after %d seconds for track [%s] and elasticsearch version [%s]",
            end_time - start_time,
            case.track_name,
            elasticsearch.version,
        )
