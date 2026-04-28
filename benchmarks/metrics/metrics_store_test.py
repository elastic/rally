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

"""
Benchmarks for the metrics store locking overhead introduced to guard
concurrent access between flush() and background sampler threads (_add()).

Three scenarios are measured:

  uncontended   single thread calling _add() in a tight loop. Represents
                worker processes (InMemoryMetricsStore) and the coordinator
                between flushes.

  contended     N sampler threads periodically calling _add() while the
                main thread calls _add() too. Represents the coordinator
                with active telemetry devices.

  no_lock       uses _add_unlocked() without lock. Provides the baseline
                cost without locking so the overhead can be quantified.
"""

# pylint: disable=protected-access

import datetime
import threading

import pytest

from esrally import config, metrics, time

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

DOC = {"name": "indexing_throughput", "value": 5000, "unit": "docs/s"}
SAMPLER_THREADS = 4  # used by the single contended test; sweep uses its own range
DOCS_PER_CALL = 1000
SAMPLER_DOCS_PER_CALL = 10


@pytest.fixture
def in_memory_store():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "benchmark")
    cfg.add(config.Scope.application, "track", "params", {})
    store = metrics.InMemoryMetricsStore(cfg, clock=time.Clock)
    store.open(
        race_id="benchmark-race",
        race_timestamp=datetime.datetime(2016, 1, 31),
        track_name="benchmark",
        challenge_name="benchmark",
        car_name="benchmark",
    )
    return store


# ---------------------------------------------------------------------------
# Uncontended: single thread, real lock
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="metrics_store_add", warmup="on", warmup_iterations=1000, disable_gc=True)
def test_add_uncontended(benchmark, in_memory_store):
    """Cost of _add() under no contention (lock always acquired immediately)."""

    def run():
        for _ in range(DOCS_PER_CALL):
            in_memory_store._add(DOC)

    benchmark(run)


# ---------------------------------------------------------------------------
# No lock: single thread
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="metrics_store_add", warmup="on", warmup_iterations=1000, disable_gc=True)
def test_add_no_lock(benchmark, in_memory_store):
    """Baseline: same loop without lock."""

    def run():
        for _ in range(DOCS_PER_CALL):
            in_memory_store._add_unlocked(DOC)

    benchmark(run)


# ---------------------------------------------------------------------------
# Contended: background sampler threads competing with the benchmark thread
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="metrics_store_add", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_contended(benchmark, in_memory_store):
    """Cost of _add() while SAMPLER_THREADS background threads also call _add()."""
    stop = threading.Event()

    def sampler():
        while not stop.wait(timeout=0.1):
            for _ in range(SAMPLER_DOCS_PER_CALL):
                in_memory_store._add(DOC)

    threads = [threading.Thread(target=sampler, daemon=True) for _ in range(SAMPLER_THREADS)]
    for t in threads:
        t.start()

    def run():
        for _ in range(DOCS_PER_CALL):
            in_memory_store._add(DOC)

    try:
        benchmark(run)
    finally:
        stop.set()
        for t in threads:
            t.join()


# ---------------------------------------------------------------------------
# No lock: background sampler threads together with the benchmark thread
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="metrics_store_add", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_no_lock_with_sampler_threads(benchmark, in_memory_store):
    """Baseline: Cost of _add_unlocked() while SAMPLER_THREADS background threads also call _add_unlocked()."""
    stop = threading.Event()

    def sampler():
        while not stop.wait(timeout=0.1):
            for _ in range(SAMPLER_DOCS_PER_CALL):
                in_memory_store._add(DOC)

    threads = [threading.Thread(target=sampler, daemon=True) for _ in range(SAMPLER_THREADS)]
    for t in threads:
        t.start()

    def run():
        for _ in range(DOCS_PER_CALL):
            in_memory_store._add_unlocked(DOC)

    try:
        benchmark(run)
    finally:
        stop.set()
        for t in threads:
            t.join()


# ---------------------------------------------------------------------------
# Thread-count sweep: 1 … 8 sampler threads (one per active telemetry device)
# ---------------------------------------------------------------------------


def _contended_add(benchmark, store, n_samplers):
    stop = threading.Event()

    def sampler():
        while not stop.wait(timeout=0.1):
            for _ in range(SAMPLER_DOCS_PER_CALL):
                store._add(DOC)

    threads = [threading.Thread(target=sampler, daemon=True) for _ in range(n_samplers)]
    for t in threads:
        t.start()

    def run():
        for _ in range(DOCS_PER_CALL):
            store._add(DOC)

    try:
        benchmark(run)
    finally:
        stop.set()
        for t in threads:
            t.join()


@pytest.mark.benchmark(group="metrics_store_add_sweep", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_contended_1_sampler(benchmark, in_memory_store):
    _contended_add(benchmark, in_memory_store, 1)


@pytest.mark.benchmark(group="metrics_store_add_sweep", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_contended_2_samplers(benchmark, in_memory_store):
    _contended_add(benchmark, in_memory_store, 2)


@pytest.mark.benchmark(group="metrics_store_add_sweep", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_contended_4_samplers(benchmark, in_memory_store):
    _contended_add(benchmark, in_memory_store, 4)


@pytest.mark.benchmark(group="metrics_store_add_sweep", warmup="on", warmup_iterations=100, disable_gc=True, max_time=10)
def test_add_contended_8_samplers(benchmark, in_memory_store):
    _contended_add(benchmark, in_memory_store, 8)
