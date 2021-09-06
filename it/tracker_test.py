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

import uuid

import pytest

import it


@pytest.fixture(scope="module")
def test_cluster():
    cluster = it.TestCluster("in-memory-it")
    # test with a recent distribution
    dist = it.DISTRIBUTIONS[-1]
    port = 19200
    race_id = str(uuid.uuid4())

    it.wait_until_port_is_free(port_number=port)
    cluster.install(distribution_version=dist, node_name="rally-node", car="4gheap,basic-license", http_port=port)
    cluster.start(race_id=race_id)
    yield cluster
    cluster.stop()


@it.rally_in_mem
def test_create_track(cfg, tmp_path, test_cluster):
    # use 0.05% of geonames corpus to generate data. We need something small but >1000 docs to properly test
    # the -1k corpus too.
    cmd = (
        f"--pipeline=benchmark-only --target-hosts=127.0.0.1:{test_cluster.http_port} --track=geonames "
        f'--challenge=append-no-conflicts-index-only --track-params="ingest_percentage:0.05" --on-error=abort '
        f'--include-tasks="delete-index,create-index,check-cluster-health,index-append" --quiet'
    )
    assert it.race(cfg, cmd) == 0

    # create the track
    track_name = f"test-track-{uuid.uuid4()}"
    track_path = tmp_path / track_name

    assert (
        it.esrally(
            cfg,
            f"create-track --target-hosts=127.0.0.1:{test_cluster.http_port} --indices=geonames "
            f"--track={track_name} --output-path={tmp_path}",
        )
        == 0
    )

    base_generated_corpora = "geonames-documents"
    expected_files = [
        "track.json",
        "geonames.json",
        f"{base_generated_corpora}-1k.json",
        f"{base_generated_corpora}.json",
        f"{base_generated_corpora}-1k.json.bz2",
        f"{base_generated_corpora}.json.bz2",
    ]

    for f in expected_files:
        full_path = track_path / f
        assert full_path.exists(), f"Expected file to exist at path [{full_path}]"

    with open(track_path / f"{base_generated_corpora}-1k.json", "rt") as f:
        num_lines = sum(1 for line in f)
    assert (
        num_lines == 1000
    ), f"Corpora [{base_generated_corpora}-1k.json] used by test-mode is [{num_lines}] lines but should be 1000 lines"

    # run a benchmark in test mode with the created track
    cmd = f"--test-mode --pipeline=benchmark-only --target-hosts=127.0.0.1:{test_cluster.http_port} --track-path={track_path}"
    assert it.race(cfg, cmd) == 0

    # and also run a normal (short) benchmark using the created track
    cmd = f"--pipeline=benchmark-only --target-hosts=127.0.0.1:{test_cluster.http_port} --track-path={track_path}"
    assert it.race(cfg, cmd) == 0
