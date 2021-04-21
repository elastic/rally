# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
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


@it.random_rally_config
def test_tar_distributions(cfg):
    port = 19200
    for dist in it.DISTRIBUTIONS:
        for track in it.TRACKS:
            it.wait_until_port_is_free(port_number=port)
            assert it.race(cfg, f"--distribution-version=\"{dist}\" --track=\"{track}\" "
                                f"--test-mode --car=4gheap --target-hosts=127.0.0.1:{port}") == 0


@it.random_rally_config
def test_docker_distribution(cfg):
    port = 19200
    # only test the most recent Docker distribution
    dist = it.DISTRIBUTIONS[-1]
    it.wait_until_port_is_free(port_number=port)
    assert it.race(cfg, f"--pipeline=\"docker\" --distribution-version=\"{dist}\" "
                        f"--track=\"geonames\" --challenge=\"append-no-conflicts-index-only\" --test-mode "
                        f"--car=4gheap --target-hosts=127.0.0.1:{port}") == 0


@it.random_rally_config
def test_does_not_benchmark_unsupported_distribution(cfg):
    port = 19200
    it.wait_until_port_is_free(port_number=port)
    assert it.race(cfg, f"--distribution-version=\"1.7.6\" --track=\"{it.TRACKS[0]}\" "
                        f"--target-hosts=127.0.0.1:{port} --test-mode --car=4gheap") != 0


@pytest.fixture(scope="module")
def test_cluster():
    cluster = it.TestCluster("in-memory-it")
    # test with a recent distribution as eventdata is not available for all versions
    dist = it.DISTRIBUTIONS[-1]
    port = 19200
    race_id = str(uuid.uuid4())

    it.wait_until_port_is_free(port_number=port)
    cluster.install(distribution_version=dist, node_name="rally-node", car="4gheap", http_port=port)
    cluster.start(race_id=race_id)
    yield cluster
    cluster.stop()


@it.random_rally_config
def test_eventdata_frozen(cfg, test_cluster):
    challenges = ["frozen-data-generation", "frozen-querying"]
    track_params = "number_of_replicas:0"
    execute_eventdata(cfg, test_cluster, challenges, track_params)


@it.random_rally_config
def test_eventdata_indexing_and_querying(cfg, test_cluster):
    challenges = ["elasticlogs-1bn-load",
                  "elasticlogs-continuous-index-and-query",
                  "combined-indexing-and-querying",
                  "elasticlogs-querying"]
    track_params = "bulk_indexing_clients:1,number_of_replicas:0,rate_limit_max:2,rate_limit_duration_secs:5," \
                   "p1_bulk_indexing_clients:1,p2_bulk_indexing_clients:1,p1_duration_secs:5,p2_duration_secs:5"
    execute_eventdata(cfg, test_cluster, challenges, track_params)


@it.random_rally_config
def test_eventdata_update(cfg, test_cluster):
    challenges = ["bulk-update"]
    track_params = "bulk_indexing_clients:1,number_of_replicas:0"
    execute_eventdata(cfg, test_cluster, challenges, track_params)


@it.random_rally_config
def test_eventdata_daily_volume(cfg, test_cluster):
    challenges = ["index-logs-fixed-daily-volume", "index-and-query-logs-fixed-daily-volume"]
    track_params = "bulk_indexing_clients:1,number_of_replicas:0,daily_logging_volume:1MB"
    execute_eventdata(cfg, test_cluster, challenges, track_params)


def execute_eventdata(cfg, test_cluster, challenges, track_params):
    for challenge in challenges:
        cmd = f"--test-mode --pipeline=benchmark-only --target-host=127.0.0.1:{test_cluster.http_port} " \
              f"--track-repository=eventdata --track=eventdata --track-params=\"{track_params}\" " \
              f"--challenge={challenge}"
        assert it.race(cfg, cmd) == 0
