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

import concurrent.futures
import io
import json
import time
from datetime import datetime
from unittest import TestCase, mock

from esrally import config, metrics
from esrally.driver import async_driver
from esrally.track import track, params
from tests import as_future


class TimerTests(TestCase):
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self):
            self.count += 1

    def test_scheduled_tasks(self):
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        timer = async_driver.Timer(wakeup_interval=0.1)
        counter = TimerTests.Counter()
        timer.add_task(fn=counter, interval=0.2)

        pool.submit(timer)

        time.sleep(0.45)
        timer.stop()
        pool.shutdown()

        self.assertEqual(2, counter.count)


class StaticClientFactory:
    SYNC_PATCHER = None
    ASYNC_PATCHER = None

    def __init__(self, *args, **kwargs):
        StaticClientFactory.SYNC_PATCHER = mock.patch("elasticsearch.Elasticsearch")
        self.es = StaticClientFactory.SYNC_PATCHER.start()
        self.es.indices.stats.return_value = {"mocked": True}
        self.es.info.return_value = {
            "cluster_name": "elasticsearch",
            "version": {
                "number": "7.3.0",
                "build_flavor": "oss",
                "build_type": "tar",
                "build_hash": "de777fa",
                "build_date": "2019-07-24T18:30:11.767338Z",
                "build_snapshot": False,
                "lucene_version": "8.1.0",
                "minimum_wire_compatibility_version": "6.8.0",
                "minimum_index_compatibility_version": "6.0.0-beta1"
            }
        }

        StaticClientFactory.ASYNC_PATCHER = mock.patch("elasticsearch.Elasticsearch")
        self.es_async = StaticClientFactory.ASYNC_PATCHER.start()
        self.es_async.init_request_context.return_value = {
            "request_start": 0,
            "request_end": 10
        }
        bulk_response = {
            "errors": False,
            "took": 5
        }
        # bulk responses are raw strings
        self.es_async.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))
        self.es_async.transport.close.return_value = as_future()

    def create(self):
        return self.es

    def create_async(self):
        return self.es_async

    @classmethod
    def close(cls):
        StaticClientFactory.SYNC_PATCHER.stop()
        StaticClientFactory.ASYNC_PATCHER.stop()


class AsyncDriverTestParamSource:
    def __init__(self, track=None, params=None, **kwargs):
        if params is None:
            params = {}
        self._indices = track.indices
        self._params = params
        self._current = 1
        self._total = params.get("size")
        self.infinite = self._total is None

    def partition(self, partition_index, total_partitions):
        return self

    @property
    def percent_completed(self):
        if self.infinite:
            return None
        return self._current / self._total

    def params(self):
        if not self.infinite and self._current > self._total:
            raise StopIteration()
        self._current += 1
        return self._params


class AsyncDriverTests(TestCase):
    class Holder:
        def __init__(self, all_hosts=None, all_client_options=None):
            self.all_hosts = all_hosts
            self.all_client_options = all_client_options

    def test_run_benchmark(self):
        cfg = config.Config()

        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start",
                datetime(year=2017, month=8, day=20, hour=1, minute=0, second=0))
        cfg.add(config.Scope.application, "system", "race.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "driver", "on.error", "abort")
        cfg.add(config.Scope.application, "driver", "profiling", False)
        cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "track", "params", {})
        cfg.add(config.Scope.application, "track", "test.mode.enabled", True)
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "telemetry", "params", {})
        cfg.add(config.Scope.application, "mechanic", "car.names", ["external"])
        cfg.add(config.Scope.application, "mechanic", "skip.rest.api.check", True)
        cfg.add(config.Scope.application, "client", "hosts",
                AsyncDriverTests.Holder(all_hosts={"default": ["localhost:9200"]}))
        cfg.add(config.Scope.application, "client", "options",
                AsyncDriverTests.Holder(all_client_options={"default": {}}))

        params.register_param_source_for_name("bulk-param-source", AsyncDriverTestParamSource)

        task = track.Task(name="bulk-index",
                          operation=track.Operation(
                              "bulk-index",
                              track.OperationType.Bulk.name,
                              params={
                                  "body": ["action_metadata_line", "index_line"],
                                  "action-metadata-present": True,
                                  "bulk-size": 1,
                                  # we need this because the parameter source does not know that we only have one
                                  # bulk and hence size() returns incorrect results
                                  "size": 1
                              },
                              param_source="bulk-param-source"),
                          warmup_iterations=0,
                          iterations=1,
                          clients=1)

        current_challenge = track.Challenge(name="default", default=True, schedule=[task])
        current_track = track.Track(name="unit-test", challenges=[current_challenge])

        driver = async_driver.AsyncDriver(cfg, current_track, current_challenge,
                                          es_client_factory_class=StaticClientFactory)

        distribution_flavor, distribution_version, revision = driver.setup()
        self.assertEqual("oss", distribution_flavor)
        self.assertEqual("7.3.0", distribution_version)
        self.assertEqual("de777fa", revision)

        metrics_store_representation = driver.run()

        metric_store = metrics.metrics_store(cfg, read_only=True, track=current_track, challenge=current_challenge)
        metric_store.bulk_add(metrics_store_representation)

        self.assertIsNotNone(metric_store.get(name="latency", task="bulk-index", sample_type=metrics.SampleType.Normal))
        self.assertIsNotNone(metric_store.get(name="service_time", task="bulk-index", sample_type=metrics.SampleType.Normal))
        self.assertIsNotNone(metric_store.get(name="processing_time", task="bulk-index", sample_type=metrics.SampleType.Normal))
        self.assertIsNotNone(metric_store.get(name="throughput", task="bulk-index", sample_type=metrics.SampleType.Normal))
        self.assertIsNotNone(metric_store.get(name="node_total_young_gen_gc_time", sample_type=metrics.SampleType.Normal))
        self.assertIsNotNone(metric_store.get(name="node_total_old_gen_gc_time", sample_type=metrics.SampleType.Normal))
        # ensure that there are not more documents than we expect
        self.assertEqual(6, len(metric_store.docs))

    def tearDown(self):
        StaticClientFactory.close()
