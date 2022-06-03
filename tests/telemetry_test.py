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

import collections
import copy
import logging
import random
import unittest.mock as mock
from collections import namedtuple
from unittest.mock import call

import elasticsearch
import pytest

from esrally import config, exceptions, metrics, telemetry
from esrally.mechanic import cluster
from esrally.metrics import MetaInfoScope
from esrally.utils import console


class TestSamplerThread:
    def test_sampler_thread_start_join(self):
        sampler = telemetry.SamplerThread(recorder=mock.Mock())
        sampler.stop = True
        sampler.start()
        sampler.join()

    def test_sampler_sleep_long_interval(self):
        time_left = 21
        sleep_record = []

        def record_sleep(seconds):
            nonlocal time_left
            nonlocal sleep_record
            nonlocal sampler
            sleep_record.append(seconds)
            time_left -= seconds
            if time_left <= 0:
                sampler.stop = True

        recorder = mock.Mock()
        recorder.sample_interval = 10
        sampler = telemetry.Sampler(recorder, sleep=record_sleep)
        sampler.run()

        assert sleep_record == [1] * 21
        assert recorder.record.call_count == 2

    def test_sampler_sleep_short_interval(self):
        time_left = 1.5
        sleep_record = []

        def record_sleep(seconds):
            nonlocal time_left
            nonlocal sleep_record
            nonlocal sampler
            sleep_record.append(seconds)
            time_left -= seconds
            if time_left <= 0:
                sampler.stop = True

        recorder = mock.Mock()
        recorder.sample_interval = 0.1
        sampler = telemetry.Sampler(recorder, sleep=record_sleep)
        sampler.run()

        assert sleep_record == [0.1] * 15
        assert recorder.record.call_count == 15


def create_config():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
    # concrete path does not matter
    cfg.add(config.Scope.application, "node", "rally.root", "/some/root/path")

    cfg.add(config.Scope.application, "reporting", "datastore.host", "localhost")
    cfg.add(config.Scope.application, "reporting", "datastore.port", "0")
    cfg.add(config.Scope.application, "reporting", "datastore.secure", False)
    cfg.add(config.Scope.application, "reporting", "datastore.user", "")
    cfg.add(config.Scope.application, "reporting", "datastore.password", "")
    # disable version probing to avoid any network calls in tests
    cfg.add(config.Scope.application, "reporting", "datastore.probe.cluster_version", False)
    # only internal devices are active
    cfg.add(config.Scope.application, "telemetry", "devices", [])
    return cfg


class MockTelemetryDevice(telemetry.InternalTelemetryDevice):
    def __init__(self, mock_java_opts):
        super().__init__()
        self.mock_java_opts = mock_java_opts

    def instrument_java_opts(self):
        return self.mock_java_opts


class TestTelemetry:
    def test_merges_options_set_by_different_devices(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", "jfr")
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "challenge-root")
        cfg.add(config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

        devices = [
            MockTelemetryDevice(["-Xms256M"]),
            MockTelemetryDevice(["-Xmx512M"]),
            MockTelemetryDevice(["-Des.network.host=127.0.0.1"]),
        ]

        t = telemetry.Telemetry(enabled_devices=None, devices=devices)

        opts = t.instrument_candidate_java_opts()

        assert opts == ["-Xms256M", "-Xmx512M", "-Des.network.host=127.0.0.1"]


class TestStartupTime:
    @mock.patch("esrally.time.StopWatch")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_store_calculated_metrics(self, metrics_store_put_value, stop_watch):
        stop_watch.total_time.return_value = 2
        metrics_store = metrics.EsMetricsStore(create_config())
        node = cluster.Node(None, "/bin", "io", "rally0", None)
        startup_time = telemetry.StartupTime()
        # replace with mock
        startup_time.timer = stop_watch

        startup_time.on_pre_node_start(node.node_name)
        # ... nodes starts up ...
        startup_time.attach_to_node(node)
        startup_time.store_system_metrics(node, metrics_store)

        metrics_store_put_value.assert_called_with("rally0", "node_startup_time", 2, "s")


class Client:
    def __init__(self, nodes=None, info=None, indices=None, transform=None, cluster=None, transport_client=None):
        self.nodes = nodes
        self._info = wrap(info)
        self.indices = indices
        self.transform = transform
        self.cluster = cluster
        if transport_client:
            self.transport = transport_client

    def info(self):
        return self._info()


class SubClient:
    def __init__(self, stats=None, info=None, recovery=None, transform_stats=None, data_streams_stats=None, state=None):
        self._stats = wrap(stats)
        self._info = wrap(info)
        self._recovery = wrap(recovery)
        self._transform_stats = wrap(transform_stats)
        self._data_streams_stats = wrap(data_streams_stats)
        self._state = wrap(state)

    def stats(self, *args, **kwargs):
        return self._stats()

    def info(self, *args, **kwargs):
        return self._info()

    def recovery(self, *args, **kwargs):
        return self._recovery()

    def get_transform_stats(self, *args, **kwargs):
        return self._transform_stats()

    def data_streams_stats(self, *args, **kwargs):
        return self._data_streams_stats()

    def state(self, *args, **kwargs):
        return self._state()


def wrap(it):
    return it if callable(it) else ResponseSupplier(it)


class ResponseSupplier:
    def __init__(self, response):
        self.response = response

    def __call__(self, *args, **kwargs):
        return self.response


class TransportErrorSupplier:
    def __call__(self, *args, **kwargs):
        raise elasticsearch.TransportError


raiseTransportError = TransportErrorSupplier()


class TransportClient:
    def __init__(self, response=None, force_error=False, error=elasticsearch.TransportError(message="transport error")):
        self._response = response
        self._force_error = force_error
        self._error = error
        self.args = []
        self.kwargs = []

    def perform_request(self, *args, **kwargs):
        self.args.append(args)
        self.kwargs.append(kwargs)
        if self._force_error:
            raise self._error
        else:
            return copy.deepcopy(self._response)


class TestJfr:
    def test_sets_options_for_pre_java_9_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(0, 8))
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:+UnlockCommercialFeatures",
            "-XX:+FlightRecorder",
            "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath=/var/log/test-recording.jfr",
            "-XX:StartFlightRecording=defaultrecording=true",
        ]

    def test_sets_options_for_java_9_or_10_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(9, 10))
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:+UnlockCommercialFeatures",
            "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename=/var/log/test-recording.jfr",
        ]

    def test_sets_options_for_java_11_or_above_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(11, 999))
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename=/var/log/test-recording.jfr",
        ]

    def test_sets_options_for_pre_java_9_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(
            telemetry_params={"recording-template": "profile"}, log_root="/var/log", java_major_version=random.randint(0, 8)
        )
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:+UnlockCommercialFeatures",
            "-XX:+FlightRecorder",
            "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath=/var/log/test-recording.jfr",
            "-XX:StartFlightRecording=defaultrecording=true,settings=profile",
        ]

    def test_sets_options_for_java_9_or_10_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(
            telemetry_params={"recording-template": "profile"}, log_root="/var/log", java_major_version=random.randint(9, 10)
        )
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:+UnlockCommercialFeatures",
            "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,"
            "filename=/var/log/test-recording.jfr,settings=profile",
        ]

    def test_sets_options_for_java_11_or_above_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(
            telemetry_params={"recording-template": "profile"}, log_root="/var/log", java_major_version=random.randint(11, 999)
        )
        assert jfr.java_opts("/var/log/test-recording.jfr") == [
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints",
            "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,"
            "filename=/var/log/test-recording.jfr,settings=profile",
        ]


class TestGc:
    def test_sets_options_for_pre_java_9(self):
        gc = telemetry.Gc(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(0, 8))
        assert gc.java_opts("/var/log/defaults-node-0.gc.log") == [
            "-Xloggc:/var/log/defaults-node-0.gc.log",
            "-XX:+PrintGCDetails",
            "-XX:+PrintGCDateStamps",
            "-XX:+PrintGCTimeStamps",
            "-XX:+PrintGCApplicationStoppedTime",
            "-XX:+PrintGCApplicationConcurrentTime",
            "-XX:+PrintTenuringDistribution",
        ]

    def test_sets_options_for_java_9_or_above(self):
        gc = telemetry.Gc(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(9, 999))
        assert gc.java_opts("/var/log/defaults-node-0.gc.log") == [
            "-Xlog:gc*=info,safepoint=info,age*=trace:file=/var/log/defaults-node-0.gc.log:utctime,uptimemillis,level,tags:filecount=0"
        ]

    def test_can_override_options_for_java_9_or_above(self):
        gc = telemetry.Gc(
            telemetry_params={"gc-log-config": "gc,safepoint"}, log_root="/var/log", java_major_version=random.randint(9, 999)
        )
        assert gc.java_opts("/var/log/defaults-node-0.gc.log") == [
            "-Xlog:gc,safepoint:file=/var/log/defaults-node-0.gc.log:utctime,uptimemillis,level,tags:filecount=0"
        ]


class TestHeapdump:
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_generates_heap_dump(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        heapdump = telemetry.Heapdump("/var/log")
        t = telemetry.Telemetry(enabled_devices=[heapdump.command], devices=[heapdump])
        node = cluster.Node(pid="1234", binary_path="/bin", host_name="localhost", node_name="rally0", telemetry=t)
        t.attach_to_node(node)
        t.detach_from_node(node, running=True)
        run_subprocess_with_logging.assert_called_with("jmap -dump:format=b,file=/var/log/heap_at_exit_1234.hprof 1234")


class TestSegmentStats:
    @mock.patch("elasticsearch.Elasticsearch")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_generates_log_file(self, file_mock, es):

        stats_response = """
        index    shard prirep ip        segment generation docs.count docs.deleted   size size.memory committed searchable version compound
        geonames 0     p      127.0.0.1 _0               0        212            0 72.3kb        9621 true      true       8.4.0   true
        """

        es.cat.segments.return_value = stats_response

        segment_stats = telemetry.SegmentStats("/var/log", es)
        segment_stats.on_benchmark_stop()
        es.cat.segments.assert_called_with(index="_all", v=True)
        file_mock.assert_has_calls(
            [
                call("/var/log/segment_stats.log", "wt"),
                call().__enter__(),
                call().write(stats_response),
                call().__exit__(None, None, None),
            ]
        )


class TestCcrStats:
    def test_negative_sample_interval_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"ccr-stats-sample-interval": -1 * random.random()}
        with pytest.raises(
            exceptions.SystemSetupError, match=r"The telemetry parameter 'ccr-stats-sample-interval' must be greater than zero but was .*\."
        ):
            telemetry.CcrStats(telemetry_params, clients, metrics_store)

    def test_wrong_cluster_name_in_ccr_stats_indices_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {
            "ccr-stats-indices": {
                "default": ["leader"],
                "wrong_cluster_name": ["follower"],
            },
        }
        client_keys = ",".join(sorted(clients.keys()))
        with pytest.raises(
            exceptions.SystemSetupError,
            match=(
                r"The telemetry parameter 'ccr-stats-indices' must be a JSON Object with keys matching "
                rf"the cluster names \[{client_keys}] specified in --target-hosts "
                r"but it had \[wrong_cluster_name\]."
            ),
        ):
            telemetry.CcrStats(telemetry_params, clients, metrics_store)


class TestCcrStatsRecorder:
    java_signed_maxlong = (2**63) - 1

    def test_raises_exception_on_transport_error(self):
        client = Client(transport_client=TransportClient(response={}, force_error=True))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        with pytest.raises(
            exceptions.RallyError,
            match=r"A transport error occurred while collecting CCR stats from the endpoint "
            r"\[/_ccr/stats\?filter_path=follow_stats\] on "
            r"cluster \[remote\]",
        ):
            telemetry.CcrStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=1).record()

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_ccr_stats(self, metrics_store_put_doc):
        java_signed_maxlong = self.java_signed_maxlong

        shard_id = random.randint(0, 999)
        remote_cluster = "leader_cluster"
        leader_index = "leader"
        follower_index = "follower"
        leader_global_checkpoint = random.randint(0, java_signed_maxlong)
        leader_max_seq_no = random.randint(0, java_signed_maxlong)
        follower_global_checkpoint = random.randint(0, java_signed_maxlong)
        follower_max_seq_no = random.randint(0, java_signed_maxlong)
        last_requested_seq_no = random.randint(0, java_signed_maxlong)
        outstanding_read_requests = random.randint(0, java_signed_maxlong)
        outstanding_write_requests = random.randint(0, java_signed_maxlong)
        write_buffer_operation_count = random.randint(0, java_signed_maxlong)
        follower_mapping_version = random.randint(0, java_signed_maxlong)
        total_read_time_millis = random.randint(0, java_signed_maxlong)
        total_read_remote_exec_time_millis = random.randint(0, java_signed_maxlong)
        successful_read_requests = random.randint(0, java_signed_maxlong)
        failed_read_requests = random.randint(0, java_signed_maxlong)
        operations_read = random.randint(0, java_signed_maxlong)
        bytes_read = random.randint(0, java_signed_maxlong)
        total_write_time_millis = random.randint(0, java_signed_maxlong)
        successful_write_requests = random.randint(0, java_signed_maxlong)
        failed_write_requests = random.randint(0, java_signed_maxlong)
        operations_written = random.randint(0, java_signed_maxlong)
        read_exceptions = []
        time_since_last_read_millis = random.randint(0, java_signed_maxlong)

        ccr_stats_follower_response = {
            "auto_follow_stats": {
                "number_of_failed_follow_indices": 0,
                "number_of_failed_remote_cluster_state_requests": 0,
                "number_of_successful_follow_indices": 0,
                "recent_auto_follow_errors": [],
            },
            "follow_stats": {
                "indices": [
                    {
                        "index": follower_index,
                        "shards": [
                            {
                                "shard_id": shard_id,
                                "remote_cluster": remote_cluster,
                                "leader_index": leader_index,
                                "follower_index": follower_index,
                                "leader_global_checkpoint": leader_global_checkpoint,
                                "leader_max_seq_no": leader_max_seq_no,
                                "follower_global_checkpoint": follower_global_checkpoint,
                                "follower_max_seq_no": follower_max_seq_no,
                                "last_requested_seq_no": last_requested_seq_no,
                                "outstanding_read_requests": outstanding_read_requests,
                                "outstanding_write_requests": outstanding_write_requests,
                                "write_buffer_operation_count": write_buffer_operation_count,
                                "follower_mapping_version": follower_mapping_version,
                                "total_read_time_millis": total_read_time_millis,
                                "total_read_remote_exec_time_millis": total_read_remote_exec_time_millis,
                                "successful_read_requests": successful_read_requests,
                                "failed_read_requests": failed_read_requests,
                                "operations_read": operations_read,
                                "bytes_read": bytes_read,
                                "total_write_time_millis": total_write_time_millis,
                                "successful_write_requests": successful_write_requests,
                                "failed_write_requests": failed_write_requests,
                                "operations_written": operations_written,
                                "read_exceptions": read_exceptions,
                                "time_since_last_read_millis": time_since_last_read_millis,
                            }
                        ],
                    }
                ]
            },
        }

        ccr_stats_filtered_follower_response = {"follow_stats": ccr_stats_follower_response["follow_stats"]}

        client = Client(transport_client=TransportClient(response=ccr_stats_filtered_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=1)
        recorder.record()

        shard_metadata = {"cluster": "remote", "index": follower_index}

        metrics_store_put_doc.assert_called_with(
            {
                "name": "ccr-stats",
                "shard": ccr_stats_filtered_follower_response["follow_stats"]["indices"][0]["shards"][0],
            },
            level=MetaInfoScope.cluster,
            meta_data=shard_metadata,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_ccr_stats_many_shards(self, metrics_store_put_doc):
        java_signed_maxlong = self.java_signed_maxlong

        remote_cluster = "leader_cluster"
        leader_index = "leader"
        follower_index = "follower"
        shard_range = range(2)
        leader_global_checkpoint = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        leader_max_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        follower_global_checkpoint = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        follower_max_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        last_requested_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        outstanding_read_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        outstanding_write_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        write_buffer_operation_count = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        follower_mapping_version = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_read_time_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_read_remote_exec_time_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        successful_read_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        failed_read_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        operations_read = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        bytes_read = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_write_time_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        successful_write_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        failed_write_requests = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        operations_written = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        read_exceptions = [[] for _ in shard_range]
        time_since_last_read_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]

        ccr_stats_follower_response = {
            "auto_follow_stats": {
                "number_of_failed_follow_indices": 0,
                "number_of_failed_remote_cluster_state_requests": 0,
                "number_of_successful_follow_indices": 0,
                "recent_auto_follow_errors": [],
            },
            "follow_stats": {
                "indices": [
                    {
                        "index": follower_index,
                        "shards": [
                            {
                                "shard_id": shard_id,
                                "remote_cluster": remote_cluster,
                                "leader_index": leader_index,
                                "follower_index": follower_index,
                                "leader_global_checkpoint": leader_global_checkpoint[shard_id],
                                "leader_max_seq_no": leader_max_seq_no[shard_id],
                                "follower_global_checkpoint": follower_global_checkpoint[shard_id],
                                "follower_max_seq_no": follower_max_seq_no[shard_id],
                                "last_requested_seq_no": last_requested_seq_no[shard_id],
                                "outstanding_read_requests": outstanding_read_requests[shard_id],
                                "outstanding_write_requests": outstanding_write_requests[shard_id],
                                "write_buffer_operation_count": write_buffer_operation_count[shard_id],
                                "follower_mapping_version": follower_mapping_version[shard_id],
                                "total_read_time_millis": total_read_time_millis[shard_id],
                                "total_read_remote_exec_time_millis": total_read_remote_exec_time_millis[shard_id],
                                "successful_read_requests": successful_read_requests[shard_id],
                                "failed_read_requests": failed_read_requests[shard_id],
                                "operations_read": operations_read[shard_id],
                                "bytes_read": bytes_read[shard_id],
                                "total_write_time_millis": total_write_time_millis[shard_id],
                                "successful_write_requests": successful_write_requests[shard_id],
                                "failed_write_requests": failed_write_requests[shard_id],
                                "operations_written": operations_written[shard_id],
                                "read_exceptions": read_exceptions[shard_id],
                                "time_since_last_read_millis": time_since_last_read_millis[shard_id],
                            }
                            for shard_id in shard_range
                        ],
                    }
                ]
            },
        }

        ccr_stats_filtered_follower_response = {"follow_stats": ccr_stats_follower_response["follow_stats"]}

        client = Client(transport_client=TransportClient(response=ccr_stats_filtered_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder("remote", client, metrics_store, 1)
        recorder.record()

        shard_metadata = [
            {
                "cluster": "remote",
                "index": "follower",
            },
            {
                "cluster": "remote",
                "index": "follower",
            },
        ]

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {"name": "ccr-stats", "shard": ccr_stats_filtered_follower_response["follow_stats"]["indices"][0]["shards"][0]},
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata[0],
                ),
                mock.call(
                    {"name": "ccr-stats", "shard": ccr_stats_filtered_follower_response["follow_stats"]["indices"][0]["shards"][1]},
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata[1],
                ),
            ],
            any_order=True,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_filtered_ccr_stats(self, metrics_store_put_doc):
        java_signed_maxlong = self.java_signed_maxlong

        remote_cluster = "leader_cluster"
        leader_index1 = "leader1"
        follower_index1 = "follower1"
        leader_index2 = "leader2"
        follower_index2 = "follower2"
        leader_global_checkpoint = random.randint(0, java_signed_maxlong)
        leader_max_seq_no = random.randint(0, java_signed_maxlong)
        follower_global_checkpoint = random.randint(0, java_signed_maxlong)
        follower_max_seq_no = random.randint(0, java_signed_maxlong)
        last_requested_seq_no = random.randint(0, java_signed_maxlong)
        outstanding_read_requests = random.randint(0, java_signed_maxlong)
        outstanding_write_requests = random.randint(0, java_signed_maxlong)
        write_buffer_operation_count = random.randint(0, java_signed_maxlong)
        follower_mapping_version = random.randint(0, java_signed_maxlong)
        total_read_time_millis = random.randint(0, java_signed_maxlong)
        total_read_remote_exec_time_millis = random.randint(0, java_signed_maxlong)
        successful_read_requests = random.randint(0, java_signed_maxlong)
        failed_read_requests = random.randint(0, java_signed_maxlong)
        operations_read = random.randint(0, java_signed_maxlong)
        bytes_read = random.randint(0, java_signed_maxlong)
        total_write_time_millis = random.randint(0, java_signed_maxlong)
        successful_write_requests = random.randint(0, java_signed_maxlong)
        failed_write_requests = random.randint(0, java_signed_maxlong)
        operations_written = random.randint(0, java_signed_maxlong)
        read_exceptions = []
        time_since_last_read_millis = random.randint(0, java_signed_maxlong)

        ccr_stats_follower_response = {
            "auto_follow_stats": {
                "number_of_failed_follow_indices": 0,
                "number_of_failed_remote_cluster_state_requests": 0,
                "number_of_successful_follow_indices": 0,
                "recent_auto_follow_errors": [],
            },
            "follow_stats": {
                "indices": [
                    {
                        "index": follower_index1,
                        "shards": [
                            {
                                "shard_id": 0,
                                "remote_cluster": remote_cluster,
                                "leader_index": leader_index1,
                                "follower_index": follower_index1,
                                "leader_global_checkpoint": leader_global_checkpoint,
                                "leader_max_seq_no": leader_max_seq_no,
                                "follower_global_checkpoint": follower_global_checkpoint,
                                "follower_max_seq_no": follower_max_seq_no,
                                "last_requested_seq_no": last_requested_seq_no,
                                "outstanding_read_requests": outstanding_read_requests,
                                "outstanding_write_requests": outstanding_write_requests,
                                "write_buffer_operation_count": write_buffer_operation_count,
                                "follower_mapping_version": follower_mapping_version,
                                "total_read_time_millis": total_read_time_millis,
                                "total_read_remote_exec_time_millis": total_read_remote_exec_time_millis,
                                "successful_read_requests": successful_read_requests,
                                "failed_read_requests": failed_read_requests,
                                "operations_read": operations_read,
                                "bytes_read": bytes_read,
                                "total_write_time_millis": total_write_time_millis,
                                "successful_write_requests": successful_write_requests,
                                "failed_write_requests": failed_write_requests,
                                "operations_written": operations_written,
                                "read_exceptions": read_exceptions,
                                "time_since_last_read_millis": time_since_last_read_millis,
                            }
                        ],
                    },
                    {
                        "index": follower_index2,
                        "shards": [
                            {
                                "shard_id": 0,
                                "remote_cluster": remote_cluster,
                                "leader_index": leader_index2,
                                "follower_index": follower_index2,
                                "leader_global_checkpoint": leader_global_checkpoint,
                                "leader_max_seq_no": leader_max_seq_no,
                                "follower_global_checkpoint": follower_global_checkpoint,
                                "follower_max_seq_no": follower_max_seq_no,
                                "last_requested_seq_no": last_requested_seq_no,
                                "outstanding_read_requests": outstanding_read_requests,
                                "outstanding_write_requests": outstanding_write_requests,
                                "write_buffer_operation_count": write_buffer_operation_count,
                                "follower_mapping_version": follower_mapping_version,
                                "total_read_time_millis": total_read_time_millis,
                                "total_read_remote_exec_time_millis": total_read_remote_exec_time_millis,
                                "successful_read_requests": successful_read_requests,
                                "failed_read_requests": failed_read_requests,
                                "operations_read": operations_read,
                                "bytes_read": bytes_read,
                                "total_write_time_millis": total_write_time_millis,
                                "successful_write_requests": successful_write_requests,
                                "failed_write_requests": failed_write_requests,
                                "operations_written": operations_written,
                                "read_exceptions": read_exceptions,
                                "time_since_last_read_millis": time_since_last_read_millis,
                            }
                        ],
                    },
                ]
            },
        }

        ccr_stats_filtered_follower_response = {"follow_stats": ccr_stats_follower_response["follow_stats"]}
        client = Client(transport_client=TransportClient(response=ccr_stats_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder("remote", client, metrics_store, 1, indices=[follower_index1])
        recorder.record()

        shard_metadata = {"cluster": "remote", "index": follower_index1}

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "ccr-stats",
                        "shard": ccr_stats_filtered_follower_response["follow_stats"]["indices"][0]["shards"][0],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata,
                )
            ],
            any_order=True,
        )


class TestRecoveryStats:
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_no_metrics_if_no_pending_recoveries(self, metrics_store_put_doc):
        response = {}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(indices=SubClient(recovery=response))
        recorder = telemetry.RecoveryStatsRecorder(
            cluster_name="leader", client=client, metrics_store=metrics_store, sample_interval=1, indices=["index1"]
        )
        recorder.record()

        assert metrics_store_put_doc.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_single_shard_stats(self, metrics_store_put_doc):
        response = {
            "index1": {
                "shards": [
                    {
                        "id": 0,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "start_time": "2014-02-24T12:38:06.349",
                        "start_time_in_millis": "1393245486349",
                        "stop_time": "2014-02-24T12:38:08.464",
                        "stop_time_in_millis": "1393245488464",
                        "total_time": "2.1s",
                        "total_time_in_millis": 2115,
                        "source": {
                            "id": "RGMdRc-yQWWKIBM4DGvwqQ",
                            "host": "my.fqdn",
                            "transport_address": "my.fqdn",
                            "ip": "10.0.1.7",
                            "name": "my_es_node",
                        },
                        "target": {
                            "id": "RGMdRc-yQWWKIBM4DGvwqQ",
                            "host": "my.fqdn",
                            "transport_address": "my.fqdn",
                            "ip": "10.0.1.7",
                            "name": "my_es_node",
                        },
                        "index": {
                            "size": {
                                "total": "24.7mb",
                                "total_in_bytes": 26001617,
                                "reused": "24.7mb",
                                "reused_in_bytes": 26001617,
                                "recovered": "0b",
                                "recovered_in_bytes": 0,
                                "percent": "100.0%",
                            },
                            "files": {"total": 26, "reused": 26, "recovered": 0, "percent": "100.0%"},
                            "total_time": "2ms",
                            "total_time_in_millis": 2,
                            "source_throttle_time": "0s",
                            "source_throttle_time_in_millis": 0,
                            "target_throttle_time": "0s",
                            "target_throttle_time_in_millis": 0,
                        },
                        "translog": {
                            "recovered": 71,
                            "total": 0,
                            "percent": "100.0%",
                            "total_on_start": 0,
                            "total_time": "2.0s",
                            "total_time_in_millis": 2025,
                        },
                        "verify_index": {
                            "check_index_time": 0,
                            "check_index_time_in_millis": 0,
                            "total_time": "88ms",
                            "total_time_in_millis": 88,
                        },
                    }
                ]
            }
        }

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(indices=SubClient(recovery=response))
        recorder = telemetry.RecoveryStatsRecorder(
            cluster_name="leader", client=client, metrics_store=metrics_store, sample_interval=1, indices=["index1"]
        )
        recorder.record()

        shard_metadata = {"cluster": "leader", "index": "index1", "shard": 0}

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {"name": "recovery-stats", "shard": response["index1"]["shards"][0]},
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata,
                )
            ],
            any_order=True,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_multi_index_multi_shard_stats(self, metrics_store_put_doc):
        response = {
            "index1": {
                "shards": [
                    {
                        # for the test we only assume a subset of the fields
                        "id": 0,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "total_time_in_millis": 100,
                    },
                    {
                        "id": 1,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "total_time_in_millis": 200,
                    },
                ]
            },
            "index2": {
                "shards": [
                    {
                        "id": 0,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "total_time_in_millis": 300,
                    },
                    {
                        "id": 1,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "total_time_in_millis": 400,
                    },
                    {
                        "id": 2,
                        "type": "STORE",
                        "stage": "DONE",
                        "primary": True,
                        "total_time_in_millis": 500,
                    },
                ]
            },
        }

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(indices=SubClient(recovery=response))
        recorder = telemetry.RecoveryStatsRecorder(
            cluster_name="leader", client=client, metrics_store=metrics_store, sample_interval=1, indices=["index1", "index2"]
        )
        recorder.record()

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "recovery-stats",
                        "shard": response["index1"]["shards"][0],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={
                        "cluster": "leader",
                        "index": "index1",
                        "shard": 0,
                    },
                ),
                mock.call(
                    {
                        "name": "recovery-stats",
                        "shard": response["index1"]["shards"][1],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={
                        "cluster": "leader",
                        "index": "index1",
                        "shard": 1,
                    },
                ),
                mock.call(
                    {
                        "name": "recovery-stats",
                        "shard": response["index2"]["shards"][0],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={
                        "cluster": "leader",
                        "index": "index2",
                        "shard": 0,
                    },
                ),
                mock.call(
                    {
                        "name": "recovery-stats",
                        "shard": response["index2"]["shards"][1],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={
                        "cluster": "leader",
                        "index": "index2",
                        "shard": 1,
                    },
                ),
                mock.call(
                    {
                        "name": "recovery-stats",
                        "shard": response["index2"]["shards"][2],
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={
                        "cluster": "leader",
                        "index": "index2",
                        "shard": 2,
                    },
                ),
            ],
            any_order=True,
        )


class TestDataStreamStats:
    def test_failure_if_feature_not_implemented_in_version(self):
        # Data Streams aren't available prior to 7.9
        clients = {"default": Client(info={"version": {"number": "7.6.0"}})}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"data-stream-stats-sample-interval": random.randint(1, 100)}
        t = telemetry.DataStreamStats(telemetry_params, clients, metrics_store)
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The data-stream-stats telemetry device can only be used with clusters from version 7.9 onwards",
        ):
            t.on_benchmark_start()

    def test_failure_if_feature_not_implemented_in_distribution(self):
        # Data Streams aren't available with the OSS distribution
        clients = {"default": Client(info={"version": {"number": "7.9.0", "build_flavor": "oss"}})}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"data-stream-stats-sample-interval": random.randint(1, 100)}
        t = telemetry.DataStreamStats(telemetry_params, clients, metrics_store)
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The data-stream-stats telemetry device cannot be used with an OSS distribution of Elasticsearch",
        ):
            t.on_benchmark_start()

    def test_negative_sample_interval_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"data-stream-stats-sample-interval": -1 * random.random()}
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The telemetry parameter 'data-stream-stats-sample-interval' must be greater than zero but was .*\.",
        ):
            telemetry.DataStreamStats(telemetry_params, clients, metrics_store)


class TestDataStreamStatsRecorder:
    data_streams_stats_response = {
        "_shards": {"total": 4, "successful": 2, "failed": 0},
        "data_stream_count": 2,
        "backing_indices": 2,
        "total_store_size_bytes": 878336,
        "data_streams": [
            {"data_stream": "my-data-stream-1", "backing_indices": 1, "store_size_bytes": 439137, "maximum_timestamp": 1579936446448},
            {"data_stream": "my-data-stream-2", "backing_indices": 1, "store_size_bytes": 439199, "maximum_timestamp": 1579936446448},
        ],
    }

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_store_multiple_data_stream_stats(self, metrics_store_put_doc):
        client = Client(indices=SubClient(data_streams_stats=self.data_streams_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.DataStreamStatsRecorder(
            cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=1 * random.randint(1, 100)
        )
        recorder.record()

        data_stream_metadata = {"cluster": "remote"}

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "data_stream": "_all",
                        "name": "data-stream-stats",
                        "shards": {"total": 4, "successful_shards": 2, "failed_shards": 0},
                        "data_stream_count": 2,
                        "backing_indices": 2,
                        "total_store_size_bytes": 878336,
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=data_stream_metadata,
                ),
                mock.call(
                    {
                        "name": "data-stream-stats",
                        "data_stream": "my-data-stream-1",
                        "backing_indices": 1,
                        "store_size_bytes": 439137,
                        "maximum_timestamp": 1579936446448,
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=data_stream_metadata,
                ),
                mock.call(
                    {
                        "name": "data-stream-stats",
                        "data_stream": "my-data-stream-2",
                        "backing_indices": 1,
                        "store_size_bytes": 439199,
                        "maximum_timestamp": 1579936446448,
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=data_stream_metadata,
                ),
            ],
            any_order=True,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_empty_data_streams_list(self, metrics_store_put_doc):
        response = {
            "_shards": {"total": 0, "successful": 0, "failed": 0},
            "data_stream_count": 0,
            "backing_indices": 0,
            "total_store_size_bytes": 0,
            "data_streams": [],
        }

        client = Client(indices=SubClient(data_streams_stats=response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.DataStreamStatsRecorder(
            cluster_name="default", client=client, metrics_store=metrics_store, sample_interval=1 * random.randint(1, 100)
        )
        recorder.record()

        # Given an empty list of 'data_streams' we should only be
        # sending a total of one metric document containing both the `_shards` and overall stats
        assert metrics_store_put_doc.call_count == 1


class TestShardStats:
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_single_shard_stats(self, metrics_store_put_doc):
        node_stats_response = {
            "cluster_name": "elasticsearch",
            "nodes": {
                "Zbl_e8EyRXmiR47gbHgPfg": {
                    "timestamp": 1524379617017,
                    "name": "rally0",
                    "transport_address": "127.0.0.1:9300",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1:9300",
                    "roles": [
                        "master",
                        "data",
                        "ingest",
                    ],
                    "indices": {
                        "docs": {
                            "count": 76892364,
                            "deleted": 324530,
                        },
                        "store": {
                            "size_in_bytes": 983409834,
                        },
                        "shards": {
                            "geonames": [
                                {
                                    "0": {
                                        "routing": {
                                            "state": "STARTED",
                                            "primary": True,
                                            "node": "gHQpKO-IT5uo8WVTPmAZXw",
                                            "relocating_node": None,
                                        },
                                        "docs": {
                                            "count": 1000,
                                            "deleted": 0,
                                        },
                                        "store": {
                                            "size_in_bytes": 212027,
                                            "reserved_in_bytes": 0,
                                        },
                                        "segments": {
                                            "count": 8,
                                            "memory_in_bytes": 46872,
                                            "terms_memory_in_bytes": 13056,
                                            "stored_fields_memory_in_bytes": 3904,
                                            "term_vectors_memory_in_bytes": 0,
                                            "norms_memory_in_bytes": 0,
                                            "points_memory_in_bytes": 0,
                                            "doc_values_memory_in_bytes": 29912,
                                            "index_writer_memory_in_bytes": 0,
                                            "version_map_memory_in_bytes": 0,
                                            "fixed_bit_set_memory_in_bytes": 0,
                                            "max_unsafe_auto_id_timestamp": -1,
                                            "file_sizes": {},
                                        },
                                    }
                                }
                            ]
                        },
                    },
                }
            },
        }

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.ShardStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=53)
        recorder.record()

        shard_metadata = {"cluster": "remote"}

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "shard-stats",
                        "shard-id": "0",
                        "index": "geonames",
                        "primary": True,
                        "docs": 1000,
                        "store": 212027,
                        "segments-count": 8,
                        "node": "rally0",
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata,
                )
            ],
            any_order=True,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_missing_properties_shard_stats(self, metrics_store_put_doc):
        node_stats_response = {
            "cluster_name": "elasticsearch",
            "nodes": {
                "Zbl_e8EyRXmiR47gbHgPfg": {
                    "timestamp": 1524379617017,
                    "name": "rally0",
                    "transport_address": "127.0.0.1:9300",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1:9300",
                    "roles": [
                        "master",
                        "data",
                        "ingest",
                    ],
                    "indices": {
                        "docs": {
                            "count": 76892364,
                            "deleted": 324530,
                        },
                        "store": {
                            "size_in_bytes": 983409834,
                        },
                        "shards": {
                            "geonames": [
                                {
                                    "0": {
                                        "routing": {
                                            "state": "STARTED",
                                            "primary": True,
                                            "node": "gHQpKO-IT5uo8WVTPmAZXw",
                                            "relocating_node": None,
                                        },
                                        "store": {
                                            "size_in_bytes": 212027,
                                            "reserved_in_bytes": 0,
                                        },
                                    }
                                }
                            ]
                        },
                    },
                }
            },
        }

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.ShardStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=53)
        recorder.record()

        shard_metadata = {"cluster": "remote"}

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "shard-stats",
                        "shard-id": "0",
                        "index": "geonames",
                        "primary": True,
                        "docs": -1,
                        "store": 212027,
                        "segments-count": -1,
                        "node": "rally0",
                    },
                    level=MetaInfoScope.cluster,
                    meta_data=shard_metadata,
                )
            ],
            any_order=True,
        )


class TestSearchableSnapshotsStats:
    response_fragment_total = [
        {
            "file_ext": "fnm",
            "num_files": 50,
            "total_size": 279900,
            "open_count": 274,
            "close_count": 274,
            "contiguous_bytes_read": {
                "count": 1644,
                "sum": 1533852,
                "min": 478,
                "max": 1024,
            },
            "non_contiguous_bytes_read": {
                "count": 0,
                "sum": 0,
                "min": 0,
                "max": 0,
            },
            "cached_bytes_read": {
                "count": 1613,
                "sum": 1502108,
                "min": 478,
                "max": 1024,
            },
            "index_cache_bytes_read": {
                "count": 31,
                "sum": 173538,
                "min": 0,
                "max": 5598,
            },
            "cached_bytes_written": {
                "count": 81,
                "sum": 453438,
                "min": 5598,
                "max": 5598,
                "time": "1.6s",
                "time_in_nanos": 1607457548,
            },
            "direct_bytes_read": {
                "count": 0,
                "sum": 0,
                "min": 0,
                "max": 0,
                "time": "0s",
                "time_in_nanos": 0,
            },
            "optimized_bytes_read": {
                "count": 0,
                "sum": 0,
                "min": 0,
                "max": 0,
                "time": "0s",
                "time_in_nanos": 0,
            },
            "forward_seeks": {
                "small": {
                    "count": 0,
                    "sum": 0,
                    "min": 0,
                    "max": 0,
                },
                "large": {
                    "count": 0,
                    "sum": 0,
                    "min": 0,
                    "max": 0,
                },
            },
            "backward_seeks": {
                "small": {
                    "count": 0,
                    "sum": 0,
                    "min": 0,
                    "max": 0,
                },
                "large": {
                    "count": 0,
                    "sum": 0,
                    "min": 0,
                    "max": 0,
                },
            },
            "blob_store_bytes_requested": {
                "count": 50,
                "sum": 279900,
                "min": 5598,
                "max": 5598,
            },
            "current_index_cache_fills": 0,
        },
        {
            "file_ext": "kdd",
            "num_files": 50,
            "total_size": 356841728759,
            "open_count": 174,
            "close_count": 174,
            "contiguous_bytes_read": {
                "count": 184852,
                "sum": 189288448,
                "min": 1024,
                "max": 1024,
            },
            "non_contiguous_bytes_read": {
                "count": 2228,
                "sum": 2281472,
                "min": 0,
                "max": 1024,
            },
            "cached_bytes_read": {
                "count": 187049,
                "sum": 191538176,
                "min": 1024,
                "max": 1024,
            },
            "index_cache_bytes_read": {
                "count": 31,
                "sum": 31744,
                "min": 0,
                "max": 1024,
            },
            "cached_bytes_written": {
                "count": 122,
                "sum": 274173997,
                "min": 1024,
                "max": 7942949,
                "time": "22.2s",
                "time_in_nanos": 22277973991,
            },
            "direct_bytes_read": {
                "count": 0,
                "sum": 0,
                "min": 0,
                "max": 0,
                "time": "0s",
                "time_in_nanos": 0,
            },
            "optimized_bytes_read": {
                "count": 0,
                "sum": 0,
                "min": 0,
                "max": 0,
                "time": "0s",
                "time_in_nanos": 0,
            },
            "forward_seeks": {
                "small": {
                    "count": 2114,
                    "sum": 11467,
                    "min": 0,
                    "max": 10,
                },
                "large": {
                    "count": 174,
                    "sum": 1241804830245,
                    "min": 7134354745,
                    "max": 7139204589,
                },
            },
            "backward_seeks": {
                "small": {
                    "count": 114,
                    "sum": 192303474,
                    "min": 0,
                    "max": 1689340,
                },
                "large": {
                    "count": 0,
                    "sum": 0,
                    "min": 0,
                    "max": 0,
                },
            },
            "blob_store_bytes_requested": {
                "count": 91,
                "sum": 274142253,
                "min": 131072,
                "max": 7942949,
            },
            "current_index_cache_fills": 0,
        },
    ]

    response_fragment_indices = {
        "elasticlogs-2020-01-01": {
            "total": [
                {
                    "file_ext": "fnm",
                    "num_files": 5,
                    "total_size": 27990,
                    "open_count": 20,
                    "close_count": 20,
                    "contiguous_bytes_read": {
                        "count": 120,
                        "sum": 111960,
                        "min": 478,
                        "max": 1024,
                    },
                    "non_contiguous_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                    },
                    "cached_bytes_read": {
                        "count": 120,
                        "sum": 111960,
                        "min": 478,
                        "max": 1024,
                    },
                    "index_cache_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                    },
                    "cached_bytes_written": {
                        "count": 5,
                        "sum": 27990,
                        "min": 5598,
                        "max": 5598,
                        "time": "193.9ms",
                        "time_in_nanos": 193930007,
                    },
                    "direct_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                        "time": "0s",
                        "time_in_nanos": 0,
                    },
                    "optimized_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                        "time": "0s",
                        "time_in_nanos": 0,
                    },
                    "forward_seeks": {
                        "small": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                        "large": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                    },
                    "backward_seeks": {
                        "small": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                        "large": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                    },
                    "blob_store_bytes_requested": {
                        "count": 5,
                        "sum": 27990,
                        "min": 5598,
                        "max": 5598,
                    },
                    "current_index_cache_fills": 0,
                },
                {
                    "file_ext": "kdd",
                    "num_files": 5,
                    "total_size": 35672988421,
                    "open_count": 10,
                    "close_count": 10,
                    "contiguous_bytes_read": {
                        "count": 10,
                        "sum": 10240,
                        "min": 1024,
                        "max": 1024,
                    },
                    "non_contiguous_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                    },
                    "cached_bytes_read": {
                        "count": 10,
                        "sum": 10240,
                        "min": 1024,
                        "max": 1024,
                    },
                    "index_cache_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                    },
                    "cached_bytes_written": {
                        "count": 5,
                        "sum": 655360,
                        "min": 131072,
                        "max": 131072,
                        "time": "414.4ms",
                        "time_in_nanos": 414455967,
                    },
                    "direct_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                        "time": "0s",
                        "time_in_nanos": 0,
                    },
                    "optimized_bytes_read": {
                        "count": 0,
                        "sum": 0,
                        "min": 0,
                        "max": 0,
                        "time": "0s",
                        "time_in_nanos": 0,
                    },
                    "forward_seeks": {
                        "small": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                        "large": {
                            "count": 10,
                            "sum": 71345966442,
                            "min": 7134354745,
                            "max": 7134854030,
                        },
                    },
                    "backward_seeks": {
                        "small": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                        "large": {
                            "count": 0,
                            "sum": 0,
                            "min": 0,
                            "max": 0,
                        },
                    },
                    "blob_store_bytes_requested": {
                        "count": 5,
                        "sum": 655360,
                        "min": 131072,
                        "max": 131072,
                    },
                    "current_index_cache_fills": 0,
                },
            ]
        }
    }

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_no_metrics_if_empty_searchable_snapshots_stats(self, metrics_store_put_doc):
        response = {}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(transport_client=TransportClient(response=response))
        recorder = telemetry.SearchableSnapshotsStatsRecorder(
            cluster_name="default", client=client, metrics_store=metrics_store, sample_interval=1, indices=["logs*"]
        )

        recorder.record()

        assert metrics_store_put_doc.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_no_metrics_if_no_searchable_snapshots_stats(self, metrics_store_put_doc):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(
            transport_client=TransportClient(
                force_error=True, error=elasticsearch.NotFoundError("", "", {"error": {"reason": "No searchable snapshots indices found"}})
            )
        )
        recorder = telemetry.SearchableSnapshotsStatsRecorder(
            cluster_name="default", client=client, metrics_store=metrics_store, sample_interval=1, indices=["logs*"]
        )

        logger = logging.getLogger("esrally.telemetry")
        with mock.patch.object(logger, "info") as mocked_info:
            recorder.record()
            mocked_info.assert_called_once_with(
                "Unable to find valid indices while collecting searchable snapshots stats on cluster [%s]", "default"
            )

        assert metrics_store_put_doc.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_total_stats(self, metrics_store_put_doc):
        response = {"total": copy.deepcopy(TestSearchableSnapshotsStats.response_fragment_total)}

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(transport_client=TransportClient(response=response))

        recorder = telemetry.SearchableSnapshotsStatsRecorder(
            cluster_name="leader", client=client, metrics_store=metrics_store, sample_interval=1
        )
        recorder.record()

        expected_calls = [
            call(
                {"name": "searchable-snapshots-stats", "lucene_file_type": stat["file_ext"], "stats": stat},
                level=MetaInfoScope.cluster,
                meta_data={"cluster": "leader", "level": "cluster"},
            )
            for stat in TestSearchableSnapshotsStats.response_fragment_total
        ]

        metrics_store_put_doc.assert_has_calls(expected_calls, any_order=True)

    @pytest.mark.parametrize("seed", range(40))
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_index_stats(self, metrics_store_put_doc, seed):
        random.seed(seed)
        response = {
            "total": copy.deepcopy(TestSearchableSnapshotsStats.response_fragment_total),
            "indices": copy.deepcopy(TestSearchableSnapshotsStats.response_fragment_indices),
        }

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        client = Client(transport_client=TransportClient(response=response))

        recorder = telemetry.SearchableSnapshotsStatsRecorder(
            cluster_name="default",
            client=client,
            metrics_store=metrics_store,
            sample_interval=1,
            indices=random.choice([["elasticlogs*"], ["elasticlogs-2020-01-01"]]),
        )
        recorder.record()

        expected_calls = [
            call(
                {"name": "searchable-snapshots-stats", "lucene_file_type": stat["file_ext"], "stats": stat},
                level=MetaInfoScope.cluster,
                meta_data={"cluster": "default", "level": "cluster"},
            )
            for stat in TestSearchableSnapshotsStats.response_fragment_total
        ]

        expected_calls.extend(
            [
                call(
                    {
                        "name": "searchable-snapshots-stats",
                        "lucene_file_type": stat["file_ext"],
                        "stats": stat,
                        "index": "elasticlogs-2020-01-01",
                    },
                    level=MetaInfoScope.cluster,
                    meta_data={"cluster": "default", "level": "index"},
                )
                for stat in TestSearchableSnapshotsStats.response_fragment_indices["elasticlogs-2020-01-01"]["total"]
            ]
        )

        metrics_store_put_doc.assert_has_calls(expected_calls, any_order=True)


class TestNodeStats:
    warning = """You have enabled the node-stats telemetry device with Elasticsearch < 7.2.0. Requests to the
          _nodes/stats Elasticsearch endpoint trigger additional refreshes and WILL SKEW results.
    """

    @mock.patch("esrally.telemetry.NodeStatsRecorder", mock.Mock())
    @mock.patch("esrally.telemetry.SamplerThread", mock.Mock())
    def test_prints_warning_using_node_stats(self):
        clients = {"default": Client(info={"version": {"number": "7.1.0"}})}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"node-stats-sample-interval": random.randint(1, 100)}
        t = telemetry.NodeStats(telemetry_params, clients, metrics_store)

        with mock.patch.object(console, "warn") as mocked_console_warn:
            t.on_benchmark_start()
        mocked_console_warn.assert_called_once_with(self.warning, logger=t.logger)

    @mock.patch("esrally.telemetry.NodeStatsRecorder", mock.Mock())
    @mock.patch("esrally.telemetry.SamplerThread", mock.Mock())
    def test_no_warning_using_node_stats_after_version(self):
        clients = {"default": Client(info={"version": {"number": "7.2.0"}})}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"node-stats-sample-interval": random.randint(1, 100)}
        t = telemetry.NodeStats(telemetry_params, clients, metrics_store)

        with mock.patch.object(console, "warn") as mocked_console_warn:
            t.on_benchmark_start()
        mocked_console_warn.assert_not_called()


class TestNodeStatsRecorder:
    node_stats_response = {
        "cluster_name": "elasticsearch",
        "nodes": {
            "Zbl_e8EyRXmiR47gbHgPfg": {
                "timestamp": 1524379617017,
                "name": "rally0",
                "transport_address": "127.0.0.1:9300",
                "host": "127.0.0.1",
                "ip": "127.0.0.1:9300",
                "roles": [
                    "master",
                    "data",
                    "ingest",
                ],
                "indices": {
                    "docs": {
                        "count": 0,
                        "deleted": 0,
                    },
                    "store": {
                        "size_in_bytes": 0,
                    },
                    "indexing": {
                        "is_throttled": False,
                        "throttle_time_in_millis": 0,
                    },
                    "search": {
                        "open_contexts": 0,
                        "query_total": 0,
                        "query_time_in_millis": 0,
                    },
                    "merges": {
                        "current": 0,
                        "current_docs": 0,
                        "current_size_in_bytes": 0,
                    },
                    "refresh": {
                        "total": 747,
                        "total_time_in_millis": 277382,
                        "listeners": 0,
                    },
                    "query_cache": {
                        "memory_size_in_bytes": 0,
                        "total_count": 0,
                        "hit_count": 0,
                        "miss_count": 0,
                        "cache_size": 0,
                        "cache_count": 0,
                        "evictions": 0,
                    },
                    "completion": {"size_in_bytes": 0},
                    "segments": {
                        "count": 0,
                        "memory_in_bytes": 0,
                        "max_unsafe_auto_id_timestamp": -9223372036854775808,
                        "file_sizes": {},
                    },
                    "translog": {
                        "operations": 0,
                        "size_in_bytes": 0,
                        "uncommitted_operations": 0,
                        "uncommitted_size_in_bytes": 0,
                    },
                    "request_cache": {
                        "memory_size_in_bytes": 0,
                        "evictions": 0,
                        "hit_count": 0,
                        "miss_count": 0,
                    },
                    "recovery": {
                        "current_as_source": 0,
                        "current_as_target": 0,
                        "throttle_time_in_millis": 0,
                    },
                },
                "jvm": {
                    "buffer_pools": {
                        "mapped": {
                            "count": 7,
                            "used_in_bytes": 3120,
                            "total_capacity_in_bytes": 9999,
                        },
                        "direct": {
                            "count": 6,
                            "used_in_bytes": 73868,
                            "total_capacity_in_bytes": 73867,
                        },
                    },
                    "classes": {
                        "current_loaded_count": 9992,
                        "total_loaded_count": 9992,
                        "total_unloaded_count": 0,
                    },
                    "mem": {
                        "heap_used_in_bytes": 119073552,
                        "heap_used_percent": 19,
                        "heap_committed_in_bytes": 626393088,
                        "heap_max_in_bytes": 626393088,
                        "non_heap_used_in_bytes": 110250424,
                        "non_heap_committed_in_bytes": 118108160,
                        "pools": {
                            "young": {
                                "used_in_bytes": 66378576,
                                "max_in_bytes": 139591680,
                                "peak_used_in_bytes": 139591680,
                                "peak_max_in_bytes": 139591680,
                            },
                            "survivor": {
                                "used_in_bytes": 358496,
                                "max_in_bytes": 17432576,
                                "peak_used_in_bytes": 17432576,
                                "peak_max_in_bytes": 17432576,
                            },
                            "old": {
                                "used_in_bytes": 52336480,
                                "max_in_bytes": 469368832,
                                "peak_used_in_bytes": 52336480,
                                "peak_max_in_bytes": 469368832,
                            },
                        },
                    },
                    "gc": {
                        "collectors": {
                            "young": {
                                "collection_count": 3,
                                "collection_time_in_millis": 309,
                            },
                            "old": {
                                "collection_count": 2,
                                "collection_time_in_millis": 229,
                            },
                        }
                    },
                },
                "process": {
                    "timestamp": 1526045135857,
                    "open_file_descriptors": 312,
                    "max_file_descriptors": 1048576,
                    "cpu": {
                        "percent": 10,
                        "total_in_millis": 56520,
                    },
                    "mem": {
                        "total_virtual_in_bytes": 2472173568,
                    },
                },
                "thread_pool": {
                    "generic": {
                        "threads": 4,
                        "queue": 0,
                        "active": 0,
                        "rejected": 0,
                        "largest": 4,
                        "completed": 8,
                    },
                },
                "breakers": {
                    "parent": {
                        "limit_size_in_bytes": 726571417,
                        "limit_size": "692.9mb",
                        "estimated_size_in_bytes": 0,
                        "estimated_size": "0b",
                        "overhead": 1.0,
                        "tripped": 0,
                    }
                },
                "indexing_pressure": {
                    "memory": {
                        "current": {
                            "combined_coordinating_and_primary_in_bytes": 0,
                            "coordinating_in_bytes": 0,
                            "primary_in_bytes": 0,
                            "replica_in_bytes": 0,
                            "all_in_bytes": 0,
                        },
                        "total": {
                            "combined_coordinating_and_primary_in_bytes": 0,
                            "coordinating_in_bytes": 0,
                            "primary_in_bytes": 0,
                            "replica_in_bytes": 0,
                            "all_in_bytes": 0,
                            "coordinating_rejections": 0,
                            "primary_rejections": 0,
                            "replica_rejections": 0,
                        },
                    }
                },
            }
        },
    }

    indices_stats_response_flattened = collections.OrderedDict(
        {
            "indices_docs_count": 0,
            "indices_docs_deleted": 0,
            "indices_store_size_in_bytes": 0,
            "indices_indexing_throttle_time_in_millis": 0,
            "indices_search_open_contexts": 0,
            "indices_search_query_total": 0,
            "indices_search_query_time_in_millis": 0,
            "indices_merges_current": 0,
            "indices_merges_current_docs": 0,
            "indices_merges_current_size_in_bytes": 0,
            "indices_refresh_total": 747,
            "indices_refresh_total_time_in_millis": 277382,
            "indices_refresh_listeners": 0,
            "indices_query_cache_memory_size_in_bytes": 0,
            "indices_query_cache_total_count": 0,
            "indices_query_cache_hit_count": 0,
            "indices_query_cache_miss_count": 0,
            "indices_query_cache_cache_size": 0,
            "indices_query_cache_cache_count": 0,
            "indices_query_cache_evictions": 0,
            "indices_completion_size_in_bytes": 0,
            "indices_segments_count": 0,
            "indices_segments_memory_in_bytes": 0,
            "indices_segments_max_unsafe_auto_id_timestamp": -9223372036854775808,
            "indices_translog_operations": 0,
            "indices_translog_size_in_bytes": 0,
            "indices_translog_uncommitted_operations": 0,
            "indices_translog_uncommitted_size_in_bytes": 0,
            "indices_request_cache_memory_size_in_bytes": 0,
            "indices_request_cache_evictions": 0,
            "indices_request_cache_hit_count": 0,
            "indices_request_cache_miss_count": 0,
            "indices_recovery_current_as_source": 0,
            "indices_recovery_current_as_target": 0,
            "indices_recovery_throttle_time_in_millis": 0,
        }
    )

    default_stats_response_flattened = collections.OrderedDict(
        {
            "jvm_buffer_pools_mapped_count": 7,
            "jvm_buffer_pools_mapped_used_in_bytes": 3120,
            "jvm_buffer_pools_mapped_total_capacity_in_bytes": 9999,
            "jvm_buffer_pools_direct_count": 6,
            "jvm_buffer_pools_direct_used_in_bytes": 73868,
            "jvm_buffer_pools_direct_total_capacity_in_bytes": 73867,
            "jvm_mem_heap_used_in_bytes": 119073552,
            "jvm_mem_heap_used_percent": 19,
            "jvm_mem_heap_committed_in_bytes": 626393088,
            "jvm_mem_heap_max_in_bytes": 626393088,
            "jvm_mem_non_heap_used_in_bytes": 110250424,
            "jvm_mem_non_heap_committed_in_bytes": 118108160,
            "jvm_mem_pools_young_used_in_bytes": 66378576,
            "jvm_mem_pools_young_max_in_bytes": 139591680,
            "jvm_mem_pools_young_peak_used_in_bytes": 139591680,
            "jvm_mem_pools_young_peak_max_in_bytes": 139591680,
            "jvm_mem_pools_survivor_used_in_bytes": 358496,
            "jvm_mem_pools_survivor_max_in_bytes": 17432576,
            "jvm_mem_pools_survivor_peak_used_in_bytes": 17432576,
            "jvm_mem_pools_survivor_peak_max_in_bytes": 17432576,
            "jvm_mem_pools_old_used_in_bytes": 52336480,
            "jvm_mem_pools_old_max_in_bytes": 469368832,
            "jvm_mem_pools_old_peak_used_in_bytes": 52336480,
            "jvm_mem_pools_old_peak_max_in_bytes": 469368832,
            "jvm_gc_collectors_young_collection_count": 3,
            "jvm_gc_collectors_young_collection_time_in_millis": 309,
            "jvm_gc_collectors_old_collection_count": 2,
            "jvm_gc_collectors_old_collection_time_in_millis": 229,
            "process_cpu_percent": 10,
            "process_cpu_total_in_millis": 56520,
            "breakers_parent_limit_size_in_bytes": 726571417,
            "breakers_parent_estimated_size_in_bytes": 0,
            "breakers_parent_overhead": 1.0,
            "breakers_parent_tripped": 0,
            "thread_pool_generic_threads": 4,
            "thread_pool_generic_queue": 0,
            "thread_pool_generic_active": 0,
            "thread_pool_generic_rejected": 0,
            "thread_pool_generic_largest": 4,
            "thread_pool_generic_completed": 8,
            "indexing_pressure_memory_current_combined_coordinating_and_primary_in_bytes": 0,
            "indexing_pressure_memory_current_coordinating_in_bytes": 0,
            "indexing_pressure_memory_current_primary_in_bytes": 0,
            "indexing_pressure_memory_current_replica_in_bytes": 0,
            "indexing_pressure_memory_current_all_in_bytes": 0,
            "indexing_pressure_memory_total_combined_coordinating_and_primary_in_bytes": 0,
            "indexing_pressure_memory_total_coordinating_in_bytes": 0,
            "indexing_pressure_memory_total_primary_in_bytes": 0,
            "indexing_pressure_memory_total_replica_in_bytes": 0,
            "indexing_pressure_memory_total_all_in_bytes": 0,
            "indexing_pressure_memory_total_coordinating_rejections": 0,
            "indexing_pressure_memory_total_primary_rejections": 0,
            "indexing_pressure_memory_total_replica_rejections": 0,
        }
    )

    def test_negative_sample_interval_forbidden(self):
        client = Client()
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"node-stats-sample-interval": -1 * random.random()}
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The telemetry parameter 'node-stats-sample-interval' must be greater than zero but was .*\.",
        ):
            telemetry.NodeStatsRecorder(telemetry_params, cluster_name="default", client=client, metrics_store=metrics_store)

    def test_flatten_indices_fields(self):
        client = Client(nodes=SubClient(stats=self.node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        flattened_fields = recorder.flatten_stats_fields(
            prefix="indices",
            stats=self.node_stats_response["nodes"]["Zbl_e8EyRXmiR47gbHgPfg"]["indices"],
        )
        assert self.indices_stats_response_flattened == flattened_fields

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_nodes_stats(self, metrics_store_put_doc):
        client = Client(nodes=SubClient(stats=self.node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        node_name = [self.node_stats_response["nodes"][node]["name"] for node in self.node_stats_response["nodes"]][0]
        metrics_store_meta_data = {"cluster": "remote", "node_name": node_name}

        telemetry_params = {}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        recorder.record()

        expected_doc = collections.OrderedDict()
        expected_doc["name"] = "node-stats"
        expected_doc.update(self.default_stats_response_flattened)

        metrics_store_put_doc.assert_called_once_with(
            expected_doc, level=MetaInfoScope.node, node_name="rally0", meta_data=metrics_store_meta_data
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_all_nodes_stats(self, metrics_store_put_doc):
        node_stats_response = {
            "cluster_name": "elasticsearch",
            "nodes": {
                "Zbl_e8EyRXmiR47gbHgPfg": {
                    "timestamp": 1524379617017,
                    "name": "rally0",
                    "transport_address": "127.0.0.1:9300",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1:9300",
                    "roles": [
                        "master",
                        "data",
                        "ingest",
                    ],
                    "indices": {
                        "docs": {
                            "count": 76892364,
                            "deleted": 324530,
                        },
                        "store": {
                            "size_in_bytes": 983409834,
                        },
                        "indexing": {
                            "is_throttled": False,
                            "throttle_time_in_millis": 0,
                        },
                        "search": {
                            "open_contexts": 0,
                            "query_total": 0,
                            "query_time_in_millis": 0,
                        },
                        "merges": {
                            "current": 0,
                            "current_docs": 0,
                            "current_size_in_bytes": 0,
                        },
                        "refresh": {
                            "total": 747,
                            "total_time_in_millis": 277382,
                            "listeners": 0,
                        },
                        "query_cache": {
                            "memory_size_in_bytes": 0,
                            "total_count": 0,
                            "hit_count": 0,
                            "miss_count": 0,
                            "cache_size": 0,
                            "cache_count": 0,
                            "evictions": 0,
                        },
                        "fielddata": {
                            "memory_size_in_bytes": 6936,
                            "evictions": 17,
                        },
                        "completion": {
                            "size_in_bytes": 0,
                        },
                        "segments": {
                            "count": 0,
                            "memory_in_bytes": 0,
                            "max_unsafe_auto_id_timestamp": -9223372036854775808,
                            "file_sizes": {},
                        },
                        "translog": {
                            "operations": 0,
                            "size_in_bytes": 0,
                            "uncommitted_operations": 0,
                            "uncommitted_size_in_bytes": 0,
                        },
                        "request_cache": {
                            "memory_size_in_bytes": 0,
                            "evictions": 0,
                            "hit_count": 0,
                            "miss_count": 0,
                        },
                        "recovery": {
                            "current_as_source": 0,
                            "current_as_target": 0,
                            "throttle_time_in_millis": 0,
                        },
                    },
                    "jvm": {
                        "buffer_pools": {
                            "mapped": {
                                "count": 7,
                                "used_in_bytes": 3120,
                                "total_capacity_in_bytes": 9999,
                            },
                            "direct": {
                                "count": 6,
                                "used_in_bytes": 73868,
                                "total_capacity_in_bytes": 73867,
                            },
                        },
                        "classes": {
                            "current_loaded_count": 9992,
                            "total_loaded_count": 9992,
                            "total_unloaded_count": 0,
                        },
                        "mem": {
                            "heap_used_in_bytes": 119073552,
                            "heap_used_percent": 19,
                            "heap_committed_in_bytes": 626393088,
                            "heap_max_in_bytes": 626393088,
                            "non_heap_used_in_bytes": 110250424,
                            "non_heap_committed_in_bytes": 118108160,
                            "pools": {
                                "young": {
                                    "used_in_bytes": 66378576,
                                    "max_in_bytes": 139591680,
                                    "peak_used_in_bytes": 139591680,
                                    "peak_max_in_bytes": 139591680,
                                },
                                "survivor": {
                                    "used_in_bytes": 358496,
                                    "max_in_bytes": 17432576,
                                    "peak_used_in_bytes": 17432576,
                                    "peak_max_in_bytes": 17432576,
                                },
                                "old": {
                                    "used_in_bytes": 52336480,
                                    "max_in_bytes": 469368832,
                                    "peak_used_in_bytes": 52336480,
                                    "peak_max_in_bytes": 469368832,
                                },
                            },
                        },
                        "gc": {
                            "collectors": {
                                "young": {
                                    "collection_count": 3,
                                    "collection_time_in_millis": 309,
                                },
                                "old": {
                                    "collection_count": 2,
                                    "collection_time_in_millis": 229,
                                },
                            }
                        },
                    },
                    "process": {
                        "timestamp": 1526045135857,
                        "open_file_descriptors": 312,
                        "max_file_descriptors": 1048576,
                        "cpu": {
                            "percent": 10,
                            "total_in_millis": 56520,
                        },
                        "mem": {
                            "total_virtual_in_bytes": 2472173568,
                        },
                    },
                    "thread_pool": {
                        "generic": {
                            "threads": 4,
                            "queue": 0,
                            "active": 0,
                            "rejected": 0,
                            "largest": 4,
                            "completed": 8,
                        },
                    },
                    "transport": {
                        "server_open": 12,
                        "rx_count": 77,
                        "rx_size_in_bytes": 98723498,
                        "tx_count": 88,
                        "tx_size_in_bytes": 23879803,
                    },
                    "breakers": {
                        "parent": {
                            "limit_size_in_bytes": 726571417,
                            "limit_size": "692.9mb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1.0,
                            "tripped": 0,
                        }
                    },
                    "indexing_pressure": {
                        "memory": {
                            "current": {
                                "combined_coordinating_and_primary_in_bytes": 0,
                                "coordinating_in_bytes": 0,
                                "primary_in_bytes": 0,
                                "replica_in_bytes": 0,
                                "all_in_bytes": 0,
                            },
                            "total": {
                                "combined_coordinating_and_primary_in_bytes": 0,
                                "coordinating_in_bytes": 0,
                                "primary_in_bytes": 0,
                                "replica_in_bytes": 0,
                                "all_in_bytes": 0,
                                "coordinating_rejections": 0,
                                "primary_rejections": 0,
                                "replica_rejections": 0,
                            },
                        }
                    },
                }
            },
        }

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        node_name = [node_stats_response["nodes"][node]["name"] for node in node_stats_response["nodes"]][0]
        metrics_store_meta_data = {"cluster": "remote", "node_name": node_name}
        telemetry_params = {"node-stats-include-indices": True}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        recorder.record()

        metrics_store_put_doc.assert_called_once_with(
            {
                "name": "node-stats",
                "indices_docs_count": 76892364,
                "indices_docs_deleted": 324530,
                "indices_fielddata_evictions": 17,
                "indices_fielddata_memory_size_in_bytes": 6936,
                "indices_indexing_throttle_time_in_millis": 0,
                "indices_merges_current": 0,
                "indices_merges_current_docs": 0,
                "indices_merges_current_size_in_bytes": 0,
                "indices_query_cache_cache_count": 0,
                "indices_query_cache_cache_size": 0,
                "indices_query_cache_evictions": 0,
                "indices_query_cache_hit_count": 0,
                "indices_query_cache_memory_size_in_bytes": 0,
                "indices_query_cache_miss_count": 0,
                "indices_query_cache_total_count": 0,
                "indices_request_cache_evictions": 0,
                "indices_request_cache_hit_count": 0,
                "indices_request_cache_memory_size_in_bytes": 0,
                "indices_request_cache_miss_count": 0,
                "indices_search_open_contexts": 0,
                "indices_search_query_time_in_millis": 0,
                "indices_search_query_total": 0,
                "indices_segments_count": 0,
                "indices_segments_max_unsafe_auto_id_timestamp": -9223372036854775808,
                "indices_segments_memory_in_bytes": 0,
                "indices_store_size_in_bytes": 983409834,
                "indices_translog_operations": 0,
                "indices_translog_size_in_bytes": 0,
                "indices_translog_uncommitted_operations": 0,
                "indices_translog_uncommitted_size_in_bytes": 0,
                "thread_pool_generic_active": 0,
                "thread_pool_generic_completed": 8,
                "thread_pool_generic_largest": 4,
                "thread_pool_generic_queue": 0,
                "thread_pool_generic_rejected": 0,
                "thread_pool_generic_threads": 4,
                "breakers_parent_estimated_size_in_bytes": 0,
                "breakers_parent_limit_size_in_bytes": 726571417,
                "breakers_parent_overhead": 1.0,
                "breakers_parent_tripped": 0,
                "jvm_buffer_pools_direct_count": 6,
                "jvm_buffer_pools_direct_total_capacity_in_bytes": 73867,
                "jvm_buffer_pools_direct_used_in_bytes": 73868,
                "jvm_buffer_pools_mapped_count": 7,
                "jvm_buffer_pools_mapped_total_capacity_in_bytes": 9999,
                "jvm_buffer_pools_mapped_used_in_bytes": 3120,
                "jvm_mem_heap_committed_in_bytes": 626393088,
                "jvm_mem_heap_max_in_bytes": 626393088,
                "jvm_mem_heap_used_in_bytes": 119073552,
                "jvm_mem_heap_used_percent": 19,
                "jvm_mem_non_heap_committed_in_bytes": 118108160,
                "jvm_mem_non_heap_used_in_bytes": 110250424,
                "jvm_mem_pools_old_max_in_bytes": 469368832,
                "jvm_mem_pools_old_peak_max_in_bytes": 469368832,
                "jvm_mem_pools_old_peak_used_in_bytes": 52336480,
                "jvm_mem_pools_old_used_in_bytes": 52336480,
                "jvm_mem_pools_survivor_max_in_bytes": 17432576,
                "jvm_mem_pools_survivor_peak_max_in_bytes": 17432576,
                "jvm_mem_pools_survivor_peak_used_in_bytes": 17432576,
                "jvm_mem_pools_survivor_used_in_bytes": 358496,
                "jvm_mem_pools_young_max_in_bytes": 139591680,
                "jvm_mem_pools_young_peak_max_in_bytes": 139591680,
                "jvm_mem_pools_young_peak_used_in_bytes": 139591680,
                "jvm_mem_pools_young_used_in_bytes": 66378576,
                "jvm_gc_collectors_young_collection_count": 3,
                "jvm_gc_collectors_young_collection_time_in_millis": 309,
                "jvm_gc_collectors_old_collection_count": 2,
                "jvm_gc_collectors_old_collection_time_in_millis": 229,
                "transport_rx_count": 77,
                "transport_rx_size_in_bytes": 98723498,
                "transport_server_open": 12,
                "transport_tx_count": 88,
                "transport_tx_size_in_bytes": 23879803,
                "process_cpu_percent": 10,
                "process_cpu_total_in_millis": 56520,
                "indexing_pressure_memory_current_combined_coordinating_and_primary_in_bytes": 0,
                "indexing_pressure_memory_current_coordinating_in_bytes": 0,
                "indexing_pressure_memory_current_primary_in_bytes": 0,
                "indexing_pressure_memory_current_replica_in_bytes": 0,
                "indexing_pressure_memory_current_all_in_bytes": 0,
                "indexing_pressure_memory_total_combined_coordinating_and_primary_in_bytes": 0,
                "indexing_pressure_memory_total_coordinating_in_bytes": 0,
                "indexing_pressure_memory_total_primary_in_bytes": 0,
                "indexing_pressure_memory_total_replica_in_bytes": 0,
                "indexing_pressure_memory_total_all_in_bytes": 0,
                "indexing_pressure_memory_total_coordinating_rejections": 0,
                "indexing_pressure_memory_total_primary_rejections": 0,
                "indexing_pressure_memory_total_replica_rejections": 0,
            },
            level=MetaInfoScope.node,
            node_name="rally0",
            meta_data=metrics_store_meta_data,
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_selected_indices_metrics_from_nodes_stats(self, metrics_store_put_doc):
        node_stats_response = {
            "cluster_name": "elasticsearch",
            "nodes": {
                "Zbl_e8EyRXmiR47gbHgPfg": {
                    "timestamp": 1524379617017,
                    "name": "rally0",
                    "transport_address": "127.0.0.1:9300",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1:9300",
                    "roles": [
                        "master",
                        "data",
                        "ingest",
                    ],
                    "indices": {
                        "docs": {
                            "count": 76892364,
                            "deleted": 324530,
                        },
                        "store": {
                            "size_in_bytes": 983409834,
                        },
                        "indexing": {
                            "is_throttled": False,
                            "throttle_time_in_millis": 0,
                        },
                        "search": {
                            "open_contexts": 0,
                            "query_total": 0,
                            "query_time_in_millis": 0,
                        },
                        "merges": {
                            "current": 0,
                            "current_docs": 0,
                            "current_size_in_bytes": 0,
                        },
                        "refresh": {
                            "total": 747,
                            "total_time_in_millis": 277382,
                            "listeners": 0,
                        },
                        "query_cache": {
                            "memory_size_in_bytes": 0,
                            "total_count": 0,
                            "hit_count": 0,
                            "miss_count": 0,
                            "cache_size": 0,
                            "cache_count": 0,
                            "evictions": 0,
                        },
                        "fielddata": {
                            "memory_size_in_bytes": 6936,
                            "evictions": 17,
                        },
                        "completion": {"size_in_bytes": 0},
                        "segments": {
                            "count": 0,
                            "memory_in_bytes": 0,
                            "max_unsafe_auto_id_timestamp": -9223372036854775808,
                            "file_sizes": {},
                        },
                        "translog": {
                            "operations": 0,
                            "size_in_bytes": 0,
                            "uncommitted_operations": 0,
                            "uncommitted_size_in_bytes": 0,
                        },
                        "request_cache": {
                            "memory_size_in_bytes": 0,
                            "evictions": 0,
                            "hit_count": 0,
                            "miss_count": 0,
                        },
                        "recovery": {
                            "current_as_source": 0,
                            "current_as_target": 0,
                            "throttle_time_in_millis": 0,
                        },
                    },
                    "jvm": {
                        "buffer_pools": {
                            "mapped": {
                                "count": 7,
                                "used_in_bytes": 3120,
                                "total_capacity_in_bytes": 9999,
                            },
                            "direct": {
                                "count": 6,
                                "used_in_bytes": 73868,
                                "total_capacity_in_bytes": 73867,
                            },
                        },
                        "classes": {
                            "current_loaded_count": 9992,
                            "total_loaded_count": 9992,
                            "total_unloaded_count": 0,
                        },
                        "mem": {
                            "heap_used_in_bytes": 119073552,
                            "heap_used_percent": 19,
                            "heap_committed_in_bytes": 626393088,
                            "heap_max_in_bytes": 626393088,
                            "non_heap_used_in_bytes": 110250424,
                            "non_heap_committed_in_bytes": 118108160,
                            "pools": {
                                "young": {
                                    "used_in_bytes": 66378576,
                                    "max_in_bytes": 139591680,
                                    "peak_used_in_bytes": 139591680,
                                    "peak_max_in_bytes": 139591680,
                                },
                                "survivor": {
                                    "used_in_bytes": 358496,
                                    "max_in_bytes": 17432576,
                                    "peak_used_in_bytes": 17432576,
                                    "peak_max_in_bytes": 17432576,
                                },
                                "old": {
                                    "used_in_bytes": 52336480,
                                    "max_in_bytes": 469368832,
                                    "peak_used_in_bytes": 52336480,
                                    "peak_max_in_bytes": 469368832,
                                },
                            },
                        },
                        "gc": {
                            "collectors": {
                                "young": {
                                    "collection_count": 3,
                                    "collection_time_in_millis": 309,
                                },
                                "old": {
                                    "collection_count": 2,
                                    "collection_time_in_millis": 229,
                                },
                            }
                        },
                    },
                    "process": {
                        "timestamp": 1526045135857,
                        "open_file_descriptors": 312,
                        "max_file_descriptors": 1048576,
                        "cpu": {"percent": 10, "total_in_millis": 56520},
                        "mem": {"total_virtual_in_bytes": 2472173568},
                    },
                    "thread_pool": {
                        "generic": {
                            "threads": 4,
                            "queue": 0,
                            "active": 0,
                            "rejected": 0,
                            "largest": 4,
                            "completed": 8,
                        },
                    },
                    "transport": {
                        "server_open": 12,
                        "rx_count": 77,
                        "rx_size_in_bytes": 98723498,
                        "tx_count": 88,
                        "tx_size_in_bytes": 23879803,
                    },
                    "breakers": {
                        "parent": {
                            "limit_size_in_bytes": 726571417,
                            "limit_size": "692.9mb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1.0,
                            "tripped": 0,
                        }
                    },
                    "indexing_pressure": {
                        "memory": {
                            "current": {
                                "combined_coordinating_and_primary_in_bytes": 0,
                                "coordinating_in_bytes": 0,
                                "primary_in_bytes": 0,
                                "replica_in_bytes": 0,
                                "all_in_bytes": 0,
                            },
                            "total": {
                                "combined_coordinating_and_primary_in_bytes": 0,
                                "coordinating_in_bytes": 0,
                                "primary_in_bytes": 0,
                                "replica_in_bytes": 0,
                                "all_in_bytes": 0,
                                "coordinating_rejections": 0,
                                "primary_rejections": 0,
                                "replica_rejections": 0,
                            },
                        }
                    },
                }
            },
        }

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        node_name = [node_stats_response["nodes"][node]["name"] for node in node_stats_response["nodes"]][0]
        metrics_store_meta_data = {"cluster": "remote", "node_name": node_name}
        telemetry_params = {"node-stats-include-indices-metrics": "refresh,docs"}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        recorder.record()

        metrics_store_put_doc.assert_called_once_with(
            {
                "name": "node-stats",
                "indices_docs_count": 76892364,
                "indices_docs_deleted": 324530,
                "indices_refresh_total": 747,
                "indices_refresh_total_time_in_millis": 277382,
                "indices_refresh_listeners": 0,
                "thread_pool_generic_active": 0,
                "thread_pool_generic_completed": 8,
                "thread_pool_generic_largest": 4,
                "thread_pool_generic_queue": 0,
                "thread_pool_generic_rejected": 0,
                "thread_pool_generic_threads": 4,
                "breakers_parent_estimated_size_in_bytes": 0,
                "breakers_parent_limit_size_in_bytes": 726571417,
                "breakers_parent_overhead": 1.0,
                "breakers_parent_tripped": 0,
                "jvm_buffer_pools_direct_count": 6,
                "jvm_buffer_pools_direct_total_capacity_in_bytes": 73867,
                "jvm_buffer_pools_direct_used_in_bytes": 73868,
                "jvm_buffer_pools_mapped_count": 7,
                "jvm_buffer_pools_mapped_total_capacity_in_bytes": 9999,
                "jvm_buffer_pools_mapped_used_in_bytes": 3120,
                "jvm_mem_heap_committed_in_bytes": 626393088,
                "jvm_mem_heap_max_in_bytes": 626393088,
                "jvm_mem_heap_used_in_bytes": 119073552,
                "jvm_mem_heap_used_percent": 19,
                "jvm_mem_non_heap_committed_in_bytes": 118108160,
                "jvm_mem_non_heap_used_in_bytes": 110250424,
                "jvm_mem_pools_old_max_in_bytes": 469368832,
                "jvm_mem_pools_old_peak_max_in_bytes": 469368832,
                "jvm_mem_pools_old_peak_used_in_bytes": 52336480,
                "jvm_mem_pools_old_used_in_bytes": 52336480,
                "jvm_mem_pools_survivor_max_in_bytes": 17432576,
                "jvm_mem_pools_survivor_peak_max_in_bytes": 17432576,
                "jvm_mem_pools_survivor_peak_used_in_bytes": 17432576,
                "jvm_mem_pools_survivor_used_in_bytes": 358496,
                "jvm_mem_pools_young_max_in_bytes": 139591680,
                "jvm_mem_pools_young_peak_max_in_bytes": 139591680,
                "jvm_mem_pools_young_peak_used_in_bytes": 139591680,
                "jvm_mem_pools_young_used_in_bytes": 66378576,
                "jvm_gc_collectors_young_collection_count": 3,
                "jvm_gc_collectors_young_collection_time_in_millis": 309,
                "jvm_gc_collectors_old_collection_count": 2,
                "jvm_gc_collectors_old_collection_time_in_millis": 229,
                "transport_rx_count": 77,
                "transport_rx_size_in_bytes": 98723498,
                "transport_server_open": 12,
                "transport_tx_count": 88,
                "transport_tx_size_in_bytes": 23879803,
                "process_cpu_percent": 10,
                "process_cpu_total_in_millis": 56520,
                "indexing_pressure_memory_current_combined_coordinating_and_primary_in_bytes": 0,
                "indexing_pressure_memory_current_coordinating_in_bytes": 0,
                "indexing_pressure_memory_current_primary_in_bytes": 0,
                "indexing_pressure_memory_current_replica_in_bytes": 0,
                "indexing_pressure_memory_current_all_in_bytes": 0,
                "indexing_pressure_memory_total_combined_coordinating_and_primary_in_bytes": 0,
                "indexing_pressure_memory_total_coordinating_in_bytes": 0,
                "indexing_pressure_memory_total_primary_in_bytes": 0,
                "indexing_pressure_memory_total_replica_in_bytes": 0,
                "indexing_pressure_memory_total_all_in_bytes": 0,
                "indexing_pressure_memory_total_coordinating_rejections": 0,
                "indexing_pressure_memory_total_primary_rejections": 0,
                "indexing_pressure_memory_total_replica_rejections": 0,
            },
            level=MetaInfoScope.node,
            node_name="rally0",
            meta_data=metrics_store_meta_data,
        )

    def test_exception_when_include_indices_metrics_not_valid(self):
        node_stats_response = {}

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"node-stats-include-indices-metrics": {"bad": "input"}}
        with pytest.raises(
            exceptions.SystemSetupError,
            match="The telemetry parameter 'node-stats-include-indices-metrics' must be a comma-separated string but was <class 'dict'>",
        ):
            telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)


class TestTransformStats:
    def test_negative_sample_interval_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"transform-stats-sample-interval": -1 * random.random()}
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The telemetry parameter 'transform-stats-sample-interval' must be greater than zero but was .*\.",
        ):
            telemetry.TransformStats(telemetry_params, clients, metrics_store)

    def test_wrong_cluster_name_in_transform_stats_indices_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"transform-stats-transforms": {"default": ["leader"], "wrong_cluster_name": ["follower"]}}
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The telemetry parameter 'transform-stats-transforms' must be a JSON Object with keys matching "
            r"the cluster names \[{}] specified in --target-hosts "
            r"but it had \[wrong_cluster_name\].".format(",".join(sorted(clients.keys()))),
        ):
            telemetry.TransformStats(telemetry_params, clients, metrics_store)


class TestTransformStatsRecorder:
    transform_stats_response = {}

    @classmethod
    def setup_class(cls):
        java_signed_maxlong = (2**63) - 1
        transform_id_prefix = "transform_job_"
        count = random.randrange(1, 10)
        transforms = []

        for i in range(0, count):
            transform = {
                "id": transform_id_prefix + str(i),
                "state": random.choice(["stopped", "indexing", "started", "failed"]),
                "stats": {
                    "pages_processed": 1,
                    "documents_processed": 240,
                    "documents_indexed": 3,
                    "trigger_count": 4,
                    "index_time_in_ms": 5,
                    "index_total": 6,
                    "index_failures": 7,
                    "search_time_in_ms": 8,
                    "search_total": 9,
                    "search_failures": 10,
                    "processing_time_in_ms": 11,
                    "processing_total": 12,
                    "exponential_avg_checkpoint_duration_ms": random.uniform(1.0, 100.0),
                    "exponential_avg_documents_indexed": random.uniform(1.0, 1000.0),
                    "exponential_avg_documents_processed": random.uniform(1.0, 10000.0),
                },
                "checkpointing": {
                    "last": {
                        "checkpoint": random.randint(0, java_signed_maxlong),
                        "timestamp_millis": random.randint(0, java_signed_maxlong),
                    },
                    "changes_last_detected_at": random.randint(0, java_signed_maxlong),
                },
            }
            transforms.append(transform)

        cls.transform_stats_response = {"count": count, "transforms": transforms}

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_stores_default_stats(self, metrics_store_put_value):
        client = Client(transform=SubClient(transform_stats=self.transform_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.TransformStatsRecorder(
            cluster_name="transform_cluster", client=client, metrics_store=metrics_store, sample_interval=1
        )
        recorder.record()

        meta_data = {"transform_id": "transform_job_0"}

        metrics_store_put_value.assert_has_calls(
            [
                mock.call("transform_pages_processed", 1, meta_data=meta_data),
                mock.call("transform_documents_processed", 240, meta_data=meta_data),
                mock.call("transform_documents_indexed", 3, meta_data=meta_data),
                mock.call("transform_index_total", 6, meta_data=meta_data),
                mock.call("transform_index_failures", 7, meta_data=meta_data),
                mock.call("transform_search_total", 9, meta_data=meta_data),
                mock.call("transform_search_failures", 10, meta_data=meta_data),
                mock.call("transform_processing_total", 12, meta_data=meta_data),
                mock.call("transform_search_time", 8, "ms", meta_data=meta_data),
                mock.call("transform_index_time", 5, "ms", meta_data=meta_data),
                mock.call("transform_processing_time", 11, "ms", meta_data=meta_data),
                mock.call("transform_throughput", 10000, "docs/s", meta_data=meta_data),
            ]
        )


class TestClusterEnvironmentInfo:
    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_stores_cluster_level_metrics_on_attach(self, metrics_store_add_meta_info):
        nodes_info = {"nodes": collections.OrderedDict()}
        nodes_info["nodes"]["FCFjozkeTiOpN-SI88YEcg"] = {
            "name": "rally0",
            "host": "127.0.0.1",
            "attributes": {
                "group": "cold_nodes",
            },
            "os": {
                "name": "Mac OS X",
                "version": "10.11.4",
                "available_processors": 8,
            },
            "jvm": {
                "version": "1.8.0_74",
                "vm_vendor": "Oracle Corporation",
            },
            "plugins": [
                {
                    "name": "ingest-geoip",
                    "version": "5.0.0",
                    "description": "Ingest processor that uses looksup geo data ...",
                    "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                    "has_native_controller": False,
                }
            ],
        }
        nodes_info["nodes"]["EEEjozkeTiOpN-SI88YEcg"] = {
            "name": "rally1",
            "host": "127.0.0.1",
            "attributes": {
                "group": "hot_nodes",
            },
            "os": {
                "name": "Mac OS X",
                "version": "10.11.5",
                "available_processors": 8,
            },
            "jvm": {
                "version": "1.8.0_102",
                "vm_vendor": "Oracle Corporation",
            },
            "plugins": [
                {
                    "name": "ingest-geoip",
                    "version": "5.0.0",
                    "description": "Ingest processor that uses looksup geo data ...",
                    "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                    "has_native_controller": False,
                }
            ],
        }

        cluster_info = {
            "version": {
                "build_hash": "abc123",
                "number": "6.0.0-alpha1",
            },
        }

        cfg = create_config()
        client = Client(nodes=SubClient(info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(cfg)
        env_device = telemetry.ClusterEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[env_device])
        t.on_benchmark_start()
        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123"),
            mock.call(metrics.MetaInfoScope.cluster, None, "distribution_version", "6.0.0-alpha1"),
            mock.call(metrics.MetaInfoScope.cluster, None, "distribution_flavor", "oss"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "jvm_version", "1.8.0_102"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally1", "plugins", ["ingest-geoip"]),
            # can push up to cluster level as all nodes have the same plugins installed
            mock.call(metrics.MetaInfoScope.cluster, None, "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally0", "attribute_group", "cold_nodes"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "attribute_group", "hot_nodes"),
        ]

        metrics_store_add_meta_info.assert_has_calls(calls)

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_resilient_if_error_response(self, metrics_store_add_meta_info):
        cfg = create_config()
        client = Client(nodes=SubClient(stats=raiseTransportError, info=raiseTransportError), info=raiseTransportError)
        metrics_store = metrics.EsMetricsStore(cfg)
        env_device = telemetry.ClusterEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[env_device])
        t.on_benchmark_start()

        assert metrics_store_add_meta_info.call_count == 0


class TestNodeEnvironmentInfo:
    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    @mock.patch("esrally.utils.sysstats.os_name")
    @mock.patch("esrally.utils.sysstats.os_version")
    @mock.patch("esrally.utils.sysstats.logical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.physical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.cpu_model")
    def test_stores_node_level_metrics(
        self, cpu_model, physical_cpu_cores, logical_cpu_cores, os_version, os_name, metrics_store_add_meta_info
    ):
        cpu_model.return_value = "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
        physical_cpu_cores.return_value = 4
        logical_cpu_cores.return_value = 8
        os_version.return_value = "4.2.0-18-generic"
        os_name.return_value = "Linux"
        node_name = "rally0"
        host_name = "io"

        metrics_store = metrics.EsMetricsStore(create_config())
        telemetry.add_metadata_for_node(metrics_store, node_name, host_name)

        calls = [
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Linux"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "4.2.0-18-generic"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_physical_cores", 4),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_model", "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", node_name),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", host_name),
        ]

        metrics_store_add_meta_info.assert_has_calls(calls)


class TestExternalEnvironmentInfo:
    def setup_method(self, method):
        self.cfg = create_config()

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_stores_all_node_metrics_on_attach(self, metrics_store_add_meta_info):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                },
            },
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "attributes": {
                        "az": "us_east1",
                    },
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8,
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation",
                    },
                    "plugins": [
                        {
                            "name": "ingest-geoip",
                            "version": "5.0.0",
                            "description": "Ingest processor that uses looksup geo data ...",
                            "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                            "has_native_controller": False,
                        }
                    ],
                }
            }
        }
        cluster_info = {
            "version": {
                "build_hash": "253032b",
                "number": "5.0.0",
            },
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.ExternalEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(devices=[env_device])
        t.on_benchmark_start()

        calls = [
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "127.0.0.1"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "plugins", ["ingest-geoip"]),
            # these are automatically pushed up to cluster level (additionally) if all nodes match
            mock.call(metrics.MetaInfoScope.cluster, None, "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally0", "attribute_az", "us_east1"),
            mock.call(metrics.MetaInfoScope.cluster, None, "attribute_az", "us_east1"),
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_fallback_when_host_not_available(self, metrics_store_add_meta_info):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                }
            }
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "os": {"name": "Mac OS X", "version": "10.11.4", "available_processors": 8},
                    "jvm": {"version": "1.8.0_74", "vm_vendor": "Oracle Corporation"},
                }
            }
        }
        cluster_info = {
            "version": {
                "build_hash": "253032b",
                "number": "5.0.0",
            },
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.ExternalEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(self.cfg, devices=[env_device])
        t.on_benchmark_start()

        calls = [
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "unknown"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_resilient_if_error_response(self, metrics_store_add_meta_info):
        client = Client(nodes=SubClient(stats=raiseTransportError, info=raiseTransportError), info=raiseTransportError)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.ExternalEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(self.cfg, devices=[env_device])
        t.on_benchmark_start()

        assert metrics_store_add_meta_info.call_count == 0


class TestDiskIo:
    @mock.patch("esrally.utils.sysstats.process_io_counters")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_diskio_process_io_counters(self, metrics_store_node_count, process_io_counters):
        Diskio = namedtuple("Diskio", "read_bytes write_bytes")
        process_start = Diskio(10, 10)
        process_stop = Diskio(11, 11)
        process_io_counters.side_effect = [process_start, process_stop]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)

        device = telemetry.DiskIo(node_count_on_host=1)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        node = cluster.Node(pid=None, binary_path="/bin", host_name="localhost", node_name="rally0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        # we assume that serializing and deserializing the telemetry device produces the same state
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)
        t.store_system_metrics(node, metrics_store)

        metrics_store_node_count.assert_has_calls(
            [
                mock.call("rally0", "disk_io_write_bytes", 1, "byte"),
                mock.call("rally0", "disk_io_read_bytes", 1, "byte"),
            ]
        )

    @mock.patch("esrally.utils.sysstats.disk_io_counters")
    @mock.patch("esrally.utils.sysstats.process_io_counters")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_diskio_disk_io_counters(self, metrics_store_node_count, process_io_counters, disk_io_counters):
        Diskio = namedtuple("Diskio", "read_bytes write_bytes")
        process_start = Diskio(10, 10)
        process_stop = Diskio(13, 13)
        disk_io_counters.side_effect = [process_start, process_stop]
        process_io_counters.side_effect = [None, None]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)

        device = telemetry.DiskIo(node_count_on_host=2)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        node = cluster.Node(pid=None, binary_path="/bin", host_name="localhost", node_name="rally0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        # we assume that serializing and deserializing the telemetry device produces the same state
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)
        t.store_system_metrics(node, metrics_store)

        # expected result is 1 byte because there are two nodes on the machine. Result is calculated
        # with total_bytes / node_count
        metrics_store_node_count.assert_has_calls(
            [
                mock.call("rally0", "disk_io_write_bytes", 1, "byte"),
                mock.call("rally0", "disk_io_read_bytes", 1, "byte"),
            ]
        )

    @mock.patch("esrally.utils.sysstats.disk_io_counters")
    @mock.patch("esrally.utils.sysstats.process_io_counters")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_diskio_writes_metrics_if_available(self, metrics_store_node_count, process_io_counters, disk_io_counters):
        Diskio = namedtuple("Diskio", "read_bytes write_bytes")
        process_start = Diskio(10, 10)
        process_stop = Diskio(10, 13)
        disk_io_counters.side_effect = [process_start, process_stop]
        process_io_counters.side_effect = [None, None]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)

        device = telemetry.DiskIo(node_count_on_host=1)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        node = cluster.Node(pid=None, binary_path="/bin", host_name="localhost", node_name="rally0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        # we assume that serializing and deserializing the telemetry device produces the same state
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)
        t.store_system_metrics(node, metrics_store)

        metrics_store_node_count.assert_has_calls(
            [
                mock.call("rally0", "disk_io_write_bytes", 3, "byte"),
                mock.call("rally0", "disk_io_read_bytes", 0, "byte"),
            ]
        )


class TestJvmStatsSummary:
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_stores_only_diff_of_gc_times(self, metrics_store_node_level, metrics_store_cluster_level, metrics_store_put_doc):
        nodes_stats_at_start = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "mem": {
                            "pools": {
                                "young": {
                                    "peak_used_in_bytes": 228432256,
                                },
                                "survivor": {
                                    "peak_used_in_bytes": 3333333,
                                },
                                "old": {
                                    "peak_used_in_bytes": 300008222,
                                },
                            }
                        },
                        "gc": {
                            "collectors": {
                                "old": {
                                    "collection_time_in_millis": 1000,
                                    "collection_count": 1,
                                },
                                "young": {
                                    "collection_time_in_millis": 500,
                                    "collection_count": 20,
                                },
                            }
                        },
                    },
                }
            }
        }

        client = Client(nodes=SubClient(nodes_stats_at_start))
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.JvmStatsSummary(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_start()
        # now we'd need to change the node stats response
        nodes_stats_at_end = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "mem": {
                            "pools": {
                                "young": {
                                    "peak_used_in_bytes": 558432256,
                                },
                                "survivor": {
                                    "peak_used_in_bytes": 69730304,
                                },
                                "old": {
                                    "peak_used_in_bytes": 3084912096,
                                },
                            }
                        },
                        "gc": {
                            "collectors": {
                                "old": {
                                    "collection_time_in_millis": 2500,
                                    "collection_count": 2,
                                },
                                "young": {
                                    "collection_time_in_millis": 1200,
                                    "collection_count": 4000,
                                },
                            }
                        },
                    },
                }
            }
        }
        client.nodes = SubClient(nodes_stats_at_end)
        t.on_benchmark_stop()

        metrics_store_node_level.assert_has_calls(
            [
                mock.call("rally0", "node_young_gen_gc_time", 700, "ms"),
                mock.call("rally0", "node_young_gen_gc_count", 3980),
                mock.call("rally0", "node_old_gen_gc_time", 1500, "ms"),
                mock.call("rally0", "node_old_gen_gc_count", 1),
            ],
            any_order=True,
        )

        metrics_store_cluster_level.assert_has_calls(
            [
                mock.call("node_total_young_gen_gc_time", 700, "ms"),
                mock.call("node_total_young_gen_gc_count", 3980),
                mock.call("node_total_old_gen_gc_time", 1500, "ms"),
                mock.call("node_total_old_gen_gc_count", 1),
            ],
            any_order=True,
        )

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "jvm_memory_pool_stats",
                        "young": {"peak_usage": 558432256, "unit": "byte"},
                        "survivor": {"peak_usage": 69730304, "unit": "byte"},
                        "old": {"peak_usage": 3084912096, "unit": "byte"},
                    },
                    level=MetaInfoScope.node,
                    node_name="rally0",
                ),
            ]
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_handles_optional_collectors(self, metrics_store_node_level, metrics_store_cluster_level, metrics_store_put_doc):
        nodes_stats_at_start = {
            "nodes": {
                "3zCI85c3RWKuR8MeJ8ajQg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "mem": {
                            "heap_used_in_bytes": 457179136,
                            "heap_used_percent": 2,
                            "heap_committed_in_bytes": 17179869184,
                            "heap_max_in_bytes": 17179869184,
                            "non_heap_used_in_bytes": 154359232,
                            "non_heap_committed_in_bytes": 159907840,
                            "pools": {},
                        },
                        "gc": {
                            "collectors": {
                                "ZGC Cycles": {"collection_count": 4, "collection_time_in_millis": 358},
                                "ZGC Pauses": {"collection_count": 12, "collection_time_in_millis": 0},
                            }
                        },
                    },
                }
            }
        }

        client = Client(nodes=SubClient(nodes_stats_at_start))
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.JvmStatsSummary(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_start()
        # now we'd need to change the node stats response
        nodes_stats_at_end = {
            "nodes": {
                "3zCI85c3RWKuR8MeJ8ajQg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "mem": {
                            "heap_used_in_bytes": 457179136,
                            "heap_used_percent": 2,
                            "heap_committed_in_bytes": 17179869184,
                            "heap_max_in_bytes": 17179869184,
                            "non_heap_used_in_bytes": 154359232,
                            "non_heap_committed_in_bytes": 159907840,
                            "pools": {},
                        },
                        "gc": {
                            "collectors": {
                                "ZGC Cycles": {"collection_count": 4, "collection_time_in_millis": 358},
                                "ZGC Pauses": {"collection_count": 12, "collection_time_in_millis": 0},
                            }
                        },
                    },
                }
            }
        }
        client.nodes = SubClient(nodes_stats_at_end)
        t.on_benchmark_stop()

        metrics_store_node_level.assert_has_calls(
            [
                mock.call("rally0", "node_zgc_cycles_gc_time", 0, "ms"),
                mock.call("rally0", "node_zgc_cycles_gc_count", 0),
                mock.call("rally0", "node_zgc_pauses_gc_time", 0, "ms"),
                mock.call("rally0", "node_zgc_pauses_gc_count", 0),
            ],
            any_order=True,
        )

        metrics_store_cluster_level.assert_has_calls(
            [
                mock.call("node_total_zgc_cycles_gc_time", 0, "ms"),
                mock.call("node_total_zgc_cycles_gc_count", 0),
                mock.call("node_total_zgc_pauses_gc_time", 0, "ms"),
                mock.call("node_total_zgc_pauses_gc_count", 0),
            ],
            any_order=True,
        )


class TestIndexStats:
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_stores_available_index_stats(self, metrics_store_cluster_value, metrics_store_put_doc):
        client = Client(
            indices=SubClient(
                {
                    "_all": {
                        "primaries": {
                            "segments": {
                                "count": 0,
                            },
                            "merges": {
                                "total_time_in_millis": 0,
                                "total_throttled_time_in_millis": 0,
                                "total": 0,
                            },
                            "indexing": {
                                "index_time_in_millis": 0,
                            },
                            "refresh": {
                                "total_time_in_millis": 0,
                                "total": 0,
                            },
                            "flush": {
                                "total_time_in_millis": 0,
                                "total": 0,
                            },
                        }
                    }
                }
            )
        )
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexStats(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_start()

        response = {
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 5,
                        "memory_in_bytes": 2048,
                        "stored_fields_memory_in_bytes": 1024,
                        "doc_values_memory_in_bytes": 128,
                        "terms_memory_in_bytes": 256,
                        "points_memory_in_bytes": 512,
                    },
                    "merges": {
                        "total_time_in_millis": 509341,
                        "total_throttled_time_in_millis": 98925,
                        "total": 3,
                    },
                    "indexing": {
                        "index_time_in_millis": 1065688,
                    },
                    "refresh": {
                        "total_time_in_millis": 158465,
                        "total": 10,
                    },
                    "flush": {
                        "total_time_in_millis": 0,
                        "total": 0,
                    },
                },
                "total": {
                    "store": {
                        "size_in_bytes": 2113867510,
                    },
                    "translog": {
                        "operations": 6840000,
                        "size_in_bytes": 2647984713,
                        "uncommitted_operations": 0,
                        "uncommitted_size_in_bytes": 430,
                    },
                },
            },
            "indices": {
                "idx-001": {
                    "shards": {
                        "0": [
                            {
                                "routing": {"primary": False},
                                "indexing": {
                                    "index_total": 2280171,
                                    "index_time_in_millis": 533662,
                                    "throttle_time_in_millis": 0,
                                },
                                "merges": {
                                    "total_time_in_millis": 280689,
                                    "total_stopped_time_in_millis": 0,
                                    "total_throttled_time_in_millis": 58846,
                                    "total_auto_throttle_in_bytes": 8085428,
                                },
                                "refresh": {
                                    "total_time_in_millis": 81004,
                                },
                                "flush": {
                                    "total_time_in_millis": 0,
                                },
                            }
                        ],
                        "1": [
                            {
                                "routing": {
                                    "primary": True,
                                },
                                "indexing": {
                                    "index_time_in_millis": 532026,
                                },
                                "merges": {
                                    "total_time_in_millis": 228652,
                                    "total_throttled_time_in_millis": 40079,
                                },
                                "refresh": {
                                    "total_time_in_millis": 77461,
                                },
                                "flush": {
                                    "total_time_in_millis": 0,
                                },
                            }
                        ],
                    }
                },
                "idx-002": {
                    "shards": {
                        "0": [
                            {
                                "routing": {
                                    "primary": True,
                                },
                                "indexing": {
                                    "index_time_in_millis": 533662,
                                },
                                "merges": {
                                    "total_time_in_millis": 280689,
                                    "total_throttled_time_in_millis": 58846,
                                },
                                "refresh": {
                                    "total_time_in_millis": 81004,
                                },
                                "flush": {
                                    "total_time_in_millis": 0,
                                },
                            }
                        ],
                        "1": [
                            {
                                "routing": {
                                    "primary": False,
                                },
                                "indexing": {
                                    "index_time_in_millis": 532026,
                                    "throttle_time_in_millis": 296,
                                },
                                "merges": {
                                    "total_time_in_millis": 228652,
                                    "total_throttled_time_in_millis": 40079,
                                },
                                "refresh": {
                                    "total_time_in_millis": 77461,
                                },
                                "flush": {
                                    "total_time_in_millis": 0,
                                },
                            }
                        ],
                    }
                },
            },
        }

        client.indices = SubClient(response)

        t.on_benchmark_stop()

        # we cannot rely on stable iteration order so we need to extract the values at runtime from the dict
        primary_shards = []
        for shards in response["indices"].values():
            for shard in shards["shards"].values():
                for shard_metrics in shard:
                    if shard_metrics["routing"]["primary"]:
                        primary_shards.append(shard_metrics)

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    doc={
                        "name": "merges_total_time",
                        "value": 509341,
                        "unit": "ms",
                        # [228652, 280689]
                        "per-shard": [s["merges"]["total_time_in_millis"] for s in primary_shards],
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(
                    doc={
                        "name": "merges_total_throttled_time",
                        "value": 98925,
                        "unit": "ms",
                        # [40079, 58846]
                        "per-shard": [s["merges"]["total_throttled_time_in_millis"] for s in primary_shards],
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(
                    doc={
                        "name": "indexing_total_time",
                        "value": 1065688,
                        "unit": "ms",
                        # [532026, 533662]
                        "per-shard": [s["indexing"]["index_time_in_millis"] for s in primary_shards],
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(
                    doc={
                        "name": "refresh_total_time",
                        "value": 158465,
                        "unit": "ms",
                        # [77461, 81004]
                        "per-shard": [s["refresh"]["total_time_in_millis"] for s in primary_shards],
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(
                    doc={
                        "name": "flush_total_time",
                        "value": 0,
                        "unit": "ms",
                        # [0, 0]
                        "per-shard": [s["flush"]["total_time_in_millis"] for s in primary_shards],
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(doc={"name": "merges_total_count", "value": 3}, level=metrics.MetaInfoScope.cluster),
                mock.call(doc={"name": "refresh_total_count", "value": 10}, level=metrics.MetaInfoScope.cluster),
                mock.call(doc={"name": "flush_total_count", "value": 0}, level=metrics.MetaInfoScope.cluster),
            ]
        )

        metrics_store_cluster_value.assert_has_calls(
            [
                mock.call("segments_count", 5),
                mock.call("segments_memory_in_bytes", 2048, "byte"),
                mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
                mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
                mock.call("segments_terms_memory_in_bytes", 256, "byte"),
                # we don't have norms, so nothing should have been called
                mock.call("store_size_in_bytes", 2113867510, "byte"),
                mock.call("translog_size_in_bytes", 2647984713, "byte"),
            ],
            any_order=True,
        )


class TestMlBucketProcessingTime:
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_error_on_retrieval_does_not_store_metrics(self, es, metrics_store_put_doc):
        es.search.side_effect = elasticsearch.TransportError("unit test error")
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_stop()

        assert metrics_store_put_doc.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_empty_result_does_not_store_metrics(self, es, metrics_store_put_doc):
        es.search.return_value = {
            "aggregations": {
                "jobs": {
                    "buckets": [],
                },
            },
        }
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_stop()

        assert metrics_store_put_doc.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_result_is_stored(self, es, metrics_store_put_doc):
        es.search.return_value = {
            "aggregations": {
                "jobs": {
                    "buckets": [
                        {
                            "key": "benchmark_ml_job_1",
                            "doc_count": 4775,
                            "max_pt": {"value": 36.0},
                            "mean_pt": {"value": 12.3},
                            "median_pt": {"values": {"50.0": 17.2}},
                            "min_pt": {"value": 2.2},
                        },
                        {
                            "key": "benchmark_ml_job_2",
                            "doc_count": 3333,
                            "max_pt": {"value": 226.3},
                            "mean_pt": {"value": 78.3},
                            "median_pt": {"values": {"50.0": 37.4}},
                            "min_pt": {"value": 32.2},
                        },
                    ]
                }
            }
        }

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_stop()

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    doc={
                        "name": "ml_processing_time",
                        "job": "benchmark_ml_job_1",
                        "min": 2.2,
                        "mean": 12.3,
                        "median": 17.2,
                        "max": 36.0,
                        "unit": "ms",
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
                mock.call(
                    doc={
                        "name": "ml_processing_time",
                        "job": "benchmark_ml_job_2",
                        "min": 32.2,
                        "mean": 78.3,
                        "median": 37.4,
                        "max": 226.3,
                        "unit": "ms",
                    },
                    level=metrics.MetaInfoScope.cluster,
                ),
            ]
        )


class TestIndexSize:
    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_stores_index_size_for_data_paths(self, metrics_store_node_value, get_size):
        get_size.side_effect = [2048, 16384]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexSize(["/var/elasticsearch/data/1", "/var/elasticsearch/data/2"])
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        node = cluster.Node(pid=None, binary_path="/bin", host_name="localhost", node_name="rally-node-0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)
        t.store_system_metrics(node, metrics_store)

        metrics_store_node_value.assert_has_calls(
            [mock.call("rally-node-0", "final_index_size_bytes", 18432, "byte")],
        )

    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_stores_nothing_if_no_data_path(self, run_subprocess, metrics_store_cluster_value, get_size):
        get_size.return_value = 2048

        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexSize(data_paths=[])
        t = telemetry.Telemetry(devices=[device])
        node = cluster.Node(pid=None, binary_path="/bin", host_name="localhost", node_name="rally-node-0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)
        t.store_system_metrics(node, metrics_store)

        assert run_subprocess.call_count == 0
        assert metrics_store_cluster_value.call_count == 0
        assert get_size.call_count == 0


class TestMasterNodeStats:
    def test_negative_sample_interval_forbidden(self):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {"master-node-stats-sample-interval": -1}
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"The telemetry parameter 'master-node-stats-sample-interval' must be greater than zero but was .*\.",
        ):
            telemetry.MasterNodeStats(telemetry_params, Client(), metrics_store)


class TestMasterNodeStatsRecorder:
    master_node_state_response = {
        "master_node": "12345",
    }

    master_node_info_response = {
        "nodes": {
            "12345": {
                "name": "rally-0",
            }
        }
    }

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_store_master_node_stats(self, metrics_store_put_doc):
        client = Client(
            cluster=SubClient(state=self.master_node_state_response),
            nodes=SubClient(info=self.master_node_info_response),
        )
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.MasterNodeStatsRecorder(client=client, metrics_store=metrics_store, sample_interval=1)
        recorder.record()

        metrics_store_put_doc.assert_has_calls(
            [
                mock.call(
                    {
                        "name": "master-node-stats",
                        "node": "rally-0",
                    },
                    level=MetaInfoScope.cluster,
                ),
            ],
        )


class TestIngestPipelineStats:

    ingest_pipeline_stats_start_response = {
        "_nodes": {"total": 1, "successful": 1, "failed": 0},
        "cluster_name": "docker-cluster",
        "nodes": {
            "ZlPBlHtYQDmG4ASbvRrFBg": {
                "timestamp": 1642121373533,
                "name": "elasticsearch31",
                "transport_address": "172.18.0.2:9300",
                "host": "172.18.0.2",
                "ip": "172.18.0.2:9300",
                "roles": [],
                "attributes": {
                    "ml.machine_memory": "7291248640",
                    "xpack.installed": "true",
                    "transform.node": "true",
                    "ml.max_open_jobs": "512",
                    "ml.max_jvm_size": "536870912",
                },
                "ingest": {
                    "total": {"count": 1, "time_in_millis": 1, "current": 1, "failed": 1},
                    "pipelines": {
                        "http-log-baseline-pipeline": {
                            "count": 1,
                            "time_in_millis": 1,
                            "current": 1,
                            "failed": 1,
                            "processors": [
                                {"uppercase": {"type": "uppercase", "stats": {"count": 1, "time_in_millis": 1, "current": 1, "failed": 1}}}
                            ],
                        },
                        "pipeline-1": {
                            "count": 1,
                            "time_in_millis": 1,
                            "current": 1,
                            "failed": 1,
                            "processors": [
                                {
                                    "append": {
                                        "type": "append",
                                        "stats": {
                                            "count": 1,
                                            "time_in_millis": 1,
                                            "current": 1,
                                            "failed": 1,
                                        },
                                    }
                                },
                                {
                                    "append": {
                                        "type": "append",
                                        "stats": {
                                            "count": 1,
                                            "time_in_millis": 1,
                                            "current": 1,
                                            "failed": 1,
                                        },
                                    }
                                },
                                {"lowercase": {"type": "lowercase", "stats": {"count": 1, "time_in_millis": 1, "current": 1, "failed": 1}}},
                            ],
                        },
                    },
                },
            },
        },
    }

    ingest_pipeline_stats_end_response = {
        "_nodes": {"total": 1, "successful": 1, "failed": 0},
        "cluster_name": "docker-cluster",
        "nodes": {
            "ZlPBlHtYQDmG4ASbvRrFBg": {
                "timestamp": 1642121373533,
                "name": "elasticsearch31",
                "transport_address": "172.18.0.2:9300",
                "host": "172.18.0.2",
                "ip": "172.18.0.2:9300",
                "roles": [],
                "attributes": {
                    "ml.machine_memory": "7291248640",
                    "xpack.installed": "true",
                    "transform.node": "true",
                    "ml.max_open_jobs": "512",
                    "ml.max_jvm_size": "536870912",
                },
                "ingest": {
                    "total": {"count": 2, "time_in_millis": 2, "current": 2, "failed": 2},
                    "pipelines": {
                        "http-log-baseline-pipeline": {
                            "count": 2,
                            "time_in_millis": 2,
                            "current": 2,
                            "failed": 2,
                            "processors": [
                                {"uppercase": {"type": "uppercase", "stats": {"count": 2, "time_in_millis": 2, "current": 2, "failed": 2}}}
                            ],
                        },
                        "pipeline-1": {
                            "count": 2,
                            "time_in_millis": 2,
                            "current": 2,
                            "failed": 2,
                            "processors": [
                                {"append": {"type": "append", "stats": {"count": 2, "time_in_millis": 2, "current": 2, "failed": 2}}},
                                {"append": {"type": "append", "stats": {"count": 2, "time_in_millis": 2, "current": 2, "failed": 2}}},
                                {"lowercase": {"type": "lowercase", "stats": {"count": 2, "time_in_millis": 2, "current": 2, "failed": 2}}},
                            ],
                        },
                    },
                },
            },
        },
    }

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_error_on_retrieval_does_not_store_metrics(self, es, metrics_store_cluster_level, metrics_store_node_level):
        es.search.side_effect = elasticsearch.TransportError("unit test error")
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IngestPipelineStats(es, metrics_store)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()

        assert metrics_store_cluster_level.call_count == 0
        assert metrics_store_node_level.call_count == 0

    def _assert_node_level_calls(self, metrics_store_node_level, *, value):
        metrics_store_node_level.assert_has_calls(
            [
                # node level stats
                mock.call("elasticsearch31", "ingest_pipeline_node_count", 1, meta_data={"cluster_name": "docker-cluster"}),
                mock.call("elasticsearch31", "ingest_pipeline_node_time", 1, "ms", meta_data={"cluster_name": "docker-cluster"}),
                mock.call("elasticsearch31", "ingest_pipeline_node_failed", 1, meta_data={"cluster_name": "docker-cluster"}),
                # pipeline level stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_count",
                    value,
                    meta_data={"pipeline_name": "http-log-baseline-pipeline", "cluster_name": "docker-cluster"},
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_time",
                    value,
                    unit="ms",
                    meta_data={"pipeline_name": "http-log-baseline-pipeline", "cluster_name": "docker-cluster"},
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_failed",
                    value,
                    meta_data={"pipeline_name": "http-log-baseline-pipeline", "cluster_name": "docker-cluster"},
                ),
                # processor level stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_count",
                    value,
                    meta_data={
                        "processor_name": "uppercase_1",
                        "type": "uppercase",
                        "pipeline_name": "http-log-baseline-pipeline",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_time",
                    value,
                    unit="ms",
                    meta_data={
                        "processor_name": "uppercase_1",
                        "type": "uppercase",
                        "pipeline_name": "http-log-baseline-pipeline",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_failed",
                    value,
                    meta_data={
                        "processor_name": "uppercase_1",
                        "type": "uppercase",
                        "pipeline_name": "http-log-baseline-pipeline",
                        "cluster_name": "docker-cluster",
                    },
                ),
                # pipeline level stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_count",
                    value,
                    meta_data={"pipeline_name": "pipeline-1", "cluster_name": "docker-cluster"},
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_time",
                    value,
                    unit="ms",
                    meta_data={"pipeline_name": "pipeline-1", "cluster_name": "docker-cluster"},
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_pipeline_failed",
                    value,
                    meta_data={"pipeline_name": "pipeline-1", "cluster_name": "docker-cluster"},
                ),
                # processor 1 stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_count",
                    value,
                    meta_data={
                        "processor_name": "append_1",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_time",
                    value,
                    unit="ms",
                    meta_data={
                        "processor_name": "append_1",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_failed",
                    value,
                    meta_data={
                        "processor_name": "append_1",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                # processor 2 stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_count",
                    value,
                    meta_data={
                        "processor_name": "append_2",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_time",
                    value,
                    unit="ms",
                    meta_data={
                        "processor_name": "append_2",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_failed",
                    value,
                    meta_data={
                        "processor_name": "append_2",
                        "type": "append",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                # processor 3 stats
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_count",
                    value,
                    meta_data={
                        "processor_name": "lowercase_3",
                        "type": "lowercase",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_time",
                    value,
                    unit="ms",
                    meta_data={
                        "processor_name": "lowercase_3",
                        "type": "lowercase",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
                mock.call(
                    "elasticsearch31",
                    "ingest_pipeline_processor_failed",
                    value,
                    meta_data={
                        "processor_name": "lowercase_3",
                        "type": "lowercase",
                        "pipeline_name": "pipeline-1",
                        "cluster_name": "docker-cluster",
                    },
                ),
            ]
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_stores_only_diff_of_ingest_pipeline_stats(self, metrics_store_cluster_level, metrics_store_node_level):
        clients = {"default": Client(nodes=SubClient(stats=self.ingest_pipeline_stats_start_response))}
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IngestPipelineStats(clients, metrics_store)
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()

        clients["default"].nodes = SubClient(stats=self.ingest_pipeline_stats_end_response)
        t.on_benchmark_stop()

        metrics_store_cluster_level.assert_has_calls(
            [
                mock.call("ingest_pipeline_cluster_count", 1, meta_data={"cluster_name": "docker-cluster"}),
                mock.call("ingest_pipeline_cluster_time", 1, "ms", meta_data={"cluster_name": "docker-cluster"}),
                mock.call("ingest_pipeline_cluster_failed", 1, meta_data={"cluster_name": "docker-cluster"}),
            ]
        )

        self._assert_node_level_calls(metrics_store_node_level, value=1)

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_pipeline_created_during_benchmark_stats(self, metrics_store_cluster_level, metrics_store_node_level):
        ingest_pipeline_stats_start_response = copy.deepcopy(self.ingest_pipeline_stats_start_response)
        ingest_pipeline_stats_start_response["nodes"]["ZlPBlHtYQDmG4ASbvRrFBg"]["ingest"]["pipelines"] = {}
        clients = {"default": Client(nodes=SubClient(stats=ingest_pipeline_stats_start_response))}
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IngestPipelineStats(clients, metrics_store)
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()

        clients["default"].nodes = SubClient(stats=self.ingest_pipeline_stats_end_response)
        t.on_benchmark_stop()

        metrics_store_cluster_level.assert_has_calls(
            [
                mock.call("ingest_pipeline_cluster_count", 1, meta_data={"cluster_name": "docker-cluster"}),
                mock.call("ingest_pipeline_cluster_time", 1, "ms", meta_data={"cluster_name": "docker-cluster"}),
                mock.call("ingest_pipeline_cluster_failed", 1, meta_data={"cluster_name": "docker-cluster"}),
            ]
        )

        self._assert_node_level_calls(metrics_store_node_level, value=2)

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_logs_warning_on_missing_stats(self, metrics_put_value_cluster_level, metrics_put_value_node_level):

        cfg = create_config()
        logger = logging.getLogger("esrally.telemetry")
        metrics_store = metrics.EsMetricsStore(cfg)
        clients = {"default": Client(nodes=SubClient(stats=self.ingest_pipeline_stats_start_response))}
        device = telemetry.IngestPipelineStats(clients, metrics_store)
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])

        #  cluster level
        t.on_benchmark_start()
        clients["default"].nodes = SubClient(stats=self.ingest_pipeline_stats_end_response)
        del device.start_stats["docker-cluster"]

        with mock.patch.object(logger, "warning") as mocked_warning:
            t.on_benchmark_stop()
            mocked_warning.assert_called_once_with(
                "Cannot determine Ingest Pipeline stats for %s (cluster stats weren't collected at the start of the benchmark).",
                "docker-cluster",
            )

        # node level
        clients["default"].nodes = SubClient(stats=self.ingest_pipeline_stats_start_response)
        t.on_benchmark_start()
        del device.start_stats["docker-cluster"]["elasticsearch31"]
        clients["default"].nodes = SubClient(stats=self.ingest_pipeline_stats_end_response)

        with mock.patch.object(logger, "warning") as mocked_warning:
            t.on_benchmark_stop()
            mocked_warning.assert_called_once_with(
                "Cannot determine Ingest Pipeline stats for %s (not in the cluster at the start of the benchmark).", "elasticsearch31"
            )


class TestDiskUsageStats:
    @mock.patch("elasticsearch.Elasticsearch")
    def test_uses_indices_by_default(self, es):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        tc = TransportClient(response={"_shards": {"failed": 0}})
        c = Client(transport_client=tc)
        device = telemetry.DiskUsageStats({}, c, metrics_store, index_names=["foo", "bar"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert tc.kwargs == [
            {"method": "POST", "path": "/foo/_disk_usage", "params": {"run_expensive_tasks": "true"}},
            {"method": "POST", "path": "/bar/_disk_usage", "params": {"run_expensive_tasks": "true"}},
        ]

    @mock.patch("elasticsearch.Elasticsearch")
    def test_uses_data_streams_by_default(self, es):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        tc = TransportClient(response={"_shards": {"failed": 0}})
        es = Client(transport_client=tc)
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=[], data_stream_names=["foo", "bar"])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert tc.kwargs == [
            {"method": "POST", "path": "/foo/_disk_usage", "params": {"run_expensive_tasks": "true"}},
            {"method": "POST", "path": "/bar/_disk_usage", "params": {"run_expensive_tasks": "true"}},
        ]

    @mock.patch("elasticsearch.Elasticsearch")
    def test_uses_indices_param_if_specified_instead_of_index_names(self, es):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        tc = TransportClient(response={"_shards": {"failed": 0}})
        es = Client(transport_client=tc)
        device = telemetry.DiskUsageStats(
            {"disk-usage-stats-indices": "foo,bar"}, es, metrics_store, index_names=["baz"], data_stream_names=[]
        )
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert tc.kwargs == [
            {"method": "POST", "path": "/foo/_disk_usage", "params": {"run_expensive_tasks": "true"}},
            {"method": "POST", "path": "/bar/_disk_usage", "params": {"run_expensive_tasks": "true"}},
        ]

    @mock.patch("elasticsearch.Elasticsearch")
    def test_uses_indices_param_if_specified_instead_of_data_stream_names(self, es):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        tc = TransportClient(response={"_shards": {"failed": 0}})
        es = Client(transport_client=tc)
        device = telemetry.DiskUsageStats(
            {"disk-usage-stats-indices": "foo,bar"}, es, metrics_store, index_names=[], data_stream_names=["baz"]
        )
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert tc.kwargs == [
            {"method": "POST", "path": "/foo/_disk_usage", "params": {"run_expensive_tasks": "true"}},
            {"method": "POST", "path": "/bar/_disk_usage", "params": {"run_expensive_tasks": "true"}},
        ]

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_error_on_retrieval_does_not_store_metrics(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                force_error=True,
                error=elasticsearch.RequestError(message="400", meta=None, body=None),
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        with pytest.raises(exceptions.RallyError):
            t.on_benchmark_stop()
        assert metrics_store_cluster_level.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_no_indices_fails(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                force_error=True,
                error=elasticsearch.RequestError(message="400", meta=None, body=None),
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=[], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        with pytest.raises(exceptions.RallyError):
            t.on_benchmark_start()
        assert metrics_store_cluster_level.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_missing_all_fails(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(force_error=True, error=elasticsearch.RequestError(message="400", meta=None, body=None))
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo", "bar"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        with pytest.raises(exceptions.RallyError):
            t.on_benchmark_stop()
        assert metrics_store_cluster_level.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_some_mising_succeeds(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)

        class TwoTransportClients:
            def __init__(self, first, rest):
                self.first = first
                self.rest = rest

            def perform_request(self, *args, **kwargs):
                if self.first:
                    first = self.first
                    self.first = None
                    return first.perform_request(args, kwargs)
                else:
                    return self.rest.perform_request(args, kwargs)

        not_found_transport_client = TransportClient(
            force_error=True,
            error=elasticsearch.NotFoundError(message="404", meta=None, body=None),
        )
        successful_client = TransportClient(
            response={
                "_shards": {"failed": 0},
                "foo": {
                    "fields": {
                        "_id": {
                            "total_in_bytes": 21079,
                            "inverted_index": {"total_in_bytes": 17110},
                            "stored_fields_in_bytes": 3969,
                        }
                    }
                },
            }
        )

        es = Client(transport_client=TwoTransportClients(not_found_transport_client, successful_client))
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo", "bar"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.call_count == 3

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_successful_shards(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"total": 1, "successful": 1, "failed": 0},
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_unsuccessful_shards(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"total": 1, "successful": 0, "failed": 1, "failures": "hello there!"},
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        with pytest.raises(exceptions.RallyError):
            t.on_benchmark_stop()
        assert metrics_store_cluster_level.call_count == 0

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_source(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"failed": 0},
                    "foo": {
                        "fields": {
                            "_source": {
                                "total_in_bytes": 40676,
                                "stored_fields_in_bytes": 40676,
                            }
                        }
                    },
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.mock_calls == [
            self._mock_store("disk_usage_total", 40676, "_source"),
            self._mock_store("disk_usage_stored_fields", 40676, "_source"),
        ]

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_id(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"failed": 0},
                    "foo": {
                        "fields": {
                            "_id": {
                                "total_in_bytes": 21079,
                                "inverted_index": {"total_in_bytes": 17110},
                                "stored_fields_in_bytes": 3969,
                            }
                        }
                    },
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.mock_calls == [
            self._mock_store("disk_usage_total", 21079, "_id"),
            self._mock_store("disk_usage_inverted_index", 17110, "_id"),
            self._mock_store("disk_usage_stored_fields", 3969, "_id"),
        ]

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_empty_field(self, metrics_store_cluster_level):
        """
        Fields like _primary_term commonly have take 0 bytes at all. But they
        are declared fields so we return their total size just so no one
        thinks they are lost. But their total size is 0.
        """
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"failed": 0},
                    "foo": {
                        "fields": {
                            "_primary_term": {
                                "total_in_bytes": 0,
                                "inverted_index": {"total_in_bytes": 0},
                                "stored_fields_in_bytes": 0,
                                "doc_values_in_bytes": 0,
                                "points_in_bytes": 0,
                            }
                        }
                    },
                }
            )
        )
        device = telemetry.DiskUsageStats({"disk-usage-stats-indices": "foo"}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.mock_calls == [
            self._mock_store("disk_usage_total", 0, "_primary_term"),
        ]

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_number(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"failed": 0},
                    "foo": {
                        "fields": {
                            "prcp": {
                                "total_in_bytes": 1498,
                                "doc_values_in_bytes": 748,
                                "points_in_bytes": 750,
                            }
                        }
                    },
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.mock_calls == [
            self._mock_store("disk_usage_total", 1498, "prcp"),
            self._mock_store("disk_usage_doc_values", 748, "prcp"),
            self._mock_store("disk_usage_points", 750, "prcp"),
        ]

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    def test_keyword(self, metrics_store_cluster_level):
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        es = Client(
            transport_client=TransportClient(
                response={
                    "_shards": {"failed": 0},
                    "foo": {
                        "fields": {
                            "station.country_code": {
                                "total_in_bytes": 346,
                                "doc_values_in_bytes": 328,
                                "points_in_bytes": 18,
                            }
                        }
                    },
                }
            )
        )
        device = telemetry.DiskUsageStats({}, es, metrics_store, index_names=["foo"], data_stream_names=[])
        t = telemetry.Telemetry(enabled_devices=[device.command], devices=[device])
        t.on_benchmark_start()
        t.on_benchmark_stop()
        assert metrics_store_cluster_level.mock_calls == [
            self._mock_store("disk_usage_total", 346, "station.country_code"),
            self._mock_store("disk_usage_doc_values", 328, "station.country_code"),
            self._mock_store("disk_usage_points", 18, "station.country_code"),
        ]

    def _mock_store(self, name, size, field):
        return mock.call(name, size, meta_data={"index": "foo", "field": field}, unit="byte")
