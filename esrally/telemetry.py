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

import collections
import logging
import os
import threading

import tabulate

from esrally import metrics, time, exceptions
from esrally.metrics import MetaInfoScope
from esrally.utils import io, sysstats, console, opts, process
from esrally.utils.versions import components


def list_telemetry():
    console.println("Available telemetry devices:\n")
    devices = [[device.command, device.human_name, device.help] for device in [JitCompiler, Gc, FlightRecorder,
                                                                               Heapdump, NodeStats, RecoveryStats,
                                                                               CcrStats, SegmentStats, TransformStats]]
    console.println(tabulate.tabulate(devices, ["Command", "Name", "Description"]))
    console.println("\nKeep in mind that each telemetry device may incur a runtime overhead which can skew results.")


class Telemetry:
    def __init__(self, enabled_devices=None, devices=None):
        if devices is None:
            devices = []
        if enabled_devices is None:
            enabled_devices = []
        self.enabled_devices = enabled_devices
        self.devices = devices

    def instrument_candidate_java_opts(self):
        opts = []
        for device in self.devices:
            if self._enabled(device):
                additional_opts = device.instrument_java_opts()
                # properly merge values with the same key
                opts.extend(additional_opts)
        return opts

    def on_pre_node_start(self, node_name):
        for device in self.devices:
            if self._enabled(device):
                device.on_pre_node_start(node_name)

    def attach_to_node(self, node):
        for device in self.devices:
            if self._enabled(device):
                device.attach_to_node(node)

    def detach_from_node(self, node, running):
        for device in self.devices:
            if self._enabled(device):
                device.detach_from_node(node, running)

    def on_benchmark_start(self):
        for device in self.devices:
            if self._enabled(device):
                device.on_benchmark_start()

    def on_benchmark_stop(self):
        for device in self.devices:
            if self._enabled(device):
                device.on_benchmark_stop()

    def store_system_metrics(self, node, metrics_store):
        for device in self.devices:
            if self._enabled(device):
                device.store_system_metrics(node, metrics_store)

    def _enabled(self, device):
        return device.internal or device.command in self.enabled_devices


########################################################################################
#
# Telemetry devices
#
########################################################################################

class TelemetryDevice:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def instrument_java_opts(self):
        return {}

    def on_pre_node_start(self, node_name):
        pass

    def attach_to_node(self, node):
        pass

    def detach_from_node(self, node, running):
        pass

    def on_benchmark_start(self):
        pass

    def on_benchmark_stop(self):
        pass

    def store_system_metrics(self, node, metrics_store):
        pass

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["logger"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = logging.getLogger(__name__)


class InternalTelemetryDevice(TelemetryDevice):
    internal = True


class SamplerThread(threading.Thread):
    def __init__(self, recorder):
        threading.Thread.__init__(self)
        self.stop = False
        self.recorder = recorder

    def finish(self):
        self.stop = True
        self.join()

    def run(self):
        # noinspection PyBroadException
        try:
            while not self.stop:
                self.recorder.record()
                time.sleep(self.recorder.sample_interval)
        except BaseException:
            logging.getLogger(__name__).exception("Could not determine %s", self.recorder)


class FlightRecorder(TelemetryDevice):
    internal = False
    command = "jfr"
    human_name = "Flight Recorder"
    help = "Enables Java Flight Recorder (requires an Oracle JDK or OpenJDK 11+)"

    def __init__(self, telemetry_params, log_root, java_major_version):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.log_root = log_root
        self.java_major_version = java_major_version

    def instrument_java_opts(self):
        io.ensure_dir(self.log_root)
        log_file = os.path.join(self.log_root, "profile.jfr")

        # JFR was integrated into OpenJDK 11 and is not a commercial feature anymore.
        if self.java_major_version < 11:
            console.println("\n***************************************************************************\n")
            console.println("[WARNING] Java flight recorder is a commercial feature of the Oracle JDK.\n")
            console.println("You are using Java flight recorder which requires that you comply with\nthe licensing terms stated in:\n")
            console.println(console.format.link("http://www.oracle.com/technetwork/java/javase/terms/license/index.html"))
            console.println("\nBy using this feature you confirm that you comply with these license terms.\n")
            console.println("Otherwise, please abort and rerun Rally without the \"jfr\" telemetry device.")
            console.println("\n***************************************************************************\n")

            time.sleep(3)

        console.info("%s: Writing flight recording to [%s]" % (self.human_name, log_file), logger=self.logger)

        java_opts = self.java_opts(log_file)

        self.logger.info("jfr: Adding JVM arguments: [%s].", java_opts)
        return java_opts

    def java_opts(self, log_file):
        recording_template = self.telemetry_params.get("recording-template")
        java_opts = ["-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"]
        jfr_cmd = ""
        if self.java_major_version < 11:
            java_opts.append("-XX:+UnlockCommercialFeatures")

        if self.java_major_version < 9:
            java_opts.append("-XX:+FlightRecorder")
            java_opts.append("-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath={}".format(log_file))
            jfr_cmd = "-XX:StartFlightRecording=defaultrecording=true"
            if recording_template:
                self.logger.info("jfr: Using recording template [%s].", recording_template)
                jfr_cmd += ",settings={}".format(recording_template)
            else:
                self.logger.info("jfr: Using default recording template.")
        else:
            jfr_cmd += "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename={}".format(log_file)
            if recording_template:
                self.logger.info("jfr: Using recording template [%s].", recording_template)
                jfr_cmd += ",settings={}".format(recording_template)
            else:
                self.logger.info("jfr: Using default recording template.")
        java_opts.append(jfr_cmd)
        return java_opts


class JitCompiler(TelemetryDevice):
    internal = False
    command = "jit"
    human_name = "JIT Compiler Profiler"
    help = "Enables JIT compiler logs."

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root

    def instrument_java_opts(self):
        io.ensure_dir(self.log_root)
        log_file = os.path.join(self.log_root, "jit.log")
        console.info("%s: Writing JIT compiler log to [%s]" % (self.human_name, log_file), logger=self.logger)
        return ["-XX:+UnlockDiagnosticVMOptions", "-XX:+TraceClassLoading", "-XX:+LogCompilation",
                "-XX:LogFile={}".format(log_file), "-XX:+PrintAssembly"]


class Gc(TelemetryDevice):
    internal = False
    command = "gc"
    human_name = "GC log"
    help = "Enables GC logs."

    def __init__(self, telemetry_params, log_root, java_major_version):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.log_root = log_root
        self.java_major_version = java_major_version

    def instrument_java_opts(self):
        io.ensure_dir(self.log_root)
        log_file = os.path.join(self.log_root, "gc.log")
        console.info("%s: Writing GC log to [%s]" % (self.human_name, log_file), logger=self.logger)
        return self.java_opts(log_file)

    def java_opts(self, log_file):
        if self.java_major_version < 9:
            return ["-Xloggc:{}".format(log_file), "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-XX:+PrintGCTimeStamps",
                    "-XX:+PrintGCApplicationStoppedTime", "-XX:+PrintGCApplicationConcurrentTime",
                    "-XX:+PrintTenuringDistribution"]
        else:
            log_config = self.telemetry_params.get("gc-log-config", "gc*=info,safepoint=info,age*=trace")
            # see https://docs.oracle.com/javase/9/tools/java.htm#JSWOR-GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5
            return [f"-Xlog:{log_config}:file={log_file}:utctime,uptimemillis,level,tags:filecount=0"]


class Heapdump(TelemetryDevice):
    internal = False
    command = "heapdump"
    human_name = "Heap Dump"
    help = "Captures a heap dump."

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root

    def detach_from_node(self, node, running):
        if running:
            heap_dump_file = os.path.join(self.log_root, "heap_at_exit_{}.hprof".format(node.pid))
            console.info("{}: Writing heap dump to [{}]".format(self.human_name, heap_dump_file), logger=self.logger)
            cmd = "jmap -dump:format=b,file={} {}".format(heap_dump_file, node.pid)
            if process.run_subprocess_with_logging(cmd):
                self.logger.warning("Could not write heap dump to [%s]", heap_dump_file)


class SegmentStats(TelemetryDevice):
    internal = False
    command = "segment-stats"
    human_name = "Segment Stats"
    help = "Determines segment stats at the end of the benchmark."

    def __init__(self, log_root, client):
        super().__init__()
        self.log_root = log_root
        self.client = client

    def on_benchmark_stop(self):
        # noinspection PyBroadException
        try:
            segment_stats = self.client.cat.segments(index="_all", v=True)
            stats_file = os.path.join(self.log_root, "segment_stats.log")
            console.info(f"{self.human_name}: Writing segment stats to [{stats_file}]", logger=self.logger)
            with open(stats_file, "wt") as f:
                f.write(segment_stats)
        except BaseException:
            self.logger.exception("Could not retrieve segment stats.")


class CcrStats(TelemetryDevice):
    internal = False
    command = "ccr-stats"
    human_name = "CCR Stats"
    help = "Regularly samples Cross Cluster Replication (CCR) related stats"

    """
    Gathers CCR stats on a cluster level
    """

    def __init__(self, telemetry_params, clients, metrics_store):
        """
        :param telemetry_params: The configuration object for telemetry_params.
            May optionally specify:
            ``ccr-stats-indices``: JSON string specifying the indices per cluster to publish statistics from.
            Not all clusters need to be specified, but any name used must be be present in target.hosts.
            Example:
            {"ccr-stats-indices": {"cluster_a": ["follower"],"default": ["leader"]}
            ``ccr-stats-sample-interval``: positive integer controlling the sampling interval. Default: 1 second.
        :param clients: A dict of clients to all clusters.
        :param metrics_store: The configured metrics store we write to.
        """
        super().__init__()

        self.telemetry_params = telemetry_params
        self.clients = clients
        self.sample_interval = telemetry_params.get("ccr-stats-sample-interval", 1)
        if self.sample_interval <= 0:
            raise exceptions.SystemSetupError(
                "The telemetry parameter 'ccr-stats-sample-interval' must be greater than zero but was {}.".format(self.sample_interval))
        self.specified_cluster_names = self.clients.keys()
        self.indices_per_cluster = self.telemetry_params.get("ccr-stats-indices", False)
        if self.indices_per_cluster:
            for cluster_name in self.indices_per_cluster.keys():
                if cluster_name not in clients:
                    raise exceptions.SystemSetupError(
                        "The telemetry parameter 'ccr-stats-indices' must be a JSON Object with keys matching "
                        "the cluster names [{}] specified in --target-hosts "
                        "but it had [{}].".format(",".join(sorted(clients.keys())), cluster_name))
            self.specified_cluster_names = self.indices_per_cluster.keys()

        self.metrics_store = metrics_store
        self.samplers = []

    def on_benchmark_start(self):
        recorder = []
        for cluster_name in self.specified_cluster_names:
            recorder = CcrStatsRecorder(cluster_name, self.clients[cluster_name], self.metrics_store, self.sample_interval,
                                        self.indices_per_cluster[cluster_name] if self.indices_per_cluster else None)
            sampler = SamplerThread(recorder)
            self.samplers.append(sampler)
            sampler.setDaemon(True)
            # we don't require starting recorders precisely at the same time
            sampler.start()

    def on_benchmark_stop(self):
        if self.samplers:
            for sampler in self.samplers:
                sampler.finish()


class CcrStatsRecorder:
    """
    Collects and pushes CCR stats for the specified cluster to the metric store.
    """

    def __init__(self, cluster_name, client, metrics_store, sample_interval, indices=None):
        """
        :param cluster_name: The cluster_name that the client connects to, as specified in target.hosts.
        :param client: The Elasticsearch client for this cluster.
        :param metrics_store: The configured metrics store we write to.
        :param sample_interval: integer controlling the interval, in seconds, between collecting samples.
        :param indices: optional list of indices to filter results from.
        """

        self.cluster_name = cluster_name
        self.client = client
        self.metrics_store = metrics_store
        self.sample_interval= sample_interval
        self.indices = indices
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return "ccr stats"

    def record(self):
        """
        Collect CCR stats for indexes (optionally) specified in telemetry parameters and push to metrics store.
        """

        # ES returns all stats values in bytes or ms via "human: false"

        import elasticsearch

        try:
            ccr_stats_api_endpoint = "/_ccr/stats"
            filter_path = "follow_stats"
            stats = self.client.transport.perform_request("GET", ccr_stats_api_endpoint, params={"human": "false",
                                                                                                 "filter_path": filter_path})
        except elasticsearch.TransportError as e:
            msg = "A transport error occurred while collecting CCR stats from the endpoint [{}?filter_path={}] on " \
                  "cluster [{}]".format(ccr_stats_api_endpoint, filter_path, self.cluster_name)
            self.logger.exception(msg)
            raise exceptions.RallyError(msg)

        if filter_path in stats and "indices" in stats[filter_path]:
            for indices in stats[filter_path]["indices"]:
                try:
                    if self.indices and indices["index"] not in self.indices:
                        # Skip metrics for indices not part of user supplied whitelist (ccr-stats-indices) in telemetry params.
                        continue
                    self.record_stats_per_index(indices["index"], indices["shards"])
                except KeyError:
                    self.logger.warning(
                        "The 'indices' key in %s does not contain an 'index' or 'shards' key "
                        "Maybe the output format of the %s endpoint has changed. Skipping.", ccr_stats_api_endpoint, ccr_stats_api_endpoint
                    )

    def record_stats_per_index(self, name, stats):
        """
        :param name: The index name.
        :param stats: A dict with returned CCR stats for the index.
        """

        for shard_stats in stats:
            if "shard_id" in shard_stats:
                doc = {
                    "name": "ccr-stats",
                    "shard": shard_stats
                }
                shard_metadata = {
                    "cluster": self.cluster_name,
                    "index": name
                }

                self.metrics_store.put_doc(doc, level=MetaInfoScope.cluster, meta_data=shard_metadata)


class RecoveryStats(TelemetryDevice):
    internal = False
    command = "recovery-stats"
    human_name = "Recovery Stats"
    help = "Regularly samples shard recovery stats"

    """
    Gathers recovery stats on a cluster level
    """

    def __init__(self, telemetry_params, clients, metrics_store):
        """
        :param telemetry_params: The configuration object for telemetry_params.
            May optionally specify:
            ``recovery-stats-indices``: JSON structure specifying the index pattern per cluster to publish stats from.
            Not all clusters need to be specified, but any name used must be be present in target.hosts. Alternatively,
            the index pattern can be specified as a string can be specified in case only one cluster is involved.
            Example:
            {"recovery-stats-indices": {"cluster_a": ["follower"],"default": ["leader"]}

            ``recovery-stats-sample-interval``: positive integer controlling the sampling interval. Default: 1 second.
        :param clients: A dict of clients to all clusters.
        :param metrics_store: The configured metrics store we write to.
        """
        super().__init__()

        self.telemetry_params = telemetry_params
        self.clients = clients
        self.sample_interval = telemetry_params.get("recovery-stats-sample-interval", 1)
        if self.sample_interval <= 0:
            raise exceptions.SystemSetupError(
                "The telemetry parameter 'recovery-stats-sample-interval' must be greater than zero but was {}."
                    .format(self.sample_interval))
        self.specified_cluster_names = self.clients.keys()
        indices_per_cluster = self.telemetry_params.get("recovery-stats-indices", False)
        # allow the user to specify either an index pattern as string or as a JSON object
        if isinstance(indices_per_cluster, str):
            self.indices_per_cluster = {opts.TargetHosts.DEFAULT: indices_per_cluster}
        else:
            self.indices_per_cluster = indices_per_cluster

        if self.indices_per_cluster:
            for cluster_name in self.indices_per_cluster.keys():
                if cluster_name not in clients:
                    raise exceptions.SystemSetupError(
                        "The telemetry parameter 'recovery-stats-indices' must be a JSON Object with keys matching "
                        "the cluster names [{}] specified in --target-hosts "
                        "but it had [{}].".format(",".join(sorted(clients.keys())), cluster_name))
            self.specified_cluster_names = self.indices_per_cluster.keys()

        self.metrics_store = metrics_store
        self.samplers = []

    def on_benchmark_start(self):
        for cluster_name in self.specified_cluster_names:
            recorder = RecoveryStatsRecorder(cluster_name, self.clients[cluster_name], self.metrics_store,
                                             self.sample_interval,
                                             self.indices_per_cluster[cluster_name] if self.indices_per_cluster else "")
            sampler = SamplerThread(recorder)
            self.samplers.append(sampler)
            sampler.setDaemon(True)
            # we don't require starting recorders precisely at the same time
            sampler.start()

    def on_benchmark_stop(self):
        if self.samplers:
            for sampler in self.samplers:
                sampler.finish()


class RecoveryStatsRecorder:
    """
    Collects and pushes recovery stats for the specified cluster to the metric store.
    """

    def __init__(self, cluster_name, client, metrics_store, sample_interval, indices=None):
        """
        :param cluster_name: The cluster_name that the client connects to, as specified in target.hosts.
        :param client: The Elasticsearch client for this cluster.
        :param metrics_store: The configured metrics store we write to.
        :param sample_interval: integer controlling the interval, in seconds, between collecting samples.
        :param indices: optional list of indices to filter results from.
        """

        self.cluster_name = cluster_name
        self.client = client
        self.metrics_store = metrics_store
        self.sample_interval = sample_interval
        self.indices = indices
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        return "recovery stats"

    def record(self):
        """
        Collect recovery stats for indexes (optionally) specified in telemetry parameters and push to metrics store.
        """
        import elasticsearch

        try:
            stats = self.client.indices.recovery(index=self.indices, active_only=True, detailed=False)
        except elasticsearch.TransportError:
            msg = "A transport error occurred while collecting recovery stats on cluster [{}]".format(self.cluster_name)
            self.logger.exception(msg)
            raise exceptions.RallyError(msg)

        for idx, idx_stats in stats.items():
            for shard in idx_stats["shards"]:
                doc = {
                    "name": "recovery-stats",
                    "shard": shard
                }
                shard_metadata = {
                    "cluster": self.cluster_name,
                    "index": idx,
                    "shard": shard["id"]
                }
                self.metrics_store.put_doc(doc, level=MetaInfoScope.cluster, meta_data=shard_metadata)


class NodeStats(TelemetryDevice):
    """
    Gathers different node stats.
    """

    internal = False
    command = "node-stats"
    human_name = "Node Stats"
    help = "Regularly samples node stats"
    warning = """You have enabled the node-stats telemetry device with Elasticsearch < 7.2.0. Requests to the
          _nodes/stats Elasticsearch endpoint trigger additional refreshes and WILL SKEW results.
    """

    def __init__(self, telemetry_params, clients, metrics_store):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.clients = clients
        self.specified_cluster_names = self.clients.keys()
        self.metrics_store = metrics_store
        self.samplers = []

    def on_benchmark_start(self):
        default_client = self.clients["default"]
        distribution_version = default_client.info()["version"]["number"]
        major, minor = components(distribution_version)[:2]

        if major < 7 or (major == 7 and minor < 2):
            console.warn(NodeStats.warning, logger=self.logger)

        for cluster_name in self.specified_cluster_names:
            recorder = NodeStatsRecorder(self.telemetry_params, cluster_name, self.clients[cluster_name], self.metrics_store)
            sampler = SamplerThread(recorder)
            self.samplers.append(sampler)
            sampler.setDaemon(True)
            # we don't require starting recorders precisely at the same time
            sampler.start()

    def on_benchmark_stop(self):
        if self.samplers:
            for sampler in self.samplers:
                sampler.finish()


class NodeStatsRecorder:
    def __init__(self, telemetry_params, cluster_name, client, metrics_store):
        self.sample_interval = telemetry_params.get("node-stats-sample-interval", 1)
        if self.sample_interval <= 0:
            raise exceptions.SystemSetupError(
                "The telemetry parameter 'node-stats-sample-interval' must be greater than zero but was {}.".format(self.sample_interval))

        self.include_indices = telemetry_params.get("node-stats-include-indices", False)
        self.include_indices_metrics = telemetry_params.get("node-stats-include-indices-metrics", False)

        if self.include_indices_metrics:
            if isinstance(self.include_indices_metrics, str):
                self.include_indices_metrics_list = opts.csv_to_list(self.include_indices_metrics)
            else:
                # we don't validate the allowable metrics as they may change across ES versions
                raise exceptions.SystemSetupError(
                    "The telemetry parameter 'node-stats-include-indices-metrics' must be a comma-separated string but was {}".format(
                        type(self.include_indices_metrics))
                    )
        else:
            self.include_indices_metrics_list = ["docs", "store", "indexing", "search", "merges", "query_cache",
                                                 "fielddata", "segments", "translog", "request_cache"]

        self.include_thread_pools = telemetry_params.get("node-stats-include-thread-pools", True)
        self.include_buffer_pools = telemetry_params.get("node-stats-include-buffer-pools", True)
        self.include_breakers = telemetry_params.get("node-stats-include-breakers", True)
        self.include_network = telemetry_params.get("node-stats-include-network", True)
        self.include_process = telemetry_params.get("node-stats-include-process", True)
        self.include_mem_stats = telemetry_params.get("node-stats-include-mem", True)
        self.include_gc_stats = telemetry_params.get("node-stats-include-gc", True)
        self.include_indexing_pressure = telemetry_params.get("node-stats-include-indexing-pressure", True)
        self.client = client
        self.metrics_store = metrics_store
        self.cluster_name = cluster_name

    def __str__(self):
        return "node stats"

    def record(self):
        current_sample = self.sample()
        for node_stats in current_sample:
            node_name = node_stats["name"]
            metrics_store_meta_data = {
                "cluster": self.cluster_name,
                "node_name": node_name
            }
            collected_node_stats = collections.OrderedDict()
            collected_node_stats["name"] = "node-stats"

            if self.include_indices or self.include_indices_metrics:
                collected_node_stats.update(
                    self.indices_stats(node_name, node_stats, include=self.include_indices_metrics_list))
            if self.include_thread_pools:
                collected_node_stats.update(self.thread_pool_stats(node_name, node_stats))
            if self.include_breakers:
                collected_node_stats.update(self.circuit_breaker_stats(node_name, node_stats))
            if self.include_buffer_pools:
                collected_node_stats.update(self.jvm_buffer_pool_stats(node_name, node_stats))
            if self.include_mem_stats:
                collected_node_stats.update(self.jvm_mem_stats(node_name, node_stats))
            if self.include_gc_stats:
                collected_node_stats.update(self.jvm_gc_stats(node_name, node_stats))
            if self.include_network:
                collected_node_stats.update(self.network_stats(node_name, node_stats))
            if self.include_process:
                collected_node_stats.update(self.process_stats(node_name, node_stats))
            if self.include_indexing_pressure:
                collected_node_stats.update(self.indexing_pressure(node_name, node_stats))

            self.metrics_store.put_doc(dict(collected_node_stats),
                                       level=MetaInfoScope.node,
                                       node_name=node_name,
                                       meta_data=metrics_store_meta_data)

    def flatten_stats_fields(self, prefix=None, stats=None):
        """
        Flatten provided dict using an optional prefix and top level key filters.

        :param prefix: The prefix for all flattened values. Defaults to None.
        :param stats: Dict with values to be flattened, using _ as a separator. Defaults to {}.
        :return: Return flattened dictionary, separated by _ and prefixed with prefix.
        """

        def iterate():
            for section_name, section_value in stats.items():
                if isinstance(section_value, dict):
                    new_prefix = "{}_{}".format(prefix, section_name)
                    # https://www.python.org/dev/peps/pep-0380/
                    yield from self.flatten_stats_fields(prefix=new_prefix, stats=section_value).items()
                # Avoid duplication for metric fields that have unit embedded in value as they are also recorded elsewhere
                # example: `breakers_parent_limit_size_in_bytes` vs `breakers_parent_limit_size`
                elif isinstance(section_value, (int, float)) and not isinstance(section_value, bool):
                    yield "{}{}".format(prefix + "_" if prefix else "", section_name), section_value

        if stats:
            return dict(iterate())
        else:
            return dict()

    def indices_stats(self, node_name, node_stats, include):
        idx_stats = node_stats["indices"]
        ordered_results = collections.OrderedDict()
        for section in include:
            if section in idx_stats:
                ordered_results.update(self.flatten_stats_fields(prefix="indices_" + section, stats=idx_stats[section]))

        return ordered_results

    def thread_pool_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="thread_pool", stats=node_stats["thread_pool"])

    def circuit_breaker_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="breakers", stats=node_stats["breakers"])

    def jvm_buffer_pool_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="jvm_buffer_pools", stats=node_stats["jvm"]["buffer_pools"])

    def jvm_mem_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="jvm_mem", stats=node_stats["jvm"]["mem"])

    def jvm_gc_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="jvm_gc", stats=node_stats["jvm"]["gc"])

    def network_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="transport", stats=node_stats.get("transport"))

    def process_stats(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="process_cpu", stats=node_stats["process"]["cpu"])

    def indexing_pressure(self, node_name, node_stats):
        return self.flatten_stats_fields(prefix="indexing_pressure", stats=node_stats["indexing_pressure"])

    def sample(self):
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            logging.getLogger(__name__).exception("Could not retrieve node stats.")
            return {}
        return stats["nodes"].values()


class TransformStats(TelemetryDevice):
    internal = False
    command = "transform-stats"
    human_name = "Transform Stats"
    help = "Regularly samples transform stats"

    """
    Gathers Transform stats
    """

    def __init__(self, telemetry_params, clients, metrics_store):
        """
        :param telemetry_params: The configuration object for telemetry_params.
        :param clients: A dict of clients to all clusters.
        :param metrics_store: The configured metrics store we write to.
        """
        super().__init__()

        self.telemetry_params = telemetry_params
        self.clients = clients
        self.sample_interval = telemetry_params.get("transform-stats-sample-interval", 1)
        if self.sample_interval <= 0:
            raise exceptions.SystemSetupError(
                f"The telemetry parameter 'transform-stats-sample-interval' must be greater than zero "
                f"but was [{self.sample_interval}].")
        self.specified_cluster_names = self.clients.keys()
        self.transforms_per_cluster = self.telemetry_params.get("transform-stats-transforms", False)
        if self.transforms_per_cluster:
            for cluster_name in self.transforms_per_cluster.keys():
                if cluster_name not in clients:
                    raise exceptions.SystemSetupError(
                        f"The telemetry parameter 'transform-stats-transforms' must be a JSON Object with keys "
                        f"matching the cluster names [{','.join(sorted(clients.keys()))}] specified in --target-hosts "
                        f"but it had [{cluster_name}].")
            self.specified_cluster_names = self.transforms_per_cluster.keys()

        self.metrics_store = metrics_store
        self.samplers = []

    def on_benchmark_start(self):
        for cluster_name in self.specified_cluster_names:
            recorder = TransformStatsRecorder(cluster_name, self.clients[cluster_name], self.metrics_store,
                                              self.sample_interval,
                                              self.transforms_per_cluster[
                                                  cluster_name] if self.transforms_per_cluster else None)
            sampler = SamplerThread(recorder)
            self.samplers.append(sampler)
            sampler.setDaemon(True)
            # we don't require starting recorders precisely at the same time
            sampler.start()

    def on_benchmark_stop(self):
        if self.samplers:
            for sampler in self.samplers:
                sampler.finish()
                # record the final stats
                sampler.recorder.record_final()


class TransformStatsRecorder:
    """
    Collects and pushes Transform stats for the specified cluster to the metric store.
    """

    def __init__(self, cluster_name, client, metrics_store, sample_interval, transforms=None):
        """
        :param cluster_name: The cluster_name that the client connects to, as specified in target.hosts.
        :param client: The Elasticsearch client for this cluster.
        :param metrics_store: The configured metrics store we write to.
        :param sample_interval: integer controlling the interval, in seconds, between collecting samples.
        :param transforms: optional list of transforms to filter results from.
        """

        self.cluster_name = cluster_name
        self.client = client
        self.metrics_store = metrics_store
        self.sample_interval = sample_interval
        self.transforms = transforms
        self.logger = logging.getLogger(__name__)

        self.logger.info("transform stats recorder")

    def __str__(self):
        return "transform stats"

    def record(self):
        """
        Collect Transform stats for transforms (optionally) specified in telemetry parameters and push to metrics store.
        """

        self._record()

    def record_final(self):
        """
        Collect final Transform stats for transforms (optionally) specified in telemetry parameters and push to metrics store.
        """

        self._record("total_")

    def _record(self, prefix=""):
        # ES returns all stats values in bytes or ms via "human: false"

        import elasticsearch

        try:
            stats = self.client.transform.get_transform_stats("_all")

        except elasticsearch.TransportError as e:
            msg = f"A transport error occurred while collecting transform stats on " \
                  f"cluster [{self.cluster_name}]"
            self.logger.exception(msg)
            raise exceptions.RallyError(msg)

        for transform in stats["transforms"]:
            try:
                if self.transforms and transform["id"] not in self.transforms:
                    # Skip metrics for transform not part of user supplied whitelist (transform-stats-transforms)
                    # in telemetry params.
                    continue
                self.record_stats_per_transform(transform["id"], transform["stats"], prefix)

            except KeyError:
                self.logger.warning(
                    "The 'transform' key does not contain a 'transform' or 'stats' key "
                    "Maybe the output format has changed. Skipping."
                )

    def record_stats_per_transform(self, transform_id, stats, prefix=""):
        """
        :param transform_id: The transform id.
        :param stats: A dict with returned transform stats for the transform.
        :param prefix: A prefix for the counters/values, e.g. for total runtimes
        """

        meta_data = {
            "transform_id": transform_id
        }

        self.metrics_store.put_count_cluster_level(prefix + "transform_pages_processed",
                                                   stats.get("pages_processed", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_documents_processed",
                                                   stats.get("documents_processed", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_documents_indexed",
                                                   stats.get("documents_indexed", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_index_total", stats.get("index_total", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_index_failures", stats.get("index_failures", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_search_total", stats.get("search_total", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_search_failures",
                                                   stats.get("search_failures", 0),
                                                   meta_data=meta_data)
        self.metrics_store.put_count_cluster_level(prefix + "transform_processing_total",
                                                   stats.get("processing_total", 0),
                                                   meta_data=meta_data)

        self.metrics_store.put_value_cluster_level(prefix + "transform_search_time",
                                                   stats.get("search_time_in_ms", 0),
                                                   "ms", meta_data=meta_data)
        self.metrics_store.put_value_cluster_level(prefix + "transform_index_time",
                                                   stats.get("index_time_in_ms", 0), "ms",
                                                   meta_data=meta_data)
        self.metrics_store.put_value_cluster_level(prefix + "transform_processing_time",
                                                   stats.get("processing_time_in_ms", 0), "ms",
                                                   meta_data=meta_data)

        documents_processed = stats.get("documents_processed", 0)
        processing_time = stats.get("search_time_in_ms", 0)
        processing_time += stats.get("processing_time_in_ms", 0)
        processing_time += stats.get("index_time_in_ms", 0)

        if processing_time > 0:
            throughput = documents_processed / processing_time * 1000
            self.metrics_store.put_value_cluster_level(prefix + "transform_throughput", throughput,
                                                       "docs/s", meta_data=meta_data)


class StartupTime(InternalTelemetryDevice):
    def __init__(self, stopwatch=time.StopWatch):
        super().__init__()
        self.timer = stopwatch()

    def on_pre_node_start(self, node_name):
        self.timer.start()

    def attach_to_node(self, node):
        self.timer.stop()

    def store_system_metrics(self, node, metrics_store):
        metrics_store.put_value_node_level(node.node_name, "node_startup_time", self.timer.total_time(), "s")


class DiskIo(InternalTelemetryDevice):
    """
    Gathers disk I/O stats.
    """
    def __init__(self, node_count_on_host):
        super().__init__()
        self.node_count_on_host = node_count_on_host
        self.read_bytes = None
        self.write_bytes = None

    def attach_to_node(self, node):
        es_process = sysstats.setup_process_stats(node.pid)
        process_start = sysstats.process_io_counters(es_process)
        if process_start:
            self.read_bytes = process_start.read_bytes
            self.write_bytes = process_start.write_bytes
            self.logger.info("Using more accurate process-based I/O counters.")
        else:
            # noinspection PyBroadException
            try:
                disk_start = sysstats.disk_io_counters()
                self.read_bytes = disk_start.read_bytes
                self.write_bytes = disk_start.write_bytes
                self.logger.warning("Process I/O counters are not supported on this platform. Falling back to less "
                                    "accurate disk I/O counters.")
            except BaseException:
                self.logger.exception("Could not determine I/O stats at benchmark start.")

    def detach_from_node(self, node, running):
        if running:
            # Be aware the semantics of write counts etc. are different for disk and process statistics.
            # Thus we're conservative and only report I/O bytes now.
            # noinspection PyBroadException
            try:
                es_process = sysstats.setup_process_stats(node.pid)
                process_end = sysstats.process_io_counters(es_process)
                # we have process-based disk counters, no need to worry how many nodes are on this host
                if process_end:
                    self.read_bytes = process_end.read_bytes - self.read_bytes
                    self.write_bytes = process_end.write_bytes - self.write_bytes
                else:
                    disk_end = sysstats.disk_io_counters()
                    if self.node_count_on_host > 1:
                        self.logger.info("There are [%d] nodes on this host and Rally fell back to disk I/O counters. "
                                         "Attributing [1/%d] of total I/O to [%s].",
                                         self.node_count_on_host, self.node_count_on_host, node.node_name)

                    self.read_bytes = (disk_end.read_bytes - self.read_bytes) // self.node_count_on_host
                    self.write_bytes = (disk_end.write_bytes - self.write_bytes) // self.node_count_on_host
            # Catching RuntimeException is not sufficient: psutil might raise AccessDenied (derived from Exception)
            except BaseException:
                self.logger.exception("Could not determine I/O stats at benchmark end.")
                # reset all counters so we don't attempt to write inconsistent numbers to the metrics store later on
                self.read_bytes = None
                self.write_bytes = None

    def store_system_metrics(self, node, metrics_store):
        if self.write_bytes is not None:
            metrics_store.put_count_node_level(node.node_name, "disk_io_write_bytes", self.write_bytes, "byte")
        if self.read_bytes is not None:
            metrics_store.put_count_node_level(node.node_name, "disk_io_read_bytes", self.read_bytes, "byte")


def store_node_attribute_metadata(metrics_store, nodes_info):
    # push up all node level attributes to cluster level iff the values are identical for all nodes
    pseudo_cluster_attributes = {}
    for node in nodes_info:
        if "attributes" in node:
            for k, v in node["attributes"].items():
                attribute_key = "attribute_%s" % str(k)
                metrics_store.add_meta_info(metrics.MetaInfoScope.node, node["name"], attribute_key, v)
                if attribute_key not in pseudo_cluster_attributes:
                    pseudo_cluster_attributes[attribute_key] = set()
                pseudo_cluster_attributes[attribute_key].add(v)

    for k, v in pseudo_cluster_attributes.items():
        if len(v) == 1:
            metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, k, next(iter(v)))


def store_plugin_metadata(metrics_store, nodes_info):
    # push up all plugins to cluster level iff all nodes have the same ones
    all_nodes_plugins = []
    all_same = False

    for node in nodes_info:
        plugins = [p["name"] for p in extract_value(node, ["plugins"], fallback=[]) if "name" in p]
        if not all_nodes_plugins:
            all_nodes_plugins = plugins.copy()
            all_same = True
        else:
            # order does not matter so we do a set comparison
            all_same = all_same and set(all_nodes_plugins) == set(plugins)

        if plugins:
            metrics_store.add_meta_info(metrics.MetaInfoScope.node, node["name"], "plugins", plugins)

    if all_same and all_nodes_plugins:
        metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "plugins", all_nodes_plugins)


def extract_value(node, path, fallback="unknown"):
    value = node
    try:
        for k in path:
            value = value[k]
    except KeyError:
        value = fallback
    return value


class ClusterEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information on a cluster level (e.g. version numbers).
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client

    def on_benchmark_start(self):
        # noinspection PyBroadException
        try:
            client_info = self.client.info()
        except BaseException:
            self.logger.exception("Could not retrieve cluster version info")
            return
        revision = client_info["version"]["build_hash"]
        distribution_version = client_info["version"]["number"]
        # older versions (pre 6.3.0) don't expose a build_flavor property because the only (implicit) flavor was "oss".
        distribution_flavor = client_info["version"].get("build_flavor", "oss")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "distribution_version", distribution_version)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "distribution_flavor", distribution_flavor)

        info = self.client.nodes.info(node_id="_all")
        nodes_info = info["nodes"].values()
        for node in nodes_info:
            node_name = node["name"]
            # while we could determine this for bare-metal nodes that are provisioned by Rally, there are other cases (Docker, externally
            # provisioned clusters) where it's not that easy.
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_vendor", extract_value(node, ["jvm", "vm_vendor"]))
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_version", extract_value(node, ["jvm", "version"]))

        store_plugin_metadata(self.metrics_store, nodes_info)
        store_node_attribute_metadata(self.metrics_store, nodes_info)


def add_metadata_for_node(metrics_store, node_name, host_name):
    """
    Gathers static environment information like OS or CPU details for Rally-provisioned nodes.
    """
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "os_name", sysstats.os_name())
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "os_version", sysstats.os_version())
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "cpu_logical_cores", sysstats.logical_cpu_cores())
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "cpu_physical_cores", sysstats.physical_cpu_cores())
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "cpu_model", sysstats.cpu_model())
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "node_name", node_name)
    metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "host_name", host_name)


class ExternalEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information for externally provisioned clusters.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client

    # noinspection PyBroadException
    def on_benchmark_start(self):
        try:
            nodes_stats = self.client.nodes.stats(metric="_all")["nodes"].values()
        except BaseException:
            self.logger.exception("Could not retrieve nodes stats")
            nodes_stats = []
        try:
            nodes_info = self.client.nodes.info(node_id="_all")["nodes"].values()
        except BaseException:
            self.logger.exception("Could not retrieve nodes info")
            nodes_info = []

        for node in nodes_stats:
            node_name = node["name"]
            host = node.get("host", "unknown")
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "node_name", node_name)
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "host_name", host)

        for node in nodes_info:
            node_name = node["name"]
            self.store_node_info(node_name, "os_name", node, ["os", "name"])
            self.store_node_info(node_name, "os_version", node, ["os", "version"])
            self.store_node_info(node_name, "cpu_logical_cores", node, ["os", "available_processors"])
            self.store_node_info(node_name, "jvm_vendor", node, ["jvm", "vm_vendor"])
            self.store_node_info(node_name, "jvm_version", node, ["jvm", "version"])

        store_plugin_metadata(self.metrics_store, nodes_info)
        store_node_attribute_metadata(self.metrics_store, nodes_info)

    def store_node_info(self, node_name, metric_key, node, path):
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, metric_key, extract_value(node, path))


class JvmStatsSummary(InternalTelemetryDevice):
    """
    Gathers a summary of various JVM statistics during the whole race.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client
        self.jvm_stats_per_node = {}

    def on_benchmark_start(self):
        self.logger.info("JvmStatsSummary on benchmark start")
        self.jvm_stats_per_node = self.jvm_stats()

    def on_benchmark_stop(self):
        jvm_stats_at_end = self.jvm_stats()
        total_old_gen_collection_time = 0
        total_old_gen_collection_count = 0
        total_young_gen_collection_time = 0
        total_young_gen_collection_count = 0

        for node_name, jvm_stats_end in jvm_stats_at_end.items():
            if node_name in self.jvm_stats_per_node:
                jvm_stats_start = self.jvm_stats_per_node[node_name]
                young_gc_time = max(jvm_stats_end["young_gc_time"] - jvm_stats_start["young_gc_time"], 0)
                young_gc_count = max(jvm_stats_end["young_gc_count"] - jvm_stats_start["young_gc_count"], 0)
                old_gc_time = max(jvm_stats_end["old_gc_time"] - jvm_stats_start["old_gc_time"], 0)
                old_gc_count = max(jvm_stats_end["old_gc_count"] - jvm_stats_start["old_gc_count"], 0)

                total_young_gen_collection_time += young_gc_time
                total_young_gen_collection_count += young_gc_count
                total_old_gen_collection_time += old_gc_time
                total_old_gen_collection_count += old_gc_count

                self.metrics_store.put_value_node_level(node_name, "node_young_gen_gc_time", young_gc_time, "ms")
                self.metrics_store.put_count_node_level(node_name, "node_young_gen_gc_count", young_gc_count)
                self.metrics_store.put_value_node_level(node_name, "node_old_gen_gc_time", old_gc_time, "ms")
                self.metrics_store.put_count_node_level(node_name, "node_old_gen_gc_count", old_gc_count)

                all_pool_stats = {
                    "name": "jvm_memory_pool_stats"
                }
                for pool_name, pool_stats in jvm_stats_end["pools"].items():
                    all_pool_stats[pool_name] = {
                        "peak_usage": pool_stats["peak"],
                        "unit": "byte"
                    }
                self.metrics_store.put_doc(all_pool_stats, level=MetaInfoScope.node, node_name=node_name)

            else:
                self.logger.warning("Cannot determine JVM stats for [%s] (not in the cluster at the start of the benchmark).", node_name)

        self.metrics_store.put_value_cluster_level("node_total_young_gen_gc_time", total_young_gen_collection_time, "ms")
        self.metrics_store.put_count_cluster_level("node_total_young_gen_gc_count", total_young_gen_collection_count)
        self.metrics_store.put_value_cluster_level("node_total_old_gen_gc_time", total_old_gen_collection_time, "ms")
        self.metrics_store.put_count_cluster_level("node_total_old_gen_gc_count", total_old_gen_collection_count)

        self.jvm_stats_per_node = None

    def jvm_stats(self):
        self.logger.debug("Gathering JVM stats")
        jvm_stats = {}
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            self.logger.exception("Could not retrieve GC times.")
            return jvm_stats
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            gc = node["jvm"]["gc"]["collectors"]
            old_gen_collection_time = gc["old"]["collection_time_in_millis"]
            old_gen_collection_count = gc["old"]["collection_count"]
            young_gen_collection_time = gc["young"]["collection_time_in_millis"]
            young_gen_collection_count = gc["young"]["collection_count"]
            jvm_stats[node_name] = {
                "young_gc_time": young_gen_collection_time,
                "young_gc_count": young_gen_collection_count,
                "old_gc_time": old_gen_collection_time,
                "old_gc_count": old_gen_collection_count,
                "pools": {}
            }
            pool_usage = node["jvm"]["mem"]["pools"]
            for pool_name, pool_stats in pool_usage.items():
                jvm_stats[node_name]["pools"][pool_name] = {
                    "peak": pool_stats["peak_used_in_bytes"]
                }
        return jvm_stats


class IndexStats(InternalTelemetryDevice):
    """
    Gathers statistics via the Elasticsearch index stats API
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.client = client
        self.metrics_store = metrics_store
        self.first_time = True

    def on_benchmark_start(self):
        # we only determine this value at the start of the benchmark. This is actually only useful for
        # the pipeline "benchmark-only" where we don't have control over the cluster and the user might not have restarted
        # the cluster so we can at least tell them.
        if self.first_time:
            for t in self.index_times(self.index_stats(), per_shard_stats=False):
                n = t["name"]
                v = t["value"]
                if t["value"] > 0:
                    console.warn("%s is %d ms indicating that the cluster is not in a defined clean state. Recorded index time "
                                 "metrics may be misleading." % (n, v), logger=self.logger)
            self.first_time = False

    def on_benchmark_stop(self):
        self.logger.info("Gathering indices stats for all primaries on benchmark stop.")
        index_stats = self.index_stats()
        # import json
        # self.logger.debug("Returned indices stats:\n%s", json.dumps(index_stats, indent=2))
        if "_all" not in index_stats or "primaries" not in index_stats["_all"]:
            return
        p = index_stats["_all"]["primaries"]
        # actually this is add_count
        self.add_metrics(self.extract_value(p, ["segments", "count"]), "segments_count")
        self.add_metrics(self.extract_value(p, ["segments", "memory_in_bytes"]), "segments_memory_in_bytes", "byte")

        for t in self.index_times(index_stats):
            self.metrics_store.put_doc(doc=t, level=metrics.MetaInfoScope.cluster)

        for ct in self.index_counts(index_stats):
            self.metrics_store.put_doc(doc=ct, level=metrics.MetaInfoScope.cluster)

        self.add_metrics(self.extract_value(p, ["segments", "doc_values_memory_in_bytes"]), "segments_doc_values_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "stored_fields_memory_in_bytes"]), "segments_stored_fields_memory_in_bytes",
                                            "byte")
        self.add_metrics(self.extract_value(p, ["segments", "terms_memory_in_bytes"]), "segments_terms_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "norms_memory_in_bytes"]), "segments_norms_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "points_memory_in_bytes"]), "segments_points_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(index_stats, ["_all", "total", "store", "size_in_bytes"]), "store_size_in_bytes", "byte")
        self.add_metrics(self.extract_value(index_stats, ["_all", "total", "translog", "size_in_bytes"]), "translog_size_in_bytes", "byte")

    def index_stats(self):
        # noinspection PyBroadException
        try:
            return self.client.indices.stats(metric="_all", level="shards")
        except BaseException:
            self.logger.exception("Could not retrieve index stats.")
            return {}

    def index_times(self, stats, per_shard_stats=True):
        times = []
        self.index_time(times, stats, "merges_total_time", ["merges", "total_time_in_millis"], per_shard_stats),
        self.index_time(times, stats, "merges_total_throttled_time", ["merges", "total_throttled_time_in_millis"], per_shard_stats),
        self.index_time(times, stats, "indexing_total_time", ["indexing", "index_time_in_millis"], per_shard_stats),
        self.index_time(times, stats, "indexing_throttle_time", ["indexing", "throttle_time_in_millis"], per_shard_stats),
        self.index_time(times, stats, "refresh_total_time", ["refresh", "total_time_in_millis"], per_shard_stats),
        self.index_time(times, stats, "flush_total_time", ["flush", "total_time_in_millis"], per_shard_stats),
        return times

    def index_time(self, values, stats, name, path, per_shard_stats):
        primary_total_stats = self.extract_value(stats, ["_all", "primaries"], default_value={})
        value = self.extract_value(primary_total_stats, path)
        if value is not None:
            doc = {
                "name": name,
                "value": value,
                "unit": "ms",
            }
            if per_shard_stats:
                doc["per-shard"] = self.primary_shard_stats(stats, path)
            values.append(doc)

    def index_counts(self, stats):
        counts = []
        self.index_count(counts, stats, "merges_total_count", ["merges", "total"])
        self.index_count(counts, stats, "refresh_total_count", ["refresh", "total"])
        self.index_count(counts, stats, "flush_total_count", ["flush", "total"])
        return counts

    def index_count(self, values, stats, name, path):
        primary_total_stats = self.extract_value(stats, ["_all", "primaries"], default_value={})
        value = self.extract_value(primary_total_stats, path)
        if value is not None:
            doc = {
                "name": name,
                "value": value
            }
            values.append(doc)

    def primary_shard_stats(self, stats, path):
        shard_stats = []
        try:
            for idx, shards in stats["indices"].items():
                for shard_number, shard in shards["shards"].items():
                    for shard_metrics in shard:
                        if shard_metrics["routing"]["primary"]:
                            shard_stats.append(self.extract_value(shard_metrics, path, default_value=0))
        except KeyError:
            self.logger.warning("Could not determine primary shard stats at path [%s].", ",".join(path))
        return shard_stats

    def add_metrics(self, value, metric_key, unit=None):
        if value is not None:
            if unit:
                self.metrics_store.put_value_cluster_level(metric_key, value, unit)
            else:
                self.metrics_store.put_count_cluster_level(metric_key, value)

    def extract_value(self, primaries, path, default_value=None):
        value = primaries
        try:
            for k in path:
                value = value[k]
            return value
        except KeyError:
            self.logger.warning("Could not determine value at path [%s]. Returning default value [%s]", ",".join(path), str(default_value))
            return default_value


class MlBucketProcessingTime(InternalTelemetryDevice):
    def __init__(self, client, metrics_store):
        super().__init__()
        self.client = client
        self.metrics_store = metrics_store

    def on_benchmark_stop(self):
        import elasticsearch
        try:
            results = self.client.search(index=".ml-anomalies-*", body={
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"result_type": "bucket"}}
                        ]
                    }
                },
                "aggs": {
                    "jobs": {
                        "terms": {
                            "field": "job_id"
                        },
                        "aggs": {
                            "min_pt": {
                                "min": {"field": "processing_time_ms"}
                            },
                            "max_pt": {
                                "max": {"field": "processing_time_ms"}
                            },
                            "mean_pt": {
                                "avg": {"field": "processing_time_ms"}
                            },
                            "median_pt": {
                                "percentiles": {"field": "processing_time_ms", "percents": [50]}
                            }
                        }
                    }
                }
            })
        except elasticsearch.TransportError:
            self.logger.exception("Could not retrieve ML bucket processing time.")
            return
        try:
            for job in results["aggregations"]["jobs"]["buckets"]:
                ml_job_stats = collections.OrderedDict()
                ml_job_stats["name"] = "ml_processing_time"
                ml_job_stats["job"] = job["key"]
                ml_job_stats["min"] = job["min_pt"]["value"]
                ml_job_stats["mean"] = job["mean_pt"]["value"]
                ml_job_stats["median"] = job["median_pt"]["values"]["50.0"]
                ml_job_stats["max"] = job["max_pt"]["value"]
                ml_job_stats["unit"] = "ms"
                self.metrics_store.put_doc(doc=dict(ml_job_stats), level=MetaInfoScope.cluster)
        except KeyError:
            # no ML running
            pass


class IndexSize(InternalTelemetryDevice):
    """
    Measures the final size of the index
    """
    def __init__(self, data_paths):
        super().__init__()
        self.data_paths = data_paths
        self.attached = False
        self.index_size_bytes = None

    def attach_to_node(self, node):
        self.attached = True

    def detach_from_node(self, node, running):
        # we need to gather the file size after the node has terminated so we can be sure that it has written all its buffers.
        if not running and self.attached and self.data_paths:
            self.attached = False
            index_size_bytes = 0
            for data_path in self.data_paths:
                index_size_bytes += io.get_size(data_path)
            self.index_size_bytes = index_size_bytes

    def store_system_metrics(self, node, metrics_store):
        if self.index_size_bytes:
            metrics_store.put_count_node_level(node.node_name, "final_index_size_bytes", self.index_size_bytes, "byte")
