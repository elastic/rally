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
import re
import signal
import subprocess
import tabulate
import threading

from esrally import metrics, time, exceptions
from esrally.utils import io, sysstats, console, versions, opts
from esrally.metrics import MetaInfoScope


def list_telemetry():
    console.println("Available telemetry devices:\n")
    devices = [[device.command, device.human_name, device.help] for device in [JitCompiler, Gc, FlightRecorder, PerfStat, NodeStats, RecoveryStats]]
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

    def instrument_candidate_env(self, car, candidate_id):
        opts = {}
        for device in self.devices:
            if self._enabled(device):
                additional_opts = device.instrument_env(car, candidate_id)
                # properly merge values with the same key
                for k, v in additional_opts.items():
                    if k in opts:
                        opts[k] = "%s %s" % (opts[k], v)
                    else:
                        opts[k] = v
        return opts

    def attach_to_cluster(self, cluster):
        for device in self.devices:
            if self._enabled(device):
                device.attach_to_cluster(cluster)

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

    def detach_from_cluster(self, cluster):
        for device in self.devices:
            if self._enabled(device):
                device.detach_from_cluster(cluster)

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

    def instrument_env(self, car, candidate_id):
        return {}

    def attach_to_cluster(self, cluster):
        pass

    def on_pre_node_start(self, node_name):
        pass

    def attach_to_node(self, node):
        pass

    def detach_from_node(self, node, running):
        pass

    def detach_from_cluster(self, cluster):
        pass

    def on_benchmark_start(self):
        pass

    def on_benchmark_stop(self):
        pass


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

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.jfr" % (self.log_root, car.safe_name, candidate_id)

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
        return {"ES_JAVA_OPTS": java_opts}

    def java_opts(self, log_file):
        recording_template = self.telemetry_params.get("recording-template")
        java_opts = "-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "

        if self.java_major_version < 11:
            java_opts += "-XX:+UnlockCommercialFeatures "

        if self.java_major_version < 9:
            java_opts += "-XX:+FlightRecorder "
            java_opts += "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath={} ".format(log_file)
            java_opts += "-XX:StartFlightRecording=defaultrecording=true"
            if recording_template:
                self.logger.info("jfr: Using recording template [%s].", recording_template)
                java_opts += ",settings={}".format(recording_template)
            else:
                self.logger.info("jfr: Using default recording template.")
        else:
            java_opts += "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename={}".format(log_file)
            if recording_template:
                self.logger.info("jfr: Using recording template [%s].", recording_template)
                java_opts += ",settings={}".format(recording_template)
            else:
                self.logger.info("jfr: Using default recording template.")
        return java_opts


class JitCompiler(TelemetryDevice):
    internal = False
    command = "jit"
    human_name = "JIT Compiler Profiler"
    help = "Enables JIT compiler logs."

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.jit.log" % (self.log_root, car.safe_name, candidate_id)
        console.info("%s: Writing JIT compiler log to [%s]" % (self.human_name, log_file), logger=self.logger)
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation "
                                "-XX:LogFile=%s -XX:+PrintAssembly" % log_file}


class Gc(TelemetryDevice):
    internal = False
    command = "gc"
    human_name = "GC log"
    help = "Enables GC logs."

    def __init__(self, log_root, java_major_version):
        super().__init__()
        self.log_root = log_root
        self.java_major_version = java_major_version

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.gc.log" % (self.log_root, car.safe_name, candidate_id)
        console.info("%s: Writing GC log to [%s]" % (self.human_name, log_file), logger=self.logger)
        return self.java_opts(log_file)

    def java_opts(self, log_file):
        if self.java_major_version < 9:
            return {"ES_JAVA_OPTS": "-Xloggc:%s -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
                                    "-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime "
                                    "-XX:+PrintTenuringDistribution" % log_file}
        else:
            # see https://docs.oracle.com/javase/9/tools/java.htm#JSWOR-GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5
            return {"ES_JAVA_OPTS": "-Xlog:gc*=info,safepoint=info,age*=trace:file=%s:utctime,uptimemillis,level,tags:filecount=0" % log_file}


class PerfStat(TelemetryDevice):
    internal = False
    command = "perf"
    human_name = "perf stat"
    help = "Reads CPU PMU counters (requires Linux and perf)"

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root
        self.process = None
        self.node = None
        self.log = None
        self.attached = False

    def attach_to_node(self, node):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s.perf.log" % (self.log_root, node.node_name)

        console.info("%s: Writing perf logs to [%s]" % (self.human_name, log_file), logger=self.logger)

        self.log = open(log_file, "wb")

        self.process = subprocess.Popen(["perf", "stat", "-p %s" % node.process.pid],
                                        stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        self.node = node
        self.attached = True

    def detach_from_node(self, node, running):
        if self.attached and running:
            self.logger.info("Dumping PMU counters for node [%s]", node.node_name)
            os.kill(self.process.pid, signal.SIGINT)
            try:
                self.process.wait(10.0)
            except subprocess.TimeoutExpired:
                self.logger.warning("perf stat did not terminate")
            self.log.close()
            self.attached = False


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

    def attach_to_cluster(self, cluster):
        # This cluster parameter does not correspond to the cluster names passed in target.hosts, see on_benchmark_start()
        super().attach_to_cluster(cluster)

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
                        "The 'indices' key in {0} does not contain an 'index' or 'shards' key "
                        "Maybe the output format of the {0} endpoint has changed. Skipping.".format(ccr_stats_api_endpoint)
                    )

    def record_stats_per_index(self, name, stats):
        """
        :param name: The index name.
        :param stats: A dict with returned CCR stats for the index.
        """

        for shard_stats in stats:
            if "shard_id" in shard_stats:
                shard_metadata = {
                    "cluster": self.cluster_name,
                    "index": name,
                    "shard": shard_stats["shard_id"],
                    "name": "ccr-stats"
                }

                self.metrics_store.put_doc(shard_stats, level=MetaInfoScope.cluster, meta_data=shard_metadata)


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
    warning = """You have enabled the node-stats telemetry device, but requests to the _nodes/stats Elasticsearch endpoint
          trigger additional refreshes and WILL SKEW results.
    """

    def __init__(self, telemetry_params, clients, metrics_store):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.clients = clients
        self.specified_cluster_names = self.clients.keys()
        self.metrics_store = metrics_store
        self.samplers = []

    def attach_to_cluster(self, cluster):
        # This cluster parameter does not correspond to the cluster names passed in target.hosts, see on_benchmark_start()
        super().attach_to_cluster(cluster)

    def on_benchmark_start(self):
        console.warn(NodeStats.warning, logger=self.logger)

        recorder = []
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

    def sample(self):
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            logging.getLogger(__name__).exception("Could not retrieve node stats.")
            return {}
        return stats["nodes"].values()


class StartupTime(InternalTelemetryDevice):
    def __init__(self, metrics_store, stopwatch=time.StopWatch):
        super().__init__()
        self.metrics_store = metrics_store
        self.timer = stopwatch()

    def on_pre_node_start(self, node_name):
        self.timer.start()

    def attach_to_node(self, node):
        self.timer.stop()
        self.metrics_store.put_value_node_level(node.node_name, "node_startup_time", self.timer.total_time(), "s")


class MergeParts(InternalTelemetryDevice):
    """
    Gathers merge parts time statistics. Note that you need to run a track setup which logs these data.
    """
    MERGE_TIME_LINE = re.compile(r": (\d+) msec to merge ([a-z ]+) \[(\d+) docs\]")

    def __init__(self, metrics_store, node_log_dir):
        super().__init__()
        self.node_log_dir = node_log_dir
        self.metrics_store = metrics_store
        self._t = None
        self.node = None

    def attach_to_node(self, node):
        self.node = node

    def on_benchmark_stop(self):
        self.logger.info("Analyzing merge times.")
        # first decompress all logs. They have unique names so it's safe to do that. It's easier to first decompress everything
        for log_file in os.listdir(self.node_log_dir):
            log_path = "%s/%s" % (self.node_log_dir, log_file)
            if io.is_archive(log_path):
                self.logger.info("Decompressing [%s] to analyze merge times...", log_path)
                io.decompress(log_path, self.node_log_dir)

        # we need to add up times from all files
        merge_times = {}
        for log_file in os.listdir(self.node_log_dir):
            log_path = "%s/%s" % (self.node_log_dir, log_file)
            if io.is_archive(log_file):
                self.logger.debug("Skipping archived logs in [%s].", log_path)
            elif io.has_extension(log_file, ".json"):
                self.logger.debug("Skipping JSON-formatted logs in [%s].", log_path)
            else:
                self.logger.debug("Analyzing merge times in [%s]", log_path)
                with open(log_path, mode="rt", encoding="utf-8") as f:
                    self._extract_merge_times(f, merge_times)
        if merge_times:
            self._store_merge_times(merge_times)
        self.logger.info("Finished analyzing merge times. Extracted [%s] different merge time components.", len(merge_times))

    def _extract_merge_times(self, file, merge_times):
        for line in file.readlines():
            match = MergeParts.MERGE_TIME_LINE.search(line)
            if match is not None:
                duration_ms, part, num_docs = match.groups()
                if part not in merge_times:
                    merge_times[part] = [0, 0]
                l = merge_times[part]
                l[0] += int(duration_ms)
                l[1] += int(num_docs)

    def _store_merge_times(self, merge_times):
        for k, v in merge_times.items():
            metric_suffix = k.replace(" ", "_")
            self.metrics_store.put_value_node_level(self.node.node_name, "merge_parts_total_time_%s" % metric_suffix, v[0], "ms")
            self.metrics_store.put_count_node_level(self.node.node_name, "merge_parts_total_docs_%s" % metric_suffix, v[1])


class DiskIo(InternalTelemetryDevice):
    """
    Gathers disk I/O stats.
    """
    def __init__(self, metrics_store, node_count_on_host):
        super().__init__()
        self.metrics_store = metrics_store
        self.node_count_on_host = node_count_on_host
        self.node = None
        self.process = None
        self.disk_start = None
        self.process_start = None

    def attach_to_node(self, node):
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)

    def on_benchmark_start(self):
        if self.process is not None:
            self.process_start = sysstats.process_io_counters(self.process)
            if self.process_start:
                self.logger.info("Using more accurate process-based I/O counters.")
            else:
                try:
                    self.disk_start = sysstats.disk_io_counters()
                    self.logger.warning("Process I/O counters are not supported on this platform. Falling back to less accurate disk "
                                        "I/O counters.")
                except RuntimeError:
                    self.logger.exception("Could not determine I/O stats at benchmark start.")

    def on_benchmark_stop(self):
        if self.process is not None:
            # Be aware the semantics of write counts etc. are different for disk and process statistics.
            # Thus we're conservative and only report I/O bytes now.
            # noinspection PyBroadException
            try:
                # we have process-based disk counters, no need to worry how many nodes are on this host
                if self.process_start:
                    process_end = sysstats.process_io_counters(self.process)
                    read_bytes = process_end.read_bytes - self.process_start.read_bytes
                    write_bytes = process_end.write_bytes - self.process_start.write_bytes
                elif self.disk_start:
                    if self.node_count_on_host > 1:
                        self.logger.info("There are [%d] nodes on this host and Rally fell back to disk I/O counters. Attributing [1/%d] "
                                         "of total I/O to [%s].", self.node_count_on_host, self.node_count_on_host, self.node.node_name)

                    disk_end = sysstats.disk_io_counters()
                    read_bytes = (disk_end.read_bytes - self.disk_start.read_bytes) // self.node_count_on_host
                    write_bytes = (disk_end.write_bytes - self.disk_start.write_bytes) // self.node_count_on_host
                else:
                    raise RuntimeError("Neither process nor disk I/O counters are available")

                self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_write_bytes", write_bytes, "byte")
                self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_read_bytes", read_bytes, "byte")
            # Catching RuntimeException is not sufficient as psutil might raise AccessDenied et.al. which is derived from Exception
            except BaseException:
                self.logger.exception("Could not determine I/O stats at benchmark end.")


class CpuUsage(InternalTelemetryDevice):
    """
    Gathers CPU usage statistics.
    """
    def __init__(self, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.sampler = None
        self.node = None

    def attach_to_node(self, node):
        self.node = node

    def on_benchmark_start(self):
        if self.node:
            recorder = CpuUsageRecorder(self.node, self.metrics_store)
            self.sampler = SamplerThread(recorder)
            self.sampler.setDaemon(True)
            self.sampler.start()

    def on_benchmark_stop(self):
        if self.sampler:
            self.sampler.finish()


class CpuUsageRecorder:
    def __init__(self, node, metrics_store):
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)
        self.metrics_store = metrics_store
        # the call is blocking already; there is no need for additional waiting in the sampler thread.
        self.sample_interval = 0

    def record(self):
        import psutil
        try:
            self.metrics_store.put_value_node_level(node_name=self.node.node_name, name="cpu_utilization_1s",
                                                    value=sysstats.cpu_utilization(self.process), unit="%")
        # this can happen when the Elasticsearch process has been terminated already and we were not quick enough to stop.
        except psutil.NoSuchProcess:
            pass

    def __str__(self):
        return "cpu utilization"


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

    def attach_to_cluster(self, cluster):
        client_info = self.client.info()
        revision = client_info["version"]["build_hash"]
        distribution_version = client_info["version"]["number"]
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "distribution_version", distribution_version)

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


class NodeEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information like OS or CPU details for Rally-provisioned nodes.
    """
    def __init__(self, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store

    def attach_to_node(self, node):
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_name", sysstats.os_name())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_version", sysstats.os_version())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_logical_cores", sysstats.logical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_physical_cores", sysstats.physical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_model", sysstats.cpu_model())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "node_name", node.node_name)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "host_name", node.host_name)


class ExternalEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information for externally provisioned clusters.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client
        self._t = None

    def attach_to_cluster(self, cluster):
        stats = self.client.nodes.stats(metric="_all")
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            host = node.get("host", "unknown")
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "node_name", node_name)
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "host_name", host)

        info = self.client.nodes.info(node_id="_all")
        nodes_info = info["nodes"].values()
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


class ClusterMetaDataInfo(InternalTelemetryDevice):
    """
    Enriches the cluster with meta-data about it and its nodes.
    """
    def __init__(self, client):
        super().__init__()
        self.client = client

    def attach_to_cluster(self, cluster):
        client_info = self.client.info()
        revision = client_info["version"]["build_hash"]
        distribution_version = client_info["version"]["number"]

        cluster.distribution_version = distribution_version
        cluster.source_revision = revision

        for node_stats in self.client.nodes.stats(metric="_all")["nodes"].values():
            node_name = node_stats["name"]
            if cluster.has_node(node_name):
                cluster_node = cluster.node(node_name)
            else:
                host = node_stats.get("host", "unknown")
                cluster_node = cluster.add_node(host, node_name)
            self.add_node_stats(cluster, cluster_node, node_stats)

        for node_info in self.client.nodes.info(node_id="_all")["nodes"].values():
            self.add_node_info(cluster, node_info)

    def add_node_info(self, cluster, node_info):
        node_name = node_info["name"]
        cluster_node = cluster.node(node_name)
        if cluster_node:
            cluster_node.ip = extract_value(node_info, ["ip"])
            cluster_node.os = {
                "name": extract_value(node_info, ["os", "name"]),
                "version": extract_value(node_info, ["os", "version"])
            }
            cluster_node.jvm = {
                "vendor": extract_value(node_info, ["jvm", "vm_vendor"]),
                "version": extract_value(node_info, ["jvm", "version"])
            }
            cluster_node.cpu = {
                "available_processors": extract_value(node_info, ["os", "available_processors"]),
                "allocated_processors": extract_value(node_info, ["os", "allocated_processors"], fallback=None),
            }
            for plugin in extract_value(node_info, ["plugins"], fallback=[]):
                if "name" in plugin:
                    cluster_node.plugins.append(plugin["name"])

            if versions.major_version(cluster.distribution_version) == 1:
                cluster_node.memory = {
                    "total_bytes": extract_value(node_info, ["os", "mem", "total_in_bytes"], fallback=None)
                }

    def add_node_stats(self, cluster, cluster_node, stats):
        if cluster_node:
            data_dirs = extract_value(stats, ["fs", "data"], fallback=[])
            for data_dir in data_dirs:
                fs_meta_data = {
                    "mount": data_dir.get("mount", "unknown"),
                    "type": data_dir.get("type", "unknown"),
                    "spins": data_dir.get("spins", "unknown")
                }
                cluster_node.fs.append(fs_meta_data)
            if versions.major_version(cluster.distribution_version) > 1:
                cluster_node.memory = {
                    "total_bytes": extract_value(stats, ["os", "mem", "total_in_bytes"], fallback=None)
                }


class GcTimesSummary(InternalTelemetryDevice):
    """
    Gathers a summary of the total young gen/old gen GC runtime during the whole race.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client
        self.gc_times_per_node = {}

    def on_benchmark_start(self):
        self.gc_times_per_node = self.gc_times()

    def on_benchmark_stop(self):
        gc_times_at_end = self.gc_times()
        total_old_gen_collection_time = 0
        total_young_gen_collection_time = 0

        for node_name, gc_times_end in gc_times_at_end.items():
            if node_name in self.gc_times_per_node:
                gc_times_start = self.gc_times_per_node[node_name]
                young_gc_time = max(gc_times_end[0] - gc_times_start[0], 0)
                old_gc_time = max(gc_times_end[1] - gc_times_start[1], 0)

                total_young_gen_collection_time += young_gc_time
                total_old_gen_collection_time += old_gc_time

                self.metrics_store.put_value_node_level(node_name, "node_young_gen_gc_time", young_gc_time, "ms")
                self.metrics_store.put_value_node_level(node_name, "node_old_gen_gc_time", old_gc_time, "ms")
            else:
                self.logger.warning("Cannot determine GC times for [%s] (not in the cluster at the start of the benchmark).", node_name)

        self.metrics_store.put_value_cluster_level("node_total_young_gen_gc_time", total_young_gen_collection_time, "ms")
        self.metrics_store.put_value_cluster_level("node_total_old_gen_gc_time", total_old_gen_collection_time, "ms")

        self.gc_times_per_node = None

    def gc_times(self):
        self.logger.debug("Gathering GC times")
        gc_times = {}
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            self.logger.exception("Could not retrieve GC times.")
            return gc_times
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            gc = node["jvm"]["gc"]["collectors"]
            old_gen_collection_time = gc["old"]["collection_time_in_millis"]
            young_gen_collection_time = gc["young"]["collection_time_in_millis"]
            gc_times[node_name] = (young_gen_collection_time, old_gen_collection_time)
        return gc_times


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
        # we only determine this value at the start of the benchmark (in the first lap). This is actually only useful for
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
        self.add_metrics(self.extract_value(p, ["segments", "stored_fields_memory_in_bytes"]), "segments_stored_fields_memory_in_bytes", "byte")
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

    def detach_from_cluster(self, cluster):
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
    def __init__(self, data_paths, metrics_store):
        super().__init__()
        self.data_paths = data_paths
        self.metrics_store = metrics_store
        self.attached = False

    def attach_to_node(self, node):
        self.attached = True

    def detach_from_node(self, node, running):
        # we need to gather the file size after the node has terminated so we can be sure that it has written all its buffers.
        if not running and self.attached and self.data_paths:
            self.attached = False
            index_size_bytes = 0
            for data_path in self.data_paths:
                index_size_bytes += io.get_size(data_path)
            self.metrics_store.put_count_node_level(node.node_name, "final_index_size_bytes", index_size_bytes, "byte")
