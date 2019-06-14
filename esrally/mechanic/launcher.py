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
import datetime
import logging
import os
import signal
import subprocess
import shlex
import psutil

from esrally import config, time, exceptions, client
from esrally.mechanic import telemetry, cluster, java_resolver
from esrally.utils import process, jvm, io
from time import monotonic as _time


def wait_for_rest_layer(es, max_attempts=20):
    for attempt in range(max_attempts):
        import elasticsearch
        try:
            es.info()
            return True
        except elasticsearch.TransportError as e:
            if e.status_code == 503 or isinstance(e, elasticsearch.ConnectionError):
                time.sleep(1)
            elif e.status_code == 401:
                time.sleep(1)
            else:
                raise e
    return False


class ClusterLauncher:
    """
    The cluster launcher performs cluster-wide tasks that need to be done in the startup / shutdown phase.

    """

    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        """

        Creates a new ClusterLauncher.

        :param cfg: The config object.
        :param metrics_store: A metrics store that is configured to receive system metrics.
        :param client_factory_class: A factory class that can create an Elasticsearch client.
        """
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class
        self.logger = logging.getLogger(__name__)

    def start(self):
        """
        Performs final startup tasks.

        Precondition: All cluster nodes have been started.
        Postcondition: The cluster is ready to receive HTTP requests or a ``LaunchError`` is raised.

        :return: A representation of the launched cluster.
        """
        enabled_devices = self.cfg.opts("mechanic", "telemetry.devices")
        telemetry_params = self.cfg.opts("mechanic", "telemetry.params")
        all_hosts = self.cfg.opts("client", "hosts").all_hosts
        default_hosts = self.cfg.opts("client", "hosts").default
        preserve = self.cfg.opts("mechanic", "preserve.install")

        es = {}
        for cluster_name, cluster_hosts in all_hosts.items():
            all_client_options = self.cfg.opts("client", "options").all_client_options
            cluster_client_options = dict(all_client_options[cluster_name])
            # Use retries to avoid aborts on long living connections for telemetry devices
            cluster_client_options["retry-on-timeout"] = True
            es[cluster_name] = self.client_factory(cluster_hosts, cluster_client_options).create()

        es_default = es["default"]

        t = telemetry.Telemetry(enabled_devices, devices=[
            telemetry.NodeStats(telemetry_params, es, self.metrics_store),
            telemetry.ClusterMetaDataInfo(es_default),
            telemetry.ClusterEnvironmentInfo(es_default, self.metrics_store),
            telemetry.JvmStatsSummary(es_default, self.metrics_store),
            telemetry.IndexStats(es_default, self.metrics_store),
            telemetry.MlBucketProcessingTime(es_default, self.metrics_store),
            telemetry.CcrStats(telemetry_params, es, self.metrics_store),
            telemetry.RecoveryStats(telemetry_params, es, self.metrics_store)
        ])

        # The list of nodes will be populated by ClusterMetaDataInfo, so no need to do it here
        c = cluster.Cluster(default_hosts, [], t, preserve)

        self.logger.info("All cluster nodes have successfully started. Checking if REST API is available.")
        if wait_for_rest_layer(es_default, max_attempts=40):
            self.logger.info("REST API is available. Attaching telemetry devices to cluster.")
            t.attach_to_cluster(c)
            self.logger.info("Telemetry devices are now attached to the cluster.")
        else:
            # Just stop the cluster here and raise. The caller is responsible for terminating individual nodes.
            self.logger.error("REST API layer is not yet available. Forcefully terminating cluster.")
            self.stop(c)
            raise exceptions.LaunchError(
                "Elasticsearch REST API layer is not available. Forcefully terminated cluster.")
        return c

    def stop(self, c):
        """
        Performs cleanup tasks. This method should be called before nodes are shut down.

        :param c: The cluster that is about to be stopped.
        """
        c.telemetry.detach_from_cluster(c)


class DockerLauncher:
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, cfg, metrics_store):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.binary_paths = {}
        self.node_name = None
        self.keep_running = self.cfg.opts("mechanic", "keep.running")
        self.logger = logging.getLogger(__name__)

    def start(self, node_configurations):
        nodes = []
        for node_configuration in node_configurations:
            node_name = node_configuration.node_name
            host_name = node_configuration.ip
            binary_path = node_configuration.binary_path
            self.binary_paths[node_name] = binary_path

            p = self._start_process(cmd="docker-compose -f %s up" % binary_path)
            # only support a subset of telemetry for Docker hosts
            # (specifically, we do not allow users to enable any devices)
            node_telemetry = [
                telemetry.DiskIo(self.metrics_store, len(node_configurations)),
                telemetry.NodeEnvironmentInfo(self.metrics_store)
            ]
            t = telemetry.Telemetry(devices=node_telemetry)
            nodes.append(cluster.Node(p, host_name, node_name, t))
        return nodes

    def _start_process(self, cmd):
        subprocess.Popen(shlex.split(cmd), close_fds=True)
        return 0

    def stop(self, nodes):
        if self.keep_running:
            self.logger.info("Keeping Docker container running.")
        else:
            self.logger.info("Stopping Docker container")
            for node in nodes:
                node.telemetry.detach_from_node(node, running=True)
                process.run_subprocess_with_logging("docker-compose -f %s down" % self.binary_paths[node.node_name])
                node.telemetry.detach_from_node(node, running=False)


class ExternalLauncher:
    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class
        self.logger = logging.getLogger(__name__)

    def start(self, node_configurations=None):
        hosts = self.cfg.opts("client", "hosts").default
        client_options = self.cfg.opts("client", "options").default
        es = self.client_factory(hosts, client_options).create()

        # cannot enable custom telemetry devices here
        t = telemetry.Telemetry(devices=[
            # This is needed to actually populate the nodes
            telemetry.ClusterMetaDataInfo(es),
            # will gather node specific meta-data for all nodes
            telemetry.ExternalEnvironmentInfo(es, self.metrics_store),
        ])
        # We create a pseudo-cluster here to get information about all nodes.
        # cluster nodes will be populated by the external environment info telemetry device. We cannot know this
        # upfront.
        c = cluster.Cluster(hosts, [], t)
        user_defined_version = self.cfg.opts("mechanic", "distribution.version", mandatory=False)
        distribution_version = es.info()["version"]["number"]
        if not user_defined_version or user_defined_version.strip() == "":
            self.logger.info("Distribution version was not specified by user. Rally-determined version is [%s]",
                             distribution_version)
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", distribution_version)
        elif user_defined_version != distribution_version:
            self.logger.warning("Distribution version '%s' on command line differs from actual cluster version '%s'.",
                                user_defined_version, distribution_version)
        t.attach_to_cluster(c)
        return c.nodes

    def stop(self, nodes):
        # nothing to do here, externally provisioned clusters / nodes don't have any specific telemetry devices
        # attached.
        pass


def wait_for_pidfile(pidfilename, timeout=60):
    endtime = _time() + timeout
    while _time() < endtime:
        try:
            with open(pidfilename, "rb") as f:
                return int(f.read())
        except FileNotFoundError:
            time.sleep(0.5)

    raise TimeoutError("pid file not available after {} seconds!".format(timeout))


class ProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.
    """
    PROCESS_WAIT_TIMEOUT_SECONDS = 90.0

    def __init__(self, cfg, metrics_store, races_root_dir, clock=time.Clock):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self._clock = clock
        self.races_root_dir = races_root_dir
        self.keep_running = self.cfg.opts("mechanic", "keep.running")
        self.logger = logging.getLogger(__name__)

    def start(self, node_configurations):
        # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store
        # running on this machine
        # The only specific trait of a Rally-related process is that is started "somewhere" in the races root directory.
        #
        # We also do this only once per host otherwise we would kill instances that we've just launched.
        process.kill_running_es_instances(self.races_root_dir)
        node_count_on_host = len(node_configurations)
        return [self._start_node(node_configuration, node_count_on_host) for node_configuration in node_configurations]

    def _start_node(self, node_configuration, node_count_on_host):
        host_name = node_configuration.ip
        node_name = node_configuration.node_name
        car = node_configuration.car
        binary_path = node_configuration.binary_path
        data_paths = node_configuration.data_paths
        node_telemetry_dir = "%s/telemetry" % node_configuration.node_root_path
        java_major_version, java_home = java_resolver.java_home(car, self.cfg)

        self.logger.info("Starting node [%s] based on car [%s].", node_name, car)

        enabled_devices = self.cfg.opts("mechanic", "telemetry.devices")
        telemetry_params = self.cfg.opts("mechanic", "telemetry.params")
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.JitCompiler(node_telemetry_dir),
            telemetry.Gc(node_telemetry_dir, java_major_version),
            telemetry.DiskIo(self.metrics_store, node_count_on_host),
            telemetry.NodeEnvironmentInfo(self.metrics_store),
            telemetry.IndexSize(data_paths, self.metrics_store),
            telemetry.MergeParts(self.metrics_store, node_configuration.log_path),
            telemetry.StartupTime(self.metrics_store),
        ]

        t = telemetry.Telemetry(enabled_devices, devices=node_telemetry)
        env = self._prepare_env(car, node_name, java_home, t)
        t.on_pre_node_start(node_name)
        node_pid = self._start_process(binary_path, env)
        node = cluster.Node(node_pid, host_name, node_name, t)
        self.logger.info("Attaching telemetry devices to node [%s].", node_name)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, car, node_name, java_home, t):
        env = {}
        env.update(os.environ)
        env.update(car.env)
        self._set_env(env, "PATH", os.path.join(java_home, "bin"), separator=os.pathsep)
        # Don't merge here!
        env["JAVA_HOME"] = java_home

        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(car, node_name).items():
            self._set_env(env, k, v)

        exit_on_oome_flag = "-XX:+ExitOnOutOfMemoryError"
        if jvm.supports_option(java_home, exit_on_oome_flag):
            self.logger.info("Setting [%s] to detect out of memory errors during the benchmark.", exit_on_oome_flag)
            self._set_env(env, "ES_JAVA_OPTS", exit_on_oome_flag)
        else:
            self.logger.info("JVM does not support [%s]. A JDK upgrade is recommended.", exit_on_oome_flag)

        self.logger.debug("env for [%s]: %s", node_name, str(env))
        return env

    def _set_env(self, env, k, v, separator=' '):
        if v is not None:
            if k not in env:
                env[k] = v
            else:  # merge
                env[k] = v + separator + env[k]

    @staticmethod
    def _start_process(binary_path, env):
        if os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch Elasticsearch as root. Please run Rally as a non-root user.")
        os.chdir(binary_path)
        cmd = ["bin/elasticsearch"]
        cmd.extend(["-d", "-p", "pid"])
        proc = subprocess.Popen(cmd,
                                env=env,
                                close_fds=True)
        ret = proc.wait()
        if ret != 0:
            msg = "Daemon startup failed with exit code[{}]".format(ret)
            logging.error(msg)
            raise exceptions.LaunchError(msg)

        return wait_for_pidfile("./pid")

    def stop(self, nodes):
        if self.keep_running:
            self.logger.info("Keeping [%d] nodes on this host running.", len(nodes))
        else:
            self.logger.info("Shutting down [%d] nodes on this host.", len(nodes))
        for node in nodes:
            proc = psutil.Process(pid=node.pid)
            node_name = node.node_name
            node.telemetry.detach_from_node(node, running=True)
            if not self.keep_running:
                stop_watch = self._clock.stop_watch()
                stop_watch.start()
                try:
                    os.kill(proc.pid, signal.SIGTERM)
                    proc.wait(10.0)
                except ProcessLookupError:
                    self.logger.warning("No process found with PID [%s] for node [%s]", proc.pid, node_name)
                except psutil.TimeoutExpired:
                    self.logger.info("kill -KILL node [%s]", node_name)
                    try:
                        # kill -9
                        proc.kill()
                    except ProcessLookupError:
                        self.logger.warning("No process found with PID [%s] for node [%s]", proc.pid, node_name)
                node.telemetry.detach_from_node(node, running=False)
                self.logger.info("Done shutdown node [%s] in [%.1f] s.", node_name, stop_watch.split_time())
