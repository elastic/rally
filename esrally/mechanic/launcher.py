import logging
import os
import signal
import socket
import subprocess
import threading
import shlex

from esrally import config, time, exceptions, client
from esrally.mechanic import telemetry, cluster
from esrally.utils import versions, console, process, jvm

logger = logging.getLogger("rally.launcher")


def wait_for_rest_layer(es):
    max_attempts = 10
    for attempt in range(max_attempts):
        import elasticsearch
        try:
            es.info()
            return True
        except elasticsearch.TransportError as e:
            if e.status_code == 503 or isinstance(e, elasticsearch.ConnectionError):
                logger.debug("Elasticsearch REST API is not available yet (probably cluster block).")
                time.sleep(2)
            elif e.status_code == 401:
                logger.debug("Could not authenticate yet (probably x-pack initializing).")
                time.sleep(2)
            else:
                raise e
    return False


class DockerLauncher:
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class
        self.binary_path = None

    def start(self, car, binary, data_paths):
        self.binary_path = binary

        hosts = self.cfg.opts("client", "hosts")
        client_options = self.cfg.opts("client", "options")
        es = self.client_factory(hosts, client_options).create()

        # Cannot enable custom telemetry devices here
        t = telemetry.Telemetry(devices=[
            # Be aware that some the meta-data are taken from the host system, not the container (e.g. number of CPU cores) so if the
            # Docker container constrains these, the metrics are actually wrong.
            telemetry.ClusterMetaDataInfo(es),
            telemetry.EnvironmentInfo(es, self.metrics_store),
            telemetry.NodeStats(es, self.metrics_store),
            telemetry.IndexStats(es, self.metrics_store)
        ])

        c = cluster.Cluster(hosts, [self._start_node(hosts[0], 0, es)], t)

        logger.info("Docker container has successfully started. Checking if REST API is available.")
        if wait_for_rest_layer(es):
            logger.info("REST API is available. Attaching telemetry devices to cluster.")
            t.attach_to_cluster(c)
            logger.info("Telemetry devices are now attached to the cluster.")
        else:
            logger.error("REST API layer is not yet available. Forcefully terminating cluster.")
            self.stop(c)
            raise exceptions.LaunchError("Elasticsearch REST API layer is not available. Forcefully terminated cluster.")
        return c

    def _start_node(self, host, node, es):
        node_name = self._node_name(node)
        p = self._start_process(cmd="docker-compose -f %s up" % self.binary_path, node_name=node_name)
        # only support a subset of telemetry for Docker hosts (specifically, we do not allow users to enable any devices)
        node_telemetry = [
            telemetry.DiskIo(self.metrics_store),
            telemetry.CpuUsage(self.metrics_store),
            telemetry.EnvironmentInfo(es, self.metrics_store)
        ]
        t = telemetry.Telemetry(devices=node_telemetry)
        return cluster.Node(p, host["host"], node_name, t)

    def _start_process(self, cmd, node_name):
        startup_event = threading.Event()
        p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        t = threading.Thread(target=self._read_output, args=(node_name, p, startup_event))
        t.setDaemon(True)
        t.start()
        if startup_event.wait(timeout=DockerLauncher.PROCESS_WAIT_TIMEOUT_SECONDS):
            logger.info("Started node=%s with pid=%s" % (node_name, p.pid))
            return p
        else:
            log_dir = self.cfg.opts("system", "log.dir")
            msg = "Could not start node '%s' within timeout period of %s seconds." % (
                node_name, InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)
            logger.error(msg)
            raise exceptions.LaunchError("%s Please check the logs in '%s' for more details." % (msg, log_dir))

    def _node_name(self, node):
        prefix = self.cfg.opts("provisioning", "node.name.prefix")
        return "%s%d" % (prefix, node)

    def _read_output(self, node_name, server, startup_event):
        """
        Reads the output from the ES (node) subprocess.
        """
        while True:
            l = server.stdout.readline().decode("utf-8")
            if len(l) == 0:
                break
            l = l.rstrip()

            if l.find("Initialization Failed") != -1:
                logger.warning("[%s] has started with initialization errors." % node_name)
                startup_event.set()

            logger.info("%s: %s" % (node_name, l.replace("\n", "\n%s (stdout): " % node_name)))
            if l.endswith("] started") and not startup_event.isSet():
                startup_event.set()
                logger.info("[%s] has successfully started." % node_name)

    def stop(self, cluster):
        logger.info("Stopping Docker container")
        process.run_subprocess_with_logging("docker-compose -f %s down" % self.binary_path)
        cluster.telemetry.detach_from_cluster(cluster)


class ExternalLauncher:
    # benchmarks with external candidates are really scary and we should warn users.
    BOGUS_RESULTS_WARNING = """
************************************************************************
************** WARNING: A dark dungeon lies ahead of you  **************
************************************************************************

Rally does not have control over the configuration of the benchmarked
Elasticsearch cluster.

Be aware that results may be misleading due to problems with the setup.
Rally is also not able to gather lots of metrics at all (like CPU usage
of the benchmarked cluster) or may even produce misleading metrics (like
the index size).

************************************************************************
****** Use this pipeline only if you are aware of the tradeoffs.  ******
*************************** Watch your step! ***************************
************************************************************************
"""

    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class

    def start(self, car=None, binary=None, data_paths=None):
        console.println(ExternalLauncher.BOGUS_RESULTS_WARNING)
        hosts = self.cfg.opts("client", "hosts")
        client_options = self.cfg.opts("client", "options")
        es = self.client_factory(hosts, client_options).create()

        # cannot enable custom telemetry devices here
        t = telemetry.Telemetry(devices=[
            telemetry.ClusterMetaDataInfo(es),
            telemetry.ExternalEnvironmentInfo(es, self.metrics_store),
            telemetry.NodeStats(es, self.metrics_store),
            telemetry.IndexStats(es, self.metrics_store)
        ])
        # cluster nodes will be populated by the external environment info telemetry device. We cannot know this upfront.
        c = cluster.Cluster(hosts, [], t)
        user_defined_version = self.cfg.opts("mechanic", "distribution.version", mandatory=False)
        distribution_version = es.info()["version"]["number"]
        if not user_defined_version or user_defined_version.strip() == "":
            logger.info("Distribution version was not specified by user. Rally-determined version is [%s]" % distribution_version)
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", distribution_version)
        elif user_defined_version != distribution_version:
            console.warn(
                "Specified distribution version '%s' on the command line differs from version '%s' reported by the cluster." %
                (user_defined_version, distribution_version), logger=logger)
        t.attach_to_cluster(c)
        return c

    def stop(self, cluster):
        cluster.telemetry.detach_from_cluster(cluster)


class InProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.

    Currently, only local launching is supported.
    """
    PROCESS_WAIT_TIMEOUT_SECONDS = 60.0

    # TODO 68: We should externalize this (see #68)
    ES_CMD_LINE_OPTS_PER_VERSION = {
        "1": {
            "processors": "-Des.processors",
            "log_path": "-Des.path.logs",
            "node_name": "-Des.node.name"
        },
        "2": {
            "processors": "-Des.processors",
            "log_path": "-Des.path.logs",
            "node_name": "-Des.node.name"
        },
        "5": {
            "processors": "-Eprocessors",
            "log_path": "-Epath.logs",
            "node_name": "-Enode.name"
        },
        "master": {
            "processors": "-Eprocessors",
            "log_path": "-Epath.logs",
            "node_name": "-Enode.name"
        }
    }

    def __init__(self, cfg, metrics_store, challenge_root_dir, log_root_dir, clock=time.Clock):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self._clock = clock
        self._servers = []
        self.node_telemetry_dir = "%s/telemetry" % challenge_root_dir
        self.node_log_dir = "%s/server" % log_root_dir
        self.java_home = self.cfg.opts("runtime", "java8.home")

    def start(self, car, binary, data_paths):
        hosts = self.cfg.opts("client", "hosts")
        client_options = self.cfg.opts("client", "options")
        es = client.EsClientFactory(hosts, client_options).create()

        # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
        node_prefix = self.cfg.opts("provisioning", "node.name.prefix")
        process.kill_running_es_instances(node_prefix)

        logger.info("Starting a cluster based on car [%s] with [%d] nodes." % (car, car.nodes))

        # TODO dm: Get rid of this config setting and replace it with a proper list
        enabled_devices = self.cfg.opts("mechanic", "telemetry.devices")

        cluster_telemetry = [
            telemetry.ClusterMetaDataInfo(es),
            # TODO dm: Once we do distributed launching, this needs to be done per node not per cluster
            telemetry.MergeParts(self.metrics_store, self.node_log_dir),
            telemetry.EnvironmentInfo(es, self.metrics_store),
            telemetry.NodeStats(es, self.metrics_store),
            telemetry.IndexStats(es, self.metrics_store),
            # TODO dm: Once we do distributed launching, this needs to be done per node not per cluster
            telemetry.IndexSize(data_paths, self.metrics_store)
        ]
        t = telemetry.Telemetry(enabled_devices, devices=cluster_telemetry)
        c = cluster.Cluster(hosts, [self._start_node(node, car, es, binary) for node in range(car.nodes)], t)
        logger.info("All cluster nodes have successfully started. Checking if REST API is available.")
        if wait_for_rest_layer(es):
            logger.info("REST API is available. Attaching telemetry devices to cluster.")
            t.attach_to_cluster(c)
            logger.info("Telemetry devices are now attached to the cluster.")
        else:
            logger.error("REST API layer is not yet available. Forcefully terminating cluster.")
            self.stop(c)
            raise exceptions.LaunchError("Elasticsearch REST API layer is not available. Forcefully terminated cluster.")
        return c

    def _start_node(self, node, car, es, binary_path):
        node_name = self._node_name(node)
        host_name = socket.gethostname()

        enabled_devices = self.cfg.opts("mechanic", "telemetry.devices")
        major_version = jvm.major_version(self.java_home)
        logger.info("Detected Java major version [%s] on node [%s]." % (major_version, node_name))
        node_telemetry = [
            telemetry.FlightRecorder(self.node_telemetry_dir),
            telemetry.JitCompiler(self.node_telemetry_dir),
            telemetry.Gc(self.node_telemetry_dir, major_version),
            telemetry.PerfStat(self.node_telemetry_dir),
            telemetry.DiskIo(self.metrics_store),
            telemetry.CpuUsage(self.metrics_store),
            telemetry.EnvironmentInfo(es, self.metrics_store),
        ]

        t = telemetry.Telemetry(enabled_devices, devices=node_telemetry)

        env = self._prepare_env(car, node_name, t)
        cmd = self.prepare_cmd(car, node_name)
        process = self._start_process(cmd, env, node_name, binary_path)
        node = cluster.Node(process, host_name, node_name, t)
        logger.info("Cluster node [%s] has successfully started. Attaching telemetry devices to node." % node_name)
        t.attach_to_node(node)
        logger.info("Telemetry devices are now attached to node [%s]." % node_name)

        return node

    def _prepare_env(self, car, node_name, t):
        env = {}
        env.update(os.environ)
        # Unix specific!:
        self._set_env(env, "PATH", "%s/bin" % self.java_home, separator=":")
        # Don't merge here!
        env["JAVA_HOME"] = self.java_home

        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(car, node_name).items():
            self._set_env(env, k, v)

        exit_on_oome_flag = "-XX:+ExitOnOutOfMemoryError"
        if jvm.supports_option(self.java_home, exit_on_oome_flag):
            logger.info("JVM supports [%s]. Setting this option to detect out of memory errors during the benchmark." % exit_on_oome_flag)
            java_opts = "%s " % exit_on_oome_flag
        else:
            logger.info("JVM does not support [%s]. Cannot detect out of memory errors. Please consider a JDK upgrade." % exit_on_oome_flag)
            java_opts = ""

        if car.heap:
            java_opts += "-Xms%s -Xmx%s " % (car.heap, car.heap)
        if car.java_opts:
            java_opts += car.java_opts
        if len(java_opts) > 0:
            self._set_env(env, "ES_JAVA_OPTS", java_opts)
        self._set_env(env, "ES_GC_OPTS", car.gc_opts)

        logger.info("ENV: %s" % str(env))
        return env

    def _set_env(self, env, k, v, separator=' '):
        if v is not None:
            if k not in env:
                env[k] = v
            else:  # merge
                env[k] = v + separator + env[k]

    def prepare_cmd(self, car, node_name):
        distribution_version = self.cfg.opts("mechanic", "distribution.version", mandatory=False)

        cmd = ["bin/elasticsearch",
               "%s=%s" % (self.cmd_line_opt(distribution_version, "node_name"), node_name),
               "%s=%s" % (self.cmd_line_opt(distribution_version, "log_path"), self.node_log_dir)
               ]
        processor_count = car.processors
        if processor_count is not None and processor_count > 1:
            cmd.append("%s=%s" % (self.cmd_line_opt(distribution_version, "processors"), processor_count))
        logger.info("ES launch: %s" % str(cmd))
        return cmd

    def cmd_line_opt(self, distribution_version, key):
        best_version = versions.best_match(InProcessLauncher.ES_CMD_LINE_OPTS_PER_VERSION.keys(), distribution_version)
        if best_version:
            return InProcessLauncher.ES_CMD_LINE_OPTS_PER_VERSION[best_version][key]
        else:
            raise exceptions.LaunchError("Cannot start cluster. Unsupported distribution version %s. "
                                         "Please raise a bug at %s." %
                                         (distribution_version, console.format.link("https://github.com/elastic/rally")))

    def _start_process(self, cmd, env, node_name, binary_path):
        if os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch Elasticsearch as root. Please run Rally as a non-root user.")
        os.chdir(binary_path)
        startup_event = threading.Event()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env)
        t = threading.Thread(target=self._read_output, args=(node_name, process, startup_event))
        t.setDaemon(True)
        t.start()
        if startup_event.wait(timeout=InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS):
            process.poll()
            # has the process terminated?
            if process.returncode:
                msg = "Node [%s] has terminated with exit code [%s]." % (node_name, str(process.returncode))
                logger.error(msg)
                raise exceptions.LaunchError(msg)
            else:
                logger.info("Started node [%s] with PID [%s]" % (node_name, process.pid))
                return process
        else:
            msg = "Could not start node [%s] within timeout period of [%s] seconds." % (
                node_name, InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)
            logger.error(msg)
            raise exceptions.LaunchError(msg)

    def _node_name(self, node):
        prefix = self.cfg.opts("provisioning", "node.name.prefix")
        return "%s%d" % (prefix, node)

    def _read_output(self, node_name, server, startup_event):
        """
        Reads the output from the ES (node) subprocess.
        """
        while True:
            l = server.stdout.readline().decode("utf-8")
            if len(l) == 0:
                # no more output -> the process has terminated. We can give up now
                startup_event.set()
                break
            l = l.rstrip()
            logger.info("%s: %s" % (node_name, l.replace("\n", "\n%s (stdout): " % node_name)))

            if l.find("Initialization Failed") != -1 or l.find("A fatal exception has occurred") != -1:
                logger.warning("[%s] encountered initialization errors." % node_name)
                startup_event.set()
            if l.endswith("started") and not startup_event.isSet():
                startup_event.set()
                logger.info("[%s] has successfully started." % node_name)

    def stop(self, cluster):
        logger.info("Shutting down ES cluster")

        # Ask all nodes to shutdown:
        stop_watch = self._clock.stop_watch()
        stop_watch.start()
        for node in cluster.nodes:
            process = node.process
            node.telemetry.detach_from_node(node)

            os.kill(process.pid, signal.SIGINT)

            try:
                process.wait(10.0)
                logger.info("Done shutdown node (%.1f sec)" % stop_watch.split_time())
            except subprocess.TimeoutExpired:
                # kill -9
                logger.warning("Server %s did not shut down itself after 10 seconds; now kill -QUIT node, to see threads:" % node.node_name)
                try:
                    os.kill(process.pid, signal.SIGQUIT)
                except OSError:
                    logger.warning("  no such process")
                    return

                try:
                    process.wait(120.0)
                    logger.info("Done shutdown node (%.1f sec)" % stop_watch.split_time())
                    return
                except subprocess.TimeoutExpired:
                    pass

                logger.info("kill -KILL node")
                try:
                    process.kill()
                except ProcessLookupError:
                    logger.warning("No such process")
        cluster.telemetry.detach_from_cluster(cluster)
        self._servers = []
