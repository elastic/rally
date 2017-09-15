import logging
import os
import signal
import subprocess
import threading
import shlex

from esrally import config, time, exceptions, client
from esrally.mechanic import telemetry, cluster
from esrally.utils import console, process, jvm

logger = logging.getLogger("rally.launcher")


def wait_for_rest_layer(es, max_attempts=10):
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


class ClusterLauncher:
    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class

    def start(self):
        hosts = self.cfg.opts("client", "hosts")
        client_options = self.cfg.opts("client", "options")
        es = self.client_factory(hosts, client_options).create()

        t = telemetry.Telemetry(devices=[
            telemetry.ClusterMetaDataInfo(es),
            telemetry.ClusterEnvironmentInfo(es, self.metrics_store),
            telemetry.NodeStats(es, self.metrics_store),
            telemetry.IndexStats(es, self.metrics_store)
        ])

        # The list of nodes will be populated by ClusterMetaDataInfo, so no need to do it here
        c = cluster.Cluster(hosts, [], t)
        logger.info("All cluster nodes have successfully started. Checking if REST API is available.")
        if wait_for_rest_layer(es, max_attempts=20):
            logger.info("REST API is available. Attaching telemetry devices to cluster.")
            t.attach_to_cluster(c)
            logger.info("Telemetry devices are now attached to the cluster.")
        else:
            # Just stop the cluster here and raise. The caller is responsible for terminating individual nodes.
            logger.error("REST API layer is not yet available. Forcefully terminating cluster.")
            self.stop(c)
            raise exceptions.LaunchError("Elasticsearch REST API layer is not available. Forcefully terminated cluster.")

        return c

    def stop(self, c):
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

    def start(self, node_configurations):
        nodes = []
        for node_configuration in node_configurations:

            node_name = node_configuration.node_name
            host_name = node_configuration.ip
            binary_path = node_configuration.binary_path
            self.binary_paths[node_name] = binary_path

            p = self._start_process(cmd="docker-compose -f %s up" % binary_path, node_name=node_name, log_dir=node_configuration.log_path)
            # only support a subset of telemetry for Docker hosts (specifically, we do not allow users to enable any devices)
            node_telemetry = [
                telemetry.DiskIo(self.metrics_store, len(node_configurations)),
                telemetry.CpuUsage(self.metrics_store),
                telemetry.NodeEnvironmentInfo(self.metrics_store)
            ]
            t = telemetry.Telemetry(devices=node_telemetry)
            nodes.append(cluster.Node(p, host_name, node_name, t))
        return nodes

    def _start_process(self, cmd, node_name, log_dir):
        startup_event = threading.Event()
        p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        t = threading.Thread(target=self._read_output, args=(node_name, p, startup_event))
        t.setDaemon(True)
        t.start()
        if startup_event.wait(timeout=DockerLauncher.PROCESS_WAIT_TIMEOUT_SECONDS):
            logger.info("Started node=%s with pid=%s" % (node_name, p.pid))
            return p
        else:
            msg = "Could not start node '%s' within timeout period of %s seconds." % (
                node_name, InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)
            logger.error(msg)
            raise exceptions.LaunchError("%s Please check the logs in '%s' for more details." % (msg, log_dir))

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

    def stop(self, nodes):
        if self.keep_running:
            logger.info("Keeping Docker container running.")
        else:
            logger.info("Stopping Docker container")
            for node in nodes:
                node.telemetry.detach_from_node(node, running=True)
                process.run_subprocess_with_logging("docker-compose -f %s down" % self.binary_paths[node.node_name])
                node.telemetry.detach_from_node(node, running=False)


class ExternalLauncher:
    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class

    def start(self, node_configurations=None):
        hosts = self.cfg.opts("client", "hosts")
        client_options = self.cfg.opts("client", "options")
        es = self.client_factory(hosts, client_options).create()

        # cannot enable custom telemetry devices here
        t = telemetry.Telemetry(devices=[
            # This is needed to actually populate the nodes
            telemetry.ClusterMetaDataInfo(es),
            # will gather node specific meta-data for all nodes
            telemetry.ExternalEnvironmentInfo(es, self.metrics_store),
        ])
        # We create a pseudo-cluster here to get information about all nodes.
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
        return c.nodes

    def stop(self, nodes):
        # nothing to do here, externally provisioned clusters / nodes don't have any specific telemetry devices attached.
        pass


class InProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.
    """
    PROCESS_WAIT_TIMEOUT_SECONDS = 90.0

    def __init__(self, cfg, metrics_store, races_root_dir, clock=time.Clock):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self._clock = clock
        self.races_root_dir = races_root_dir
        self.java_home = self.cfg.opts("runtime", "java.home")
        self.keep_running = self.cfg.opts("mechanic", "keep.running")

    def start(self, node_configurations):
        # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
        # The only specific trait of a Rally-related process is that is started "somewhere" in the races root directory.
        #
        # We also do this only once per host otherwise we would kill instances that we've just launched.
        process.kill_running_es_instances(self.races_root_dir)
        java_major_version = jvm.major_version(self.java_home)
        logger.info("Detected Java major version [%s]." % java_major_version)

        node_count_on_host = len(node_configurations)
        return [self._start_node(node_configuration, node_count_on_host, java_major_version) for node_configuration in node_configurations]

    def _start_node(self, node_configuration, node_count_on_host, java_major_version):
        host_name = node_configuration.ip
        node_name = node_configuration.node_name
        car = node_configuration.car
        binary_path = node_configuration.binary_path
        data_paths = node_configuration.data_paths
        node_telemetry_dir = "%s/telemetry" % node_configuration.node_root_path

        logger.info("Starting node [%s] based on car [%s]." % (node_name, car))

        enabled_devices = self.cfg.opts("mechanic", "telemetry.devices")
        node_telemetry = [
            telemetry.FlightRecorder(node_telemetry_dir),
            telemetry.JitCompiler(node_telemetry_dir),
            telemetry.Gc(node_telemetry_dir, java_major_version),
            telemetry.PerfStat(node_telemetry_dir),
            telemetry.DiskIo(self.metrics_store, node_count_on_host),
            telemetry.CpuUsage(self.metrics_store),
            telemetry.NodeEnvironmentInfo(self.metrics_store),
            telemetry.IndexSize(data_paths, self.metrics_store),
            telemetry.MergeParts(self.metrics_store, node_configuration.log_path),
        ]

        t = telemetry.Telemetry(enabled_devices, devices=node_telemetry)

        env = self._prepare_env(car, node_name, t)
        node_process = self._start_process(env, node_name, binary_path)
        node = cluster.Node(node_process, host_name, node_name, t)
        logger.info("Node [%s] has successfully started. Attaching telemetry devices." % node_name)
        t.attach_to_node(node)
        logger.info("Telemetry devices are now attached to node [%s]." % node_name)

        return node

    def _prepare_env(self, car, node_name, t):
        env = {}
        env.update(os.environ)
        env.update(car.env)
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
            self._set_env(env, "ES_JAVA_OPTS", exit_on_oome_flag)
        else:
            logger.info("JVM does not support [%s]. Cannot detect out of memory errors. Please consider a JDK upgrade." % exit_on_oome_flag)

        logger.info("env for [%s]: %s" % (node_name, str(env)))
        return env

    def _set_env(self, env, k, v, separator=' '):
        if v is not None:
            if k not in env:
                env[k] = v
            else:  # merge
                env[k] = v + separator + env[k]

    def _start_process(self, env, node_name, binary_path):
        if os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch Elasticsearch as root. Please run Rally as a non-root user.")
        os.chdir(binary_path)
        startup_event = threading.Event()
        cmd = ["bin/elasticsearch"]
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
            # check if the process has terminated already
            process.poll()
            if process.returncode:
                msg += " The process has already terminated with exit code [%s]." % str(process.returncode)
            else:
                msg += " The process seems to be still running with PID [%s]." % process.pid
            logger.error(msg)
            raise exceptions.LaunchError(msg)

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
                logger.error("[%s] encountered initialization errors." % node_name)
                startup_event.set()
            if l.endswith("started") and not startup_event.isSet():
                startup_event.set()
                logger.info("[%s] has successfully started." % node_name)

    def stop(self, nodes):
        if self.keep_running:
            logger.info("Keeping [%d] nodes on this host running." % len(nodes))
        else:
            logger.info("Shutting down [%d] nodes on this host." % len(nodes))
        for node in nodes:
            process = node.process
            node_name = node.node_name
            node.telemetry.detach_from_node(node, running=True)
            if not self.keep_running:
                stop_watch = self._clock.stop_watch()
                stop_watch.start()
                os.kill(process.pid, signal.SIGINT)
                try:
                    process.wait(10.0)
                    logger.info("Done shutdown node [%s] in [%.1f] s." % (node_name, stop_watch.split_time()))
                except subprocess.TimeoutExpired:
                    # kill -9
                    logger.warning("Node [%s] did not shut down itself after 10 seconds; now kill -QUIT node, to see threads:" % node_name)
                    try:
                        os.kill(process.pid, signal.SIGQUIT)
                    except OSError:
                        logger.warning("No process found with PID [%s] for node [%s]" % (process.pid, node_name))
                        break
                    try:
                        process.wait(120.0)
                        logger.info("Done shutdown node [%s] in [%.1f] s." % (node_name, stop_watch.split_time()))
                        break
                    except subprocess.TimeoutExpired:
                        pass
                    logger.info("kill -KILL node [%s]" % node_name)
                    try:
                        process.kill()
                    except ProcessLookupError:
                        logger.warning("No process found with PID [%s] for node [%s]" % (process.pid, node_name))
                node.telemetry.detach_from_node(node, running=False)
