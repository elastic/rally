import os
import threading
import subprocess
import socket
import signal
import logging

from esrally.mechanic import gear
from esrally import config, cluster, telemetry, time, exceptions

logger = logging.getLogger("rally.launcher")


class ClusterFactory:
    def create(self, hosts, nodes, metrics_store, telemetry):
        return cluster.Cluster(hosts, nodes, metrics_store, telemetry)


class ExternalLauncher:
    def __init__(self, cfg, cluster_factory_class=ClusterFactory):
        self.cfg = cfg
        self.cluster_factory = cluster_factory_class()

    def start(self, track, setup, metrics_store):
        configured_host_list = self.cfg.opts("launcher", "external.target.hosts")
        hosts = []
        try:
            for authority in configured_host_list:
                host, port = authority.split(":")
                hosts.append({"host": host, "port": port})
        except ValueError:
            msg = "Could not initialize external cluster. Invalid format for %s. Expected a comma-separated list of host:port pairs, " \
                  "e.g. host1:9200,host2:9200." % configured_host_list
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)

        t = telemetry.Telemetry(self.cfg, metrics_store, devices=[
            telemetry.ExternalEnvironmentInfo(self.cfg, metrics_store),
            telemetry.NodeStats(self.cfg, metrics_store),
            telemetry.IndexStats(self.cfg, metrics_store)
        ])
        c = self.cluster_factory.create(hosts, [], metrics_store, t)
        t.attach_to_cluster(c)
        return c


class InProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.

    Currently, only local launching is supported.
    """
    PROCESS_WAIT_TIMEOUT_SECONDS = 20.0

    def __init__(self, cfg, clock=time.Clock, cluster_factory_class=ClusterFactory):
        self._config = cfg
        self._clock = clock
        self._servers = []
        self._cluster_factory = cluster_factory_class()

    def start(self, track, setup, metrics_store):
        if self._servers:
            logger.warn("There are still referenced servers on startup. Did the previous shutdown succeed?")
        num_nodes = setup.candidate.nodes
        first_http_port = self._config.opts("provisioning", "node.http.port")

        t = telemetry.Telemetry(self._config, metrics_store)
        c = self._cluster_factory.create(
            [{"host": "localhost", "port": first_http_port}],
            [self._start_node(node, setup, metrics_store) for node in range(num_nodes)],
            metrics_store, t
        )
        t.attach_to_cluster(c)

        return c

    def _start_node(self, node, setup, metrics_store):
        node_name = self._node_name(node)
        host_name = socket.gethostname()
        t = telemetry.Telemetry(self._config, metrics_store)

        env = self._prepare_env(setup, node_name, t)
        cmd = self.prepare_cmd(setup, node_name)
        process = self._start_process(cmd, env, node_name)
        node = cluster.Node(process, host_name, node_name, t)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, setup, node_name, t):
        env = {}
        env.update(os.environ)
        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(setup, node_name).items():
            self._set_env(env, k, v)

        java_opts = ""
        if setup.candidate.heap:
            java_opts += "-Xms%s -Xmx%s " % (setup.candidate.heap, setup.candidate.heap)
        if setup.candidate.java_opts:
            java_opts += setup.candidate.java_opts
        if len(java_opts) > 0:
            self._set_env(env, "ES_JAVA_OPTS", java_opts)
        self._set_env(env, "ES_GC_OPTS", setup.candidate.gc_opts)

        java_home = gear.Gear(self._config).capability(gear.Capability.java)
        # Unix specific!:
        self._set_env(env, "PATH", "%s/bin" % java_home, separator=":")
        # Don't merge here!
        env["JAVA_HOME"] = java_home
        logger.info("ENV: %s" % str(env))
        return env

    def _set_env(self, env, k, v, separator=' '):
        if v is not None:
            if k not in env:
                env[k] = v
            else:  # merge
                env[k] = v + separator + env[k]

    def prepare_cmd(self, setup, node_name):
        server_log_dir = "%s/server" % self._config.opts("system", "track.setup.log.dir")
        self._config.add(config.Scope.invocation, "launcher", "candidate.log.dir", server_log_dir)
        processor_count = setup.candidate.processors

        cmd = ["bin/elasticsearch", "-Ees.node.name=%s" % node_name]
        if processor_count is not None and processor_count > 1:
            cmd.append("-Ees.processors=%s" % processor_count)
        cmd.append("-Ees.path.logs=%s" % server_log_dir)
        logger.info("ES launch: %s" % str(cmd))
        return cmd

    def _start_process(self, cmd, env, node_name):
        install_dir = self._config.opts("provisioning", "local.binary.path")
        os.chdir(install_dir)
        startup_event = threading.Event()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env)
        t = threading.Thread(target=self._read_output, args=(node_name, process, startup_event))
        t.setDaemon(True)
        t.start()
        if startup_event.wait(timeout=InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS):
            logger.info("Started node=%s with pid=%s" % (node_name, process.pid))
            return process
        else:
            log_dir = self._config.opts("system", "log.dir")
            msg = "Could not start node '%s' within timeout period of %s seconds." % (
            node_name, InProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)
            logger.error(msg)
            raise exceptions.LaunchError("%s Please check the logs in '%s' for more details." % (msg, log_dir))

    def _node_name(self, node):
        prefix = self._config.opts("provisioning", "node.name.prefix")
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
                startup_event.set()

            logger.info("%s: %s" % (node_name, l.replace("\n", "\n%s (stdout): " % node_name)))
            if l.endswith("started") and not startup_event.isSet():
                startup_event.set()
                logger.info("%s: started" % node_name)

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
                logger.warn("Server %s did not shut down itself after 10 seconds; now kill -QUIT node, to see threads:" % node.node_name)
                try:
                    os.kill(process.pid, signal.SIGQUIT)
                except OSError:
                    logger.warn("  no such process")
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
                    logger.warn("No such process")
        self._servers = []
