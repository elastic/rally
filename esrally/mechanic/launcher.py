import logging
import os
import signal
import socket
import subprocess
import threading

from esrally import config, time, exceptions, client
from esrally.mechanic import gear, telemetry, cluster
from esrally.utils import versions, console, process

logger = logging.getLogger("rally.launcher")


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

    def start(self, car=None):
        console.println(ExternalLauncher.BOGUS_RESULTS_WARNING)

        hosts = self.cfg.opts("launcher", "external.target.hosts")
        client_options = self.cfg.opts("launcher", "client.options")
        # unified client config
        self.cfg.add(config.Scope.benchmark, "client", "hosts", hosts)
        self.cfg.add(config.Scope.benchmark, "client", "options", client_options)

        es = self.client_factory(hosts, client_options).create()

        t = telemetry.Telemetry(self.cfg, client=es, metrics_store=self.metrics_store, devices=[
            telemetry.ExternalEnvironmentInfo(self.cfg, es, self.metrics_store),
            telemetry.NodeStats(self.cfg, es, self.metrics_store),
            telemetry.IndexStats(self.cfg, es, self.metrics_store)
        ])
        c = cluster.Cluster([], t)
        user_defined_version = self.cfg.opts("source", "distribution.version", mandatory=False)
        distribution_version = es.info()["version"]["number"]
        if not user_defined_version or user_defined_version.strip() == "":
            logger.info("Distribution version was not specified by user. Rally-determined version is [%s]" % distribution_version)
            self.cfg.add(config.Scope.benchmark, "source", "distribution.version", distribution_version)
        elif user_defined_version != distribution_version:
            console.println(
                "Warning: Specified distribution version '%s' on the command line differs from version '%s' reported by the cluster." %
                (user_defined_version, distribution_version), logger=logger.warn)
        t.attach_to_cluster(c)
        return c

    def stop(self, cluster):
        pass


class InProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.

    Currently, only local launching is supported.
    """
    PROCESS_WAIT_TIMEOUT_SECONDS = 20.0

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

    def __init__(self, cfg, metrics_store, clock=time.Clock):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self._clock = clock
        self._servers = []

    def start(self, car):
        port = self.cfg.opts("provisioning", "node.http.port")
        hosts = [{"host": "localhost", "port": port}]
        client_options = self.cfg.opts("launcher", "client.options")
        # unified client config
        self.cfg.add(config.Scope.benchmark, "client", "hosts", hosts)
        self.cfg.add(config.Scope.benchmark, "client", "options", client_options)

        es = client.EsClientFactory(hosts, client_options).create()

        # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
        node_prefix = self.cfg.opts("provisioning", "node.name.prefix")
        process.kill_running_es_instances(node_prefix)

        logger.info("Starting a cluster based on car [%s] with [%d] nodes." % (car, car.nodes))

        t = telemetry.Telemetry(self.cfg, es, self.metrics_store)
        c = cluster.Cluster([self._start_node(node, car, es, self.metrics_store) for node in range(car.nodes)], t)
        t.attach_to_cluster(c)
        return c

    def _start_node(self, node, car, client, metrics_store):
        node_name = self._node_name(node)
        host_name = socket.gethostname()
        t = telemetry.Telemetry(self.cfg, client, metrics_store)

        env = self._prepare_env(car, node_name, t)
        cmd = self.prepare_cmd(car, node_name)
        process = self._start_process(cmd, env, node_name)
        node = cluster.Node(process, host_name, node_name, t)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, car, node_name, t):
        env = {}
        env.update(os.environ)
        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(car, node_name).items():
            self._set_env(env, k, v)

        java_opts = ""
        if car.heap:
            java_opts += "-Xms%s -Xmx%s " % (car.heap, car.heap)
        if car.java_opts:
            java_opts += car.java_opts
        if len(java_opts) > 0:
            self._set_env(env, "ES_JAVA_OPTS", java_opts)
        self._set_env(env, "ES_GC_OPTS", car.gc_opts)

        java_home = gear.Gear(self.cfg).capability(gear.Capability.java)
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

    def prepare_cmd(self, car, node_name):
        server_log_dir = "%s/server" % self.cfg.opts("system", "challenge.log.dir")
        self.cfg.add(config.Scope.invocation, "launcher", "candidate.log.dir", server_log_dir)
        distribution_version = self.cfg.opts("source", "distribution.version", mandatory=False)

        cmd = ["bin/elasticsearch",
               "%s=%s" % (self.cmd_line_opt(distribution_version, "node_name"), node_name),
               "%s=%s" % (self.cmd_line_opt(distribution_version, "log_path"), server_log_dir)
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

    def _start_process(self, cmd, env, node_name):
        install_dir = self.cfg.opts("provisioning", "local.binary.path")
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
                logger.warn("[%s] has started with initialization errors." % node_name)
                startup_event.set()

            logger.info("%s: %s" % (node_name, l.replace("\n", "\n%s (stdout): " % node_name)))
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
        cluster.telemetry.detach_from_cluster(cluster)
        self._servers = []
