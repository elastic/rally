import logging
import os
import signal
import socket
import subprocess
import threading
import shlex

import jinja2
import psutil

from esrally import config, time, exceptions, client
from esrally.mechanic import gear, telemetry, cluster
from esrally.utils import versions, console, process, io, convert

logger = logging.getLogger("rally.launcher")


class DockerLauncher:
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, cfg, metrics_store, client_factory_class=client.EsClientFactory):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.client_factory = client_factory_class

    def start(self, car):
        # hardcoded for the moment, should actually be identical to internal launcher
        # Only needed on Mac:
        # hosts = [{"host": process.run_subprocess_with_output("docker-machine ip default")[0].strip(), "port": 9200}]
        hosts = [{"host": "localhost", "port": 9200}]
        client_options = self.cfg.opts("launcher", "client.options")
        # unified client config
        self.cfg.add(config.Scope.benchmark, "client", "hosts", hosts)
        self.cfg.add(config.Scope.benchmark, "client", "options", client_options)

        es = self.client_factory(hosts, client_options).create()

        t = telemetry.Telemetry(self.cfg, devices=[
            # Be aware that some the meta-data are taken from the host system, not the container (e.g. number of CPU cores) so if the
            # Docker container constrains these, the metrics are actually wrong.
            telemetry.EnvironmentInfo(self.cfg, es, self.metrics_store),
            telemetry.NodeStats(self.cfg, es, self.metrics_store),
            telemetry.IndexStats(self.cfg, es, self.metrics_store),
            telemetry.DiskIo(self.cfg, self.metrics_store),
            telemetry.CpuUsage(self.cfg, self.metrics_store)
        ])

        distribution_version = self.cfg.opts("source", "distribution.version", mandatory=False)

        install_dir = self._install_dir()
        io.ensure_dir(install_dir)

        java_opts = ""
        if car.heap:
            java_opts += "-Xms%s -Xmx%s " % (car.heap, car.heap)
        if car.java_opts:
            java_opts += car.java_opts

        vars = {
            "es_java_opts": java_opts,
            "container_memory_gb": "%dg" % (convert.bytes_to_gb(psutil.virtual_memory().total) // 2),
            "es_data_dir": "%s/data" % install_dir,
            "es_version": distribution_version
        }

        docker_cfg = self._render_template_from_file(vars)
        logger.info("Starting Docker container with configuration:\n%s" % docker_cfg)
        docker_cfg_path = self._docker_cfg_path()
        with open(docker_cfg_path, "wt") as f:
            f.write(docker_cfg)

        c = cluster.Cluster([], t)

        self._start_process(cmd="docker-compose -f %s up" % docker_cfg_path, node_name="rally0")
        # Wait for a little while: Plugins may still be initializing although the node has already started.
        time.sleep(10)

        t.attach_to_cluster(c)
        logger.info("Successfully started Docker container")
        return c

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
                logger.warn("[%s] has started with initialization errors." % node_name)
                startup_event.set()

            logger.info("%s: %s" % (node_name, l.replace("\n", "\n%s (stdout): " % node_name)))
            if l.endswith("] started") and not startup_event.isSet():
                startup_event.set()
                logger.info("[%s] has successfully started." % node_name)

    def stop(self, cluster):
        logger.info("Stopping Docker container")
        docker_cfg_path = self._docker_cfg_path()
        process.run_subprocess_with_logging("docker-compose down -v -f %s" % docker_cfg_path)

    def _docker_cfg_path(self):
        install_dir = self._install_dir()
        return "%s/docker-compose.yml" % install_dir

    def _render_template(self, loader, template_name, variables):
        env = jinja2.Environment(loader=loader)
        for k, v in variables.items():
            env.globals[k] = v
        template = env.get_template(template_name)

        return template.render()

    def _render_template_from_file(self, variables):
        compose_file = "%s/resources/docker-compose.yml" % (self.cfg.opts("system", "rally.root"))
        return self._render_template(loader=jinja2.FileSystemLoader(io.dirname(compose_file)),
                                     template_name=io.basename(compose_file),
                                     variables=variables)

    def _install_dir(self):
        root = self.cfg.opts("system", "challenge.root.dir")
        install = self.cfg.opts("provisioning", "local.install.dir")
        return "%s/%s" % (root, install)


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

        t = telemetry.Telemetry(self.cfg, devices=[
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

        cluster_telemetry = [
            # TODO dm: Once we do distributed launching, this needs to be done per node not per cluster
            telemetry.MergeParts(self.cfg, self.metrics_store),
            telemetry.EnvironmentInfo(self.cfg, es, self.metrics_store),
            telemetry.NodeStats(self.cfg, es, self.metrics_store),
            telemetry.IndexStats(self.cfg, es, self.metrics_store),
            # TODO dm: Once we do distributed launching, this needs to be done per node not per cluster
            telemetry.IndexSize(self.cfg, self.metrics_store)
        ]

        t = telemetry.Telemetry(self.cfg, devices=cluster_telemetry)
        c = cluster.Cluster([self._start_node(node, car, es) for node in range(car.nodes)], t)
        t.attach_to_cluster(c)
        return c

    def _start_node(self, node, car, es):
        node_name = self._node_name(node)
        host_name = socket.gethostname()

        node_telemetry = [
            telemetry.FlightRecorder(self.cfg, self.metrics_store),
            telemetry.JitCompiler(self.cfg, self.metrics_store),
            telemetry.Gc(self.cfg, self.metrics_store),
            telemetry.PerfStat(self.cfg, self.metrics_store),
            telemetry.DiskIo(self.cfg, self.metrics_store),
            telemetry.CpuUsage(self.cfg, self.metrics_store),
            telemetry.EnvironmentInfo(self.cfg, es, self.metrics_store),
        ]

        t = telemetry.Telemetry(self.cfg, devices=node_telemetry)

        env = self._prepare_env(car, node_name, t)
        cmd = self.prepare_cmd(car, node_name)
        process = self._start_process(cmd, env, node_name)
        node = cluster.Node(process, host_name, node_name, t)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, car, node_name, t):
        env = {}
        env.update(os.environ)
        java_home = gear.Gear(self.cfg).capability(gear.Capability.java)
        # Unix specific!:
        self._set_env(env, "PATH", "%s/bin" % java_home, separator=":")
        # Don't merge here!
        env["JAVA_HOME"] = java_home

        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(car, node_name).items():
            self._set_env(env, k, v)

        # probe if this JVM supports +ExitOnOutOfMemoryError
        if process.run_subprocess_with_logging("%s/bin/java -XX:+ExitOnOutOfMemoryError -version" % java_home):
            logger.info("JVM supports +ExitOnOutOfMemoryError. Setting this option to detect out of memory errors during the benchmark.")
            java_opts = "-XX:+ExitOnOutOfMemoryError "
        else:
            logger.info("JVM does not support +ExitOnOutOfMemoryError. Cannot detect out of memory errors. Please consider a JDK upgrade.")
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
