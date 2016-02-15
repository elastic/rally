import os
import threading
import subprocess
import signal
import logging

from rally.mechanic import gear
from rally import config, cluster, telemetry, time, exceptions

logger = logging.getLogger("rally.launcher")


class Launcher:
    PROCESS_WAIT_TIMEOUT_SECONDS = 20.0
    """
    Launcher is responsible for starting and stopping the benchmark candidate.

    Currently, only local launching is supported.
    """

    def __init__(self, cfg, clock=time.Clock):
        self._config = cfg
        self._clock = clock
        self._servers = []

    def start(self, track, setup, metrics_store):
        if self._servers:
            logger.warn("There are still referenced servers on startup. Did the previous shutdown succeed?")
        num_nodes = setup.candidate_settings.nodes
        first_http_port = self._config.opts("provisioning", "node.http.port")

        return cluster.Cluster(
            [{"host": "localhost", "port": first_http_port}],
            [self._start_node(node, setup, metrics_store) for node in range(num_nodes)],
            metrics_store
        )

    def _start_node(self, node, setup, metrics_store):
        node_name = self._node_name(node)

        t = telemetry.Telemetry(self._config, metrics_store)

        env = self._prepare_env(setup, t)
        cmd = self.prepare_cmd(setup, node_name)
        process = self._start_process(cmd, env, node_name)
        t.attach_to_process(process)

        return cluster.Server(process, t)

    def _prepare_env(self, setup, t):
        env = {}
        env.update(os.environ)
        # we just blindly trust telemetry here...
        for k, v in t.instrument_candidate_env(setup).items():
            self._set_env(env, k, v)

        self._set_env(env, "ES_HEAP_SIZE", setup.candidate_settings.heap)
        self._set_env(env, "ES_JAVA_OPTS", setup.candidate_settings.java_opts)
        self._set_env(env, "ES_GC_OPTS", setup.candidate_settings.gc_opts)

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
        processor_count = setup.candidate_settings.processors

        cmd = ["bin/elasticsearch", "-Des.node.name=%s" % node_name]
        if processor_count is not None and processor_count > 1:
            cmd.append("-Des.processors=%s" % processor_count)
        cmd.append("-Dpath.logs=%s" % server_log_dir)
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
        if startup_event.wait(timeout=Launcher.PROCESS_WAIT_TIMEOUT_SECONDS):
            logger.info("Started node=%s with pid=%s" % (node_name, process.pid))
            return process
        else:
            log_dir = self._config.opts("system", "log.dir")
            msg = "Could not start node '%s' within timeout period of %s seconds." % (node_name, Launcher.PROCESS_WAIT_TIMEOUT_SECONDS)
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
        for idx, server in enumerate(cluster.servers):
            process = server.process
            server.telemetry.detach_from_process(process)
            node = self._node_name(idx)

            os.kill(process.pid, signal.SIGINT)

            try:
                process.wait(10.0)
                logger.info("Done shutdown server (%.1f sec)" % stop_watch.split_time())
            except subprocess.TimeoutExpired:
                # kill -9
                logger.warn("Server %s did not shut down itself after 10 seconds; now kill -QUIT server, to see threads:" % node)
                try:
                    os.kill(process.pid, signal.SIGQUIT)
                except OSError:
                    logger.warn("  no such process")
                    return

                try:
                    process.wait(120.0)
                    logger.info("Done shutdown server (%.1f sec)" % stop_watch.split_time())
                    return
                except subprocess.TimeoutExpired:
                    pass

                logger.info("kill -KILL server")
                try:
                    process.kill()
                except ProcessLookupError:
                    logger.warn("No such process")
        self._servers = []
