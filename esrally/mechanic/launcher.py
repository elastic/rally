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
import errno
import hashlib
import logging
import os
import shlex
import socket
import subprocess
import time as std_time

import psutil

from esrally import exceptions, telemetry, time, types
from esrally.mechanic import cluster, java_resolver
from esrally.utils import io, opts, process

# Cached compose command: "docker compose" (v2) or "docker-compose" (v1)
_compose_cmd = None


def wait_until_port_is_free(port_number, timeout=120):
    """
    Wait until nothing is listening on the given port (connection refused).
    Avoids Docker port-binding conflicts when the port is already in use on the host.
    """
    logger = logging.getLogger(__name__)
    deadline = std_time.perf_counter() + timeout
    while std_time.perf_counter() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(("127.0.0.1", port_number))
                if result == errno.ECONNREFUSED:
                    logger.debug("Port [%s] is free.", port_number)
                    return
        except OSError:
            pass
        std_time.sleep(0.5)
    raise exceptions.LaunchError(
        "Port [%s] is still in use after [%s] seconds. "
        "Free the port or stop the process using it to avoid Docker port-forwarding conflicts." % (port_number, timeout)
    )


def _compose_command():
    """Return the docker compose command to use: 'docker compose' (v2) or 'docker-compose' (v1)."""
    global _compose_cmd
    if _compose_cmd is not None:
        return _compose_cmd
    try:
        ret = subprocess.call(
            shlex.split("docker compose version"),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ret == 0:
            _compose_cmd = "docker compose"
            return _compose_cmd
    except (OSError, ValueError):
        pass
    _compose_cmd = "docker-compose"
    return _compose_cmd


class DockerLauncher:
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, cfg: types.Config, clock=time.Clock):
        self.cfg = cfg
        self.clock = clock
        self.logger = logging.getLogger(__name__)

    def start(self, node_configurations):
        nodes = []
        for node_configuration in node_configurations:
            node_name = node_configuration.node_name
            host_name = node_configuration.ip
            binary_path = node_configuration.binary_path
            http_port = getattr(node_configuration, "http_port", None)
            if http_port is not None:
                self.logger.info("Ensuring port [%s] is free before starting Docker (avoid port conflict).", http_port)
                wait_until_port_is_free(http_port)
            self.logger.info("Starting node [%s] in Docker.", node_name)
            self._start_process(binary_path)
            node_telemetry = [
                # Don't attach any telemetry devices for now but keep the infrastructure in place
            ]
            t = telemetry.Telemetry(devices=node_telemetry)
            node = cluster.Node(0, binary_path, host_name, node_name, t)
            t.attach_to_node(node)
            nodes.append(node)
        return nodes

    def _start_process(self, binary_path):
        compose_file = os.path.join(binary_path, "docker-compose.yml")
        compose_dir = binary_path
        # Unique project name so multiple races do not collide (default name "install" is shared by all).
        project_name = "rally-" + hashlib.sha256(compose_dir.encode()).hexdigest()[:16]
        compose_cmd = self._docker_compose(compose_file, compose_dir, project_name, "up -d")

        self.logger.info("Running: %s", compose_cmd)
        ret = process.run_subprocess_with_logging(compose_cmd)
        if ret != 0:
            msg = f"Docker daemon startup failed with exit code [{ret}]"
            logging.error(msg)
            raise exceptions.LaunchError(msg)

        container_id = self._get_container_id(compose_file, compose_dir, project_name)
        self._wait_for_healthy_running_container(container_id, DockerLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)

    def _docker_compose(self, compose_file, compose_dir, project_name, cmd):
        compose_cmd = _compose_command()
        # Use -f, --project-directory, and -p so the project is unambiguous and paths resolve correctly.
        return f'{compose_cmd} -f "{compose_file}" --project-directory "{compose_dir}" -p {project_name} {cmd}'

    def _get_container_id(self, compose_file, compose_dir, project_name):
        compose_ps_cmd = self._docker_compose(compose_file, compose_dir, project_name, "ps -q")
        return process.run_subprocess_with_output(compose_ps_cmd)[0]

    def _wait_for_healthy_running_container(self, container_id, timeout):
        cmd = f'docker ps -a --filter "id={container_id}" --filter "status=running" --filter "health=healthy" -q'
        stop_watch = self.clock.stop_watch()
        stop_watch.start()
        while stop_watch.split_time() < timeout:
            containers = process.run_subprocess_with_output(cmd)
            if len(containers) > 0:
                return
            time.sleep(0.5)
        msg = f"No healthy running container after {timeout} seconds!"
        logging.error(msg)
        raise exceptions.LaunchError(msg)

    def stop(self, nodes, metrics_store=None):
        self.logger.info("Shutting down [%d] nodes running in Docker on this host.", len(nodes))
        for node in nodes:
            self.logger.info("Stopping node [%s].", node.node_name)
            if metrics_store is not None:
                telemetry.add_metadata_for_node(metrics_store, node.node_name, node.host_name)
                node.telemetry.detach_from_node(node, running=True)

            compose_dir = node.binary_path
            compose_file = os.path.join(compose_dir, "docker-compose.yml")
            project_name = "rally-" + hashlib.sha256(compose_dir.encode()).hexdigest()[:16]
            process.run_subprocess_with_logging(self._docker_compose(compose_file, compose_dir, project_name, "down"))
            if metrics_store is not None:
                node.telemetry.detach_from_node(node, running=False)
                node.telemetry.store_system_metrics(node, metrics_store)


def wait_for_pidfile(pidfilename, timeout=60, clock=time.Clock):
    stop_watch = clock.stop_watch()
    stop_watch.start()
    while stop_watch.split_time() < timeout:
        try:
            with open(pidfilename, "rb") as f:
                buf = f.read()
                if not buf:
                    raise EOFError
                return int(buf)
        except (FileNotFoundError, EOFError):
            time.sleep(0.5)

    msg = f"pid file not available after {timeout} seconds!"
    logging.error(msg)
    raise exceptions.LaunchError(msg)


class ProcessLauncher:
    """
    Launcher is responsible for starting and stopping the benchmark candidate.
    """

    PROCESS_WAIT_TIMEOUT_SECONDS = 90.0

    def __init__(self, cfg: types.Config, clock=time.Clock):
        self.cfg = cfg
        self._clock = clock
        self.logger = logging.getLogger(__name__)
        self.pass_env_vars = opts.csv_to_list(self.cfg.opts("system", "passenv", mandatory=False, default_value="PATH"))

    def start(self, node_configurations):
        node_count_on_host = len(node_configurations)
        return [self._start_node(node_configuration, node_count_on_host) for node_configuration in node_configurations]

    def _start_node(self, node_configuration, node_count_on_host):
        host_name = node_configuration.ip
        node_name = node_configuration.node_name
        binary_path = node_configuration.binary_path
        data_paths = node_configuration.data_paths
        node_telemetry_dir = os.path.join(node_configuration.node_root_path, "telemetry")

        java_major_version, java_home = java_resolver.java_home(
            node_configuration.car_runtime_jdks, self.cfg.opts("mechanic", "runtime.jdk"), node_configuration.car_provides_bundled_jdk
        )

        self.logger.info("Starting node [%s].", node_name)

        enabled_devices = self.cfg.opts("telemetry", "devices")
        telemetry_params = self.cfg.opts("telemetry", "params")
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.JitCompiler(node_telemetry_dir, java_major_version),
            telemetry.Gc(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.Heapdump(node_telemetry_dir),
            telemetry.DiskIo(node_count_on_host),
            telemetry.IndexSize(data_paths),
            telemetry.StartupTime(),
        ]

        t = telemetry.Telemetry(enabled_devices, devices=node_telemetry)
        env = self._prepare_env(node_name, java_home, t)
        t.on_pre_node_start(node_name)
        node_pid = self._start_process(binary_path, env)
        self.logger.info("Successfully started node [%s] with PID [%s].", node_name, node_pid)
        node = cluster.Node(node_pid, binary_path, host_name, node_name, t)

        self.logger.info("Attaching telemetry devices to node [%s].", node_name)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, node_name, java_home, t):
        env = {k: v for k, v in os.environ.items() if k in self.pass_env_vars}
        if java_home:
            self._set_env(env, "PATH", os.path.join(java_home, "bin"), separator=os.pathsep, prepend=True)
            # This property is the higher priority starting in ES 7.12.0, and is the only supported java home in >=8.0
            env["ES_JAVA_HOME"] = java_home
            # TODO remove this when ES <8.0 becomes unsupported by Rally
            env["JAVA_HOME"] = java_home
        if not env.get("ES_JAVA_OPTS"):
            env["ES_JAVA_OPTS"] = "-XX:+ExitOnOutOfMemoryError"

        # we just blindly trust telemetry here...
        for v in t.instrument_candidate_java_opts():
            self._set_env(env, "ES_JAVA_OPTS", v)

        self.logger.debug("env for [%s]: %s", node_name, str(env))
        return env

    def _set_env(self, env, k, v, separator=" ", prepend=False):
        if v is not None:
            if k not in env:
                env[k] = v
            elif prepend:
                env[k] = v + separator + env[k]
            else:
                env[k] = env[k] + separator + v

    @staticmethod
    def _run_subprocess(command_line, env):
        command_line_args = shlex.split(command_line)

        with subprocess.Popen(
            command_line_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env, start_new_session=True
        ) as command_line_process:
            # wait for it to finish
            command_line_process.wait()
        return command_line_process.returncode

    @staticmethod
    def _start_process(binary_path, env):
        if os.name == "posix" and os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch Elasticsearch as root. Please run Rally as a non-root user.")
        os.chdir(binary_path)
        cmd = [io.escape_path(os.path.join(".", "bin", "elasticsearch"))]
        pid_path = io.escape_path(os.path.join(".", "pid"))
        cmd.extend(["-d", "-p", pid_path])
        ret = ProcessLauncher._run_subprocess(command_line=" ".join(cmd), env=env)
        if ret != 0:
            msg = f"Daemon startup failed with exit code [{ret}]"
            logging.error(msg)
            raise exceptions.LaunchError(msg)

        return wait_for_pidfile(pid_path)

    def stop(self, nodes, metrics_store=None):
        self.logger.info("Shutting down [%d] nodes on this host.", len(nodes))
        stopped_nodes = []
        for node in nodes:
            node_name = node.node_name
            if metrics_store is not None:
                telemetry.add_metadata_for_node(metrics_store, node_name, node.host_name)
                node.telemetry.detach_from_node(node, running=True)
            try:
                es = psutil.Process(pid=node.pid)
            except psutil.NoSuchProcess:
                self.logger.warning("No process found with PID [%s] for node [%s].", node.pid, node_name)
                es = None

            if es:
                stop_watch = self._clock.stop_watch()
                stop_watch.start()
                try:
                    es.terminate()
                    es.wait(10)
                    stopped_nodes.append(node)
                except psutil.NoSuchProcess:
                    self.logger.warning("No process found with PID [%s] for node [%s].", es.pid, node_name)
                except psutil.TimeoutExpired:
                    self.logger.info("kill -KILL node [%s]", node_name)
                    try:
                        # kill -9
                        es.kill()
                        stopped_nodes.append(node)
                    except psutil.NoSuchProcess:
                        self.logger.warning("No process found with PID [%s] for node [%s].", es.pid, node_name)
                self.logger.info("Done shutting down node [%s] in [%.1f] s.", node_name, stop_watch.split_time())
                if metrics_store is not None:
                    node.telemetry.detach_from_node(node, running=False)
            # store system metrics in any case (telemetry devices may derive system metrics while the node is running)
            if metrics_store is not None:
                node.telemetry.store_system_metrics(node, metrics_store)
        return stopped_nodes
