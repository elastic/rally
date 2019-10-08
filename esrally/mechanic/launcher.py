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
import logging
import os
import shlex
import signal
import subprocess
from time import monotonic as _time

import psutil

from esrally import time, exceptions, telemetry
from esrally.mechanic import cluster, java_resolver
from esrally.utils import process


def _get_container_id(compose_config):
    compose_ps_cmd = _get_docker_compose_cmd(compose_config, "ps -q")

    output = subprocess.check_output(args=shlex.split(compose_ps_cmd))
    return output.decode("utf-8").rstrip()


def _wait_for_healthy_running_container(container_id, timeout=60):
    cmd = 'docker ps -a --filter "id={}" --filter "status=running" --filter "health=healthy" -q'.format(container_id)
    endtime = _time() + timeout
    while _time() < endtime:
        output = subprocess.check_output(shlex.split(cmd))
        containers = output.decode("utf-8").rstrip()
        if len(containers) > 0:
            return
        time.sleep(0.5)
    msg = "No healthy running container after {} seconds!".format(timeout)
    logging.error(msg)
    raise exceptions.LaunchError(msg)


def _get_docker_compose_cmd(compose_config, cmd):
    return "docker-compose -f {} {}".format(compose_config, cmd)


class DockerLauncher:
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, cfg, metrics_store):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.binary_paths = {}
        self.keep_running = self.cfg.opts("mechanic", "keep.running")
        self.logger = logging.getLogger(__name__)

    def start(self, node_configurations):
        nodes = []
        for node_configuration in node_configurations:
            node_name = node_configuration.node_name
            host_name = node_configuration.ip
            binary_path = node_configuration.binary_path
            node_telemetry_dir = os.path.join(node_configuration.node_root_path, "telemetry")
            self.binary_paths[node_name] = binary_path
            self._start_process(binary_path)
            # only support a subset of telemetry for Docker hosts
            # (specifically, we do not allow users to enable any devices)
            node_telemetry = [
                telemetry.DiskIo(self.metrics_store, len(node_configurations), node_telemetry_dir, node_name),
            ]
            t = telemetry.Telemetry(devices=node_telemetry)
            telemetry.add_metadata_for_node(self.metrics_store, node_name, host_name)
            node = cluster.Node(0, host_name, node_name, t)
            t.attach_to_node(node)
            nodes.append(node)
        return nodes

    def _start_process(self, binary_path):
        compose_cmd = _get_docker_compose_cmd(binary_path, "up -d")

        ret = process.run_subprocess_with_logging(compose_cmd)
        if ret != 0:
            msg = "Docker daemon startup failed with exit code[{}]".format(ret)
            logging.error(msg)
            raise exceptions.LaunchError(msg)

        container_id = _get_container_id(binary_path)
        _wait_for_healthy_running_container(container_id)

    def stop(self, nodes):
        if self.keep_running:
            self.logger.info("Keeping Docker container running.")
        else:
            self.logger.info("Stopping Docker container")
            for node in nodes:
                node.telemetry.detach_from_node(node, running=True)
                process.run_subprocess_with_logging(_get_docker_compose_cmd(self.binary_paths[node.node_name], "down"))
                node.telemetry.detach_from_node(node, running=False)


def wait_for_pidfile(pidfilename, timeout=60):
    endtime = _time() + timeout
    while _time() < endtime:
        try:
            with open(pidfilename, "rb") as f:
                return int(f.read())
        except FileNotFoundError:
            time.sleep(0.5)

    msg = "pid file not available after {} seconds!".format(timeout)
    logging.error(msg)
    raise exceptions.LaunchError(msg)


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
        node_telemetry_dir = os.path.join(node_configuration.node_root_path, "telemetry")

        java_major_version, java_home = java_resolver.java_home(car, self.cfg)

        telemetry.add_metadata_for_node(self.metrics_store, node_name, host_name)

        self.logger.info("Starting node [%s] based on car [%s].", node_name, car)

        enabled_devices = self.cfg.opts("telemetry", "devices")
        telemetry_params = self.cfg.opts("telemetry", "params")
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.JitCompiler(node_telemetry_dir),
            telemetry.Gc(node_telemetry_dir, java_major_version),
            telemetry.DiskIo(self.metrics_store, node_count_on_host, node_telemetry_dir, node_name),
            telemetry.IndexSize(data_paths, self.metrics_store),
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
        self._set_env(env, "PATH", os.path.join(java_home, "bin"), separator=os.pathsep, prepend=True)
        # Don't merge here!
        env["JAVA_HOME"] = java_home
        env["ES_JAVA_OPTS"] = "-XX:+ExitOnOutOfMemoryError"
        
        # we just blindly trust telemetry here...
        for v in t.instrument_candidate_java_opts(car, node_name):
            self._set_env(env, "ES_JAVA_OPTS", v)

        self.logger.debug("env for [%s]: %s", node_name, str(env))
        return env

    def _set_env(self, env, k, v, separator=' ', prepend=False):
        if v is not None:
            if k not in env:
                env[k] = v
            elif prepend:
                    env[k] = v + separator + env[k]
            else:
                    env[k] = env[k] + separator + v

    @staticmethod
    def _start_process(binary_path, env):
        if os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch Elasticsearch as root. Please run Rally as a non-root user.")
        os.chdir(binary_path)
        cmd = ["bin/elasticsearch"]
        cmd.extend(["-d", "-p", "pid"])
        ret = process.run_subprocess_with_logging(command_line=" ".join(cmd), env=env)
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
