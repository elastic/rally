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

import shlex
import logging
import subprocess
import os
import psutil
import time


def run_subprocess(command_line):
    return os.system(command_line)


def run_subprocess_with_output(command_line):
    command_line_args = shlex.split(command_line)
    with subprocess.Popen(command_line_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as command_line_process:
        has_output = True
        lines = []
        while has_output:
            line = command_line_process.stdout.readline()
            if line:
                lines.append(line.decode("UTF-8").strip())
            else:
                has_output = False
    return lines


def exit_status_as_bool(runnable, quiet=False):
    """

    :param runnable: A runnable returning an int as exit status assuming ``0`` is meaning success.
    :param quiet: Suppress any output (default: False).
    :return: True iff the runnable has terminated successfully.
    """
    try:
        return_code = runnable()
        return return_code == 0 or return_code is None
    except OSError:
        if not quiet:
            logging.getLogger(__name__).exception("Could not execute command.")
        return False


def run_subprocess_with_logging(command_line, header=None, level=logging.INFO, env=None):
    """
    Runs the provided command line in a subprocess. All output will be captured by a logger.

    :param command_line: The command line of the subprocess to launch.
    :param header: An optional header line that should be logged (this will be logged on info level, regardless of the defined log level).
    :param level: The log level to use for output (default: logging.INFO).
    :param env: Use specific environment variables (default: None).
    :return: The process exit code as an int.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Running subprocess [%s] with logging.", command_line)
    command_line_args = shlex.split(command_line)
    if header is not None:
        logger.info(header)
    logger.debug("Invoking subprocess '%s'", command_line)
    with subprocess.Popen(command_line_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env) as command_line_process:
        has_output = True
        while has_output:
            line = command_line_process.stdout.readline()
            if line:
                logger.log(level=level, msg=line)
            else:
                has_output = False
    logger.debug("Subprocess [%s] finished with return code [%s].", command_line, str(command_line_process.returncode))
    return command_line_process.returncode


def kill_running_es_instances(trait):
    """
    Kills all instances of Elasticsearch that are currently running on the local machine by sending SIGKILL.

    :param trait some trait of the process in the command line.
    """

    def elasticsearch_process(p):
        return p.name() == "java" and any("elasticsearch" in e for e in p.cmdline()) and any(trait in e for e in p.cmdline())

    logging.getLogger(__name__).info("Killing all processes which match [java], [elasticsearch] and [%s]", trait)
    kill_all(elasticsearch_process)


def is_rally_process(p):
    return p.name() == "esrally" or \
           p.name() == "rally" or \
           (p.name().lower().startswith("python")
            and any("esrally" in e for e in p.cmdline())
            and not any("esrallyd" in e for e in p.cmdline()))


def find_all_other_rally_processes():
    others = []
    for_all_other_processes(is_rally_process, lambda p: others.append(p))
    return others


def kill_all(predicate):
    def kill(p):
        logging.getLogger(__name__).info("Killing lingering process with PID [%s] and command line [%s].", p.pid, p.cmdline())
        p.kill()
        # wait until process has terminated, at most 3 seconds. Otherwise we might run into race conditions with actor system
        # sockets that are still open.
        for i in range(3):
            try:
                p.status()
                time.sleep(1)
            except psutil.NoSuchProcess:
                break

    for_all_other_processes(predicate, kill)


def for_all_other_processes(predicate, action):
    # no harakiri please
    my_pid = os.getpid()
    for p in psutil.process_iter():
        try:
            if p.pid != my_pid and predicate(p):
                action(p)
        except (psutil.ZombieProcess, psutil.AccessDenied, psutil.NoSuchProcess):
            pass
