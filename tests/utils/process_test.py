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

import logging
import os
import signal
import subprocess
import time
from unittest import mock

import psutil

from esrally.utils import process


class Process:
    def __init__(self, pid, name, cmdline):
        self.pid = pid
        self._name = name
        self._cmdline = cmdline
        self.killed = False

    def name(self):
        return self._name

    def cmdline(self):
        return self._cmdline

    def kill(self):
        self.killed = True

    def status(self):
        if self.killed:
            raise psutil.NoSuchProcess(self.pid)
        return "running"


class TestProcess:
    @mock.patch("psutil.process_iter")
    def test_find_other_rally_processes(self, process_iter):
        rally_es_5_process = Process(
            100,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Enode.name=rally-node0",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        rally_es_1_process = Process(
            101,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Des.node.name=rally-node0",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        metrics_store_process = Process(
            102,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Des.path.home=~/rally/metrics/",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        random_python = Process(103, "python3", ["/some/django/app"])
        other_process = Process(104, "init", ["/usr/sbin/init"])
        rally_process_p = Process(105, "python3", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_r = Process(106, "rally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_e = Process(107, "esrally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_mac = Process(108, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        # fake own process by determining our pid
        own_rally_process = Process(os.getpid(), "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        night_rally_process = Process(110, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/night_rally"])

        process_iter.return_value = [
            rally_es_1_process,
            rally_es_5_process,
            metrics_store_process,
            random_python,
            other_process,
            rally_process_p,
            rally_process_r,
            rally_process_e,
            rally_process_mac,
            own_rally_process,
            night_rally_process,
        ]

        assert process.find_all_other_rally_processes() == [rally_process_p, rally_process_r, rally_process_e, rally_process_mac]

    @mock.patch("psutil.process_iter")
    def test_find_no_other_rally_process_running(self, process_iter):
        metrics_store_process = Process(
            102,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Des.path.home=~/rally/metrics/",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        random_python = Process(103, "python3", ["/some/django/app"])

        process_iter.return_value = [metrics_store_process, random_python]

        assert len(process.find_all_other_rally_processes()) == 0

    @mock.patch("psutil.process_iter")
    def test_kills_only_rally_processes(self, process_iter):
        rally_es_5_process = Process(
            100,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Enode.name=rally-node0",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        rally_es_1_process = Process(
            101,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Des.node.name=rally-node0",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        metrics_store_process = Process(
            102,
            "java",
            [
                "/usr/lib/jvm/java-8-oracle/bin/java",
                "-Xms2g",
                "-Xmx2g",
                "-Des.path.home=~/rally/metrics/",
                "org.elasticsearch.bootstrap.Elasticsearch",
            ],
        )
        random_python = Process(103, "python3", ["/some/django/app"])
        other_process = Process(104, "init", ["/usr/sbin/init"])
        rally_process_p = Process(105, "python3", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_r = Process(106, "rally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_e = Process(107, "esrally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_mac = Process(108, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        # fake own process by determining our pid
        own_rally_process = Process(os.getpid(), "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        night_rally_process = Process(110, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/night_rally"])

        process_iter.return_value = [
            rally_es_1_process,
            rally_es_5_process,
            metrics_store_process,
            random_python,
            other_process,
            rally_process_p,
            rally_process_r,
            rally_process_e,
            rally_process_mac,
            own_rally_process,
            night_rally_process,
        ]

        process.kill_running_rally_instances()

        assert not rally_es_5_process.killed
        assert not rally_es_1_process.killed
        assert not metrics_store_process.killed
        assert not random_python.killed
        assert not other_process.killed
        assert rally_process_p.killed
        assert rally_process_r.killed
        assert rally_process_e.killed
        assert rally_process_mac.killed
        assert not own_rally_process.killed
        assert not night_rally_process.killed


def test_run_subprocess():
    cmd = "ls . not-a-file"
    completed_process = process.run_subprocess_with_logging_and_output(cmd)

    assert completed_process.returncode != 0
    assert completed_process.stdout != ""
    assert completed_process.stderr is None


def test_run_subprocess_with_logging_timeout_kills_process_group(caplog, tmp_path):
    pid_file = tmp_path / "grandchild.pid"
    # The shell backgrounds `sleep 600` in the same process group; both should die on timeout.
    cmd = f"sh -c 'sleep 600 & echo $! > \"{pid_file}\"; wait'"
    timeout = 1

    with caplog.at_level(logging.ERROR, logger="esrally.utils.process"):
        returncode = process.run_subprocess_with_logging(cmd, timeout=timeout)
    grandchild_pid = int(pid_file.read_text().strip())

    assert returncode == -signal.SIGKILL
    # the grandchild PID is reaped asynchronously, so poll until its PID is gone
    deadline = time.monotonic() + 10
    while psutil.pid_exists(grandchild_pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not psutil.pid_exists(grandchild_pid), f"grandchild PID {grandchild_pid} survived process-group kill"
    expected = f"Subprocess [{cmd}] exceeded timeout of [{timeout}]s and was terminated with return code [{-signal.SIGKILL}]."
    assert any(
        r.levelno == logging.ERROR and r.getMessage().startswith(expected) for r in caplog.records
    ), f"expected ERROR log starting with {expected!r}, got: {[r.getMessage() for r in caplog.records]}"


@mock.patch("esrally.utils.process.os.killpg", side_effect=ProcessLookupError)
@mock.patch("esrally.utils.process.subprocess.Popen")
def test_run_subprocess_with_logging_timeout_handles_already_exited_process(popen, killpg):
    proc = popen.return_value.__enter__.return_value
    proc.pid = 4242
    proc.returncode = -signal.SIGKILL
    # first communicate() times out, second (after the kill) drains the pipes
    proc.communicate.side_effect = [subprocess.TimeoutExpired(cmd="sleep 600", timeout=1), ("", None)]

    # the process group exiting between the timeout and the kill must not propagate as an error
    returncode = process.run_subprocess_with_logging("sleep 600", timeout=1)

    assert returncode == -signal.SIGKILL
    killpg.assert_called_once_with(4242, signal.SIGKILL)
    assert proc.communicate.call_count == 2
