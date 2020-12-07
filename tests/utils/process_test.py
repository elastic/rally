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

import os
import unittest.mock as mock
from unittest import TestCase

import psutil

from esrally.utils import process


class ProcessTests(TestCase):
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
            else:
                return "running"

    @mock.patch("psutil.process_iter")
    def test_find_other_rally_processes(self, process_iter):
        rally_es_5_process = ProcessTests.Process(100, "java",
                                                  ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Enode.name=rally-node0",
                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        rally_es_1_process = ProcessTests.Process(101, "java",
                                                  ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Des.node.name=rally-node0",
                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        metrics_store_process = ProcessTests.Process(102, "java", ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g",
                                                                   "-Des.path.home=~/rally/metrics/",
                                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        random_python = ProcessTests.Process(103, "python3", ["/some/django/app"])
        other_process = ProcessTests.Process(104, "init", ["/usr/sbin/init"])
        rally_process_p = ProcessTests.Process(105, "python3", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_r = ProcessTests.Process(106, "rally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_e = ProcessTests.Process(107, "esrally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_mac = ProcessTests.Process(108, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        # fake own process by determining our pid
        own_rally_process = ProcessTests.Process(os.getpid(), "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        night_rally_process = ProcessTests.Process(110, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/night_rally"])

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

        self.assertEqual([rally_process_p, rally_process_r, rally_process_e, rally_process_mac],
                         process.find_all_other_rally_processes())

    @mock.patch("psutil.process_iter")
    def test_find_no_other_rally_process_running(self, process_iter):
        metrics_store_process = ProcessTests.Process(102, "java", ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g",
                                                                   "-Des.path.home=~/rally/metrics/",
                                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        random_python = ProcessTests.Process(103, "python3", ["/some/django/app"])

        process_iter.return_value = [ metrics_store_process, random_python]

        self.assertEqual(0, len(process.find_all_other_rally_processes()))

    @mock.patch("psutil.process_iter")
    def test_kills_only_rally_processes(self, process_iter):
        rally_es_5_process = ProcessTests.Process(100, "java",
                                                  ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Enode.name=rally-node0",
                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        rally_es_1_process = ProcessTests.Process(101, "java",
                                                  ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Des.node.name=rally-node0",
                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        metrics_store_process = ProcessTests.Process(102, "java", ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g",
                                                                   "-Des.path.home=~/rally/metrics/",
                                                                   "org.elasticsearch.bootstrap.Elasticsearch"])
        random_python = ProcessTests.Process(103, "python3", ["/some/django/app"])
        other_process = ProcessTests.Process(104, "init", ["/usr/sbin/init"])
        rally_process_p = ProcessTests.Process(105, "python3", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_r = ProcessTests.Process(106, "rally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_e = ProcessTests.Process(107, "esrally", ["/usr/bin/python3", "~/.local/bin/esrally"])
        rally_process_mac = ProcessTests.Process(108, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        # fake own process by determining our pid
        own_rally_process = ProcessTests.Process(os.getpid(), "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/esrally"])
        night_rally_process = ProcessTests.Process(110, "Python", ["/Python.app/Contents/MacOS/Python", "~/.local/bin/night_rally"])

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

        self.assertFalse(rally_es_5_process.killed)
        self.assertFalse(rally_es_1_process.killed)
        self.assertFalse(metrics_store_process.killed)
        self.assertFalse(random_python.killed)
        self.assertFalse(other_process.killed)
        self.assertTrue(rally_process_p.killed)
        self.assertTrue(rally_process_r.killed)
        self.assertTrue(rally_process_e.killed)
        self.assertTrue(rally_process_mac.killed)
        self.assertFalse(own_rally_process.killed)
        self.assertFalse(night_rally_process.killed)
