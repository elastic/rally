import unittest.mock as mock
from unittest import TestCase

from esrally.utils import process


class TestProcess:
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


class ProcessTests(TestCase):
    @mock.patch("psutil.process_iter")
    def test_kills_only_rally_es_processes(self, process_iter):
        rally_es_5_process = TestProcess(100, "java",
                                         ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Enode.name=rally-node0",
                                          "org.elasticsearch.bootstrap.Elasticsearch"])
        rally_es_1_process = TestProcess(101, "java",
                                         ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "-Des.node.name=rally-node0",
                                          "org.elasticsearch.bootstrap.Elasticsearch"])
        metrics_store_process = TestProcess(102, "java", ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g",
                                                          "-Des.path.home=~/rally/metrics/", "org.elasticsearch.bootstrap.Elasticsearch"])
        random_java = TestProcess(103, "java", ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xms2g", "-Xmx2g", "jenkins.main"])
        other_process = TestProcess(104, "init", ["/usr/sbin/init"])
        rally_process = TestProcess(105, "python3", ["/usr/bin/python3", "~/.local/bin/esrally"])

        process_iter.return_value = [
            rally_es_1_process,
            rally_es_5_process,
            metrics_store_process,
            random_java,
            other_process,
            rally_process
        ]

        process.kill_running_es_instances("rally")

        self.assertTrue(rally_es_5_process.killed)
        self.assertTrue(rally_es_1_process.killed)
        self.assertFalse(metrics_store_process.killed)
        self.assertFalse(random_java.killed)
        self.assertFalse(other_process.killed)
        self.assertFalse(rally_process.killed)
