import os
import tempfile
import unittest.mock as mock
from unittest import TestCase

from esrally import config
from esrally.mechanic import provisioner
from esrally.utils import convert


class ProvisionerTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing_on_preserve(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "car.name", "defaults")
        cfg.add(config.Scope.application, "mechanic", "preserve.install", True)
        cfg.add(config.Scope.application, "mechanic", "node.datapaths", ["/tmp/some/data-path-dir"])

        p = provisioner.Provisioner(cfg, cluster_settings=None, install_dir="es-bin", node_log_dir="rally-logs", single_machine=True)
        p.cleanup()

        mock_path_exists.assert_not_called()
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        cfg.add(config.Scope.application, "mechanic", "car.name", "defaults")
        cfg.add(config.Scope.application, "mechanic", "node.datapaths", ["/tmp/some/data-path-dir"])

        p = provisioner.Provisioner(cfg, cluster_settings={}, install_dir="es-bin", node_log_dir="rally-logs", single_machine=True)
        p.data_paths = ["/tmp/some/data-path-dir"]
        p.cleanup()

        expected_dir_calls = [mock.call("/tmp/some/data-path-dir"), mock.call("/rally-root/track/challenge/es-bin")]
        mock_path_exists.mock_calls = expected_dir_calls
        mock_rm.mock_calls = expected_dir_calls

    @mock.patch("builtins.open")
    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_prepare(self, mock_path_exists, mock_rm, mock_ensure_dir, mock_decompress, mock_open):
        mock_path_exists.return_value = True

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "car.name", "defaults")
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        cfg.add(config.Scope.application, "mechanic", "node.datapaths", ["/var/elasticsearch"])

        p = provisioner.Provisioner(cfg,
                                    cluster_settings={"indices.query.bool.max_clause_count": 5000},
                                    install_dir="es-bin",
                                    node_log_dir="rally-logs",
                                    single_machine=True)
        p.prepare("/data/builds/distributions/")

        self.assertEqual(p.binary_path, "/install/elasticsearch-5.0.0-SNAPSHOT")
        self.assertEqual(p.data_paths, ["/var/elasticsearch/data"])
        self.assertEqual(
"""
cluster.name: rally-benchmark
node.max_local_storage_nodes: 1
path.data: /var/elasticsearch/data
http.port: 39200-39300
transport.tcp.port: 39300-39400
indices.query.bool.max_clause_count: 5000""", p._node_configuration())


class DockerProvisionerTests(TestCase):
    @mock.patch("esrally.utils.sysstats.total_memory")
    def test_docker_vars(self, total_memory):
        total_memory.return_value = convert.gb_to_bytes(64)
        tmp = tempfile.gettempdir()

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../esrally"))

        docker = provisioner.DockerProvisioner("defaults", {"indices.query.bool.max_clause_count": 5000}, "39200", tmp, "5.0.0", rally_root)

        self.assertEqual({
            "es_java_opts": "",
            "container_memory_gb": "32g",
            "es_data_dir": "%s/data" % tmp,
            "es_version": "5.0.0",
            "http_port": "39200",
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 5000
            }
        }, docker.docker_vars)

        docker_cfg = docker._render_template_from_file(docker.docker_vars)
        self.assertEqual(
"""version: '2'
services:
  elasticsearch1:
    cap_add:
      - IPC_LOCK
    environment:
      - discovery.zen.minimum_master_nodes=1
      - bootstrap.memory_lock=true
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS="
      - indices.query.bool.max_clause_count=5000
    image: "docker.elastic.co/elasticsearch/elasticsearch:5.0.0"
    mem_limit: 32g
    ports:
      - 39200:9200
      - 9300
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - esdata1:%s/data
volumes:
  esdata1:
    driver: local""" % tmp
, docker_cfg
        )

