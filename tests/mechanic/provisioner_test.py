import os
import tempfile
import unittest.mock as mock
from unittest import TestCase

from esrally.mechanic import provisioner, car
from esrally.utils import convert


class BareProvisionerTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing_on_preserve(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        p = provisioner.BareProvisioner(car=car.Car("defaults", "/tmp"),
                                        node_name_prefix="rally-node-",
                                        http_port=9200,
                                        cluster_settings=None,
                                        install_dir="es-bin",
                                        data_root_paths=["/tmp/some-data-path"],
                                        node_log_dir="rally-logs",
                                        single_machine=True,
                                        preserve=True)
        p.cleanup()

        mock_path_exists.assert_not_called()
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        p = provisioner.BareProvisioner(car=car.Car("defaults", "/tmp"),
                                        node_name_prefix="rally-node-",
                                        http_port=9200,
                                        cluster_settings=None,
                                        install_dir="es-bin",
                                        data_root_paths=["/tmp/some-data-path"],
                                        node_log_dir="rally-logs",
                                        single_machine=True,
                                        preserve=True)

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

        p = provisioner.BareProvisioner(car=car.Car("defaults", "/tmp"),
                                        node_name_prefix="rally-node-",
                                        http_port=9200,
                                        cluster_settings={"indices.query.bool.max_clause_count": 5000},
                                        install_dir="es-bin",
                                        data_root_paths=["/var/elasticsearch"],
                                        node_log_dir="rally-logs",
                                        single_machine=False,
                                        preserve=False)

        p._install_binary("/data/builds/distributions")
        self.assertEqual(p.binary_path, "/install/elasticsearch-5.0.0-SNAPSHOT")

        vars = p._provisioner_variables()
        print(vars)
        self.assertEqual({
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/var/elasticsearch/data"],
            "log_path": "rally-logs",
            "network_host": "0.0.0.0",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "node_count_per_host": 1,
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 5000
            }

        }, vars)

        self.assertEqual(p.data_paths, ["/var/elasticsearch/data"])


class DockerProvisionerTests(TestCase):
    @mock.patch("esrally.utils.sysstats.total_memory")
    def test_provisioning(self, total_memory):
        total_memory.return_value = convert.gb_to_bytes(64)
        install_dir = tempfile.gettempdir()
        log_dir = "/tmp/rally-unittest/logs"

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../esrally"))

        c = car.Car("unit-test-car", "/tmp", variables={
            "xpack.security.enabled": False
        })

        docker = provisioner.DockerProvisioner(c, "rally-node-", {"indices.query.bool.max_clause_count": 5000}, 39200, install_dir,
                                               log_dir, "5.0.0", rally_root, preserve=False)

        self.assertEqual({
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/usr/share/elasticsearch/data"],
            "log_path": "/var/log/elasticsearch",
            "network_host": "0.0.0.0",
            "http_port": "39200-39300",
            "transport_port": "39300-39400",
            "node_count_per_host": 1,
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 5000
            },
            "xpack.security.enabled": False
        }, docker.config_vars)

        self.assertEqual({
            "es_data_dir": "%s/data" % install_dir,
            "es_log_dir": log_dir,
            "es_version": "5.0.0",
            "http_port": 39200,
            "mounts": {}
        }, docker.docker_vars(mounts={}))

        docker_cfg = docker._render_template_from_file(docker.docker_vars(mounts={}))

        self.assertEqual(
"""version: '2'
services:
  elasticsearch1:
    cap_add:
      - IPC_LOCK
    image: "docker.elastic.co/elasticsearch/elasticsearch:5.0.0"
    ports:
      - 39200:39200
      - 9300
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - %s/data:/usr/share/elasticsearch/data
      - %s:/var/log/elasticsearch""" % (install_dir, log_dir), docker_cfg)

