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

        p = provisioner.Provisioner(cfg, install_dir="es-bin", single_machine=True)
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

        p = provisioner.Provisioner(cfg, install_dir="es-bin", single_machine=True)
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

        p = provisioner.Provisioner(cfg, install_dir="es-bin", single_machine=True)
        p.prepare("/data/builds/distributions/")

        self.assertEqual(p.binary_path, "/install/elasticsearch-5.0.0-SNAPSHOT")
        self.assertEqual(p.data_paths, ["/var/elasticsearch/data"])


class DockerProvisionerTests(TestCase):
    @mock.patch("esrally.utils.sysstats.total_memory")
    def test_docker_vars(self, total_memory):
        total_memory.return_value = convert.gb_to_bytes(64)
        tmp = tempfile.gettempdir()

        docker = provisioner.DockerProvisioner("defaults", tmp, "5.0.0", "../../")

        self.assertEqual({
            "es_java_opts": "",
            "container_memory_gb": "32g",
            "es_data_dir": "%s/data" % tmp,
            "es_version": "5.0.0"
        }, docker.docker_vars)

