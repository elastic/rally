import unittest.mock as mock
from unittest import TestCase

from esrally import config, car
from esrally.mechanic import provisioner


class ProvisionerTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "/rally-root/track/challenge")
        cfg.add(config.Scope.application, "provisioning", "local.install.dir", "es-bin")
        cfg.add(config.Scope.application, "provisioning", "install.preserve", False)

        p = provisioner.Provisioner(cfg)
        p.cleanup()

        mock_path_exists.assert_called_once_with("/rally-root/track/challenge/es-bin")
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing_on_preserve(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "/rally-root/track/challenge")
        cfg.add(config.Scope.application, "provisioning", "local.install.dir", "es-bin")
        cfg.add(config.Scope.application, "provisioning", "install.preserve", True)
        cfg.add(config.Scope.application, "provisioning", "datapaths", ["/tmp/some/data-path-dir"])

        p = provisioner.Provisioner(cfg)
        p.cleanup()

        mock_path_exists.assert_not_called()
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "/rally-root/track/challenge")
        cfg.add(config.Scope.application, "provisioning", "local.install.dir", "es-bin")
        cfg.add(config.Scope.application, "provisioning", "install.preserve", False)
        cfg.add(config.Scope.application, "provisioning", "datapaths", ["/tmp/some/data-path-dir"])

        p = provisioner.Provisioner(cfg)
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
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "/rally-root/track/challenge")
        cfg.add(config.Scope.application, "builder", "candidate.bin.path", "/data/builds/distributions/")
        cfg.add(config.Scope.application, "provisioning", "local.install.dir", "es-bin")
        cfg.add(config.Scope.application, "provisioning", "datapaths", [])

        p = provisioner.Provisioner(cfg)
        p.prepare(car.Car(name="test-car"), 39200)

        self.assertEqual(cfg.opts("provisioning", "local.binary.path"), "/install/elasticsearch-5.0.0-SNAPSHOT")
