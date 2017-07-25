import os
import tempfile
import unittest.mock as mock
from unittest import TestCase

from esrally.mechanic import provisioner, team
from esrally.utils import convert
from esrally import exceptions


class BareProvisionerTests(TestCase):
    @mock.patch("glob.glob", lambda p: ["/opt/elasticsearch-5.0.0"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_without_plugins(self, mock_rm, mock_ensure_dir, mock_decompress):
        apply_config_calls = []

        def null_apply_config(source_root_path, target_root_path, config_vars):
            apply_config_calls.append((source_root_path, target_root_path, config_vars))

        installer = provisioner.ElasticsearchInstaller(car=
                                                       team.Car(
                                                           name="unit-test-car",
                                                           config_paths=["~/.rally/benchmarks/teams/default/my-car"],
                                                           variables={"heap": "4g"}),
                                                       node_name_prefix="rally-node-",
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       install_dir="es-bin",
                                                       data_root_paths=["/var/elasticsearch"],
                                                       node_log_dir="rally-logs")

        p = provisioner.BareProvisioner(cluster_settings={"indices.query.bool.max_clause_count": 50000},
                                        es_installer=installer,
                                        plugin_installers=[],
                                        preserve=True,
                                        apply_config=null_apply_config
                                        )

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-5.0.0.tar.gz"})
        self.assertEqual("rally-node-0", node_config.node_name)
        self.assertEqual(installer.car, node_config.car)
        self.assertEqual("/opt/elasticsearch-5.0.0", node_config.binary_path)
        self.assertEqual(["/var/elasticsearch/data"], node_config.data_paths)

        self.assertEqual(1, len(apply_config_calls))
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        self.assertEqual("~/.rally/benchmarks/teams/default/my-car", source_root_path)
        self.assertEqual("/opt/elasticsearch-5.0.0", target_root_path)
        self.assertEqual({
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 50000,
            },
            "heap": "4g",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/var/elasticsearch/data"],
            "log_path": "rally-logs",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "node_count_per_host": 1,
            "install_root_path": "/opt/elasticsearch-5.0.0"
        }, config_vars)


class ElasticsearchInstallerTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing_on_preserve(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        installer = provisioner.ElasticsearchInstaller(car=team.Car("defaults", "/tmp"),
                                                       node_name_prefix="rally-node-",
                                                       ip="127.0.0.1",
                                                       http_port=9200,
                                                       install_dir="es-bin",
                                                       data_root_paths=["/tmp/some-data-path"],
                                                       node_log_dir="rally-logs")
        installer.cleanup(preserve=True)

        mock_path_exists.assert_not_called()
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        installer = provisioner.ElasticsearchInstaller(car=team.Car("defaults", "/tmp"),
                                                       node_name_prefix="rally-node-",
                                                       ip="127.0.0.1",
                                                       http_port=9200,
                                                       install_dir="es-bin",
                                                       data_root_paths=["/tmp/some-data-path"],
                                                       node_log_dir="rally-logs")

        installer.data_paths = ["/tmp/some/data-path-dir"]
        installer.cleanup(preserve=True)

        expected_dir_calls = [mock.call("/tmp/some/data-path-dir"), mock.call("/rally-root/track/challenge/es-bin")]
        mock_path_exists.mock_calls = expected_dir_calls
        mock_rm.mock_calls = expected_dir_calls

    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.ElasticsearchInstaller(car=team.Car("defaults", "/tmp"),
                                                       node_name_prefix="rally-node-",
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       install_dir="es-bin",
                                                       data_root_paths=["/var/elasticsearch"],
                                                       node_log_dir="rally-logs")

        installer.install("/data/builds/distributions")
        self.assertEqual(installer.es_home_path, "/install/elasticsearch-5.0.0-SNAPSHOT")

        self.assertEqual({
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/var/elasticsearch/data"],
            "log_path": "rally-logs",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "node_count_per_host": 1,
            "install_root_path": "/install/elasticsearch-5.0.0-SNAPSHOT"
        }, installer.variables)

        self.assertEqual(installer.data_paths, ["/var/elasticsearch/data"])


class PluginInstallerTests(TestCase):
    class NoopHookHandler:
        def __init__(self, plugin):
            self.hook_calls = {}

        def can_load(self):
            return False

        def invoke(self, phase, variables):
            self.hook_calls[phase] = variables

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_successfully(self, installer_subprocess):
        installer_subprocess.return_value = 0

        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        installer.install(es_home_path="/opt/elasticsearch")

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unit-test-plugin"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_unknown_plugin(self, installer_subprocess):
        # unknown plugin
        installer_subprocess.return_value = 64

        plugin = team.PluginDescriptor(name="unknown")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("Unknown plugin [unknown]", ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unknown"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_io_error(self, installer_subprocess):
        # I/O error
        installer_subprocess.return_value = 74

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        with self.assertRaises(exceptions.SupplyError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("I/O error while trying to install [simple]", ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_unknown_error(self, installer_subprocess):
        # some other error
        installer_subprocess.return_value = 12987

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        with self.assertRaises(exceptions.RallyError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("Unknown error while trying to install [simple] (installer return code [12987]). Please check the logs.",
                         ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"')

    def test_pass_plugin_properties(self):
        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", config_path="/etc/plugin", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        self.assertEqual("unit-test-plugin", installer.plugin_name)
        self.assertEqual({"active": True}, installer.variables)
        self.assertEqual("/etc/plugin", installer.config_source_path)

    def test_invokes_hook(self):
        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", config_path="/etc/plugin", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=PluginInstallerTests.NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(provisioner.ProvisioningPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"])


class InstallHookHandlerTests(TestCase):
    class UnitTestComponentLoader:
        def __init__(self, root_path, component_entry_point, recurse):
            self.root_path = root_path
            self.component_entry_point = component_entry_point
            self.recurse = recurse
            self.registration_function = None

        def load(self):
            return self.registration_function

    class UnitTestHook:
        def __init__(self, phase="post_install"):
            self.phase = phase
            self.call_counter = 0

        def post_install_hook(self, config_name, variables, **kwargs):
            self.call_counter += variables["increment"]

        def register(self, handler):
            # we can register multiple hooks here
            handler.register(self.phase, self.post_install_hook)
            handler.register(self.phase, self.post_install_hook)

    def test_loads_module(self):
        plugin = team.PluginDescriptor("unittest-plugin")
        hook = InstallHookHandlerTests.UnitTestHook()
        handler = provisioner.InstallHookHandler(plugin, loader_class=InstallHookHandlerTests.UnitTestComponentLoader)

        handler.loader.registration_function = hook
        handler.load()

        handler.invoke("post_install", {"increment": 4})

        # we registered our hook twice. Check that it has been called twice.
        self.assertEqual(hook.call_counter, 2 * 4)

    def test_cannot_register_for_unknown_phase(self):
        plugin = team.PluginDescriptor("unittest-plugin")
        hook = InstallHookHandlerTests.UnitTestHook(phase="this_is_an_unknown_install_phase")
        handler = provisioner.InstallHookHandler(plugin, loader_class=InstallHookHandlerTests.UnitTestComponentLoader)

        handler.loader.registration_function = hook
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            handler.load()
        self.assertEqual("Provisioning phase [this_is_an_unknown_install_phase] is unknown. Valid phases are: ['post_install'].",
                         ctx.exception.args[0])


class DockerProvisionerTests(TestCase):
    @mock.patch("esrally.utils.sysstats.total_memory")
    def test_provisioning(self, total_memory):
        total_memory.return_value = convert.gb_to_bytes(64)
        install_dir = tempfile.gettempdir()
        log_dir = "/tmp/rally-unittest/logs"

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../esrally"))

        c = team.Car("unit-test-car", "/tmp", variables={
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
                "xpack.security.enabled": "false",
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
