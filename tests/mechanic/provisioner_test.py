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
            names="unit-test-car",
            root_path=None,
            config_paths=["~/.rally/benchmarks/teams/default/my-car"],
            variables={"heap": "4g"}),
            node_name="rally-node-0",
            node_root_dir="~/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            ip="10.17.22.23",
            http_port=9200)

        p = provisioner.BareProvisioner(cluster_settings={"indices.query.bool.max_clause_count": 50000},
                                        es_installer=installer,
                                        plugin_installers=[],
                                        preserve=True,
                                        apply_config=null_apply_config)

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-5.0.0.tar.gz"})
        self.assertEqual(installer.car, node_config.car)
        self.assertEqual("/opt/elasticsearch-5.0.0", node_config.binary_path)
        self.assertEqual(["/opt/elasticsearch-5.0.0/data"], node_config.data_paths)

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
            "data_paths": ["/opt/elasticsearch-5.0.0/data"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "node_count_per_host": 1,
            "install_root_path": "/opt/elasticsearch-5.0.0"
        }, config_vars)

    class NoopHookHandler:
        def __init__(self, plugin):
            self.hook_calls = {}

        def can_load(self):
            return False

        def invoke(self, phase, variables):
            self.hook_calls[phase] = variables

    class MockRallyTeamXPackPlugin:
        """
        Mock XPackPlugin settings as found in rally-team repo:
        https://github.com/elastic/rally-teams/blob/6/plugins/x_pack/security.ini
        """
        def __init__(self):
            self.name = "x-pack"
            self.core_plugin = False
            self.config = {
                'base': 'internal_base,security'
            }
            self.root_path = None
            self.config_paths = []
            self.variables = {
                'xpack_security_enabled': True,
                'plugin_name': 'x-pack-security'
            }

        def __str__(self):
            return "Plugin descriptor for [%s]" % self.name

        def __repr__(self):
            r = []
            for prop, value in vars(self).items():
                r.append("%s = [%s]" % (prop, repr(value)))
            return ", ".join(r)

    @mock.patch("glob.glob", lambda p: ["/opt/elasticsearch-5.0.0"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.mechanic.provisioner.PluginInstaller.install")
    @mock.patch("shutil.rmtree")
    def test_prepare_distribution_lt_63_with_plugins(self, mock_rm, mock_ensure_dir, mock_install, mock_decompress):
        """
        Test that plugin.mandatory is set to the specific plugin name (e.g. `x-pack-security`) and not
        the meta plugin name (e.g. `x-pack`) for Elasticsearch <6.3

        See: https://github.com/elastic/elasticsearch/pull/28710
        """
        apply_config_calls = []

        def null_apply_config(source_root_path, target_root_path, config_vars):
            apply_config_calls.append((source_root_path, target_root_path, config_vars))

        installer = provisioner.ElasticsearchInstaller(car=
        team.Car(
            names="unit-test-car",
            root_path=None,
            config_paths=["~/.rally/benchmarks/teams/default/my-car"],
            variables={"heap": "4g"}),
            node_name="rally-node-0",
            node_root_dir="~/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            ip="10.17.22.23",
            http_port=9200)

        p = provisioner.BareProvisioner(cluster_settings={"indices.query.bool.max_clause_count": 50000},
                                        es_installer=installer,
                                        plugin_installers=[
                                            provisioner.PluginInstaller(BareProvisionerTests.MockRallyTeamXPackPlugin(),
                                                                        hook_handler_class=BareProvisionerTests.NoopHookHandler)
                                        ],
                                        preserve=True,
                                        distribution_version="6.2.3",
                                        apply_config=null_apply_config)

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-5.0.0.tar.gz"})
        self.assertEqual(installer.car, node_config.car)
        self.assertEqual("/opt/elasticsearch-5.0.0", node_config.binary_path)
        self.assertEqual(["/opt/elasticsearch-5.0.0/data"], node_config.data_paths)

        self.assertEqual(1, len(apply_config_calls))
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        self.assertEqual("~/.rally/benchmarks/teams/default/my-car", source_root_path)
        self.assertEqual("/opt/elasticsearch-5.0.0", target_root_path)

        self.maxDiff = None

        self.assertEqual({
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 50000,
                "plugin.mandatory": ["x-pack-security"]
            },
            "heap": "4g",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/opt/elasticsearch-5.0.0/data"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "node_count_per_host": 1,
            "install_root_path": "/opt/elasticsearch-5.0.0",
            "plugin_name": "x-pack-security",
            "xpack_security_enabled": True

        }, config_vars)

    @mock.patch("glob.glob", lambda p: ["/opt/elasticsearch-6.3.0"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.mechanic.provisioner.PluginInstaller.install")
    @mock.patch("shutil.rmtree")
    def test_prepare_distribution_ge_63_with_plugins(self, mock_rm, mock_ensure_dir, mock_install, mock_decompress):
        """
        Test that plugin.mandatory is set to the meta plugin name (e.g. `x-pack`) and not
        the specific plugin name (e.g. `x-pack-security`) for Elasticsearch >=6.3.0

        See: https://github.com/elastic/elasticsearch/pull/28710
        """
        apply_config_calls = []

        def null_apply_config(source_root_path, target_root_path, config_vars):
            apply_config_calls.append((source_root_path, target_root_path, config_vars))

        installer = provisioner.ElasticsearchInstaller(car=
        team.Car(
            names="unit-test-car",
            root_path=None,
            config_paths=["~/.rally/benchmarks/teams/default/my-car"],
            variables={"heap": "4g"}),
            node_name="rally-node-0",
            node_root_dir="~/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            ip="10.17.22.23",
            http_port=9200)

        p = provisioner.BareProvisioner(cluster_settings={"indices.query.bool.max_clause_count": 50000},
                                        es_installer=installer,
                                        plugin_installers=[
                                            provisioner.PluginInstaller(BareProvisionerTests.MockRallyTeamXPackPlugin(),
                                                                        hook_handler_class=BareProvisionerTests.NoopHookHandler)
                                        ],
                                        preserve=True,
                                        distribution_version="6.3.0",
                                        apply_config=null_apply_config)

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-6.3.0.tar.gz"})
        self.assertEqual(installer.car, node_config.car)
        self.assertEqual("/opt/elasticsearch-6.3.0", node_config.binary_path)
        self.assertEqual(["/opt/elasticsearch-6.3.0/data"], node_config.data_paths)

        self.assertEqual(1, len(apply_config_calls))
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        self.assertEqual("~/.rally/benchmarks/teams/default/my-car", source_root_path)
        self.assertEqual("/opt/elasticsearch-6.3.0", target_root_path)

        self.maxDiff = None

        self.assertEqual({
            "cluster_settings": {
                "indices.query.bool.max_clause_count": 50000,
                "plugin.mandatory": ["x-pack"]
            },
            "heap": "4g",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/opt/elasticsearch-6.3.0/data"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "node_count_per_host": 1,
            "install_root_path": "/opt/elasticsearch-6.3.0",
            "plugin_name": "x-pack-security",
            "xpack_security_enabled": True

        }, config_vars)


class NoopHookHandler:
    def __init__(self, component):
        self.hook_calls = {}

    def can_load(self):
        return False

    def invoke(self, phase, variables):
        self.hook_calls[phase] = variables


class ElasticsearchInstallerTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup_nothing_on_preserve(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = False

        installer = provisioner.ElasticsearchInstaller(car=team.Car("defaults", None, "/tmp"),
                                                       node_name="rally-node-0",
                                                       all_node_ips={"127.0.0.1"},
                                                       ip="127.0.0.1",
                                                       http_port=9200,
                                                       node_root_dir="~/.rally/benchmarks/races/unittest")
        installer.cleanup(preserve=True)

        mock_path_exists.assert_not_called()
        mock_rm.assert_not_called()

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        installer = provisioner.ElasticsearchInstaller(car=team.Car(names="defaults",
                                                                    root_path=None,
                                                                    config_paths="/tmp",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       node_name="rally-node-0",
                                                       all_node_ips={"127.0.0.1"},
                                                       ip="127.0.0.1",
                                                       http_port=9200,
                                                       node_root_dir="~/.rally/benchmarks/races/unittest")
        installer.cleanup(preserve=True)

        expected_dir_calls = [mock.call("/tmp/some/data-path-dir"), mock.call("/rally-root/track/challenge/es-bin")]
        mock_path_exists.mock_calls = expected_dir_calls
        mock_rm.mock_calls = expected_dir_calls

    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_default_data_paths(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.ElasticsearchInstaller(car=team.Car(names="defaults",
                                                                    root_path=None,
                                                                    config_paths="/tmp"),
                                                       node_name="rally-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.rally/benchmarks/races/unittest")

        installer.install("/data/builds/distributions")
        self.assertEqual(installer.es_home_path, "/install/elasticsearch-5.0.0-SNAPSHOT")

        self.assertEqual({
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/install/elasticsearch-5.0.0-SNAPSHOT/data"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "node_count_per_host": 1,
            "install_root_path": "/install/elasticsearch-5.0.0-SNAPSHOT"
        }, installer.variables)

        self.assertEqual(installer.data_paths, ["/install/elasticsearch-5.0.0-SNAPSHOT/data"])

    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_user_provided_data_path(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.ElasticsearchInstaller(car=team.Car(names="defaults",
                                                                    root_path=None,
                                                                    config_paths="/tmp",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       node_name="rally-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.rally/benchmarks/races/unittest")

        installer.install("/data/builds/distributions")
        self.assertEqual(installer.es_home_path, "/install/elasticsearch-5.0.0-SNAPSHOT")

        self.assertEqual({
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/tmp/some/data-path-dir"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200-9300",
            "transport_port": "9300-9400",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "node_count_per_host": 1,
            "install_root_path": "/install/elasticsearch-5.0.0-SNAPSHOT"
        }, installer.variables)

        self.assertEqual(installer.data_paths, ["/tmp/some/data-path-dir"])

    def test_invokes_hook(self):
        installer = provisioner.ElasticsearchInstaller(car=team.Car(names="defaults",
                                                                    root_path="/tmp",
                                                                    config_paths="/tmp/templates",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       node_name="rally-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.rally/benchmarks/races/unittest",
                                                       hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"])


class PluginInstallerTests(TestCase):
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_successfully(self, installer_subprocess):
        installer_subprocess.return_value = 0

        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        installer.install(es_home_path="/opt/elasticsearch")

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unit-test-plugin"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_unknown_plugin(self, installer_subprocess):
        # unknown plugin
        installer_subprocess.return_value = 64

        plugin = team.PluginDescriptor(name="unknown")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("Unknown plugin [unknown]", ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unknown"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_io_error(self, installer_subprocess):
        # I/O error
        installer_subprocess.return_value = 74

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.SupplyError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("I/O error while trying to install [simple]", ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"')

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_unknown_error(self, installer_subprocess):
        # some other error
        installer_subprocess.return_value = 12987

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.RallyError) as ctx:
            installer.install(es_home_path="/opt/elasticsearch")
        self.assertEqual("Unknown error while trying to install [simple] (installer return code [12987]). Please check the logs.",
                         ctx.exception.args[0])

        installer_subprocess.assert_called_with('/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"')

    def test_pass_plugin_properties(self):
        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", config_paths=["/etc/plugin"], variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        self.assertEqual("unit-test-plugin", installer.plugin_name)
        self.assertEqual({"active": True}, installer.variables)
        self.assertEqual(["/etc/plugin"], installer.config_source_paths)

    def test_invokes_hook(self):
        plugin = team.PluginDescriptor(name="unit-test-plugin", config="default", config_paths=["/etc/plugin"], variables={"active": True})
        installer = provisioner.PluginInstaller(plugin, hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"])


class DockerProvisionerTests(TestCase):
    @mock.patch("esrally.utils.sysstats.total_memory")
    @mock.patch("uuid.uuid4")
    def test_provisioning(self, uuid4, total_memory):
        total_memory.return_value = convert.gb_to_bytes(64)
        uuid4.return_value = "9dbc682e-d32a-4669-8fbe-56fb77120dd4"
        node_root_dir = tempfile.gettempdir()
        log_dir = os.path.join(node_root_dir, "logs", "server")
        data_dir = "%s/data/9dbc682e-d32a-4669-8fbe-56fb77120dd4" % node_root_dir

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../esrally"))

        c = team.Car("unit-test-car", None, "/tmp", variables={
            "xpack.security.enabled": False
        })

        docker = provisioner.DockerProvisioner(car=c,
                                               node_name="rally-node-0",
                                               cluster_settings={"indices.query.bool.max_clause_count": 5000},
                                               ip="10.17.22.33",
                                               http_port=39200,
                                               node_root_dir=node_root_dir,
                                               distribution_version="5.0.0",
                                               rally_root=rally_root,
                                               preserve=False)

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
                "xpack.ml.enabled": "false",
                "xpack.monitoring.enabled": "false",
                "xpack.watcher.enabled": "false",
                "indices.query.bool.max_clause_count": 5000
            },
            "xpack.security.enabled": False
        }, docker.config_vars)

        self.assertEqual({
            "es_data_dir": data_dir,
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
      - %s:/usr/share/elasticsearch/data
      - %s:/var/log/elasticsearch""" % (data_dir, log_dir), docker_cfg)
