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
# pylint: disable=protected-access

import os
import tempfile
import unittest.mock as mock

import pytest

from esrally import exceptions
from esrally.mechanic import provisioner, team

HOME_DIR = os.path.expanduser("~")


class TestBareProvisioner:
    @mock.patch("glob.glob", lambda p: ["/opt/elasticsearch-5.0.0"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_without_plugins(self, mock_rm, mock_ensure_dir, mock_decompress):
        apply_config_calls = []

        def null_apply_config(source_root_path, target_root_path, config_vars):
            apply_config_calls.append((source_root_path, target_root_path, config_vars))

        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="unit-test-car",
                root_path=None,
                config_paths=[HOME_DIR + "/.rally/benchmarks/teams/default/my-car"],
                variables={
                    "heap": "4g",
                    "runtime.jdk": "8",
                    "runtime.jdk.bundled": "true",
                },
            ),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            node_root_dir=HOME_DIR + "/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
        )

        p = provisioner.BareProvisioner(es_installer=installer, plugin_installers=[], apply_config=null_apply_config)

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-5.0.0.tar.gz"})
        assert node_config.car_runtime_jdks == "8"
        assert node_config.binary_path == "/opt/elasticsearch-5.0.0"
        assert node_config.data_paths == ["/opt/elasticsearch-5.0.0/data"]

        assert len(apply_config_calls) == 1
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        assert HOME_DIR + "/.rally/benchmarks/teams/default/my-car" == source_root_path
        assert target_root_path == "/opt/elasticsearch-5.0.0"
        assert config_vars == {
            "cluster_settings": {},
            "heap": "4g",
            "runtime.jdk": "8",
            "runtime.jdk.bundled": "true",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/opt/elasticsearch-5.0.0/data"],
            "log_path": HOME_DIR + "/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": '["10.17.22.22","10.17.22.23"]',
            "all_node_names": '["rally-node-0","rally-node-1"]',
            "minimum_master_nodes": 2,
            "install_root_path": "/opt/elasticsearch-5.0.0",
        }

    class NoopHookHandler:
        def __init__(self, plugin):
            self.hook_calls = {}

        def can_load(self):
            return False

        def invoke(self, phase, variables, **kwargs):
            self.hook_calls[phase] = {"variables": variables, "kwargs": kwargs}

    class MockRallyTeamXPackPlugin:
        """
        Mock XPackPlugin settings as found in rally-team repo:
        https://github.com/elastic/rally-teams/blob/6/plugins/x_pack/security.ini
        """

        def __init__(self):
            self.name = "x-pack"
            self.core_plugin = False
            self.config = {"base": "internal_base,security"}
            self.root_path = None
            self.config_paths = []
            self.variables = {
                "xpack_security_enabled": True,
                "plugin_name": "x-pack-security",
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

        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="unit-test-car",
                root_path=None,
                config_paths=[HOME_DIR + "/.rally/benchmarks/teams/default/my-car"],
                variables={
                    "heap": "4g",
                    "runtime.jdk": "8",
                    "runtime.jdk.bundled": "true",
                },
            ),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            node_root_dir=HOME_DIR + "/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
        )

        p = provisioner.BareProvisioner(
            es_installer=installer,
            plugin_installers=[
                provisioner.PluginInstaller(
                    self.MockRallyTeamXPackPlugin(),
                    java_home="/usr/local/javas/java8",
                    hook_handler_class=self.NoopHookHandler,
                )
            ],
            distribution_version="6.2.3",
            apply_config=null_apply_config,
        )

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-5.0.0.tar.gz"})
        assert node_config.car_runtime_jdks == "8"
        assert node_config.binary_path == "/opt/elasticsearch-5.0.0"
        assert node_config.data_paths == ["/opt/elasticsearch-5.0.0/data"]

        assert len(apply_config_calls) == 1
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        assert source_root_path == HOME_DIR + "/.rally/benchmarks/teams/default/my-car"
        assert target_root_path == "/opt/elasticsearch-5.0.0"

        self.maxDiff = None

        assert config_vars == {
            "cluster_settings": {"plugin.mandatory": ["x-pack-security"]},
            "heap": "4g",
            "runtime.jdk": "8",
            "runtime.jdk.bundled": "true",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/opt/elasticsearch-5.0.0/data"],
            "log_path": HOME_DIR + "/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": '["10.17.22.22","10.17.22.23"]',
            "all_node_names": '["rally-node-0","rally-node-1"]',
            "minimum_master_nodes": 2,
            "install_root_path": "/opt/elasticsearch-5.0.0",
            "plugin_name": "x-pack-security",
            "xpack_security_enabled": True,
        }

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

        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="unit-test-car",
                root_path=None,
                config_paths=[HOME_DIR + "/.rally/benchmarks/teams/default/my-car"],
                variables={
                    "heap": "4g",
                    "runtime.jdk": "8",
                    "runtime.jdk.bundled": "true",
                },
            ),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            node_root_dir=HOME_DIR + "/.rally/benchmarks/races/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
        )

        p = provisioner.BareProvisioner(
            es_installer=installer,
            plugin_installers=[
                provisioner.PluginInstaller(
                    self.MockRallyTeamXPackPlugin(),
                    java_home="/usr/local/javas/java8",
                    hook_handler_class=self.NoopHookHandler,
                )
            ],
            distribution_version="6.3.0",
            apply_config=null_apply_config,
        )

        node_config = p.prepare({"elasticsearch": "/opt/elasticsearch-6.3.0.tar.gz"})
        assert node_config.car_runtime_jdks == "8"
        assert node_config.binary_path == "/opt/elasticsearch-6.3.0"
        assert node_config.data_paths == ["/opt/elasticsearch-6.3.0/data"]

        assert len(apply_config_calls) == 1
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        assert source_root_path == HOME_DIR + "/.rally/benchmarks/teams/default/my-car"
        assert target_root_path == "/opt/elasticsearch-6.3.0"

        self.maxDiff = None

        assert config_vars == {
            "cluster_settings": {"plugin.mandatory": ["x-pack"]},
            "heap": "4g",
            "runtime.jdk": "8",
            "runtime.jdk.bundled": "true",
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/opt/elasticsearch-6.3.0/data"],
            "log_path": HOME_DIR + "/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": '["10.17.22.22","10.17.22.23"]',
            "all_node_names": '["rally-node-0","rally-node-1"]',
            "minimum_master_nodes": 2,
            "install_root_path": "/opt/elasticsearch-6.3.0",
            "plugin_name": "x-pack-security",
            "xpack_security_enabled": True,
        }


class NoopHookHandler:
    def __init__(self, component):
        self.hook_calls = {}

    def can_load(self):
        return False

    def invoke(self, phase, variables, **kwargs):
        self.hook_calls[phase] = {
            "variables": variables,
            "kwargs": kwargs,
        }


class TestElasticsearchInstaller:
    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_default_data_paths(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(names="defaults", root_path=None, config_paths="/tmp"),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
            node_root_dir=HOME_DIR + "/.rally/benchmarks/races/unittest",
        )

        installer.install("/data/builds/distributions")
        assert installer.es_home_path == "/install/elasticsearch-5.0.0-SNAPSHOT"

        assert installer.variables == {
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/install/elasticsearch-5.0.0-SNAPSHOT/data"],
            "log_path": HOME_DIR + "/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": '["10.17.22.22","10.17.22.23"]',
            "all_node_names": '["rally-node-0","rally-node-1"]',
            "minimum_master_nodes": 2,
            "install_root_path": "/install/elasticsearch-5.0.0-SNAPSHOT",
        }

        assert installer.data_paths == ["/install/elasticsearch-5.0.0-SNAPSHOT/data"]

    @mock.patch("glob.glob", lambda p: ["/install/elasticsearch-5.0.0-SNAPSHOT"])
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_user_provided_data_path(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="defaults",
                root_path=None,
                config_paths="/tmp",
                variables={
                    "data_paths": "/tmp/some/data-path-dir",
                },
            ),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
            node_root_dir="~/.rally/benchmarks/races/unittest",
        )

        installer.install("/data/builds/distributions")
        assert installer.es_home_path == "/install/elasticsearch-5.0.0-SNAPSHOT"

        assert installer.variables == {
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "data_paths": ["/tmp/some/data-path-dir"],
            "log_path": "~/.rally/benchmarks/races/unittest/logs/server",
            "heap_dump_path": "~/.rally/benchmarks/races/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": '["10.17.22.22","10.17.22.23"]',
            "all_node_names": '["rally-node-0","rally-node-1"]',
            "minimum_master_nodes": 2,
            "install_root_path": "/install/elasticsearch-5.0.0-SNAPSHOT",
        }

        assert installer.data_paths == ["/tmp/some/data-path-dir"]

    def test_invokes_hook_with_java_home(self):
        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="defaults",
                root_path="/tmp",
                config_paths="/tmp/templates",
                variables={
                    "data_paths": "/tmp/some/data-path-dir",
                },
            ),
            java_home="/usr/local/javas/java8",
            node_name="rally-node-0",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
            node_root_dir="~/.rally/benchmarks/races/unittest",
            hook_handler_class=NoopHookHandler,
        )

        assert len(installer.hook_handler.hook_calls) == 0
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        assert len(installer.hook_handler.hook_calls) == 1
        assert installer.hook_handler.hook_calls["post_install"]["variables"] == {"foo": "bar"}
        assert installer.hook_handler.hook_calls["post_install"]["kwargs"] == {"env": {"JAVA_HOME": "/usr/local/javas/java8"}}

    def test_invokes_hook_no_java_home(self):
        installer = provisioner.ElasticsearchInstaller(
            car=team.Car(
                names="defaults",
                root_path="/tmp",
                config_paths="/tmp/templates",
                variables={
                    "data_paths": "/tmp/some/data-path-dir",
                },
            ),
            java_home=None,
            node_name="rally-node-0",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["rally-node-0", "rally-node-1"],
            ip="10.17.22.23",
            http_port=9200,
            node_root_dir="~/.rally/benchmarks/races/unittest",
            hook_handler_class=NoopHookHandler,
        )

        assert len(installer.hook_handler.hook_calls) == 0
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        assert len(installer.hook_handler.hook_calls) == 1
        assert installer.hook_handler.hook_calls["post_install"]["variables"] == {"foo": "bar"}
        assert installer.hook_handler.hook_calls["post_install"]["kwargs"] == {"env": {}}


class TestPluginInstaller:
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_successfully(self, installer_subprocess):
        installer_subprocess.return_value = 0

        plugin = team.PluginDescriptor(
            name="unit-test-plugin",
            config="default",
            variables={
                "active": True,
            },
        )
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        installer.install(es_home_path="/opt/elasticsearch")

        installer_subprocess.assert_called_with(
            '/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unit-test-plugin"',
            env={"JAVA_HOME": "/usr/local/javas/java8"},
        )

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_bundled_jdk(self, installer_subprocess):
        installer_subprocess.return_value = 0

        plugin = team.PluginDescriptor(
            name="unit-test-plugin",
            config="default",
            variables={
                "active": True,
            },
        )
        installer = provisioner.PluginInstaller(
            plugin,
            # bundled JDK
            java_home=None,
            hook_handler_class=NoopHookHandler,
        )

        installer.install(es_home_path="/opt/elasticsearch")

        installer_subprocess.assert_called_with(
            '/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unit-test-plugin"',
            env={},
        )

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_unknown_plugin(self, installer_subprocess):
        # unknown plugin
        installer_subprocess.return_value = 64

        plugin = team.PluginDescriptor(name="unknown")
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        with pytest.raises(exceptions.SystemSetupError) as exc:
            installer.install(es_home_path="/opt/elasticsearch")
        assert exc.value.args[0] == "Unknown plugin [unknown]"

        installer_subprocess.assert_called_with(
            '/opt/elasticsearch/bin/elasticsearch-plugin install --batch "unknown"',
            env={"JAVA_HOME": "/usr/local/javas/java8"},
        )

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_io_error(self, installer_subprocess):
        # I/O error
        installer_subprocess.return_value = 74

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        with pytest.raises(exceptions.SupplyError) as exc:
            installer.install(es_home_path="/opt/elasticsearch")
        assert exc.value.args[0] == "I/O error while trying to install [simple]"

        installer_subprocess.assert_called_with(
            '/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"',
            env={"JAVA_HOME": "/usr/local/javas/java8"},
        )

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_unknown_error(self, installer_subprocess):
        # some other error
        installer_subprocess.return_value = 12987

        plugin = team.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        with pytest.raises(exceptions.RallyError) as exc:
            installer.install(es_home_path="/opt/elasticsearch")
        assert exc.value.args[0] == "Unknown error while trying to install [simple] (installer return code [12987]). Please check the logs."

        installer_subprocess.assert_called_with(
            '/opt/elasticsearch/bin/elasticsearch-plugin install --batch "simple"',
            env={"JAVA_HOME": "/usr/local/javas/java8"},
        )

    def test_pass_plugin_properties(self):
        plugin = team.PluginDescriptor(
            name="unit-test-plugin",
            config="default",
            config_paths=["/etc/plugin"],
            variables={
                "active": True,
            },
        )
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        assert installer.plugin_name == "unit-test-plugin"
        assert installer.variables == {"active": True}
        assert installer.config_source_paths == ["/etc/plugin"]

    def test_invokes_hook_with_java_home(self):
        plugin = team.PluginDescriptor(
            name="unit-test-plugin",
            config="default",
            config_paths=["/etc/plugin"],
            variables={
                "active": True,
            },
        )
        installer = provisioner.PluginInstaller(plugin, java_home="/usr/local/javas/java8", hook_handler_class=NoopHookHandler)

        assert len(installer.hook_handler.hook_calls) == 0
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        assert len(installer.hook_handler.hook_calls) == 1
        assert installer.hook_handler.hook_calls["post_install"]["variables"] == {"foo": "bar"}
        assert installer.hook_handler.hook_calls["post_install"]["kwargs"] == {"env": {"JAVA_HOME": "/usr/local/javas/java8"}}

    def test_invokes_hook_no_java_home(self):
        plugin = team.PluginDescriptor(
            name="unit-test-plugin",
            config="default",
            config_paths=["/etc/plugin"],
            variables={
                "active": True,
            },
        )
        installer = provisioner.PluginInstaller(plugin, java_home=None, hook_handler_class=NoopHookHandler)

        assert len(installer.hook_handler.hook_calls) == 0
        installer.invoke_install_hook(team.BootstrapPhase.post_install, {"foo": "bar"})
        assert len(installer.hook_handler.hook_calls) == 1
        assert installer.hook_handler.hook_calls["post_install"]["variables"] == {"foo": "bar"}
        assert installer.hook_handler.hook_calls["post_install"]["kwargs"] == {"env": {}}


class TestDockerProvisioner:
    @mock.patch("uuid.uuid4")
    def test_provisioning_with_defaults(self, uuid4):
        uuid4.return_value = "9dbc682e-d32a-4669-8fbe-56fb77120dd4"
        node_ip = "10.17.22.33"
        node_root_dir = tempfile.gettempdir()
        log_dir = os.path.join(node_root_dir, "logs", "server")
        heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        data_dir = os.path.join(node_root_dir, "data", "9dbc682e-d32a-4669-8fbe-56fb77120dd4")

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, "esrally"))

        c = team.Car(
            "unit-test-car",
            None,
            "/tmp",
            variables={
                "docker_image": "docker.elastic.co/elasticsearch/elasticsearch-oss",
            },
        )

        docker = provisioner.DockerProvisioner(
            car=c,
            node_name="rally-node-0",
            ip=node_ip,
            http_port=39200,
            node_root_dir=node_root_dir,
            distribution_version="6.3.0",
            rally_root=rally_root,
        )

        assert docker.config_vars == {
            "cluster_name": "rally-benchmark",
            "node_name": "rally-node-0",
            "install_root_path": "/usr/share/elasticsearch",
            "data_paths": ["/usr/share/elasticsearch/data"],
            "log_path": "/var/log/elasticsearch",
            "heap_dump_path": "/usr/share/elasticsearch/heapdump",
            "discovery_type": "single-node",
            "network_host": "0.0.0.0",
            "http_port": "39200",
            "transport_port": "39300",
            "cluster_settings": {},
            "docker_image": "docker.elastic.co/elasticsearch/elasticsearch-oss",
        }

        assert docker.docker_vars(mounts={}) == {
            "es_data_dir": data_dir,
            "es_log_dir": log_dir,
            "es_heap_dump_dir": heap_dump_dir,
            "es_version": "6.3.0",
            "docker_image": "docker.elastic.co/elasticsearch/elasticsearch-oss",
            "http_port": 39200,
            "node_ip": node_ip,
            "mounts": {},
        }

        docker_cfg = docker._render_template_from_file(docker.docker_vars(mounts={}))

        assert docker_cfg == (
            f'''version: '2.2'
services:
  elasticsearch1:
    cap_add:
      - IPC_LOCK
    image: "docker.elastic.co/elasticsearch/elasticsearch-oss:6.3.0"
    labels:
      io.rally.description: "elasticsearch-rally"
    ports:
      - 39200:39200
      - 9300
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - {data_dir}:/usr/share/elasticsearch/data
      - {log_dir}:/var/log/elasticsearch
      - {heap_dump_dir}:/usr/share/elasticsearch/heapdump
    healthcheck:
      test: nc -z 127.0.0.1 39200
      interval: 5s
      timeout: 2s
      retries: 10
    networks:
      - rally-es
networks:
  rally-es:
    driver_opts:
      com.docker.network.bridge.host_binding_ipv4: "{node_ip}"'''
        )

    @mock.patch("uuid.uuid4")
    def test_provisioning_with_variables(self, uuid4):
        uuid4.return_value = "86f42ae0-5840-4b5b-918d-41e7907cb644"
        node_root_dir = tempfile.gettempdir()
        node_ip = "10.17.22.33"
        log_dir = os.path.join(node_root_dir, "logs", "server")
        heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        data_dir = os.path.join(node_root_dir, "data", "86f42ae0-5840-4b5b-918d-41e7907cb644")

        rally_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, "esrally"))

        c = team.Car(
            "unit-test-car",
            None,
            "/tmp",
            variables={
                "docker_image": "docker.elastic.co/elasticsearch/elasticsearch",
                "docker_mem_limit": "256m",
                "docker_cpu_count": 2,
            },
        )

        docker = provisioner.DockerProvisioner(
            car=c,
            node_name="rally-node-0",
            ip=node_ip,
            http_port=39200,
            node_root_dir=node_root_dir,
            distribution_version="6.3.0",
            rally_root=rally_root,
        )

        docker_cfg = docker._render_template_from_file(docker.docker_vars(mounts={}))

        assert (
            docker_cfg
            == f'''version: '2.2'
services:
  elasticsearch1:
    cap_add:
      - IPC_LOCK
    image: "docker.elastic.co/elasticsearch/elasticsearch:6.3.0"
    labels:
      io.rally.description: "elasticsearch-rally"
    cpu_count: 2
    mem_limit: 256m
    ports:
      - 39200:39200
      - 9300
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - {data_dir}:/usr/share/elasticsearch/data
      - {log_dir}:/var/log/elasticsearch
      - {heap_dump_dir}:/usr/share/elasticsearch/heapdump
    healthcheck:
      test: nc -z 127.0.0.1 39200
      interval: 5s
      timeout: 2s
      retries: 10
    networks:
      - rally-es
networks:
  rally-es:
    driver_opts:
      com.docker.network.bridge.host_binding_ipv4: "{node_ip}"'''
        )


class TestCleanup:
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_preserves(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        provisioner.cleanup(preserve=True, install_dir="./rally/races/install", data_paths=["./rally/races/data"])

        assert mock_path_exists.call_count == 0
        assert mock_rm.call_count == 0

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        provisioner.cleanup(preserve=False, install_dir="./rally/races/install", data_paths=["./rally/races/data"])

        expected_dir_calls = [mock.call("/tmp/some/data-path-dir"), mock.call("/rally-root/track/challenge/es-bin")]
        mock_path_exists.mock_calls = expected_dir_calls
        mock_rm.mock_calls = expected_dir_calls
