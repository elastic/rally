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

import os
import re

import pytest

from esrally import exceptions
from esrally.mechanic import team

current_dir = os.path.dirname(os.path.abspath(__file__))


class TestCarLoader:
    def setup_method(self, method):
        self.team_dir = os.path.join(current_dir, "data")
        self.loader = team.CarLoader(self.team_dir)

    def test_lists_car_names(self):
        assert set(self.loader.car_names()) == {
            "default",
            "with_hook",
            "32gheap",
            "missing_cfg_base",
            "empty_cfg_base",
            "ea",
            "verbose",
            "multi_hook",
            "another_with_hook",
        }

    def test_load_known_car(self):
        car = team.load_car(self.team_dir, ["default"], car_params={"data_paths": ["/mnt/disk0", "/mnt/disk1"]})
        assert car.name == "default"
        assert car.config_paths == [os.path.join(current_dir, "data", "cars", "v1", "vanilla", "templates")]
        assert car.root_path is None
        assert car.variables == {"heap_size": "1g", "clean_command": "./gradlew clean", "data_paths": ["/mnt/disk0", "/mnt/disk1"]}
        assert car.root_path is None

    def test_load_car_with_mixin_single_config_base(self):
        car = team.load_car(self.team_dir, ["32gheap", "ea"])
        assert car.name == "32gheap+ea"
        assert car.config_paths == [os.path.join(current_dir, "data", "cars", "v1", "vanilla", "templates")]
        assert car.root_path is None
        assert car.variables == {"heap_size": "32g", "clean_command": "./gradlew clean", "assertions": "true"}
        assert car.root_path is None

    def test_load_car_with_mixin_multiple_config_bases(self):
        car = team.load_car(self.team_dir, ["32gheap", "ea", "verbose"])
        assert car.name == "32gheap+ea+verbose"
        assert car.config_paths == [
            os.path.join(current_dir, "data", "cars", "v1", "vanilla", "templates"),
            os.path.join(current_dir, "data", "cars", "v1", "verbose_logging", "templates"),
        ]
        assert car.root_path is None
        assert car.variables == {"heap_size": "32g", "clean_command": "./gradlew clean", "verbose_logging": "true", "assertions": "true"}

    def test_load_car_with_install_hook(self):
        car = team.load_car(self.team_dir, ["default", "with_hook"], car_params={"data_paths": ["/mnt/disk0", "/mnt/disk1"]})
        assert car.name == "default+with_hook"
        assert car.config_paths == [
            os.path.join(current_dir, "data", "cars", "v1", "vanilla", "templates"),
            os.path.join(current_dir, "data", "cars", "v1", "with_hook", "templates"),
        ]
        assert car.root_path == os.path.join(current_dir, "data", "cars", "v1", "with_hook")
        assert car.variables == {"heap_size": "1g", "clean_command": "./gradlew clean", "data_paths": ["/mnt/disk0", "/mnt/disk1"]}

    def test_load_car_with_multiple_bases_referring_same_install_hook(self):
        car = team.load_car(self.team_dir, ["with_hook", "another_with_hook"])
        assert car.name == "with_hook+another_with_hook"
        assert car.config_paths == [
            os.path.join(current_dir, "data", "cars", "v1", "vanilla", "templates"),
            os.path.join(current_dir, "data", "cars", "v1", "with_hook", "templates"),
            os.path.join(current_dir, "data", "cars", "v1", "verbose_logging", "templates"),
        ]
        assert car.root_path == os.path.join(current_dir, "data", "cars", "v1", "with_hook")
        assert car.variables == {"heap_size": "16g", "clean_command": "./gradlew clean", "verbose_logging": "true"}

    def test_raises_error_on_unknown_car(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"Unknown car \[don_t-know-you\]. List the available cars with [^\s]+ list cars.",
        ):
            team.load_car(self.team_dir, ["don_t-know-you"])

    def test_raises_error_on_empty_config_base(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"At least one config base is required for car \['empty_cfg_base'\]",
        ):
            team.load_car(self.team_dir, ["empty_cfg_base"])

    def test_raises_error_on_missing_config_base(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"At least one config base is required for car \['missing_cfg_base'\]",
        ):
            team.load_car(self.team_dir, ["missing_cfg_base"])

    def test_raises_error_if_more_than_one_different_install_hook(self):
        with pytest.raises(
            exceptions.SystemSetupError,
            match=r"Invalid car: \['multi_hook'\]. Multiple bootstrap hooks are forbidden.",
        ):
            team.load_car(self.team_dir, ["multi_hook"])


class TestPluginLoader:
    def setup_method(self, method):
        self.loader = team.PluginLoader(os.path.join(current_dir, "data"))

    def test_lists_plugins(self):
        assert set(self.loader.plugins()) == {
            team.PluginDescriptor(name="complex-plugin", config="config-a"),
            team.PluginDescriptor(name="complex-plugin", config="config-b"),
            team.PluginDescriptor(name="my-analysis-plugin", core_plugin=True),
            team.PluginDescriptor(name="my-ingest-plugin", core_plugin=True),
            team.PluginDescriptor(name="my-core-plugin-with-config", core_plugin=True),
        }

    def test_loads_core_plugin(self):
        assert self.loader.load_plugin("my-analysis-plugin", config_names=None, plugin_params={"dbg": True}) == team.PluginDescriptor(
            name="my-analysis-plugin", core_plugin=True, variables={"dbg": True}
        )

    def test_loads_core_plugin_with_config(self):
        plugin = self.loader.load_plugin("my-core-plugin-with-config", config_names=None, plugin_params={"dbg": True})
        assert plugin.name == "my-core-plugin-with-config"
        assert plugin.core_plugin

        expected_root_path = os.path.join(current_dir, "data", "plugins", "v1", "my_core_plugin_with_config")

        assert plugin.root_path == expected_root_path
        assert len(plugin.config_paths) == 0

        assert plugin.variables == {
            # from plugin params
            "dbg": True
        }

    def test_cannot_load_plugin_with_missing_config(self):
        with pytest.raises(exceptions.SystemSetupError) as exc:
            self.loader.load_plugin("my-analysis-plugin", ["missing-config"])
        assert re.search(
            r"Plugin \[my-analysis-plugin\] does not provide configuration \[missing-config\]. List the"
            r" available plugins and configurations with [^\s]+ list elasticsearch-plugins "
            r"--distribution-version=VERSION.",
            exc.value.args[0],
        )

    def test_loads_community_plugin_without_configuration(self):
        assert self.loader.load_plugin("my-community-plugin", None) == team.PluginDescriptor("my-community-plugin")

    def test_cannot_load_community_plugin_with_missing_config(self):
        with pytest.raises(exceptions.SystemSetupError) as exc:
            self.loader.load_plugin("my-community-plugin", "some-configuration")
        assert re.search(
            r"Unknown plugin \[my-community-plugin\]. List the available plugins with [^\s]+ list "
            r"elasticsearch-plugins --distribution-version=VERSION.",
            exc.value.args[0],
        )

    def test_loads_configured_plugin(self):
        plugin = self.loader.load_plugin("complex-plugin", ["config-a", "config-b"], plugin_params={"dbg": True})
        assert plugin.name == "complex-plugin"
        assert not plugin.core_plugin
        assert plugin.config == ["config-a", "config-b"]

        expected_root_path = os.path.join(current_dir, "data", "plugins", "v1", "complex_plugin")

        assert plugin.root_path == expected_root_path
        # order does matter here! We should not swap it
        assert plugin.config_paths == [
            os.path.join(expected_root_path, "default", "templates"),
            os.path.join(expected_root_path, "special", "templates"),
        ]

        assert plugin.variables == {
            "foo": "bar",
            "baz": "foo",
            "var": "0",
            "hello": "true",
            # from plugin params
            "dbg": True,
        }


class TestBootstrapHookHandler:
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

        def post_install_hook(self, config_names, variables, **kwargs):
            self.call_counter += variables["increment"]

        def register(self, handler):
            # we can register multiple hooks here
            handler.register(self.phase, self.post_install_hook)
            handler.register(self.phase, self.post_install_hook)

    def test_loads_module(self):
        plugin = team.PluginDescriptor("unittest-plugin")
        hook = self.UnitTestHook()
        handler = team.BootstrapHookHandler(plugin, loader_class=self.UnitTestComponentLoader)

        handler.loader.registration_function = hook
        handler.load()

        handler.invoke("post_install", variables={"increment": 4})

        # we registered our hook twice. Check that it has been called twice.
        assert hook.call_counter == 2 * 4

    def test_cannot_register_for_unknown_phase(self):
        plugin = team.PluginDescriptor("unittest-plugin")
        hook = self.UnitTestHook(phase="this_is_an_unknown_install_phase")
        handler = team.BootstrapHookHandler(plugin, loader_class=self.UnitTestComponentLoader)

        handler.loader.registration_function = hook
        with pytest.raises(exceptions.SystemSetupError) as exc:
            handler.load()
        assert exc.value.args[0] == "Unknown bootstrap phase [this_is_an_unknown_install_phase]. Valid phases are: ['post_install']."
