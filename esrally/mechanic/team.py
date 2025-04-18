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

import configparser
import logging
import os
from collections.abc import Collection, Iterator, Mapping, MutableMapping
from enum import Enum
from types import ModuleType
from typing import Any, Callable, Optional, Union

import tabulate

from esrally import PROGRAM_NAME, config, exceptions, types
from esrally.utils import console, io, modules, repo

TEAM_FORMAT_VERSION = 1


def _path_for(team_root_path: str, team_member_type: str) -> str:
    root_path = os.path.join(team_root_path, team_member_type, f"v{TEAM_FORMAT_VERSION}")
    if not os.path.exists(root_path):
        raise exceptions.SystemSetupError(f"Path {root_path} for {team_member_type} does not exist.")
    return root_path


def list_cars(cfg: types.Config) -> None:
    loader = CarLoader(team_path(cfg))
    cars = []
    for name in loader.car_names():
        cars.append(loader.load_car(name))
    # first by type, then by name (we need to run the sort in reverse for that)
    # idiomatic way according to https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts
    cars = sorted(sorted(cars, key=lambda c: c.name), key=lambda c: c.type)
    console.println("Available cars:\n")
    console.println(tabulate.tabulate([[c.name, c.type, c.description] for c in cars], headers=["Name", "Type", "Description"]))


def load_car(repo: str, name: Collection[str], car_params: Optional[Mapping] = None) -> "Car":
    class Component:
        def __init__(self, root_path: str, entry_point: str):
            self.root_path = root_path
            self.entry_point = entry_point

    root_paths = []
    # preserve order as we append to existing config files later during provisioning.
    all_config_paths = []
    all_config_base_vars: MutableMapping[str, str] = {}
    all_car_vars: MutableMapping[str, str] = {}

    for n in name:
        descriptor = CarLoader(repo).load_car(n, car_params)
        for p in descriptor.config_paths:
            if p not in all_config_paths:
                all_config_paths.append(p)
        for p in descriptor.root_paths:
            # probe whether we have a root path
            if BootstrapHookHandler(Component(root_path=p, entry_point=Car.entry_point)).can_load():
                if p not in root_paths:
                    root_paths.append(p)
        all_config_base_vars.update(descriptor.config_base_variables)
        all_car_vars.update(descriptor.variables)

    if len(all_config_paths) == 0:
        raise exceptions.SystemSetupError(f"At least one config base is required for car {name}")
    variables: MutableMapping[str, str] = {}
    # car variables *always* take precedence over config base variables
    variables.update(all_config_base_vars)
    variables.update(all_car_vars)

    return Car(name, root_paths, all_config_paths, variables)


def list_plugins(cfg: types.Config) -> None:
    plugins = PluginLoader(team_path(cfg)).plugins()
    if plugins:
        console.println("Available Elasticsearch plugins:\n")
        console.println(tabulate.tabulate([[p.name, p.config] for p in plugins], headers=["Name", "Configuration"]))
    else:
        console.println("No Elasticsearch plugins are available.\n")


def load_plugin(
    repo: str, name: str, config_names: Optional[Collection[str]], plugin_params: Optional[Mapping[str, str]] = None
) -> "PluginDescriptor":
    return PluginLoader(repo).load_plugin(name, config_names, plugin_params)


def load_plugins(
    repo: str, plugin_names: Collection[str], plugin_params: Optional[Mapping[str, str]] = None
) -> Collection["PluginDescriptor"]:
    def name_and_config(p: str) -> tuple[str, Optional[Collection[str]]]:
        plugin_spec = p.split(":")
        if len(plugin_spec) == 1:
            return plugin_spec[0], None
        elif len(plugin_spec) == 2:
            return plugin_spec[0], plugin_spec[1].split("+")
        else:
            raise ValueError("Unrecognized plugin specification [%s]. Use either 'PLUGIN_NAME' or 'PLUGIN_NAME:PLUGIN_CONFIG'." % p)

    plugins = []
    if plugin_names:
        for plugin in plugin_names:
            plugin_name, plugin_config = name_and_config(plugin)
            plugins.append(load_plugin(repo, plugin_name, plugin_config, plugin_params))
    return plugins


def team_path(cfg: types.Config) -> str:
    root_path = cfg.opts("mechanic", "team.path", mandatory=False)
    if root_path:
        return root_path
    else:
        distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
        repo_name = cfg.opts("mechanic", "repository.name")
        repo_revision = cfg.opts("mechanic", "repository.revision")
        offline = cfg.opts("system", "offline.mode")
        # TODO remove the below ignore when introducing LiteralString on Python 3.11+
        remote_url = cfg.opts("teams", "%s.url" % repo_name, mandatory=False)  # type: ignore[arg-type]
        root = cfg.opts("node", "root.dir")
        team_repositories = cfg.opts("mechanic", "team.repository.dir")
        teams_dir = os.path.join(root, team_repositories)

        current_team_repo = repo.RallyRepository(remote_url, teams_dir, repo_name, "teams", offline)
        if repo_revision:
            current_team_repo.checkout(repo_revision)
        else:
            current_team_repo.update(distribution_version)
            cfg.add(config.Scope.applicationOverride, "mechanic", "repository.revision", current_team_repo.revision)
        return current_team_repo.repo_dir


class CarLoader:
    def __init__(self, team_root_path: str):
        self.cars_dir = _path_for(team_root_path, "cars")
        self.logger = logging.getLogger(__name__)

    def car_names(self) -> Iterator[str]:
        def __car_name(path: str) -> str:
            p, _ = io.splitext(path)
            return io.basename(p)

        def __is_car(path: str) -> bool:
            _, extension = io.splitext(path)
            return extension == ".ini"

        return map(__car_name, filter(__is_car, os.listdir(self.cars_dir)))

    def _car_file(self, name: str) -> str:
        return os.path.join(self.cars_dir, f"{name}.ini")

    def load_car(self, name: str, car_params: Optional[Mapping[str, Any]] = None) -> "CarDescriptor":
        car_config_file = self._car_file(name)
        if not io.exists(car_config_file):
            raise exceptions.SystemSetupError(f"Unknown car [{name}]. List the available cars with {PROGRAM_NAME} list cars.")
        config = self._config_loader(car_config_file)
        root_paths: list[str] = []
        config_paths: list[str] = []
        config_base_vars: MutableMapping[str, Any] = {}

        description = self._value(config, ["meta", "description"], default="")
        assert isinstance(description, str), f"Car [{name}] defines an invalid description [{description}]."

        car_type = self._value(config, ["meta", "type"], default="car")
        assert isinstance(car_type, str), f"Car [{name}] defines an invalid type [{car_type}]."

        config_base = self._value(config, ["config", "base"], default="")
        assert config_base is not None, f"Car [{name}] does not define a config base."
        assert isinstance(config_base, str), f"Car [{name}] defines an invalid config base [{config_base}]."
        config_bases = config_base.split(",")

        for base in config_bases:
            if base:
                root_path = os.path.join(self.cars_dir, base)
                root_paths.append(root_path)
                config_paths.append(os.path.join(root_path, "templates"))
                config_file = os.path.join(root_path, "config.ini")
                if io.exists(config_file):
                    base_config = self._config_loader(config_file)
                    self._copy_section(base_config, "variables", config_base_vars)

        # it's possible that some cars don't have a config base, e.g. mixins which only override variables
        if len(config_paths) == 0:
            self.logger.info("Car [%s] does not define any config paths. Assuming that it is used as a mixin.", name)
        variables = self._copy_section(config, "variables", {})
        # add all car params here to override any defaults
        if car_params:
            variables.update(car_params)

        return CarDescriptor(name, description, car_type, root_paths, config_paths, config_base_vars, variables)

    def _config_loader(self, file_name: str) -> "configparser.ConfigParser":
        config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # Do not modify the case of option keys but read them as is
        config.optionxform = lambda optionstr: optionstr  # type: ignore[method-assign]
        config.read(file_name)
        return config

    def _value(
        self, cfg: "configparser.ConfigParser", section_path: Union[str, Collection[str]], default: Optional[str] = None
    ) -> Optional[Union[str, Mapping[str, Any]]]:
        path: Collection[str] = [section_path] if (isinstance(section_path, str)) else section_path
        current_cfg: Union["configparser.ConfigParser", Mapping[str, Any], str] = cfg
        for k in path:
            if not isinstance(current_cfg, str) and k in current_cfg:
                current_cfg = current_cfg[k]
            else:
                return default
        return current_cfg

    def _copy_section(self, cfg: "configparser.ConfigParser", section: str, target: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        if section in cfg.sections():
            for k, v in cfg[section].items():
                target[k] = v
        return target


class CarDescriptor:
    def __init__(
        self,
        name: str,
        description: str,
        type: str,
        root_paths: Collection[str],
        config_paths: Collection[str],
        config_base_variables: Mapping[str, str],
        variables: Mapping[str, str],
    ):
        self.name = name
        self.description = description
        self.type = type
        self.root_paths = root_paths
        self.config_paths = config_paths
        self.config_base_variables = config_base_variables
        self.variables = variables

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.name == other.name


class Car:
    # name of the initial Python file to load for cars.
    entry_point = "config"

    def __init__(
        self,
        names: Collection[str],
        root_path: Union[None, str, Collection[str]],
        config_paths: Collection[str],
        variables: Optional[Mapping[str, Any]] = None,
    ):
        """
        Creates new settings for a benchmark candidate.

        :param names: Descriptive name(s) for this car.
        :param root_path: The root path(s) from which bootstrap hooks should be loaded if any. May be ``[]``.
        :param config_paths: A non-empty list of paths where the raw config can be found.
        :param variables: A dict containing variable definitions that need to be replaced.
        """
        if variables is None:
            variables = {}
        if isinstance(names, str):
            self.names: Collection[str] = [names]
        else:
            self.names = names

        if root_path is None:
            self.root_path: Collection[str] = []
        elif isinstance(root_path, str):
            self.root_path = [root_path]
        else:
            self.root_path = root_path
        self.config_paths = config_paths
        self.variables = variables

    def mandatory_var(self, name: str) -> str:
        try:
            return self.variables[name]
        except KeyError:
            raise exceptions.SystemSetupError(f'Car "{self.name}" requires config key "{name}"')

    @property
    def name(self) -> str:
        return "+".join(self.names)

    # Adapter method for BootstrapHookHandler
    @property
    def config(self) -> str:
        return self.name

    @property
    def safe_name(self) -> str:
        return "_".join(self.names)

    def __str__(self) -> str:
        return self.name


class PluginLoader:
    def __init__(self, team_root_path: str):
        self.plugins_root_path = _path_for(team_root_path, "plugins")
        self.logger = logging.getLogger(__name__)

    def plugins(self, variables: Optional[Mapping[str, str]] = None) -> list["PluginDescriptor"]:
        known_plugins = self._core_plugins(variables) + self._configured_plugins(variables)
        sorted(known_plugins, key=lambda p: p.name)
        return known_plugins

    def _core_plugins(self, variables: Optional[Mapping[str, str]] = None) -> list["PluginDescriptor"]:
        core_plugins = []
        core_plugins_path = os.path.join(self.plugins_root_path, "core-plugins.txt")
        if os.path.exists(core_plugins_path):
            with open(core_plugins_path, encoding="utf-8") as f:
                for line in f:
                    if not line.startswith("#"):
                        # be forward compatible and allow additional values (comma-separated). At the moment, we only use the plugin name.
                        values = line.strip().split(",")
                        core_plugins.append(PluginDescriptor(name=values[0], core_plugin=True, variables=variables))
        return core_plugins

    def _configured_plugins(self, variables: Optional[Mapping[str, str]] = None) -> list["PluginDescriptor"]:
        configured_plugins = []
        # each directory is a plugin, each .ini is a config (just go one level deep)
        for entry in os.listdir(self.plugins_root_path):
            plugin_path = os.path.join(self.plugins_root_path, entry)
            if os.path.isdir(plugin_path):
                for child_entry in os.listdir(plugin_path):
                    if os.path.isfile(os.path.join(plugin_path, child_entry)) and io.has_extension(child_entry, ".ini"):
                        f, _ = io.splitext(child_entry)
                        plugin_name = self._file_to_plugin_name(entry)
                        config = io.basename(f)
                        configured_plugins.append(PluginDescriptor(name=plugin_name, config=config, variables=variables))
        return configured_plugins

    def _plugin_file(self, name: str, config: str) -> str:
        return os.path.join(self._plugin_root_path(name), "%s.ini" % config)

    def _plugin_root_path(self, name: str) -> str:
        return os.path.join(self.plugins_root_path, self._plugin_name_to_file(name))

    # As we allow to store Python files in the plugin directory and the plugin directory also serves as the root path of the corresponding
    # module, we need to adhere to the Python restrictions here. For us, this is that hyphens in module names are not allowed. Hence, we
    # need to switch from underscores to hyphens and vice versa.
    #
    # We are implicitly assuming that plugin names stick to the convention of hyphen separation to simplify implementation and usage a bit.
    def _file_to_plugin_name(self, file_name: str) -> str:
        return file_name.replace("_", "-")

    def _plugin_name_to_file(self, plugin_name: str) -> str:
        return plugin_name.replace("-", "_")

    def _core_plugin(self, name: str, variables: Optional[Mapping[str, str]] = None) -> Optional["PluginDescriptor"]:
        return next((p for p in self._core_plugins(variables) if p.name == name and p.config is None), None)

    def load_plugin(
        self, name: str, config_names: Optional[Collection[str]], plugin_params: Optional[Mapping[str, str]] = None
    ) -> "PluginDescriptor":
        if config_names is not None:
            self.logger.info("Loading plugin [%s] with configuration(s) [%s].", name, config_names)
        else:
            self.logger.info("Loading plugin [%s] with default configuration.", name)

        root_path = self._plugin_root_path(name)
        # used to determine whether this is a core plugin
        core_plugin = self._core_plugin(name)
        if not config_names:
            # maybe we only have a config folder but nothing else (e.g. if there is only an install hook)
            if io.exists(root_path):
                return PluginDescriptor(
                    name=name, core_plugin=core_plugin is not None, config=config_names, root_path=root_path, variables=plugin_params
                )
            else:
                if core_plugin:
                    return core_plugin
                # If we just have a plugin name then we assume that this is a community plugin and the user has specified a download URL
                else:
                    self.logger.info(
                        "The plugin [%s] is neither a configured nor an official plugin. Assuming that this is a community "
                        "plugin not requiring any configuration and you have set a proper download URL.",
                        name,
                    )
                    return PluginDescriptor(name, variables=plugin_params)
        else:
            variables = {}
            config_paths = []
            # used for deduplication
            known_config_bases = set()

            for config_name in config_names:
                config_file = self._plugin_file(name, config_name)
                # Do we have an explicit configuration for this plugin?
                if not io.exists(config_file):
                    if core_plugin:
                        raise exceptions.SystemSetupError(
                            "Plugin [%s] does not provide configuration [%s]. List the available plugins "
                            "and configurations with %s list elasticsearch-plugins "
                            "--distribution-version=VERSION." % (name, config_name, PROGRAM_NAME)
                        )
                    raise exceptions.SystemSetupError(
                        "Unknown plugin [%s]. List the available plugins with %s list "
                        "elasticsearch-plugins --distribution-version=VERSION." % (name, PROGRAM_NAME)
                    )

                config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
                # Do not modify the case of option keys but read them as is
                config.optionxform = lambda optionstr: optionstr  # type: ignore[method-assign]
                config.read(config_file)
                if "config" in config and "base" in config["config"]:
                    config_bases = config["config"]["base"].split(",")
                    for base in config_bases:
                        if base and base not in known_config_bases:
                            config_paths.append(os.path.join(root_path, base, "templates"))
                        known_config_bases.add(base)

                if "variables" in config.sections():
                    for k, v in config["variables"].items():
                        variables[k] = v
                # add all plugin params here to override any defaults
                if plugin_params:
                    variables.update(plugin_params)

            # maybe one of the configs is really just for providing variables. However, we still require one config base overall.
            if len(config_paths) == 0:
                raise exceptions.SystemSetupError("At least one config base is required for plugin [%s]" % name)
            return PluginDescriptor(
                name=name,
                core_plugin=core_plugin is not None,
                config=config_names,
                root_path=root_path,
                config_paths=config_paths,
                variables=variables,
            )


class PluginDescriptor:
    # name of the initial Python file to load for plugins.
    entry_point = "plugin"

    def __init__(
        self,
        name: str,
        core_plugin: bool = False,
        config: Optional[Collection[str]] = None,
        root_path: Optional[str] = None,
        config_paths: Optional[Collection[str]] = None,
        variables: Optional[Mapping[str, Any]] = None,
    ):
        if config_paths is None:
            config_paths = []
        if variables is None:
            variables = {}
        self.name = name
        self.core_plugin = core_plugin
        self.config = config
        self.root_path = root_path
        self.config_paths = config_paths
        self.variables = variables

    def __str__(self) -> str:
        return f"Plugin descriptor for [{self.name}]"

    def __repr__(self) -> str:
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    @property
    def moved_to_module(self) -> bool:
        # For a BWC escape hatch we first check if the plugin is listed in rally-teams' "core-plugin.txt",
        # thus allowing users to override the teams path or revision to include the repository-s3/azure/gcs plugins in
        # "core-plugin.txt"
        # TODO: https://github.com/elastic/rally/issues/1622
        return self.name in ["repository-s3", "repository-gcs", "repository-azure"] and not self.core_plugin

    def __hash__(self) -> int:
        return hash(self.name) ^ hash(self.config) ^ hash(self.core_plugin)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and (self.name, self.config, self.core_plugin) == (other.name, other.config, other.core_plugin)


class BootstrapPhase(Enum):
    post_install = 10

    @classmethod
    def valid(cls, name: str) -> bool:
        for n in BootstrapPhase.names():
            if n == name:
                return True
        return False

    @classmethod
    def names(cls) -> Collection[str]:
        return [p.name for p in list(BootstrapPhase)]


class BootstrapHookHandler:
    """
    Responsible for loading and executing component-specific intitialization code.
    """

    def __init__(self, component: Any, loader_class: Callable = modules.ComponentLoader):
        """
        Creates a new BootstrapHookHandler.

        :param component: The component that should be loaded. In practice, this is a PluginDescriptor or a Car instance.
        :param loader_class: The implementation that loads the provided component's code.
        """
        self.component = component
        # Don't allow the loader to recurse. The subdirectories may contain Elasticsearch specific files which we do not want to add to
        # Rally's Python load path. We may need to define a more advanced strategy in the future.
        if isinstance(self.component.root_path, list):
            root_path = self.component.root_path
        else:
            root_path = [self.component.root_path]
        self.loader = loader_class(root_path=root_path, component_entry_point=self.component.entry_point, recurse=False)
        self.hooks: MutableMapping[str, list[Callable]] = {}
        self.logger = logging.getLogger(__name__)

    def can_load(self) -> bool:
        return self.loader.can_load()

    def load(self) -> None:
        root_modules: Collection[ModuleType] = self.loader.load()
        try:
            # every module needs to have a register() method
            for module in root_modules:
                module.register(self)
        except exceptions.RallyError:
            # just pass our own exceptions transparently.
            raise
        except BaseException:
            msg = f"Could not load bootstrap hooks in [{self.loader.root_path}]"
            self.logger.exception(msg)
            raise exceptions.SystemSetupError(msg)

    def register(self, phase: str, hook: Callable) -> None:
        self.logger.info("Registering bootstrap hook [%s] for phase [%s] in component [%s]", hook.__name__, phase, self.component.name)
        if not BootstrapPhase.valid(phase):
            raise exceptions.SystemSetupError(f"Unknown bootstrap phase [{phase}]. Valid phases are: {BootstrapPhase.names()}.")
        if phase not in self.hooks:
            empty: list[Callable] = []
            self.hooks[phase] = empty
        self.hooks[phase].append(hook)

    def invoke(self, phase: str, **kwargs: Mapping[str, Any]) -> None:
        if phase in self.hooks:
            self.logger.info("Invoking phase [%s] for component [%s] in config [%s]", phase, self.component.name, self.component.config)
            for hook in self.hooks[phase]:
                self.logger.info("Invoking bootstrap hook [%s].", hook.__name__)
                # hooks should only take keyword arguments to be forwards compatible with Rally!
                hook(config_names=self.component.config, **kwargs)
        else:
            self.logger.debug(
                "Component [%s] in config [%s] has no hook registered for phase [%s].", self.component.name, self.component.config, phase
            )
