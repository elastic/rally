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
from __future__ import annotations

import collections
import configparser
import logging
import os.path
import shutil
from enum import Enum
from string import Template
from typing import Any, Literal, NamedTuple

from esrally import PROGRAM_NAME, exceptions
from esrally.utils import convert, io


class Scope(Enum):
    # Valid for all benchmarks, typically read from the configuration file
    application = 1
    # Valid for all benchmarks, intended to allow overriding of values in the config file from the command line
    applicationOverride = 2
    # A sole benchmark
    benchmark = 3
    # Single benchmark track setup (e.g. default, multinode, ...)
    challenge = 4
    # property for every invocation, i.e. for backtesting
    invocation = 5


Section = Literal[
    "benchmarks",
    "client",
    "defaults",
    "distributions",
    "driver",
    "generator",
    "mechanic",
    "meta",
    "no_copy",
    "node",
    "provisioning",
    "race",
    "reporting",
    "source",
    "system",
    "teams",
    "telemetry",
    "tests",
    "track",
    "tracks",
    "unit-test",
]
Key = Literal[
    "add.chart_name",
    "add.chart_type",
    "add.config.option",
    "add.message",
    "add.race_timestamp",
    "admin.dry_run",
    "admin.track",
    "assertions",
    "async.debug",
    "available.cores",
    "batch_size",
    "build.type",
    "cache",
    "cache.days",
    "car.names",
    "car.params",
    "car.plugins",
    "challenge.name",
    "challenge.root.dir",
    "cluster.name",
    "config.version",
    "data_streams",
    "datastore.host",
    "datastore.number_of_replicas",
    "datastore.number_of_shards",
    "datastore.overwrite_existing_templates",
    "datastore.password",
    "datastore.port",
    "datastore.probe.cluster_version",
    "datastore.secure",
    "datastore.ssl.certificate_authorities",
    "datastore.ssl.verification_mode",
    "datastore.type",
    "datastore.user",
    "delete.config.option",
    "delete.id",
    "devices",
    "distribution.dir",
    "distribution.flavor",
    "distribution.repository",
    "distribution.version",
    "elasticsearch.src.subdir",
    "env.name",
    "exclude.tasks",
    "format",
    "hosts",
    "include.tasks",
    "indices",
    "install.id",
    "list.challenge",
    "list.config.option",
    "list.from_date",
    "list.max_results",
    "list.races.benchmark_name",
    "list.to_date",
    "load_driver_hosts",
    "local.dataset.cache",
    "master.nodes",
    "metrics.log.dir",
    "metrics.request.downsample.factor",
    "metrics.url",
    "network.host",
    "network.http.port",
    "node.http.port",
    "node.ids",
    "node.name",
    "node.name.prefix",
    "numbers.align",
    "offline.mode",
    "on.error",
    "options",
    "other.key",
    "output.path",
    "output.processingtime",
    "params",
    "passenv",
    "pipeline",
    "plugin.community-plugin.src.dir",
    "plugin.community-plugin.src.subdir",
    "plugin.params",
    "preserve.install",
    "preserve_benchmark_candidate",
    "private.url",
    "profiling",
    "quiet.mode",
    "race.id",
    "rally.cwd",
    "rally.root",
    "release.cache",
    "release.url",
    "remote.benchmarking.supported",
    "remote.repo.url",
    "repository.name",
    "repository.revision",
    "root.dir",
    "runtime.jdk",
    "sample.key",
    "sample.property",
    "sample.queue.size",
    "seed.hosts",
    "serverless.mode",
    "serverless.operator",
    "skip.rest.api.check",
    "snapshot.cache",
    "source.build.method",
    "source.revision",
    "src.root.dir",
    "target.arch",
    "target.os",
    "team.default.repository",
    "team.path",
    "team.repository.dir",
    "test.mode.enabled",
    "time.start",
    "track.default.repository",
    "track.name",
    "track.path",
    "track.repository.dir",
    "user.tags",
    "values",
]


class ConfigFile:
    def __init__(self, config_name: str | None = None):
        self.config_name = config_name

    @property
    def present(self):
        """
        :return: true iff a config file already exists.
        """
        return os.path.isfile(self.location)

    def load(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        with open(self.location, encoding="utf-8") as src:
            contents = src.read()
        contents = Template(contents).substitute(CONFIG_DIR=self.config_dir)
        config.read_string(contents, source=self.location)
        return config

    def store_default_config(self, template_path: str | None = None) -> None:
        io.ensure_dir(self.config_dir)
        if template_path:
            source_path = template_path
        else:
            source_path = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "rally.ini"))
        with open(self.location, "w", encoding="utf-8") as target:
            with open(source_path, encoding="utf-8") as src:
                contents = src.read()
                target.write(contents)

    def store(self, config: configparser.ConfigParser) -> None:
        io.ensure_dir(self.config_dir)
        with open(self.location, "w", encoding="utf-8") as configfile:
            config.write(configfile)

    def backup(self):
        config_file = self.location
        logging.getLogger(__name__).info("Creating a backup of the current config file at [%s].", config_file)
        shutil.copyfile(config_file, f"{config_file}.bak")

    @property
    def config_dir(self):
        return rally_confdir()

    @property
    def location(self):
        if self.config_name:
            config_name_suffix = f"-{self.config_name}"
        else:
            config_name_suffix = ""
        return os.path.join(self.config_dir, f"rally{config_name_suffix}.ini")


def auto_load_local_config(
    base_config: Config, additional_sections: list[Section] | None = None, config_file: ConfigFile | None = None
) -> Config:
    """
    Loads a node-local configuration based on a ``base_config``. If an appropriate node-local configuration file is present, it will be
    used (and potentially upgraded to the newest config version). Otherwise, a new one will be created and as many settings as possible
    will be reused from the ``base_config``.

    :param base_config: The base config to use.
    :param config_file config file to use to import data from. Only relevant for testing.
    :param additional_sections: A list of any additional config sections to copy from the base config (will not end up in the config file).
    :return: A fully-configured node local config.
    """
    cfg = Config(config_name=base_config.name, config_file=config_file)
    if not cfg.config_present():
        cfg.install_default_config()

    cfg.load_config(auto_upgrade=True)
    # we override our some configuration with the one from the coordinator because it may contain more entries and we should be
    # consistent across all nodes here.
    cfg.add_all(base_config, "reporting")
    cfg.add_all(base_config, "tracks")
    cfg.add_all(base_config, "teams")
    cfg.add_all(base_config, "distributions")
    cfg.add_all(base_config, "defaults")
    # needed e.g. for "time.start"
    cfg.add_all(base_config, "system")

    if additional_sections is not None:
        for section in additional_sections:
            cfg.add_all(base_config, section)
    return cfg


class Config:
    EARLIEST_SUPPORTED_VERSION = 17

    CURRENT_CONFIG_VERSION = 17

    """
    Config is the main entry point to retrieve and set benchmark properties. It provides multiple scopes to allow overriding of values on
    different levels (e.g. a command line flag can override the same configuration property in the config file). These levels are
    transparently resolved when a property is retrieved and the value on the most specific level is returned.
    """

    def __init__(self, config_name: str | None = None, config_file: ConfigFile | None = None):
        self.name = config_name
        if config_file is None:
            config_file = ConfigFile(config_name)
        self.config_file = config_file
        self._opts = collections.defaultdict[Section, dict[Key, _V]](dict)
        self._clear_config()

    def add(self, scope: Scope | None, section: Section, key: Key, value: Any) -> None:
        """
        Adds or overrides a new configuration property.

        :param scope: The scope of this property. More specific scopes (higher values) override more generic ones (lower values).
        :param section: The configuration section.
        :param key: The configuration key within this section. Same keys in different sections will not collide.
        :param value: The associated value.
        """
        if scope is None:
            scope = Scope.application
        v = self._opts[section].get(key)
        if v is None or v.scope.value <= scope.value:
            self._opts[section][key] = _V(value, scope)

    def add_all(self, source: Config, section: Section) -> None:
        """
        Adds all config items within the given `section` from the `source` config object.

        :param source: The source config object.
        :param section: A section in the source config object. Ignored if it does not exist.
        """
        # pylint: disable=protected-access
        for key, v in source._opts[section].items():
            self.add(v.scope, section, key, v.value)

    def opts(self, section: Section, key: Key, default_value: Any = None, mandatory: bool = True) -> Any:
        """
        Resolves a configuration property.

        :param section: The configuration section.
        :param key: The configuration key.
        :param default_value: The default value to use for optional properties as a fallback. Default: None
        :param mandatory: Whether a value is expected to exist for the given section and key. Note that the default_value is ignored for
        mandatory properties. It must be ensured that a value exists. Default: True
        :return: The associated value.
        """
        v = self._opts[section].get(key)
        if v is None:
            if mandatory:
                raise exceptions.ConfigError(f"No value for mandatory configuration: section='{section}', key='{key}'")
            return default_value
        return v.value

    def boolean(self, section: Section, key: Key, default: bool | None = None) -> bool:
        """It reads a boolean value from the configuration.

        :param section: section represents the section name in the configuration.
        :param key: section represents the option name in the configuration.
        :param default: default specifies a value to be returned when any is found in the configuration. If it is None
            and any is found in the configuration then ConfigError is raised.
        :return: the boolean value if found in the configuration or the default value otherwise if not None.
        """
        return convert.to_bool(self.opts(section=section, key=key, default_value=default, mandatory=default is None))

    def all_opts(self, section: Section) -> dict[Key, Any]:
        """
        Finds all options in a section and returns them in a dict.

        :param section: The configuration section.
        :return: A dict of matching key-value pairs. If the section is not found or no keys are in this section, an empty dict is returned.
        """
        return {k: v.value for k, v in self._opts[section].items()}

    def exists(self, section: Section, key: Key) -> bool:
        """
        :param section: The configuration section.
        :param key: The configuration key.
        :return: True iff a value for the specified key exists in the specified configuration section.
        """
        return key in self._opts[section]

    def config_present(self) -> bool:
        """
        :return: true iff a config file already exists.
        """
        return self.config_file.present

    def install_default_config(self) -> None:
        self.config_file.store_default_config()

    def load_config(self, auto_upgrade: bool = False) -> None:
        """
        Loads an existing config file.
        """
        self._do_load_config()
        if auto_upgrade and not self.config_compatible():
            self.migrate_config()
            # Reload config after upgrading
            self._do_load_config()

    def _do_load_config(self) -> None:
        config = self.config_file.load()
        # It's possible that we just reload the configuration
        self._clear_config()
        self._fill_from_config_file(config)

    def _clear_config(self) -> None:
        # This map contains default options that we don't want to sprinkle all over the source code but we don't want users to change
        # them either
        self._opts.clear()
        self.add(Scope.application, "source", "distribution.dir", "distributions")
        self.add(Scope.application, "benchmarks", "track.repository.dir", "tracks")
        self.add(Scope.application, "benchmarks", "track.default.repository", "default")
        self.add(Scope.application, "provisioning", "node.name.prefix", "rally-node")
        self.add(Scope.application, "provisioning", "node.http.port", 39200)
        self.add(Scope.application, "mechanic", "team.repository.dir", "teams")
        self.add(Scope.application, "mechanic", "team.default.repository", "default")

    def _fill_from_config_file(self, config):
        for section in config.sections():
            for key in config[section]:
                self.add(Scope.application, section, key, config[section][key])

    def config_compatible(self):
        return self.CURRENT_CONFIG_VERSION == self._stored_config_version()

    def migrate_config(self):
        migrate(self.config_file, self._stored_config_version(), Config.CURRENT_CONFIG_VERSION)

    def _stored_config_version(self):
        return int(self.opts("meta", "config.version", default_value=0, mandatory=False))


def migrate(config_file, current_version, target_version, out=print, i=input):
    logger = logging.getLogger(__name__)
    if current_version == target_version:
        logger.info("Config file is already at version [%s]. Skipping migration.", target_version)
        return
    if current_version < Config.EARLIEST_SUPPORTED_VERSION:
        raise exceptions.ConfigError(
            f"The config file in {config_file.location} is too old. Please delete it "
            f"and reconfigure Rally from scratch with {PROGRAM_NAME} configure."
        )

    logger.info("Upgrading configuration from version [%s] to [%s].", current_version, target_version)
    # Something is really fishy. We don't want to downgrade the configuration.
    if current_version >= target_version:
        raise exceptions.ConfigError(
            f"The existing config file is available in a later version already. "
            f"Expected version <= [{target_version}] but found [{current_version}]"
        )
    # but first a backup...
    config_file.backup()
    config = config_file.load()

    # *** Add per version migrations here ***

    # all migrations done
    config_file.store(config)
    logger.info("Successfully self-upgraded configuration to version [%s]", target_version)


class _K(NamedTuple):
    section: Section
    key: Key


class _V(NamedTuple):
    value: Any
    scope: Scope = Scope.application


def rally_confdir():
    default_home = os.path.expanduser("~")
    return os.path.join(os.getenv("RALLY_HOME", default_home), ".rally")
