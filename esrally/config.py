# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import configparser
import logging
import os.path
import shutil
from enum import Enum
from string import Template

from esrally import PROGRAM_NAME, exceptions, paths
from esrally.utils import io


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


class ConfigFile:
    def __init__(self, config_name=None, **kwargs):
        self.config_name = config_name

    @property
    def present(self):
        """
        :return: true iff a config file already exists.
        """
        return os.path.isfile(self.location)

    def load(self):
        config = configparser.ConfigParser()
        config.read(self.location, encoding="utf-8")
        return config

    def store_default_config(self, template_path=None):
        io.ensure_dir(self.config_dir)
        if template_path:
            source_path = template_path
        else:
            source_path = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "rally.ini"))
        with open(self.location, "wt", encoding="utf-8") as target:
            with open(source_path, "rt", encoding="utf-8") as src:
                contents = src.read()
                target.write(Template(contents).substitute(CONFIG_DIR=self.config_dir))

    def store(self, config):
        io.ensure_dir(self.config_dir)
        with open(self.location, "wt", encoding="utf-8") as configfile:
            config.write(configfile)

    def backup(self):
        config_file = self.location
        logging.getLogger(__name__).info("Creating a backup of the current config file at [%s].", config_file)
        shutil.copyfile(config_file, "{}.bak".format(config_file))

    @property
    def config_dir(self):
        return paths.rally_confdir()

    @property
    def location(self):
        if self.config_name:
            config_name_suffix = "-{}".format(self.config_name)
        else:
            config_name_suffix = ""
        return os.path.join(self.config_dir, "rally{}.ini".format(config_name_suffix))


def auto_load_local_config(base_config, additional_sections=None, config_file_class=ConfigFile, **kwargs):
    """
    Loads a node-local configuration based on a ``base_config``. If an appropriate node-local configuration file is present, it will be
    used (and potentially upgraded to the newest config version). Otherwise, a new one will be created and as many settings as possible
    will be reused from the ``base_config``.

    :param base_config: The base config to use.
    :param config_file_class class of the config file to use. Only relevant for testing.
    :param additional_sections: A list of any additional config sections to copy from the base config (will not end up in the config file).
    :return: A fully-configured node local config.
    """
    cfg = Config(config_name=base_config.name, config_file_class=config_file_class, **kwargs)
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

    if additional_sections:
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

    def __init__(self, config_name=None, config_file_class=ConfigFile, **kwargs):
        self.name = config_name
        self.config_file = config_file_class(config_name, **kwargs)
        self._opts = {}
        self._clear_config()

    def add(self, scope, section, key, value):
        """
        Adds or overrides a new configuration property.

        :param scope: The scope of this property. More specific scopes (higher values) override more generic ones (lower values).
        :param section: The configuration section.
        :param key: The configuration key within this section. Same keys in different sections will not collide.
        :param value: The associated value.
        """
        self._opts[self._k(scope, section, key)] = value

    def add_all(self, source, section):
        """
        Adds all config items within the given `section` from the `source` config object.

        :param source: The source config object.
        :param section: A section in the source config object. Ignored if it does not exist.
        """
        # pylint: disable=protected-access
        for k, v in source._opts.items():
            scope, source_section, key = k
            if source_section == section:
                self.add(scope, source_section, key, v)

    def opts(self, section, key, default_value=None, mandatory=True):
        """
        Resolves a configuration property.

        :param section: The configuration section.
        :param key: The configuration key.
        :param default_value: The default value to use for optional properties as a fallback. Default: None
        :param mandatory: Whether a value is expected to exist for the given section and key. Note that the default_value is ignored for
        mandatory properties. It must be ensured that a value exists. Default: True
        :return: The associated value.
        """
        try:
            scope = self._resolve_scope(section, key)
            return self._opts[self._k(scope, section, key)]
        except KeyError:
            if not mandatory:
                return default_value
            else:
                raise exceptions.ConfigError(f"No value for mandatory configuration: section='{section}', key='{key}'")

    def all_opts(self, section):
        """
        Finds all options in a section and returns them in a dict.

        :param section: The configuration section.
        :return: A dict of matching key-value pairs. If the section is not found or no keys are in this section, an empty dict is returned.
        """
        opts_in_section = {}
        scopes_per_key = {}
        for k, v in self._opts.items():
            scope, source_section, key = k
            if source_section == section:
                # check whether it's a new key OR we need to override
                if key not in opts_in_section or scopes_per_key[key].value < scope.value:
                    opts_in_section[key] = v
                    scopes_per_key[key] = scope
        return opts_in_section

    def exists(self, section, key):
        """
        :param section: The configuration section.
        :param key: The configuration key.
        :return: True iff a value for the specified key exists in the specified configuration section.
        """
        return self.opts(section, key, mandatory=False) is not None

    def config_present(self):
        """
        :return: true iff a config file already exists.
        """
        return self.config_file.present

    def install_default_config(self):
        self.config_file.store_default_config()

    def load_config(self, auto_upgrade=False):
        """
        Loads an existing config file.
        """
        self._do_load_config()
        if auto_upgrade and not self.config_compatible():
            self.migrate_config()
            # Reload config after upgrading
            self._do_load_config()

    def _do_load_config(self):
        config = self.config_file.load()
        # It's possible that we just reload the configuration
        self._clear_config()
        self._fill_from_config_file(config)

    def _clear_config(self):
        # This map contains default options that we don't want to sprinkle all over the source code but we don't want users to change
        # them either
        self._opts = {
            (Scope.application, "source", "distribution.dir"): "distributions",
            (Scope.application, "benchmarks", "track.repository.dir"): "tracks",
            (Scope.application, "benchmarks", "track.default.repository"): "default",
            (Scope.application, "provisioning", "node.name.prefix"): "rally-node",
            (Scope.application, "provisioning", "node.http.port"): 39200,
            (Scope.application, "mechanic", "team.repository.dir"): "teams",
            (Scope.application, "mechanic", "team.default.repository"): "default",

        }

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

    # recursively find the most narrow scope for a key
    def _resolve_scope(self, section, key, start_from=Scope.invocation):
        if self._k(start_from, section, key) in self._opts:
            return start_from
        elif start_from == Scope.application:
            return Scope.application
        else:
            # continue search in the enclosing scope
            return self._resolve_scope(section, key, Scope(start_from.value - 1))

    def _k(self, scope, section, key):
        if scope is None or scope == Scope.application:
            return Scope.application, section, key
        else:
            return scope, section, key


def migrate(config_file, current_version, target_version, out=print, i=input):
    logger = logging.getLogger(__name__)
    if current_version == target_version:
        logger.info("Config file is already at version [%s]. Skipping migration.", target_version)
        return
    if current_version < Config.EARLIEST_SUPPORTED_VERSION:
        raise exceptions.ConfigError(f"The config file in {config_file.location} is too old. Please delete it "
                                     f"and reconfigure Rally from scratch with {PROGRAM_NAME} configure.")

    logger.info("Upgrading configuration from version [%s] to [%s].", current_version, target_version)
    # Something is really fishy. We don't want to downgrade the configuration.
    if current_version >= target_version:
        raise exceptions.ConfigError(f"The existing config file is available in a later version already. "
                                     f"Expected version <= [{target_version}] but found [{current_version}]")
    # but first a backup...
    config_file.backup()
    config = config_file.load()

    # *** Add per version migrations here ***

    # all migrations done
    config_file.store(config)
    logger.info("Successfully self-upgraded configuration to version [%s]", target_version)
