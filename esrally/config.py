import os.path
import shutil
import re
import logging
import configparser
from enum import Enum

from esrally.utils import io

logger = logging.getLogger("rally.config")


class ConfigError(BaseException):
    pass


class Scope(Enum):
    # Valid for all benchmarks, typically read from the configuration file
    application = 1
    # Valid for all benchmarks, intended to allow overriding of values in the config file from the command line
    applicationOverride = 2
    # A sole benchmark
    benchmark = 3
    # Single benchmark track setup (e.g. default, multinode, ...)
    trackSetup = 4
    # property for every invocation, i.e. for backtesting
    invocation = 5


class ConfigFile:
    def __init__(self, config_name):
        self.config_name = config_name

    def present(self):
        """
        :return: true iff a config file already exists.
        """
        return os.path.isfile(self.location)

    def load(self, interpolation=configparser.ExtendedInterpolation()):
        config = configparser.ConfigParser(interpolation=interpolation)
        config.read(self.location)
        return config

    def store(self, config):
        io.ensure_dir(self._config_dir())
        with open(self.location, "w") as configfile:
            config.write(configfile)

    def backup(self):
        config_file = self.location
        logger.info("Creating a backup of the current config file at [%s]." % config_file)
        shutil.copyfile(config_file, "%s.bak" % config_file)

    def _config_dir(self):
        return "%s/.rally" % os.getenv("HOME")

    @property
    def location(self):
        if self.config_name:
            config_name_suffix = "-%s" % self.config_name
        else:
            config_name_suffix = ""
        return "%s/rally%s.ini" % (self._config_dir(), config_name_suffix)


class Config:
    CURRENT_CONFIG_VERSION = 3

    ENV_NAME_PATTERN = re.compile("^[a-zA-Z_-]+$")

    PORT_RANGE_PATTERN = re.compile("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$")

    BOOLEAN_PATTERN = re.compile("^(True|true|Yes|yes|t|y|False|false|f|No|no|n)$")

    def _to_bool(self, value):
        return value in ["True", "true", "Yes", "yes", "t", "y"]

    """
    Config is the main entry point to retrieve and set benchmark properties. It provides multiple scopes to allow overriding of values on
    different levels (e.g. a command line flag can override the same configuration property in the config file). These levels are
    transparently resolved when a property is retrieved and the value on the most specific level is returned.
    """

    def __init__(self, config_name=None):
        self._config_file = ConfigFile(config_name)
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
                raise ConfigError("No value for mandatory configuration: section='%s', key='%s'" % (section, key))

    def config_present(self):
        """
        :return: true iff a config file already exists.
        """
        return self._config_file.present()

    def load_config(self):
        """
        Loads an existing config file.
        """
        config = self._config_file.load()
        # It's possible that we just reload the configuration
        self._clear_config()
        self._fill_from_config_file(config)

    def _clear_config(self):
        # This map contains default options that we don't want to sprinkle all over the source code but we don't want users to change
        # them either
        self._opts = {
            "source::distribution.dir": "distributions",
            "build::gradle.tasks.clean": "clean",
            "build::gradle.tasks.package": ":distribution:zip:assemble",
            "build::log.dir": "build",
            "benchmarks::metrics.log.dir": "telemetry",
            # No more specific configuration per benchmark - if needed this has to be put into the track specification
            "benchmarks::index.clients": "8",
            "provisioning::node.name.prefix": "rally-node",
            "provisioning::node.http.port": 39200,
        }

    def _fill_from_config_file(self, config):
        for section in config.sections():
            for key in config[section]:
                self.add(Scope.application, section, key, config[section][key])

    def config_compatible(self):
        return self.CURRENT_CONFIG_VERSION == self._stored_config_version()

    def migrate_config(self):
        # do migration one at a time, starting at the current one...
        current_version = self._stored_config_version()
        # Something is really fishy. We don't want to downgrade the configuration.
        if current_version >= self.CURRENT_CONFIG_VERSION:
            raise ConfigError("The existing config file is available in a later version already. Expected version <= [%s] but found [%s]"
                              % (self.CURRENT_CONFIG_VERSION, current_version))
        logger.info("Upgrading configuration from version [%s] to [%s]." % (current_version, self.CURRENT_CONFIG_VERSION))
        # but first a backup...
        self._config_file.backup()
        config = self._config_file.load(interpolation=None)

        if current_version == 0:
            logger.info("Migrating config from version [0] to [1]")
            current_version = 1
            config["meta"] = {}
            config["meta"]["config.version"] = str(current_version)
            # in version 1 we changed some directories from being absolute to being relative
            config["system"]["log.root.dir"] = "logs"
            config["provisioning"]["local.install.dir"] = "install"
            config["reporting"]["report.base.dir"] = "reports"
        if current_version == 1:
            logger.info("Migrating config from version [1] to [2]")
            current_version = 2
            config["meta"]["config.version"] = str(current_version)
            # Give the user a hint what's going on
            print("Metrics data are now stored in a dedicated Elasticsearch instance. Please provide details below")
            data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = self._ask_data_store()
            config["reporting"]["datastore.host"] = data_store_host
            config["reporting"]["datastore.port"] = data_store_port
            config["reporting"]["datastore.secure"] = data_store_secure
            config["reporting"]["datastore.user"] = data_store_user
            config["reporting"]["datastore.password"] = data_store_password
            env_name = self._ask_env_name()
            config["system"]["env.name"] = env_name
        if current_version == 2:
            logger.info("Migrating config from version [2] to [3]")
            current_version = 3
            config["meta"]["config.version"] = str(current_version)
            # Remove obsolete settings
            config["reporting"].pop("report.base.dir")
            config["reporting"].pop("output.html.report.filename")


            # all migrations done
        self._config_file.store(config)
        logger.info("Successfully self-upgraded configuration to version [%s]" % self.CURRENT_CONFIG_VERSION)

    def _stored_config_version(self):
        return int(self.opts("meta", "config.version", default_value=0, mandatory=False))

    # full_config -> intended for nightlies
    def create_config(self, advanced_config=False):
        """
        Either creates a new configuration file or overwrites an existing one. Will ask the user for input on configurable properties
        and writes them to the configuration file in ~/.rally/rally.ini.

        :param advanced_config: Whether to ask for properties that are not necessary for everyday use (on a dev machine). Default: False.
        """
        if self.config_present():
            print("\nWARNING: Will overwrite existing config file at [%s]\n" % self._config_file.location)
        else:
            print("Rally has not detected any configuration file at %s. It will ask a few questions about the environment.\n\n" %
                  self._config_file.location)

        print("The benchmark root directory contains benchmark data, logs, etc. . It will consume several GB of free space depending on "
              "which benchmarks are executed (expect at least 10 GB).")
        benchmark_root_dir = self._ask_property("Enter the benchmark root directory (will be created automatically)")
        env_name = self._ask_env_name()

        only_binary = self._ask_property("Do you want to benchmark only official binary distributions of Elasticsearch? Note: If you "
                                         "answer with 'Yes' you will not need certain software (like Gradle or git) but you will not "
                                         "be able to benchmark the latest development versions from Elasticsearch unless you reconfigure "
                                         "Rally with esrally --configure again.",
                                         check_pattern=Config.BOOLEAN_PATTERN, default_value="No")

        if not (self._to_bool(only_binary)):
            default_src_dir = "%s/src" % benchmark_root_dir
            source_dir = self._ask_property("Enter the directory where sources are located (your Elasticsearch project directory)",
                                            default_value=default_src_dir)
            # Not everybody might have SSH access. Play safe with the default. It may be slower but this will work for everybody.
            repo_url = self._ask_property("Enter the Elasticsearch repo URL", default_value="https://github.com/elastic/elasticsearch.git")
            default_gradle_location = io.guess_install_location("gradle", fallback="/usr/local/bin/gradle")
            gradle_bin = self._ask_property("Enter the full path to the Gradle binary", default_value=default_gradle_location,
                                            check_path_exists=True)
            if advanced_config:
                default_mvn_location = io.guess_install_location("mvn", fallback="/usr/local/bin/mvn")
                maven_bin = self._ask_property("Enter the full path to the Maven 3 binary", default_value=default_mvn_location,
                                               check_path_exists=True)
            else:
                maven_bin = ""
        default_jdk_8 = io.guess_java_home(major_version=8, fallback="")
        jdk8_home = self._ask_property(
            "Enter the JDK 8 root directory (e.g. something like /Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home on a Mac)",
            default_value=default_jdk_8,
            check_path_exists=True)
        if advanced_config:
            stats_disk_device = self._ask_property("Enter the HDD device name for stats (e.g. /dev/disk1)")
        else:
            stats_disk_device = ""

        data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = self._ask_data_store()

        config = configparser.ConfigParser()
        config["meta"] = {}
        config["meta"]["config.version"] = str(self.CURRENT_CONFIG_VERSION)

        config["system"] = {}
        config["system"]["root.dir"] = benchmark_root_dir
        config["system"]["log.root.dir"] = "logs"
        config["system"]["env.name"] = env_name

        if not (self._to_bool(only_binary)):
            config["source"] = {}
            config["source"]["local.src.dir"] = source_dir
            config["source"]["remote.repo.url"] = repo_url

            config["build"] = {}
            config["build"]["gradle.bin"] = gradle_bin
            config["build"]["maven.bin"] = maven_bin

        config["provisioning"] = {}
        config["provisioning"]["local.install.dir"] = "install"

        config["runtime"] = {}
        config["runtime"]["java8.home"] = jdk8_home

        config["benchmarks"] = {}
        config["benchmarks"]["local.dataset.cache"] = "${system:root.dir}/data"
        config["benchmarks"]["metrics.stats.disk.device"] = stats_disk_device

        config["reporting"] = {}
        config["reporting"]["datastore.host"] = data_store_host
        config["reporting"]["datastore.port"] = data_store_port
        config["reporting"]["datastore.secure"] = data_store_secure
        config["reporting"]["datastore.user"] = data_store_user
        config["reporting"]["datastore.password"] = data_store_password

        self._config_file.store(config)

        print("\nConfiguration successfully written to '%s'. Please rerun esrally now." % self._config_file.location)

    def _ask_data_store(self):
        data_store_host = self._ask_property("Enter the host name of the ES metrics store", default_value="localhost")
        data_store_port = self._ask_property("Enter the port of the ES metrics store", check_pattern=Config.PORT_RANGE_PATTERN)
        data_store_secure = self._ask_property("Use secure connection (True, False)", default_value=False,
                                               check_pattern=Config.BOOLEAN_PATTERN)
        data_store_user = self._ask_property("Username for basic authentication (empty if not needed)", mandatory=False, default_value="")
        data_store_password = self._ask_property("Password for basic authentication (empty if not needed)", mandatory=False,
                                                 default_value="")
        # do an intermediate conversion to bool in order to normalize input
        return data_store_host, data_store_port, str(self._to_bool(data_store_secure)), data_store_user, data_store_password

    def _ask_env_name(self):
        return self._ask_property("Enter a descriptive name for this benchmark environment (ASCII, no spaces)",
                                  check_pattern=Config.ENV_NAME_PATTERN)

    def _ask_property(self, prompt, mandatory=True, check_path_exists=False, check_pattern=None, default_value=None):
        while True:
            if default_value is not None:
                value = input("%s [default: '%s']: " % (prompt, default_value))
            else:
                value = input("%s: " % prompt)

            if not value or value.strip() == "":
                if mandatory and default_value is None:
                    print("  Value is required. Please retry.")
                    continue
                else:
                    print("  Using default value '%s'" % default_value)
                    # this way, we can still check the path...
                    value = default_value

            if check_path_exists and not os.path.exists(value):
                print("'%s' does not exist. Please check and retry." % value)
                continue
            if check_pattern is not None and not check_pattern.match(str(value)):
                print("Input does not match pattern [%s]. Please check and retry." % check_pattern.pattern)
                continue
            print("")
            # user entered a valid value
            return value

    # recursively find the most narrow scope for a key
    def _resolve_scope(self, section, key, start_from=Scope.invocation):
        if self._k(start_from, section, key) in self._opts:
            return start_from
        elif start_from == Scope.application:
            return None
        else:
            # continue search in the enclosing scope
            return self._resolve_scope(section, key, Scope(start_from.value - 1))

    def _k(self, scope, section, key):
        # keep global config keys a bit shorter / nicer for now
        if scope is None or scope == Scope.application:
            return "%s::%s" % (section, key)
        else:
            return "%s::%s::%s" % (scope.name, section, key)
