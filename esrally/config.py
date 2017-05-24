import configparser
import getpass
import logging
import os.path
import re
import shutil
from enum import Enum

from esrally import time, PROGRAM_NAME, DOC_LINK
from esrally.utils import io, git, console, convert

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
    challenge = 4
    # property for every invocation, i.e. for backtesting
    invocation = 5


class ConfigFile:
    def __init__(self, config_name=None):
        self.config_name = config_name

    @property
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
        io.ensure_dir(self.config_dir)
        with open(self.location, "w") as configfile:
            config.write(configfile)

    def backup(self):
        config_file = self.location
        logger.info("Creating a backup of the current config file at [%s]." % config_file)
        shutil.copyfile(config_file, "%s.bak" % config_file)

    @property
    def config_dir(self):
        return "%s/.rally" % os.path.expanduser("~")

    @property
    def location(self):
        if self.config_name:
            config_name_suffix = "-%s" % self.config_name
        else:
            config_name_suffix = ""
        return "%s/rally%s.ini" % (self.config_dir, config_name_suffix)


class Config:
    CURRENT_CONFIG_VERSION = 8

    """
    Config is the main entry point to retrieve and set benchmark properties. It provides multiple scopes to allow overriding of values on
    different levels (e.g. a command line flag can override the same configuration property in the config file). These levels are
    transparently resolved when a property is retrieved and the value on the most specific level is returned.
    """

    def __init__(self, config_name=None, config_file_class=ConfigFile):
        self.name = config_name
        self.config_file = config_file_class(config_name)
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
                raise ConfigError("No value for mandatory configuration: section='%s', key='%s'" % (section, key))

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

    def load_config(self):
        """
        Loads an existing config file.
        """
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


class ConfigFactory:
    ENV_NAME_PATTERN = re.compile("^[a-zA-Z_-]+$")

    PORT_RANGE_PATTERN = re.compile("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$")

    BOOLEAN_PATTERN = re.compile("^(True|true|Yes|yes|t|y|False|false|f|No|no|n)$")

    def __init__(self, i=input, sec_i=getpass.getpass, o=console.println):
        self.i = i
        self.sec_i = sec_i
        self.o = o
        self.assume_defaults = False

    def create_config(self, config_file, advanced_config=False, assume_defaults=False):
        """
        Either creates a new configuration file or overwrites an existing one. Will ask the user for input on configurable properties
        and writes them to the configuration file in ~/.rally/rally.ini.

        :param config_file:
        :param advanced_config: Whether to ask for properties that are not necessary for everyday use (on a dev machine). Default: False.
        :param assume_defaults: If True, assume the user accepted all values for which defaults are provided. Mainly intended for automatic
        configuration in CI run. Default: False.
        """
        self.assume_defaults = assume_defaults
        if advanced_config:
            self.o("Running advanced configuration. You can get additional help at:")
            self.o("")
            self.o("  %s" % console.format.link("%sconfiguration.html" % DOC_LINK))
            self.o("")

            logger.info("Running advanced configuration routine.")
            self.o("")
        else:
            self.o("Running simple configuration. Run the advanced configuration with:")
            self.o("")
            self.o("  %s configure --advanced-config" % PROGRAM_NAME)
            self.o("")
            logger.info("Running simple configuration routine.")

        if config_file.present:
            self.o("\nWARNING: Will overwrite existing config file at [%s]\n" % config_file.location)
            logger.debug("Detected an existing configuration file at [%s]" % config_file.location)
        else:
            logger.debug("Did not detect a configuration file at [%s]. Running initial configuration routine." % config_file.location)

        # Autodetect settings
        self.o("* Autodetecting available third-party software")
        git_path = io.guess_install_location("git")
        gradle_bin = io.guess_install_location("gradle")
        default_jdk_8 = io.guess_java_home(major_version=8)

        self.print_detection_result("git    ", git_path)
        self.print_detection_result("gradle ", gradle_bin)
        self.print_detection_result("JDK 8  ", default_jdk_8,
                                    warn_if_missing=True,
                                    additional_message="You cannot benchmark Elasticsearch 5.x without a JDK 8 installation")
        # self.print_detection_result("JDK 9 ", default_jdk_9, warn_if_missing=True)
        self.o("")

        # users that don't have Gradle available cannot benchmark from sources
        benchmark_from_sources = gradle_bin

        if not benchmark_from_sources:
            self.o("**********************************************************************************")
            self.o("You don't have the necessary software to benchmark source builds of Elasticsearch.")
            self.o("")
            self.o("You can still benchmark binary distributions with e.g.:")
            self.o("")
            self.o("  %s --distribution-version=5.0.0" % PROGRAM_NAME)
            self.o("**********************************************************************************")
            self.o("")

        root_dir = "%s/benchmarks" % config_file.config_dir
        if advanced_config:
            root_dir = io.normalize_path(self._ask_property("Enter the benchmark data directory:", default_value=root_dir))
        else:
            self.o("* Setting up benchmark data directory in [%s] (needs several GB)." % root_dir)

        if benchmark_from_sources:
            # We try to autodetect an existing ES source directory
            guess = self._guess_es_src_dir()
            if guess:
                source_dir = guess
                self.o("Autodetected Elasticsearch project directory at [%s]." % source_dir)
                logger.debug("Autodetected Elasticsearch project directory at [%s]." % source_dir)
            else:
                default_src_dir = "%s/src" % root_dir
                logger.debug("Could not autodetect Elasticsearch project directory. Providing [%s] as default." % default_src_dir)
                source_dir = io.normalize_path(self._ask_property("Enter your Elasticsearch project directory:",
                                                                  default_value=default_src_dir))
            # Not everybody might have SSH access. Play safe with the default. It may be slower but this will work for everybody.
            repo_url = "https://github.com/elastic/elasticsearch.git"

        if default_jdk_8:
            jdk8_home = default_jdk_8
        else:
            self.o("")
            jdk8_home = io.normalize_path(self._ask_property("Enter the JDK 8 root directory:", check_path_exists=True))

        if advanced_config:
            env_name = self._ask_env_name()
            data_store_type = "elasticsearch"
            data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = self._ask_data_store()

            preserve_install = convert.to_bool(self._ask_property("Do you want Rally to keep the Elasticsearch benchmark candidate "
                                                                  "installation including the index (will use lots of disk space)?",
                                                                  default_value=False))
        else:
            # Does not matter too much for an in-memory store
            env_name = "local"
            data_store_type = "in-memory"
            data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = "", "", "", "", ""
            preserve_install = False

        config = configparser.ConfigParser()
        config["meta"] = {}
        config["meta"]["config.version"] = str(Config.CURRENT_CONFIG_VERSION)

        config["system"] = {}
        config["system"]["env.name"] = env_name

        config["node"] = {}
        config["node"]["root.dir"] = root_dir

        if benchmark_from_sources:
            config["source"] = {}
            config["source"]["local.src.dir"] = source_dir
            config["source"]["remote.repo.url"] = repo_url

            config["build"] = {}
            config["build"]["gradle.bin"] = gradle_bin

        config["runtime"] = {}
        config["runtime"]["java8.home"] = jdk8_home

        config["benchmarks"] = {}
        config["benchmarks"]["local.dataset.cache"] = "${node:root.dir}/data"

        config["reporting"] = {}
        config["reporting"]["datastore.type"] = data_store_type
        config["reporting"]["datastore.host"] = data_store_host
        config["reporting"]["datastore.port"] = data_store_port
        config["reporting"]["datastore.secure"] = data_store_secure
        config["reporting"]["datastore.user"] = data_store_user
        config["reporting"]["datastore.password"] = data_store_password

        config["tracks"] = {}
        config["tracks"]["default.url"] = "https://github.com/elastic/rally-tracks"

        config["defaults"] = {}
        config["defaults"]["preserve_benchmark_candidate"] = str(preserve_install)

        config_file.store(config)

        self.o("Configuration successfully written to [%s]. Happy benchmarking!" % config_file.location)
        self.o("")
        if benchmark_from_sources:
            self.o("To benchmark Elasticsearch with the default benchmark run:")
            self.o("")
            self.o("  %s" % PROGRAM_NAME)
        else:
            self.o("To benchmark Elasticsearch 5.0.0 with the default benchmark run:")
            self.o("")
            self.o("  %s --distribution-version=5.0.0" % PROGRAM_NAME)

        self.o("")
        self.o("For help, type %s --help or see the user documentation at %s"
               % (PROGRAM_NAME, console.format.link(DOC_LINK)))

    def print_detection_result(self, what, result, warn_if_missing=False, additional_message=None):
        logger.debug("Autodetected %s at [%s]" % (what, result))
        if additional_message:
            message = " (%s)" % additional_message
        else:
            message = ""

        if result:
            self.o("  %s: [%s]" % (what, console.format.green("OK")))
        elif warn_if_missing:
            self.o("  %s: [%s]%s" % (what, console.format.yellow("MISSING"), message))
        else:
            self.o("  %s: [%s]%s" % (what, console.format.red("MISSING"), message))

    def _guess_es_src_dir(self):
        current_dir = os.getcwd()
        # try sibling elasticsearch directory (assuming that Rally is checked out alongside Elasticsearch)
        #
        # Note that if the current directory is the elasticsearch project directory, it will also be detected. We just cannot check
        # the current directory directly, otherwise any directory that is a git working copy will be detected as Elasticsearch project
        # directory.
        sibling_es_dir = os.path.abspath(os.path.join(current_dir, os.pardir, "elasticsearch"))
        child_es_dir = os.path.abspath(os.path.join(current_dir, "elasticsearch"))

        for candidate in [sibling_es_dir, child_es_dir]:
            if git.is_working_copy(candidate):
                return candidate
        return None

    def _ask_data_store(self):
        data_store_host = self._ask_property("Enter the host name of the ES metrics store", default_value="localhost")
        data_store_port = self._ask_property("Enter the port of the ES metrics store", check_pattern=ConfigFactory.PORT_RANGE_PATTERN)
        data_store_secure = self._ask_property("Use secure connection (True, False)", default_value=False,
                                               check_pattern=ConfigFactory.BOOLEAN_PATTERN)
        data_store_user = self._ask_property("Username for basic authentication (empty if not needed)", mandatory=False, default_value="")
        data_store_password = self._ask_property("Password for basic authentication (empty if not needed)", mandatory=False,
                                                 default_value="", sensitive=True)
        # do an intermediate conversion to bool in order to normalize input
        return data_store_host, data_store_port, str(convert.to_bool(data_store_secure)), data_store_user, data_store_password

    def _ask_env_name(self):
        return self._ask_property("Enter a descriptive name for this benchmark environment (ASCII, no spaces)",
                                  check_pattern=ConfigFactory.ENV_NAME_PATTERN)

    def _ask_property(self, prompt, mandatory=True, check_path_exists=False, check_pattern=None, sensitive=False, default_value=None):
        if default_value is not None:
            final_prompt = "%s [default: '%s']: " % (prompt, default_value)
        else:
            final_prompt = "%s: " % prompt
        while True:
            if self.assume_defaults and default_value is not None:
                self.o(final_prompt)
                value = None
            elif sensitive:
                value = self.sec_i(final_prompt)
            else:
                value = self.i(final_prompt)

            if not value or value.strip() == "":
                if mandatory and default_value is None:
                    self.o("  Value is required. Please retry.")
                    continue
                else:
                    self.o("  Using default value '%s'" % default_value)
                    # this way, we can still check the path...
                    value = default_value

            if check_path_exists and not os.path.exists(value):
                self.o("'%s' does not exist. Please check and retry." % value)
                continue
            if check_pattern is not None and not check_pattern.match(str(value)):
                self.o("Input does not match pattern [%s]. Please check and retry." % check_pattern.pattern)
                continue
            self.o("")
            # user entered a valid value
            return value


def migrate(config_file, current_version, target_version, out=print):
    logger.info("Upgrading configuration from version [%s] to [%s]." % (current_version, target_version))
    # Something is really fishy. We don't want to downgrade the configuration.
    if current_version >= target_version:
        raise ConfigError("The existing config file is available in a later version already. Expected version <= [%s] but found [%s]"
                          % (target_version, current_version))
    # but first a backup...
    config_file.backup()
    config = config_file.load(interpolation=None)

    if current_version == 0 and target_version > current_version:
        logger.info("Migrating config from version [0] to [1]")
        current_version = 1
        config["meta"] = {}
        config["meta"]["config.version"] = str(current_version)
        # in version 1 we changed some directories from being absolute to being relative
        config["system"]["log.root.dir"] = "logs"
        config["provisioning"]["local.install.dir"] = "install"
        config["reporting"]["report.base.dir"] = "reports"
    if current_version == 1 and target_version > current_version:
        logger.info("Migrating config from version [1] to [2]")
        current_version = 2
        config["meta"]["config.version"] = str(current_version)
        # no need to ask the user now if we are about to upgrade to version 4
        config["reporting"]["datastore.type"] = "in-memory"
        config["reporting"]["datastore.host"] = ""
        config["reporting"]["datastore.port"] = ""
        config["reporting"]["datastore.secure"] = ""
        config["reporting"]["datastore.user"] = ""
        config["reporting"]["datastore.password"] = ""
        config["system"]["env.name"] = "local"
    if current_version == 2 and target_version > current_version:
        logger.info("Migrating config from version [2] to [3]")
        current_version = 3
        config["meta"]["config.version"] = str(current_version)
        # Remove obsolete settings
        config["reporting"].pop("report.base.dir")
        config["reporting"].pop("output.html.report.filename")
    if current_version == 3 and target_version > current_version:
        root_dir = config["system"]["root.dir"]
        out("*****************************************************************************************")
        out("")
        out("You have an old configuration of Rally. Rally has now a much simpler setup")
        out("routine which will autodetect lots of settings for you and it also does not")
        out("require you to setup a metrics store anymore.")
        out("")
        out("Rally will now migrate your configuration but if you don't need advanced features")
        out("like a metrics store, then you should delete the configuration directory:")
        out("")
        out("  rm -rf %s" % config_file.config_dir)
        out("")
        out("and then rerun Rally's configuration routine:")
        out("")
        out("  %s configure" % PROGRAM_NAME)
        out("")
        out("Please also note you have %.1f GB of data in your current benchmark directory at"
            % convert.bytes_to_gb(io.get_size(root_dir)))
        out()
        out("  %s" % root_dir)
        out("")
        out("You might want to clean up this directory also.")
        out()
        out("For more details please see %s" % console.format.link("https://github.com/elastic/rally/blob/master/CHANGELOG.md#030"))
        out("")
        out("*****************************************************************************************")
        out("")
        out("Pausing for 10 seconds to let you consider this message.")
        time.sleep(10)
        logger.info("Migrating config from version [3] to [4]")
        current_version = 4
        config["meta"]["config.version"] = str(current_version)
        if len(config["reporting"]["datastore.host"]) > 0:
            config["reporting"]["datastore.type"] = "elasticsearch"
        else:
            config["reporting"]["datastore.type"] = "in-memory"
        # Remove obsolete settings
        config["build"].pop("maven.bin")
        config["benchmarks"].pop("metrics.stats.disk.device")

    if current_version == 4 and target_version > current_version:
        config["tracks"] = {}
        config["tracks"]["default.url"] = "https://github.com/elastic/rally-tracks"
        current_version = 5
        config["meta"]["config.version"] = str(current_version)

    if current_version == 5 and target_version > current_version:
        config["defaults"] = {}
        config["defaults"]["preserve_benchmark_candidate"] = str(False)
        current_version = 6
        config["meta"]["config.version"] = str(current_version)

    if current_version == 6 and target_version > current_version:
        # Remove obsolete settings
        config.pop("provisioning")
        config["system"].pop("log.root.dir")
        current_version = 7
        config["meta"]["config.version"] = str(current_version)

    if current_version == 7 and target_version > current_version:
        # move [system][root.dir] to [node][root.dir]
        if "node" not in config:
            config["node"] = {}
        config["node"]["root.dir"] = config["system"].pop("root.dir")
        # also move all references!
        for section in config:
            for k, v in config[section].items():
                config[section][k] = v.replace("${system:root.dir}", "${node:root.dir}")
        current_version = 8
        config["meta"]["config.version"] = str(current_version)

    # all migrations done
    config_file.store(config)
    logger.info("Successfully self-upgraded configuration to version [%s]" % target_version)
