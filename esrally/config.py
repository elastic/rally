import configparser
import getpass
import logging
import os.path
import re
import shutil
from enum import Enum

from esrally import time, PROGRAM_NAME, DOC_LINK, exceptions
from esrally.utils import io, git, console, convert

logger = logging.getLogger("rally.config")


class ConfigError(exceptions.RallyError):
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
    def __init__(self, config_name=None, **kwargs):
        self.config_name = config_name

    @property
    def present(self):
        """
        :return: true iff a config file already exists.
        """
        return os.path.isfile(self.location)

    def load(self, interpolation=configparser.ExtendedInterpolation()):
        config = configparser.ConfigParser(interpolation=interpolation)
        config.read(self.location, encoding="utf-8")
        return config

    def store(self, config):
        io.ensure_dir(self.config_dir)
        with open(self.location, "wt", encoding="utf-8") as configfile:
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
    if cfg.config_present():
        cfg.load_config(auto_upgrade=True)
    else:
        # force unattended configuration - we don't need to raise errors if some bits are missing. Depending on the node role and the
        # configuration it may be fine that e.g. Java is missing (no need for that on a load driver node).
        ConfigFactory(o=logger.info).create_config(cfg.config_file, advanced_config=False, assume_defaults=True)
        # reload and continue
        if cfg.config_present():
            cfg.load_config()
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
    EARLIEST_SUPPORTED_VERSION = 12

    CURRENT_CONFIG_VERSION = 16

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


class ConfigFactory:
    ENV_NAME_PATTERN = re.compile("^[a-zA-Z_-]+$")

    PORT_RANGE_PATTERN = re.compile("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$")

    BOOLEAN_PATTERN = re.compile("^(True|true|Yes|yes|t|y|False|false|f|No|no|n)$")

    def __init__(self, i=input, sec_i=getpass.getpass, o=console.println):
        self.i = i
        self.sec_i = sec_i
        self.o = o
        self.prompter = None

    def create_config(self, config_file, advanced_config=False, assume_defaults=False, java_home=None, runtime_java_home=None):
        """
        Either creates a new configuration file or overwrites an existing one. Will ask the user for input on configurable properties
        and writes them to the configuration file in ~/.rally/rally.ini.

        :param config_file:
        :param advanced_config: Whether to ask for properties that are not necessary for everyday use (on a dev machine). Default: False.
        :param assume_defaults: If True, assume the user accepted all values for which defaults are provided. Mainly intended for automatic
        configuration in CI run. Default: False.
        """
        benchmark_from_sources = True
        self.prompter = Prompter(self.i, self.sec_i, self.o, assume_defaults)

        if advanced_config:
            self.o("Running advanced configuration. You can get additional help at:")
            self.o("")
            self.o("  %s" % console.format.link("%sconfiguration.html" % DOC_LINK))
            self.o("")
        else:
            self.o("Running simple configuration. Run the advanced configuration with:")
            self.o("")
            self.o("  %s configure --advanced-config" % PROGRAM_NAME)
            self.o("")

        if config_file.present:
            self.o("\nWARNING: Will overwrite existing config file at [%s]\n" % config_file.location)
            logger.debug("Detected an existing configuration file at [%s]" % config_file.location)
        else:
            logger.debug("Did not detect a configuration file at [%s]. Running initial configuration routine." % config_file.location)

        # Autodetect settings
        self.o("* Autodetecting available third-party software")
        git_path = io.guess_install_location("git")

        java_8_home = runtime_java_home if runtime_java_home else io.guess_java_home(major_version=8)
        java_10_home = java_home if java_home else io.guess_java_home(major_version=10)
        from esrally.utils import jvm
        if java_8_home:
            auto_detected_java_home = java_8_home
        # Don't auto-detect an EA release and bring trouble to the user later on. They can still configure it manually if they want to.
        elif java_10_home and not jvm.is_early_access_release(java_10_home):
            auto_detected_java_home = java_10_home
        else:
            auto_detected_java_home = None

        self.print_detection_result("git    ", git_path)
        self.print_detection_result("JDK    ", auto_detected_java_home,
                                    warn_if_missing=True,
                                    additional_message="You cannot benchmark Elasticsearch on this machine without a JDK.")
        self.o("")

        root_dir = io.normalize_path(os.path.abspath(os.path.join(config_file.config_dir, "benchmarks")))
        if advanced_config:
            root_dir = io.normalize_path(self._ask_property("Enter the benchmark data directory", default_value=root_dir))
        else:
            self.o("* Setting up benchmark data directory in %s" % root_dir)

        if not java_10_home or jvm.is_early_access_release(java_10_home):
            raw_java_10_home = self._ask_property("Enter the JDK 10 root directory", check_path_exists=True, mandatory=False)
            if raw_java_10_home and jvm.major_version(raw_java_10_home) == 10 and not jvm.is_early_access_release(raw_java_10_home):
                java_10_home = io.normalize_path(raw_java_10_home) if raw_java_10_home else None
            else:
                benchmark_from_sources = False
                self.o("********************************************************************************")
                self.o("You don't have a valid JDK 10 installation and cannot benchmark source builds.")
                self.o("")
                self.o("You can still benchmark binary distributions with e.g.:")
                self.o("")
                self.o("  %s --distribution-version=6.0.0" % PROGRAM_NAME)
                self.o("********************************************************************************")
                self.o("")

        # We try to autodetect an existing ES source directory
        guess = self._guess_es_src_dir()
        if guess:
            source_dir = guess
            logger.debug("Autodetected Elasticsearch project directory at [%s]." % source_dir)
        else:
            default_src_dir = os.path.join(root_dir, "src", "elasticsearch")
            logger.debug("Could not autodetect Elasticsearch project directory. Providing [%s] as default." % default_src_dir)
            source_dir = default_src_dir

        if advanced_config:
            source_dir = io.normalize_path(self._ask_property("Enter your Elasticsearch project directory:",
                                                              default_value=source_dir))
        if not advanced_config:
            self.o("* Setting up benchmark source directory in %s" % source_dir)
            self.o("")

        # Not everybody might have SSH access. Play safe with the default. It may be slower but this will work for everybody.
        repo_url = "https://github.com/elastic/elasticsearch.git"

        if auto_detected_java_home:
            java_home = auto_detected_java_home
            local_benchmarks = True
        else:
            raw_java_home = self._ask_property("Enter the JDK root directory (version 8 or later)", check_path_exists=True, mandatory=False)
            java_home = io.normalize_path(raw_java_home) if raw_java_home else None
            if not java_home:
                local_benchmarks = False
                self.o("")
                self.o("********************************************************************************")
                self.o("You don't have a JDK installed but Elasticsearch requires one to run. This means")
                self.o("that you cannot benchmark Elasticsearch on this machine.")
                self.o("")
                self.o("You can still benchmark against remote machines e.g.:")
                self.o("")
                self.o("  %s --pipeline=benchmark-only --target-host=\"NODE_IP:9200\"" % PROGRAM_NAME)
                self.o("")
                self.o("See %s for further info." % console.format.link("%srecipes.html" % DOC_LINK))
                self.o("********************************************************************************")
                self.o("")
            else:
                local_benchmarks = True

        if advanced_config:
            data_store_choice = self._ask_property("Where should metrics be kept?"
                                                   "\n\n"
                                                   "(1) In memory (simpler but less options for analysis)\n"
                                                   "(2) Elasticsearch (requires a separate ES instance, keeps all raw samples for analysis)"
                                                   "\n\n", default_value="1", choices=["1", "2"])
            if data_store_choice == "1":
                env_name = "local"
                data_store_type = "in-memory"
                data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = "", "", "", "", ""
            else:
                data_store_type = "elasticsearch"
                data_store_host, data_store_port, data_store_secure, data_store_user, data_store_password = self._ask_data_store()

                env_name = self._ask_env_name()

            preserve_install = convert.to_bool(self._ask_property("Do you want Rally to keep the Elasticsearch benchmark candidate "
                                                                  "installation including the index (will use several GB per trial run)?",
                                                                  default_value=False))
        else:
            # Does not matter for an in-memory store
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

        final_source_dir = io.normalize_path(os.path.abspath(os.path.join(source_dir, os.pardir)))
        config["node"]["src.root.dir"] = final_source_dir

        config["source"] = {}
        config["source"]["remote.repo.url"] = repo_url
        # the Elasticsearch directory is just the last path component (relative to the source root directory)
        config["source"]["elasticsearch.src.subdir"] = io.basename(source_dir)

        config["runtime"] = {}
        if java_home:
            config["runtime"]["java.home"] = java_home
        if java_10_home:
            config["runtime"]["java10.home"] = java_10_home

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

        config["teams"] = {}
        config["teams"]["default.url"] = "https://github.com/elastic/rally-teams"

        config["defaults"] = {}
        config["defaults"]["preserve_benchmark_candidate"] = str(preserve_install)

        config["distributions"] = {}
        config["distributions"]["release.cache"] = "true"

        config_file.store(config)

        self.o("Configuration successfully written to %s. Happy benchmarking!" % config_file.location)
        self.o("")
        if local_benchmarks and benchmark_from_sources:
            self.o("To benchmark Elasticsearch with the default benchmark, run:")
            self.o("")
            self.o("  %s" % PROGRAM_NAME)
            self.o("")
        elif local_benchmarks:
            self.o("To benchmark Elasticsearch 6.0.0 with the default benchmark, run:")
            self.o("")
            self.o("  %s --distribution-version=6.0.0" % PROGRAM_NAME)
            self.o("")
        else:
            # we've already printed an info for the user. No need to repeat that.
            pass

        self.o("More info about Rally:")
        self.o("")
        self.o("* Type %s --help" % PROGRAM_NAME)
        self.o("* Read the documentation at %s" % console.format.link(DOC_LINK))
        self.o("* Ask a question on the forum at %s" % console.format.link("https://discuss.elastic.co/c/elasticsearch/rally"))

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
                                  check_pattern=ConfigFactory.ENV_NAME_PATTERN, default_value="local")

    def _ask_property(self, prompt, mandatory=True, check_path_exists=False, check_pattern=None, choices=None, sensitive=False,
                      default_value=None):
        return self.prompter.ask_property(prompt, mandatory, check_path_exists, check_pattern, choices, sensitive, default_value)


class Prompter:
    def __init__(self, i=input, sec_i=getpass.getpass, o=console.println, assume_defaults=False):
        self.i = i
        self.sec_i = sec_i
        self.o = o
        self.assume_defaults = assume_defaults

    def ask_property(self, prompt, mandatory=True, check_path_exists=False, check_pattern=None, choices=None, sensitive=False,
                     default_value=None):
        if default_value is not None:
            final_prompt = "%s (default: %s): " % (prompt, default_value)
        elif not mandatory:
            final_prompt = "%s (Press Enter to skip): " % prompt
        else:
            final_prompt = "%s: " % prompt
        while True:
            if self.assume_defaults and (default_value is not None or not mandatory):
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
                    # suppress output when the default is empty
                    if default_value:
                        self.o("  Using default value '%s'" % default_value)
                    # this way, we can still check the path...
                    value = default_value

            if mandatory or value is not None:
                if check_path_exists and not io.exists(value):
                    self.o("'%s' does not exist. Please check and retry." % value)
                    continue
                if check_pattern is not None and not check_pattern.match(str(value)):
                    self.o("Input does not match pattern [%s]. Please check and retry." % check_pattern.pattern)
                    continue
                if choices is not None and str(value) not in choices:
                    self.o("Input is not one of the valid choices %s. Please check and retry." % choices)
                    continue
                self.o("")
            # user entered a valid value
            return value


def migrate(config_file, current_version, target_version, out=print, i=input):
    if current_version < Config.EARLIEST_SUPPORTED_VERSION:
        raise ConfigError("The config file in {} is too old. Please delete it and reconfigure Rally from scratch with {} configure."
                          .format(config_file.location, PROGRAM_NAME))

    prompter = Prompter(i=i, o=out, assume_defaults=False)
    logger.info("Upgrading configuration from version [%s] to [%s]." % (current_version, target_version))
    # Something is really fishy. We don't want to downgrade the configuration.
    if current_version >= target_version:
        raise ConfigError("The existing config file is available in a later version already. Expected version <= [%s] but found [%s]"
                          % (target_version, current_version))
    # but first a backup...
    config_file.backup()
    config = config_file.load(interpolation=None)

    if current_version == 12 and target_version > current_version:
        # the current configuration allows to benchmark from sources
        if "build" in config and "gradle.bin" in config["build"]:
            java_9_home = io.guess_java_home(major_version=9)
            from esrally.utils import jvm
            if java_9_home and not jvm.is_early_access_release(java_9_home):
                logger.debug("Autodetected a JDK 9 installation at [%s]" % java_9_home)
                if "runtime" not in config:
                    config["runtime"] = {}
                config["runtime"]["java9.home"] = java_9_home
            else:
                logger.debug("Could not autodetect a JDK 9 installation. Checking [java.home] already points to a JDK 9.")
                detected = False
                if "runtime" in config:
                    java_home = config["runtime"]["java.home"]
                    if jvm.major_version(java_home) == 9 and not jvm.is_early_access_release(java_home):
                        config["runtime"]["java9.home"] = java_home
                        detected = True

                if not detected:
                    logger.debug("Could not autodetect a JDK 9 installation. Asking user.")
                    raw_java_9_home = prompter.ask_property("Enter the JDK 9 root directory", check_path_exists=True, mandatory=False)
                    if raw_java_9_home and jvm.major_version(raw_java_9_home) == 9 and not jvm.is_early_access_release(raw_java_9_home):
                        java_9_home = io.normalize_path(raw_java_9_home) if raw_java_9_home else None
                        config["runtime"]["java9.home"] = java_9_home
                    else:
                        out("********************************************************************************")
                        out("You don't have a valid JDK 9 installation and cannot benchmark source builds.")
                        out("")
                        out("You can still benchmark binary distributions with e.g.:")
                        out("")
                        out("  %s --distribution-version=6.0.0" % PROGRAM_NAME)
                        out("********************************************************************************")
                        out("")

        current_version = 13
        config["meta"]["config.version"] = str(current_version)

    if current_version == 13 and target_version > current_version:
        # This version replaced java9.home with java10.home
        if "build" in config and "gradle.bin" in config["build"]:
            java_10_home = io.guess_java_home(major_version=10)
            from esrally.utils import jvm
            if java_10_home and not jvm.is_early_access_release(java_10_home):
                logger.debug("Autodetected a JDK 10 installation at [%s]" % java_10_home)
                if "runtime" not in config:
                    config["runtime"] = {}
                config["runtime"]["java10.home"] = java_10_home
            else:
                logger.debug("Could not autodetect a JDK 10 installation. Checking [java.home] already points to a JDK 10.")
                detected = False
                if "runtime" in config:
                    java_home = config["runtime"]["java.home"]
                    if jvm.major_version(java_home) == 10 and not jvm.is_early_access_release(java_home):
                        config["runtime"]["java10.home"] = java_home
                        detected = True

                if not detected:
                    logger.debug("Could not autodetect a JDK 10 installation. Asking user.")
                    raw_java_10_home = prompter.ask_property("Enter the JDK 10 root directory", check_path_exists=True, mandatory=False)
                    if raw_java_10_home and jvm.major_version(raw_java_10_home) == 10 and not jvm.is_early_access_release(raw_java_10_home):
                        java_10_home = io.normalize_path(raw_java_10_home) if raw_java_10_home else None
                        config["runtime"]["java10.home"] = java_10_home
                    else:
                        out("********************************************************************************")
                        out("You don't have a valid JDK 10 installation and cannot benchmark source builds.")
                        out("")
                        out("You can still benchmark binary distributions with e.g.:")
                        out("")
                        out("  %s --distribution-version=6.0.0" % PROGRAM_NAME)
                        out("********************************************************************************")
                        out("")

        current_version = 14
        config["meta"]["config.version"] = str(current_version)

    if current_version == 14 and target_version > current_version:
        # Be agnostic about build tools. Let use specify build commands for plugins and elasticsearch
        # but also use gradlew by default for Elasticsearch and Core plugin builds, if nothing else has been specified.

        def warn_if_plugin_build_task_is_in_use(config):
            if "source" not in config:
                return
            for k, v in config["source"].items():
                plugin_match = re.match('^plugin\.([^.]+)\.build\.task$',k)
                if plugin_match != None and len(plugin_match.groups()) > 0 :
                    plugin_name = plugin_match.group(1)
                    new_key = "plugin.{}.build.command".format(plugin_name)
                    out("\n"
                        "WARNING:"
                        "  The build.task property for plugins has been obsoleted in favor of the full build.command."
                        "  You will need to edit the plugin [{}] section in {} and change from:"
                        "  [{} = {}] to [{} = <the full command>]."
                        "  Please refer to the documentation for more details:"
                        "  {}.\n".format(plugin_match.group(1), config_file.location, k, v, new_key,
                                         console.format.link("%selasticsearch_plugins.html#running-a-benchmark-with-plugins" % DOC_LINK)))

        logger.info("Migrating configuration version from 14 to 15.")
        if "build" in config:
            logger.info("Removing Gradle configuration as Rally now uses the Gradle Wrapper to build Elasticsearch.")
            config.pop("build", None)
        warn_if_plugin_build_task_is_in_use(config)

        current_version = 15
        config["meta"]["config.version"] = str(current_version)

    if current_version == 15 and target_version > current_version:
        if "distributions" in config:
            # Remove obsolete settings
            config["distributions"].pop("release.1.url", None)
            config["distributions"].pop("release.2.url", None)
            config["distributions"].pop("release.url", None)
        current_version = 16
        config["meta"]["config.version"] = str(current_version)

    # all migrations done
    config_file.store(config)
    logger.info("Successfully self-upgraded configuration to version [%s]" % target_version)
