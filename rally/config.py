import os.path
import configparser
from enum import Enum

import rally.utils.io


class ConfigError(BaseException):
  pass


class Scope(Enum):
  # Valid for all benchmarks, typically read from the configuration file
  globalScope = 1
  # Valid for all benchmarks, intended to allow overriding of values in the config file from the command line
  globalOverrideScope = 2
  # A sole benchmark
  benchmarkScope = 3
  # Single benchmark track setup (e.g. default, multinode, ...)
  trackSetupScope = 4
  # property for every invocation, i.e. for backtesting
  invocationScope = 5


# TODO dm: Explicitly clean all values after they've lost their scope to avoid leaving behind outdated entries by accident
# Abstracts the configuration format.
class Config:
  # This map contains default options that we don't want to sprinkle all over the source code but we don't want users to change them either
  _opts = {
    "build::gradle.tasks.clean": "clean",
    # #TODO dm: tests.jvm should depend on the number of cores - how to abstract this? we can get the value with sysstats.number_of_cpu_cores()
    # # We have to encode this probably in builder.py...
    # "build::gradle.tasks.package": "check -Dtests.seed=0 -Dtests.jvms=12",
    # We just build the ZIP distribution directly for now (instead of the 'check' target)
    "build::gradle.tasks.package": "assemble",
    "build::log.dir": "build",
    "benchmarks::metrics.log.dir": "metrics",
    # Specific configuration per benchmark
    "benchmarks.logging::index.client.threads": "8",
    "benchmarks.countries::index.client.threads": "8",
  }

  def __init__(self):
    pass

  def add(self, scope, section, key, value):
    self._opts[self._k(scope, section, key)] = value

  def opts(self, section, key, default_value=None, mandatory=True):
    try:
      scope = self._resolve_scope(section, key)
      return self._opts[self._k(scope, section, key)]
    except KeyError:
      if not mandatory:
        return default_value
      else:
        raise ConfigError("No value for mandatory configuration: section='%s', key='%s'" % (section, key))

  def config_present(self):
    return os.path.isfile(self._config_file())

  def load_config(self):
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read(self._config_file())
    for section in config.sections():
      for key in config[section]:
        self.add(Scope.globalScope, section, key, config[section][key])

  # full_config -> intended for nightlies
  def create_config(self, advanced_config=False):
    if self.config_present():
      print("\n!!!!!!! WARNING: Will overwrite existing config file: '%s' !!!!!!!\n", self._config_file())
    else:
      print("Creating new configuration file in '%s'\n" % self._config_file())

    print("The benchmark root directory contains benchmark data, logs, etc.")
    print("It will consume several tens of GB of free space (expect at least 20 GB).")
    benchmark_root_dir = self._ask_property("Enter the benchmark root directory (will be created automatically)")
    source_dir = self._ask_property("Enter the directory where sources are located (your Elasticsearch project directory)")
    # Ask, because not everybody might have SSH access
    repo_url = self._ask_property("Enter the Elasticsearch repo URL", default_value="git@github.com:elastic/elasticsearch.git")
    gradle_bin = self._ask_property("Enter the full path to the Gradle binary", default_value="/usr/local/bin/gradle",
                                    check_path_exists=True)
    if advanced_config:
      maven_bin = self._ask_property("Enter the full path to the Maven 3 binary", default_value="/usr/local/bin/mvn",
                                     check_path_exists=True)
    else:
      maven_bin = ""
    jdk8_home = self._ask_property("Enter the JDK 8 root directory", check_path_exists=True)
    # TODO dm: Check with Mike. It looks this is just interesting for nightlies.
    if advanced_config:
      stats_disk_device = self._ask_property("Enter the HDD device name for stats (e.g. /dev/disk1)")
    else:
      stats_disk_device = ""

    config = configparser.ConfigParser()
    config["system"] = {}
    config["system"]["root.dir"] = benchmark_root_dir
    config["system"]["log.root.dir"] = "${system:root.dir}/logs"

    config["source"] = {}
    config["source"]["local.src.dir"] = source_dir
    config["source"]["remote.repo.url"] = repo_url

    config["build"] = {}
    config["build"]["gradle.bin"] = gradle_bin
    config["build"]["maven.bin"] = maven_bin

    config["provisioning"] = {}
    config["provisioning"]["local.install.dir"] = "${system:root.dir}/install"

    # TODO dm: Add also java7.home (and maybe we need to be more fine-grained, such as "java7update25.home" but we'll see..
    config["runtime"] = {}
    config["runtime"]["java8.home"] = jdk8_home

    config["benchmarks"] = {}
    config["benchmarks"]["local.dataset.cache"] = "${system:root.dir}/data"
    config["benchmarks"]["metrics.stats.disk.device"] = stats_disk_device

    config["reporting"] = {}
    config["reporting"]["report.base.dir"] = "${system:root.dir}/reports"
    config["reporting"]["output.html.report.filename"] = "index.html"

    rally.utils.io.ensure_dir(self._config_dir())
    with open(self._config_file(), 'w') as configfile:
      config.write(configfile)

    print("Configuration successfully written to '%s'. Please rerun rally now." % self._config_file())

  def _ask_property(self, prompt, mandatory=True, check_path_exists=False, default_value=None):
    while True:
      if default_value:
        value = input("%s [default: %s]: " % (prompt, default_value))
      else:
        value = input("%s: " % prompt)

      if not value or value.strip() == "":
        if mandatory and not default_value:
          print("  Value is required. Please retry.")
          continue
        else:
          print("  Using default value '%s'" % default_value)
          # this way, we can still check the path...
          value = default_value

      if check_path_exists and not os.path.exists(value):
        print("'%s' does not exist. Please check and retry." % value)
        continue
      # user entered a valid value
      return value

  def _config_dir(self):
    return "%s/.rally" % os.getenv("HOME")

  def _config_file(self):
    return "%s/rally.ini" % self._config_dir()

  # recursively find the most narrow scope for a key
  def _resolve_scope(self, section, key, start_from=Scope.invocationScope):
    if self._k(start_from, section, key) in self._opts:
      return start_from
    elif start_from == Scope.globalScope:
      return None
    else:
      # continue search in the enclosing scope
      return self._resolve_scope(section, key, Scope(start_from.value - 1))

  def _k(self, scope, section, key):
    # keep global config keys a bit shorter / nicer for now
    if scope is None or scope == Scope.globalScope:
      return "%s::%s" % (section, key)
    else:
      return "%s::%s::%s" % (scope.name, section, key)
