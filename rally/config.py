from enum import Enum


class ConfigError(BaseException):
  pass


class Scope(Enum):
  # Valid for all benchmarks
  globalScope = 1
  # A sole benchmark
  benchmarkScope = 2
  # Single benchmark track
  trackScope = 3
  # property for every invocation, i.e. for backtesting
  invocationScope = 4


# TODO dm: Explicitly clean all values after they've lost their scope to avoid leaving behind outdated entries by accident
# Abstracts the configuration format.
class Config:
  # TODO dm: Later we'll use ConfigParser, for now it's just a map. ConfigParser uses sections and keys, we separate sections from the key
  #          with two double colons.
  _opts = {
    "source::local.src.dir": "/Users/dm/Downloads/scratch/rally/elasticsearch",
    "source::remote.repo.url": "git@github.com:elastic/elasticsearch.git",
    #TODO dm: Add support for Maven (-> backtesting)
    "build::gradle.bin": "/usr/local/bin/gradle",
    "build::gradle.tasks.clean": "clean",
    #TODO dm: tests.jvm should depend on the number of cores - how to abstract this? we can get the value with sysstats.number_of_cpu_cores()
    # We have to encode this probably in builder.py...
    # "build::gradle.tasks.package": "check -Dtests.seed=0 -Dtests.jvms=12",
    # We just build the ZIP distribution directly for now (instead of the 'check' target)
    "build::gradle.tasks.package": "assemble",
    "build::log.dir": "/Users/dm/Downloads/scratch/rally/build_logs",
    # Where to install the benchmark candidate, i.e. Elasticsearch
    "provisioning::local.install.dir": "/Users/dm/Downloads/scratch/rally/install",

    "runtime::java8.home": "/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home",
    # TODO dm: Add also java7.home (and maybe we need to be more fine-grained, such as "java7update25.home" but we'll see..

    # Where to download raw benchmark datasets?
    "benchmarks::local.dataset.cache": "/Users/dm/Projects/data/benchmarks",
    "benchmarks::metrics.stats.disk.device": "/dev/disk1",
    # Specific configuration per benchmark
    "benchmarks.logging::index.client.threads": "8",
    # separate directory from output file name for now, we may want to gather multiple reports and they should not override each other
    "reporting::report.base.dir": "/Users/dm/Downloads/scratch/rally/reports",
    # We may want to consider output formats (console (summary), html, ES, ...)
    "reporting::output.html.report.filename": "index.html"
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
