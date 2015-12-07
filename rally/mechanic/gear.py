# Encapsulate such things as Java versions over time, i.e. f(date) -> Java version, where date is an indicator of the Elasticsearch source version.
# Possibly we need even more parameters (e.g. Java options, ES config parameters, ...).

from enum import Enum


class Capability(Enum):
  """
  A capability defines a third party dependency either for building or running the benchmark candidate which may change over time.
  """
  java = 1
  build_tool = 2


class Gear:
  """
  Gear allows to select the correct version of a third party dependency.
  """
  def __init__(self, config):
    self._config = config

  # TODO dm: Introduce some kind of timestamp or version parameter (we can determine the correct versions for backtesting then)
  # Ideally, this should not be based on timestamp but rather on Elasticsearch versions. If we choose it this way, we can also
  # support benchmarks in older branches and still use the correct tool versions (e.g. Maven on 2.x branch)
  def capability(self, capability):
    # For now, everything is hardcoded and we ignore the timestamp (i.e. no proper backtesting)
    if capability == Capability.java:
      return self._config.opts("runtime", "java8.home")
    elif capability == Capability.build_tool:
      return "gradle"
    else:
      raise RuntimeError("Unrecognized capability %s" % capability)
