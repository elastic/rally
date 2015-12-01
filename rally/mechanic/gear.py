# Encapsulate such things as Java versions over time, i.e. f(date) -> Java version, where date is an indicator of the Elasticsearch source version.
# Possibly we need even more parameters (e.g. Java options, ES config parameters, ...).

from enum import Enum


class Capability(Enum):
  java = 1
  build_tool = 2


# Encapsulates all system software that is necessary to benchmark Elasticsearch that might change over time (e.g. Java version)
class Gear:
  def __init__(self, config):
    self._config = config

  # TODO dm: Introduce some kind of timestamp or version parameter (we can determine the correct versions for backtesting then)
  def capability(self, capability):
    # For now, everything is hardcoded and we ignore the timestamp (i.e. no proper backtesting)
    if capability == Capability.java:
      return self._config.opts("runtime", "java8.home")
    elif capability == Capability.build_tool:
      return "gradle"
    else:
      raise RuntimeError("Unrecognized capability %s" % capability)
