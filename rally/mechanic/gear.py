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

    def capability(self, capability):
        # For now, everything is hardcoded and we ignore the timestamp (i.e. no proper backtesting)
        if capability == Capability.java:
            return self._config.opts("runtime", "java8.home")
        elif capability == Capability.build_tool:
            return "gradle"
        else:
            raise RuntimeError("Unrecognized capability %s" % capability)
