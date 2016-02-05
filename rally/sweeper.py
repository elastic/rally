import shutil

from rally import paths
from rally.utils import io


class Sweeper:
    def __init__(self, config):
        self._config = config

    def run(self):
        invocation_root = paths.Paths(self._config).invocation_root()
        log_root = paths.Paths(self._config).log_root()
        io.zip(log_root, "%s/logs.zip" % invocation_root)
        shutil.rmtree(log_root)

