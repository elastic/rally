import shutil

from esrally import paths
from esrally.utils import io


class Sweeper:
    def __init__(self, config):
        self._config = config

    def run(self, track):
        invocation_root = paths.Paths(self._config).invocation_root()
        log_root = paths.Paths(self._config).log_root()
        io.compress(log_root, "%s/logs-%s.zip" % (invocation_root, track.name))
        shutil.rmtree(log_root)

