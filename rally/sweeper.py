import shutil

import rally.paths
import rally.utils.io


class Sweeper:
    def __init__(self, config):
        self._config = config

    def run(self):
        invocation_root = rally.paths.Paths(self._config).invocation_root()
        log_root = rally.paths.Paths(self._config).log_root()
        rally.utils.io.zip(log_root, "%s/logs.zip" % invocation_root)
        shutil.rmtree(log_root)

