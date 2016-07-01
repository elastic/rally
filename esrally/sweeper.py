import shutil

from esrally import paths
from esrally.utils import io


class Sweeper:
    def __init__(self, config):
        self._config = config

    def __call__(self, track, challenge, car):
        invocation_root = paths.Paths(self._config).invocation_root()
        log_root = paths.Paths(self._config).log_root()
        # for external benchmarks, there is no match to a car
        if car:
            car_suffix = "-%s" % car.name
        else:
            car_suffix = ""
        archive_path = "%s/logs-%s-%s%s.zip" % (invocation_root, track.name, challenge.name, car_suffix)
        io.compress(log_root, archive_path)
        print("\nLogs for this race are archived in %s" % archive_path)
        shutil.rmtree(log_root)

