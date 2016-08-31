import shutil

from esrally import paths
from esrally.utils import io


def sweep(ctx):
    invocation_root = paths.Paths(ctx.config).invocation_root()
    track_name = ctx.config.opts("system", "track")
    challenge_name = ctx.config.opts("benchmarks", "challenge")
    car_name = ctx.config.opts("benchmarks", "car")

    log_root = paths.Paths(ctx.config).log_root()
    # for external benchmarks, there is no match to a car
    car_suffix = "-%s" % car_name if car_name else ""
    archive_path = "%s/logs-%s-%s%s.zip" % (invocation_root, track_name, challenge_name, car_suffix)
    io.compress(log_root, archive_path)
    print("\nLogs for this race are archived in %s" % archive_path)
    shutil.rmtree(log_root)

