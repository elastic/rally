import logging
import os
import urllib.error

import tabulate

from esrally import config, driver, exceptions, paths, telemetry, sweeper, reporter, metrics, time, track, car, PROGRAM_NAME
from esrally.mechanic import mechanic
from esrally.utils import process, net, io, versions

logger = logging.getLogger("rally.racecontrol")


class PipelineStep:
    """
    Describes a single step in an execution pipeline, e.g. "build" or "benchmark".
    """
    def __init__(self, name, ctx, command):
        self.name = name
        self.ctx = ctx
        self.command = command

    def __call__(self, challenge=None, car=None):
        if challenge is None and car is None:
            self.command(self.ctx)
        else:
            self.command(self.ctx, challenge, car)


class ComponentSelector:
    """
    A special step in a pipeline which extracts car and challenge based on the current configuration.
    """
    def __init__(self, ctx, step):
        self.ctx = ctx
        self.step = step

    def __call__(self):
        track = self.ctx.track
        challenge = self.find_challenge(track)
        car = self.find_car()
        race_paths = paths.Paths(self.ctx.config)
        self.ctx.config.add(config.Scope.challenge, "system", "challenge.root.dir",
                            race_paths.challenge_root(track.name, challenge.name))
        self.ctx.config.add(config.Scope.challenge, "system", "challenge.log.dir",
                            race_paths.challenge_logs(track.name, challenge.name))
        print("Racing on track [%s] and challenge [%s] with car [%s]" % (track.name, challenge.name, car.name))
        self.step(challenge, car)

    def find_challenge(self, track):
        selected_challenge = self.ctx.config.opts("benchmarks", "challenge")
        for challenge in track.challenges:
            if challenge.name == selected_challenge:
                return challenge

        raise exceptions.ImproperlyConfigured("Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
                                              "challenges with %s list tracks." % (selected_challenge, track.name, PROGRAM_NAME))

    def find_car(self):
        selected_car = self.ctx.config.opts("benchmarks", "car")
        for c in car.cars:
            if c.name == selected_car:
                return c
        raise exceptions.ImproperlyConfigured("Unknown car [%s]. You can list the available cars with %s list cars."
                                              % (selected_car, PROGRAM_NAME))


class Pipeline:
    """
    Describes a whole execution pipeline. A pipeline can consist of one or more steps. Each pipeline should contain roughly of the following
    steps:

    * Prepare the benchmark candidate: It can build Elasticsearch from sources, download a ZIP from somewhere etc.
    * Launch the benchmark candidate: This can be done directly, with tools like Ansible or it can assume the candidate is already launched
    * Run the benchmark
    * Report results
    """

    def __init__(self, name, description, steps):
        """
        Creates a new pipeline.

        :param name: A short name of the pipeline. This name will be used to reference it from the command line.
        :param description: A human-readable description what the pipeline does.
        :param steps: The concrete steps to execute. The steps will be executed in the provided order. A pipeline should consist of at
        least one step.
        """
        self.name = name
        self.description = description
        self.steps = steps

    def __call__(self):
        for step in self.steps:
            step()

#################################################
#
# Internal helper functions for pipeline steps
#
# If a helper function is short enough it can
# also be added just as a lambda.
#
#################################################


def check_can_handle_source_distribution(ctx):
    try:
        ctx.config.opts("source", "local.src.dir")
    except config.ConfigError:
        logging.exception("Rally is not configured to build from sources")
        raise exceptions.SystemSetupError("Rally is not setup to build from sources. You can either benchmark a binary distribution or "
                                          "install the required software and reconfigure Rally with %s --configure." % PROGRAM_NAME)


def kill(ctx):
    # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
    node_prefix = ctx.config.opts("provisioning", "node.name.prefix")
    process.kill_running_es_instances(node_prefix)


def prepare_track(ctx):
    track_name = ctx.config.opts("system", "track")
    try:
        ctx.track = track.load_track(ctx.config, track_name)
    except FileNotFoundError:
        logger.error("Cannot load track [%s]" % track_name)
        raise exceptions.ImproperlyConfigured("Cannot load track %s. You can list the available tracks with %s list tracks." %
                                              (track_name, PROGRAM_NAME))

    track.prepare_track(ctx.track, ctx.config)
    race_paths = paths.Paths(ctx.config)
    track_root = race_paths.track_root(track_name)
    ctx.config.add(config.Scope.benchmark, "system", "track.root.dir", track_root)


def store_race(ctx):
    metrics.race_store(ctx.config).store_race(ctx.track)


# benchmark when we provision ourselves
def benchmark_internal(ctx, challenge, car):
    track = ctx.track
    ctx.mechanic.start_metrics(track, challenge, car)
    cluster = ctx.mechanic.start_engine(car)
    ctx.mechanic.setup_index(cluster, track, challenge)
    driver.Driver(ctx.config, cluster, track, challenge).go()
    ctx.mechanic.stop_engine(cluster)
    ctx.mechanic.revise_candidate()
    ctx.mechanic.stop_metrics()


def prepare_benchmark_external(ctx):
    track_name = ctx.config.opts("system", "track")
    challenge_name = ctx.config.opts("benchmarks", "challenge")
    car_name = ctx.config.opts("benchmarks", "car")
    ctx.mechanic.start_metrics(track_name, challenge_name, car_name)
    ctx.cluster = ctx.mechanic.start_engine_external(car)


# benchmark assuming Elasticsearch is already running externally
def benchmark_external(ctx, challenge, car):
    track = ctx.track
    ctx.mechanic.setup_index(ctx.cluster, track, challenge)
    driver.Driver(ctx.config, ctx.cluster, track, challenge).go()
    ctx.mechanic.stop_metrics()


def download_benchmark_candidate(ctx):
    version = ctx.config.opts("source", "distribution.version")
    if version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")

    distributions_root = "%s/%s" % (ctx.config.opts("system", "root.dir"), ctx.config.opts("source", "distribution.dir"))
    io.ensure_dir(distributions_root)
    distribution_path = "%s/elasticsearch-%s.tar.gz" % (distributions_root, version)

    major_version = int(versions.components(version)["major"])

    if major_version > 1:
        download_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/%s/" \
                       "elasticsearch-%s.tar.gz" % (version, version)
    else:
        download_url = "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-%s.tar.gz" % version
    if not os.path.isfile(distribution_path):
        try:
            print("Downloading Elasticsearch %s ..." % version)
            net.download(download_url, distribution_path)
        except urllib.error.HTTPError:
            logging.exception("Cannot download Elasticsearch distribution for version [%s] from [%s]." % (version, download_url))
            raise exceptions.SystemSetupError("Cannot download Elasticsearch distribution from [%s]. Please check that the specified "
                                              "version [%s] is correct." % (download_url, version))
    else:
        logger.info("Skipping download for version [%s]. Found an existing binary locally at [%s]." % (version, distribution_path))

    ctx.config.add(config.Scope.invocation, "builder", "candidate.bin.path", distribution_path)


# benchmarks with external candidates are really scary and we should warn users.
bogus_results_warning = """
************************************************************************
************** WARNING: A dark dungeon lies ahead of you  **************
************************************************************************

Rally dos not have control over the configuration of the benchmarked
Elasticsearch cluster.

Be aware that results may be misleading due to problems with the setup.
Rally is also not able to gather lots of metrics at all (like CPU usage
of the benchmarked cluster) or may even produce misleading metrics (like
the index size).

************************************************************************
****** Use this pipeline only if you are aware of the tradeoffs.  ******
*************************** Watch your step! ***************************
************************************************************************
"""

pipelines = {
    "from-sources-complete":
        lambda ctx: Pipeline("from-sources-complete", "Builds and provisions Elasticsearch, runs a benchmark and reports results.",
                         [
                             PipelineStep("check-can-handle-sources", ctx, check_can_handle_source_distribution),
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("build", ctx, lambda ctx: ctx.mechanic.prepare_candidate()),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             ComponentSelector(ctx, PipelineStep("benchmark", ctx, benchmark_internal)),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, lambda ctx: ctx.sweeper.run(ctx.track))
                         ]
                         ),
    "from-sources-skip-build":
        lambda ctx: Pipeline("from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results.",
                         [
                             PipelineStep("check-can-handle-sources", ctx, check_can_handle_source_distribution),
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             ComponentSelector(ctx, PipelineStep("benchmark", ctx, benchmark_internal)),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, lambda ctx: ctx.sweeper.run(ctx.track))
                         ]

                         ),
    "from-distribution":
        lambda ctx: Pipeline("from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports "
                                                  "results.",
                             [
                                 PipelineStep("kill-es", ctx, kill),
                                 PipelineStep("download-candidate", ctx, download_benchmark_candidate),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("store-race", ctx, store_race),
                                 ComponentSelector(ctx, PipelineStep("benchmark", ctx, benchmark_internal)),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, lambda ctx: ctx.sweeper.run(ctx.track))
                             ]

                             ),
    "benchmark-only":
        lambda ctx: Pipeline("benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results",
                             [
                                 PipelineStep("warn-bogus", ctx, lambda ctx: print(bogus_results_warning)),
                                 PipelineStep("prepare-benchmark-external", ctx, prepare_benchmark_external),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("store-race", ctx, store_race),
                                 ComponentSelector(ctx, PipelineStep("benchmark", ctx, benchmark_external)),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, lambda ctx: ctx.sweeper.run(ctx.track))
                             ]
                             ),

}


def list_pipelines():
    print("Available pipelines:\n")
    # TODO dm: Get rid of the need for ctx here (workaround: use None)
    print(tabulate.tabulate([[pipeline(None).name, pipeline(None).description] for pipeline in pipelines.values()],
                            headers=["Name", "Description"]))


def run(cfg):
    name = cfg.opts("system", "pipeline")
    try:
        pipeline = pipelines[name](RacingContext(cfg))
        pipeline()
    except KeyError:
        raise exceptions.ImproperlyConfigured(
            "Unknown pipeline [%s]. You can list the available pipelines with %s list pipelines." % (name, PROGRAM_NAME))


class RacingContext:
    def __init__(self, cfg):
        self.config = cfg
        self.mechanic = mechanic.Mechanic(cfg)
        self.reporter = reporter.SummaryReporter(cfg)
        self.sweeper = sweeper.Sweeper(cfg)
        self.track = None
        self.cluster = None
