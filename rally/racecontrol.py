import logging

from rally import config, driver, exceptions, paths, telemetry, sweeper, summary_reporter
from rally.mechanic import mechanic
from rally.utils import process
# This is one of the few occasions where we really want to use a star import. As new tracks are added we want to "autodiscover" them
from rally.track import *

logger = logging.getLogger("rally.racecontrol")


class PipelineStep:
    """
    Describes a single step in an execution pipeline, e.g. "build" or "benchmark".
    """
    def __init__(self, name, ctx, command):
        self.name = name
        self.ctx = ctx
        self.command = command

    def run(self, track, track_setup=None):
        if track_setup is None:
            self.command(self.ctx, track)
        else:
            self.command(self.ctx, track, track_setup)


class TrackSetupIterator:
    """
    A special step in a pipeline which changes the iteration granularity from tracks to track setups.
    """
    def __init__(self, ctx, steps):
        self.ctx = ctx
        self.steps = steps

    def run(self, track):
        selected_setups = self.ctx.config.opts("benchmarks", "tracksetups.selected")
        for track_setup in track.track_setups:
            if track_setup.name in selected_setups:
                race_paths = paths.Paths(self.ctx.config)
                self.ctx.config.add(config.Scope.trackSetup, "system", "track.setup.root.dir",
                                    race_paths.track_setup_root(track.name, track_setup.name))
                self.ctx.config.add(config.Scope.trackSetup, "system", "track.setup.log.dir",
                                    race_paths.track_setup_logs(track.name, track_setup.name))
                print("Racing on track '%s' with setup '%s'" % (track.name, track_setup.name))
                for step in self.steps:
                    step.run(track, track_setup)
            else:
                logger.debug("Skipping track setup [%s] (not selected)." % track_setup.name)


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

    def run(self, track):
        print("Overall ETA: %d minutes (depending on your hardware)\n" % track.estimated_benchmark_time_in_minutes)
        for step in self.steps:
            step.run(track)

#################################################
#
# Internal helper functions for pipeline steps
#
# If a helper function is short enough it can
# also be added just as a lambda.
#
#################################################


def kill(ctx, track):
    # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
    node_prefix = ctx.config.opts("provisioning", "node.name.prefix")
    process.kill_running_es_instances(node_prefix)


def prepare_track(ctx, track):
    ctx.marshal.setup(track)
    race_paths = paths.Paths(ctx.config)
    track_root = race_paths.track_root(track.name)
    ctx.config.add(config.Scope.benchmark, "system", "track.root.dir", track_root)


# benchmark when we provision ourselves
def benchmark_internal(ctx, track, track_setup):
    ctx.mechanic.start_metrics(track, track_setup)
    cluster = ctx.mechanic.start_engine(track, track_setup)
    ctx.driver.setup(cluster, track, track_setup)
    ctx.driver.go(cluster, track, track_setup)
    ctx.mechanic.stop_engine(cluster)
    ctx.driver.tear_down(track, track_setup)
    ctx.mechanic.revise_candidate()
    ctx.mechanic.stop_metrics()


# benchmark assuming Elasticsearch is already running externally
def benchmark_external(ctx, track, track_setup):
    ctx.mechanic.start_metrics(track, track_setup)
    cluster = ctx.mechanic.start_engine_external(track, track_setup)
    ctx.driver.setup(cluster, track, track_setup)
    ctx.driver.go(cluster, track, track_setup)
    ctx.driver.tear_down(track, track_setup)
    ctx.mechanic.stop_metrics()

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
        lambda ctx: Pipeline("from-sources-complete", "Builds and provisions Elasticsearch, runs a benchmark and reports results",
                         [
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("build", ctx, lambda ctx, track: ctx.mechanic.prepare_candidate()),
                             TrackSetupIterator(ctx, [PipelineStep("benchmark", ctx, benchmark_internal)]),
                             PipelineStep("sweep", ctx, lambda ctx, track: ctx.sweeper.run()),
                             PipelineStep("report", ctx, lambda ctx, track: ctx.reporter.report(track)),
                         ]
                         ),
    "from-sources-skip-build":
        lambda ctx: Pipeline("from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results",
                         [
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             TrackSetupIterator(ctx, [PipelineStep("benchmark", ctx, benchmark_internal)]),
                             PipelineStep("sweep", ctx, lambda ctx, track: ctx.sweeper.run()),
                             PipelineStep("report", ctx, lambda ctx, track: ctx.reporter.report(track)),
                         ]

                         ),
    "benchmark-only":
        lambda ctx: Pipeline("benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results",
                             [
                                 PipelineStep("warn-bogus", ctx, lambda ctx, track: print(bogus_results_warning)),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 TrackSetupIterator(ctx, [PipelineStep("benchmark", ctx, benchmark_external)]),
                                 PipelineStep("sweep", ctx, lambda ctx, track: ctx.sweeper.run()),
                                 PipelineStep("report", ctx, lambda ctx, track: ctx.reporter.report(track)),
                             ]
                             ),

}


class RaceControl:
    def __init__(self, cfg):
        self._config = cfg

    def start(self, command):
        ctx = RacingContext(self._config)
        if command == "list":
            self._list(ctx)
        elif command == "race":
            pipeline = self._choose(pipelines, "pipeline")(ctx)
            t = self._choose(track.tracks, "track")
            pipeline.run(t)
        else:
            raise exceptions.ImproperlyConfigured("Unknown command [%s]" % command)

    def _choose(self, source, what):
        try:
            name = self._config.opts("system", what)
            return source[name]
        except KeyError:
            raise exceptions.ImproperlyConfigured("Unknown %s [%s]" % (what, name))

    def _list(self, ctx):
        what = ctx.config.opts("system", "list.config.option")
        if what == "telemetry":
            telemetry.Telemetry(ctx.config).list()
        elif what == "tracks":
            print("Available tracks:\n")
            for t in track.tracks.values():
                print("* %s: %s" % (t.name, t.description))
                print("\tTrack setups for this track:")
                for track_setup in track.track_setups:
                    print("\t* %s" % track_setup.name)
                print("")
        elif what == "pipelines":
            print("Available pipelines:\n")
            for p in pipelines.values():
                pipeline = p(ctx)
                print("* %s: %s" % (pipeline.name, pipeline.description))
        else:
            raise exceptions.ImproperlyConfigured("Cannot list unknown configuration option [%s]" % what)


class RacingContext:
    def __init__(self, cfg):
        self.config = cfg
        self.mechanic = mechanic.Mechanic(cfg)
        self.driver = driver.Driver(cfg)
        self.marshal = track.Marshal(cfg)
        self.reporter = summary_reporter.SummaryReporter(cfg)
        self.sweeper = sweeper.Sweeper(cfg)
