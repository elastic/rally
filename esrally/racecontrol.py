import logging
import os
import urllib.error
# Sorry, but Maven relies on XML...
import xml.etree.ElementTree

import tabulate

from esrally import config, driver, exceptions, paths, sweeper, reporter, metrics, track, car, PROGRAM_NAME
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

    selected_challenge = ctx.config.opts("benchmarks", "challenge")
    for challenge in ctx.track.challenges:
        if challenge.name == selected_challenge:
            ctx.challenge = challenge

    if not ctx.challenge:
        raise exceptions.ImproperlyConfigured("Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
                                              "challenges with %s list tracks." % (selected_challenge, ctx.track.name, PROGRAM_NAME))

    race_paths = paths.Paths(ctx.config)
    ctx.config.add(config.Scope.challenge, "system", "challenge.root.dir",
                        race_paths.challenge_root(ctx.track.name, ctx.challenge.name))
    ctx.config.add(config.Scope.challenge, "system", "challenge.log.dir",
                        race_paths.challenge_logs(ctx.track.name, ctx.challenge.name))


def prepare_car(ctx):
    selected_car = ctx.config.opts("benchmarks", "car")
    for c in car.cars:
        if c.name == selected_car:
            ctx.car = c

    if not ctx.car:
        raise exceptions.ImproperlyConfigured("Unknown car [%s]. You can list the available cars with %s list cars."
                                              % (selected_car, PROGRAM_NAME))


def store_race(ctx):
    metrics.race_store(ctx.config).store_race(ctx.track)


# benchmark when we provision ourselves
def benchmark_internal(ctx):
    track = ctx.track
    challenge = ctx.challenge
    car = ctx.car
    print("Racing on track [%s] and challenge [%s] with car [%s]" % (track.name, challenge.name, car.name))

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
    ctx.mechanic.start_metrics(track_name, challenge_name, "external")
    ctx.cluster = ctx.mechanic.start_engine_external()


# benchmark assuming Elasticsearch is already running externally
def benchmark_external(ctx):
    track = ctx.track
    challenge = ctx.challenge
    print("Racing on track [%s] and challenge [%s]" % (track.name, challenge.name))
    ctx.mechanic.setup_index(ctx.cluster, track, challenge)
    driver.Driver(ctx.config, ctx.cluster, track, challenge).go()
    ctx.mechanic.stop_metrics()


def download_benchmark_candidate(ctx):
    version = ctx.config.opts("source", "distribution.version")
    repo_name = ctx.config.opts("source", "distribution.repository")
    if version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")
    distributions_root = "%s/%s" % (ctx.config.opts("system", "root.dir"), ctx.config.opts("source", "distribution.dir"))
    io.ensure_dir(distributions_root)
    distribution_path = "%s/elasticsearch-%s.tar.gz" % (distributions_root, version)

    try:
        repo = distribution_repos[repo_name]
    except KeyError:
        raise exceptions.SystemSetupError("Unknown distribution repository [%s]. Valid values are: [%s]"
                                          % (repo_name, ",".join(distribution_repos.keys())))

    download_url = repo.download_url(version)
    logger.info("Resolved download URL [%s] for version [%s]" % (download_url, version))
    if not os.path.isfile(distribution_path) or repo.must_download:
        logger.info("Downloading distribution for version [%s]." % version)
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


class ReleaseDistributionRepo:
    def __init__(self):
        self.must_download = False

    def download_url(self, version):
        major_version = int(versions.components(version)["major"])
        if major_version > 1:
            download_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/%s/" \
                           "elasticsearch-%s.tar.gz" % (version, version)
        else:
            download_url = "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-%s.tar.gz" % version

        return download_url


class SnapshotDistributionRepo:
    def __init__(self):
        self.must_download = True

    def download_url(self, version):
        root_path = "https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/distribution/tar/elasticsearch/%s" % version
        metadata_url = "%s/maven-metadata.xml" % root_path
        try:
            metadata = net.retrieve_content_as_string(metadata_url)
            x = xml.etree.ElementTree.fromstring(metadata)
            found_snapshot_versions = x.findall("./versioning/snapshotVersions/snapshotVersion/[extension='tar.gz']/value")
        except Exception:
            logger.exception("Could not retrieve a valid metadata.xml file from remote URL [%s]." % metadata_url)
            raise exceptions.SystemSetupError("Cannot derive download URL for Elasticsearch %s" % version)

        if len(found_snapshot_versions) == 1:
            snapshot_id = found_snapshot_versions[0].text
            return "%s/elasticsearch-%s.tar.gz" % (root_path, snapshot_id)
        else:
            logger.error("Found [%d] version identifiers in [%s]. Contents: %s" % (len(found_snapshot_versions), metadata_url, metadata))
            raise exceptions.SystemSetupError("Cannot derive download URL for Elasticsearch %s" % version)

distribution_repos = {
    "release": ReleaseDistributionRepo(),
    "snapshot": SnapshotDistributionRepo()
}


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
        lambda ctx=None: Pipeline("from-sources-complete", "Builds and provisions Elasticsearch, runs a benchmark and reports results.",
                         [
                             PipelineStep("check-can-handle-sources", ctx, check_can_handle_source_distribution),
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("prepare-car", ctx, prepare_car),
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("build", ctx, lambda ctx: ctx.mechanic.prepare_candidate()),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             PipelineStep("benchmark", ctx, benchmark_internal),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, lambda ctx: ctx.sweep(ctx.track, ctx.challenge, ctx.car))
                         ]
                         ),
    "from-sources-skip-build":
        lambda ctx=None: Pipeline("from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results.",
                         [
                             PipelineStep("check-can-handle-sources", ctx, check_can_handle_source_distribution),
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("prepare-car", ctx, prepare_car),
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             PipelineStep("benchmark", ctx, benchmark_internal),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, lambda ctx: ctx.sweep(ctx.track, ctx.challenge, ctx.car))
                         ]

                         ),
    "from-distribution":
        lambda ctx=None: Pipeline("from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports "
                                                  "results.",
                             [
                                 PipelineStep("kill-es", ctx, kill),
                                 PipelineStep("download-candidate", ctx, download_benchmark_candidate),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("prepare-car", ctx, prepare_car),
                                 PipelineStep("store-race", ctx, store_race),
                                 PipelineStep("benchmark", ctx, benchmark_internal),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, lambda ctx: ctx.sweep(ctx.track, ctx.challenge, ctx.car))
                             ]

                             ),
    "benchmark-only":
        lambda ctx=None: Pipeline("benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results",
                             [
                                 PipelineStep("warn-bogus", ctx, lambda ctx: print(bogus_results_warning)),
                                 PipelineStep("prepare-benchmark-external", ctx, prepare_benchmark_external),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("store-race", ctx, store_race),
                                 PipelineStep("benchmark", ctx, benchmark_external),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, lambda ctx: ctx.sweep(ctx.track, ctx.challenge, ctx.car))
                             ]
                             ),

}


def list_pipelines():
    print("Available pipelines:\n")
    print(tabulate.tabulate([[pipeline().name, pipeline().description] for pipeline in pipelines.values()],
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
        self.sweep = sweeper.Sweeper(cfg)
        self.track = None
        self.challenge = None
        self.car = None
        self.cluster = None
