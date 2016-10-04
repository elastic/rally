import logging
import os
import sys
import urllib.error
# Sorry, but Maven relies on XML...
import xml.etree.ElementTree

import tabulate
import thespian.actors

from esrally import config, driver, exceptions, sweeper, track, reporter, metrics, car, client, PROGRAM_NAME
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


# TODO dm module refactoring: mechanic
def check_can_handle_source_distribution(ctx):
    try:
        ctx.config.opts("source", "local.src.dir")
    except config.ConfigError:
        logging.exception("Rally is not configured to build from sources")
        raise exceptions.SystemSetupError("Rally is not setup to build from sources. You can either benchmark a binary distribution or "
                                          "install the required software and reconfigure Rally with %s --configure." % PROGRAM_NAME)


# TODO dm module refactoring: mechanic (once per node or actually once per machine)
def kill(ctx):
    # we're very specific which nodes we kill as there is potentially also an Elasticsearch based metrics store running on this machine
    node_prefix = ctx.config.opts("provisioning", "node.name.prefix")
    process.kill_running_es_instances(node_prefix)


# TODO dm module refactoring: reporter?
def store_race(ctx):
    metrics.race_store(ctx.config).store_race(ctx.track)


def prepare_track(ctx):
    track_name = ctx.config.opts("system", "track")
    try:
        ctx.track = track.load_track(ctx.config, track_name)
    except FileNotFoundError:
        logger.error("Cannot load track [%s]" % track_name)
        raise exceptions.ImproperlyConfigured("Cannot load track %s. You can list the available tracks with %s list tracks." %
                                              (track_name, PROGRAM_NAME))
    # TODO #71: Reconsider this in case we distribute drivers. *For now* the driver will only be on a single machine, so we're safe.
    track.prepare_track(ctx.track, ctx.config)


# benchmark when we provision ourselves
def benchmark_internal(ctx):
    track_name = ctx.config.opts("system", "track")
    challenge_name = ctx.config.opts("benchmarks", "challenge")
    selected_car_name = ctx.config.opts("benchmarks", "car")

    print("Racing on track [%s] and challenge [%s] with car [%s]" % (track_name, challenge_name, selected_car_name))
    # TODO dm module refactoring: mechanic
    selected_car = None
    for c in car.cars:
        if c.name == selected_car_name:
            selected_car = c

    if not selected_car:
        raise exceptions.ImproperlyConfigured("Unknown car [%s]. You can list the available cars with %s list cars."
                                              % (selected_car_name, PROGRAM_NAME))

    port = ctx.config.opts("provisioning", "node.http.port")
    hosts = [{"host": "localhost", "port": port}]
    client_options = ctx.config.opts("launcher", "client.options")
    # unified client config
    ctx.config.add(config.Scope.benchmark, "client", "hosts", hosts)
    ctx.config.add(config.Scope.benchmark, "client", "options", client_options)

    es_client = client.EsClientFactory(hosts, client_options).create()

    # TODO dm module refactoring: separate module? don't let the mechanic handle the metrics store but rather just provide it
    ctx.mechanic.start_metrics(track_name, challenge_name, selected_car_name)
    cluster = ctx.mechanic.start_engine(selected_car, es_client, port)
    actors = thespian.actors.ActorSystem()
    main_driver = actors.createActor(driver.Driver)

    #TODO dm: Retrieving the metrics store here is *dirty*...
    metrics_store = ctx.mechanic._metrics_store

    cluster.on_benchmark_start()
    completed = actors.ask(main_driver, driver.StartBenchmark(ctx.config, ctx.track, metrics_store.meta_info))
    cluster.on_benchmark_stop()
    if not hasattr(completed, "metrics"):
        raise exceptions.RallyError("Driver has returned no metrics but instead [%s]. Terminating race without result." % str(completed))
    metrics_store.bulk_add(completed.metrics)

    ctx.mechanic.stop_engine(cluster)
    ctx.mechanic.revise_candidate()
    ctx.mechanic.stop_metrics()


# TODO dm module refactoring: mechanic
def prepare_benchmark_external(ctx):
    track_name = ctx.config.opts("system", "track")
    challenge_name = ctx.config.opts("benchmarks", "challenge")
    # override externally used car name for this benchmark. We'll use a fixed one for external benchmarks.
    car_name = "external"
    ctx.config.add(config.Scope.benchmark, "benchmarks", "car", car_name)

    ctx.mechanic.start_metrics(track_name, challenge_name, car_name)

    hosts = ctx.config.opts("launcher", "external.target.hosts")
    client_options = ctx.config.opts("launcher", "client.options")
    # unified client config
    ctx.config.add(config.Scope.benchmark, "client", "hosts", hosts)
    ctx.config.add(config.Scope.benchmark, "client", "options", client_options)

    es_client = client.EsClientFactory(hosts, client_options).create()
    ctx.cluster = ctx.mechanic.start_engine_external(es_client)


# benchmark assuming Elasticsearch is already running externally
def benchmark_external(ctx):
    # TODO dm module refactoring: we can just inline prepare_benchmark_external and simplify this code a bit
    track_name = ctx.config.opts("system", "track")
    challenge_name = ctx.config.opts("benchmarks", "challenge")
    print("Racing on track [%s] and challenge [%s]" % (track_name, challenge_name))
    actors = thespian.actors.ActorSystem()
    main_driver = actors.createActor(driver.Driver)
    #TODO dm: Retrieving the metrics store here is *dirty*...
    metrics_store = ctx.mechanic._metrics_store

    ctx.cluster.on_benchmark_start()
    completed = actors.ask(main_driver, driver.StartBenchmark(ctx.config, ctx.track, metrics_store.meta_info))
    ctx.cluster.on_benchmark_stop()
    if not hasattr(completed, "metrics"):
        raise exceptions.RallyError("Driver has returned no metrics but instead [%s]. Terminating race without result." % str(completed))
    metrics_store.bulk_add(completed.metrics)
    ctx.mechanic.stop_metrics()


# TODO dm module refactoring: mechanic
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
        if major_version > 1 and not self.on_or_after_5_0_0_beta1(version):
            download_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/%s/" \
                           "elasticsearch-%s.tar.gz" % (version, version)
        elif self.on_or_after_5_0_0_beta1(version):
            download_url = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-%s.tar.gz" % version
        else:
            download_url = "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-%s.tar.gz" % version
        return download_url

    def on_or_after_5_0_0_beta1(self, version):
        components = versions.components(version)
        major_version = int(components["major"])
        minor_version = int(components["minor"])
        patch_version = int(components["patch"])
        suffix = components["suffix"] if "suffix" in components else None

        if major_version < 5:
            return False
        elif major_version == 5 and minor_version == 0 and patch_version == 0 and suffix and suffix.startswith("alpha"):
            return False
        return True

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

Rally does not have control over the configuration of the benchmarked
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
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("build", ctx, lambda ctx: ctx.mechanic.prepare_candidate()),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             PipelineStep("benchmark", ctx, benchmark_internal),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, sweeper.sweep)
                         ]
                         ),
    "from-sources-skip-build":
        lambda ctx=None: Pipeline("from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results.",
                         [
                             PipelineStep("check-can-handle-sources", ctx, check_can_handle_source_distribution),
                             PipelineStep("kill-es", ctx, kill),
                             PipelineStep("prepare-track", ctx, prepare_track),
                             PipelineStep("store-race", ctx, store_race),
                             PipelineStep("find-candidate", ctx, lambda ctx: ctx.mechanic.find_candidate()),
                             PipelineStep("benchmark", ctx, benchmark_internal),
                             PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                             PipelineStep("sweep", ctx, sweeper.sweep)
                         ]

                         ),
    "from-distribution":
        lambda ctx=None: Pipeline("from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports "
                                                  "results.",
                             [
                                 PipelineStep("kill-es", ctx, kill),
                                 PipelineStep("download-candidate", ctx, download_benchmark_candidate),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("store-race", ctx, store_race),
                                 PipelineStep("benchmark", ctx, benchmark_internal),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, sweeper.sweep)
                             ]

                             ),
    "benchmark-only":
        lambda ctx=None: Pipeline("benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results",
                             [
                                 PipelineStep("warn-bogus", ctx, lambda ctx: print(bogus_results_warning)),
                                 PipelineStep("prepare-track", ctx, prepare_track),
                                 PipelineStep("prepare-benchmark-external", ctx, prepare_benchmark_external),
                                 PipelineStep("store-race", ctx, store_race),
                                 PipelineStep("benchmark", ctx, benchmark_external),
                                 PipelineStep("report", ctx, lambda ctx: ctx.reporter.report(ctx.track)),
                                 PipelineStep("sweep", ctx, sweeper.sweep)
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
    except KeyError:
        raise exceptions.ImproperlyConfigured(
            "Unknown pipeline [%s]. You can list the available pipelines with %s list pipelines." % (name, PROGRAM_NAME))
    try:
        pipeline()
    except exceptions.RallyError as e:
        # just pass on our own errors. It should be treated differently on top-level
        raise e
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.RallyError("This race ended early with a fatal crash. For details please see the logs.").with_traceback(tb)


class RacingContext:
    def __init__(self, cfg):
        self.config = cfg
        self.mechanic = mechanic.Mechanic(cfg)
        self.reporter = reporter.SummaryReporter(cfg)
        self.track = None
        self.cluster = None
