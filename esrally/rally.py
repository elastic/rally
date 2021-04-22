# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import datetime
import logging
import os
import platform
import sys
import time
import uuid

import thespian.actors

from esrally import PROGRAM_NAME, BANNER, FORUM_LINK, SKULL, check_python_version, doc_link, telemetry
from esrally import version, actor, config, paths, racecontrol, reporter, metrics, track, chart_generator, exceptions, \
    log
from esrally.mechanic import team, mechanic
from esrally.tracker import tracker
from esrally.utils import io, convert, process, console, net, opts, versions


def create_arg_parser():
    def positive_number(v):
        value = int(v)
        if value <= 0:
            raise argparse.ArgumentTypeError(f"must be positive but was {value}")
        return value

    def non_empty_list(arg):
        lst = opts.csv_to_list(arg)
        if len(lst) < 1:
            raise argparse.ArgumentError(argument=None, message="At least one argument required!")
        return lst

    def runtime_jdk(v):
        if v == "bundled":
            return v
        else:
            try:
                return positive_number(v)
            except argparse.ArgumentTypeError:
                raise argparse.ArgumentTypeError(f"must be a positive number or 'bundled' but was {v}")

    def supported_es_version(v):
        if v:
            min_es_version = versions.Version.from_string(version.minimum_es_version())
            specified_version = versions.Version.from_string(v)
            if specified_version < min_es_version:
                raise argparse.ArgumentTypeError(f"must be at least {min_es_version} but was {v}")
        return v

    def add_track_source(subparser):
        track_source_group = subparser.add_mutually_exclusive_group()
        track_source_group.add_argument(
            "--track-repository",
            help="Define the repository from where Rally will load tracks (default: default).",
            # argparse is smart enough to use this default only if the user did not use --track-path and also did not specify anything
            default="default"
        )
        track_source_group.add_argument(
            "--track-path",
            help="Define the path to a track.")
        subparser.add_argument(
            "--track-revision",
            help="Define a specific revision in the track repository that Rally should use.",
            default=None)

    # try to preload configurable defaults, but this does not work together with `--configuration-name` (which is undocumented anyway)
    cfg = config.Config()
    if cfg.config_present():
        cfg.load_config()
        preserve_install = cfg.opts("defaults", "preserve_benchmark_candidate", default_value=False, mandatory=False)
    else:
        preserve_install = False

    parser = argparse.ArgumentParser(prog=PROGRAM_NAME,
                                     description=BANNER + "\n\n You Know, for Benchmarking Elasticsearch.",
                                     epilog="Find out more about Rally at {}".format(console.format.link(doc_link())),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version="%(prog)s " + version.version())

    subparsers = parser.add_subparsers(
        title="subcommands",
        dest="subcommand",
        help="")

    race_parser = subparsers.add_parser("race", help="Run a benchmark")
    # change in favor of "list telemetry", "list tracks", "list pipelines"
    list_parser = subparsers.add_parser("list", help="List configuration options")
    list_parser.add_argument(
        "configuration",
        metavar="configuration",
        help="The configuration for which Rally should show the available options. "
             "Possible values are: telemetry, tracks, pipelines, races, cars, elasticsearch-plugins",
        choices=["telemetry", "tracks", "pipelines", "races", "cars", "elasticsearch-plugins"])
    list_parser.add_argument(
        "--limit",
        help="Limit the number of search results for recent races (default: 10).",
        default=10,
    )
    add_track_source(list_parser)

    info_parser = subparsers.add_parser("info", help="Show info about a track")
    add_track_source(info_parser)
    info_parser.add_argument(
        "--track",
        help=f"Define the track to use. List possible tracks with `{PROGRAM_NAME} list tracks`."
        # we set the default value later on because we need to determine whether the user has provided this value.
        # default="geonames"
    )

    info_parser.add_argument(
        "--track-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to the track as variables.",
        default=""
    )
    info_parser.add_argument(
        "--challenge",
        help=f"Define the challenge to use. List possible challenges for tracks with `{PROGRAM_NAME} list tracks`."
    )
    info_task_filter_group = info_parser.add_mutually_exclusive_group()
    info_task_filter_group.add_argument(
        "--include-tasks",
        help="Defines a comma-separated list of tasks to run. By default all tasks of a challenge are run.")
    info_task_filter_group.add_argument(
        "--exclude-tasks",
        help="Defines a comma-separated list of tasks not to run. By default all tasks of a challenge are run.")

    create_track_parser = subparsers.add_parser("create-track", help="Create a Rally track from existing data")
    create_track_parser.add_argument(
        "--track",
        required=True,
        help="Name of the generated track")
    create_track_parser.add_argument(
        "--indices",
        type=non_empty_list,
        required=True,
        help="Comma-separated list of indices to include in the track")
    create_track_parser.add_argument(
        "--target-hosts",
        default="",
        required=True,
        help="Comma-separated list of host:port pairs which should be targeted")
    create_track_parser.add_argument(
        "--client-options",
        default=opts.ClientOptions.DEFAULT_CLIENT_OPTIONS,
        help=f"Comma-separated list of client options to use. (default: {opts.ClientOptions.DEFAULT_CLIENT_OPTIONS})")
    create_track_parser.add_argument(
        "--output-path",
        default=os.path.join(os.getcwd(), "tracks"),
        help="Track output directory (default: tracks/)")

    generate_parser = subparsers.add_parser("generate", help="Generate artifacts")
    generate_parser.add_argument(
        "artifact",
        metavar="artifact",
        help="The artifact to create. Possible values are: charts",
        choices=["charts"])
    # We allow to either have a chart-spec-path *or* define a chart-spec on the fly with track, challenge and car. Convincing
    # argparse to validate that everything is correct *might* be doable but it is simpler to just do this manually.
    generate_parser.add_argument(
        "--chart-spec-path",
        required=True,
        help="Path to a JSON file(s) containing all combinations of charts to generate. Wildcard patterns can be used to specify "
             "multiple files.")
    generate_parser.add_argument(
        "--chart-type",
        help="Chart type to generate (default: time-series).",
        choices=["time-series", "bar"],
        default="time-series")
    generate_parser.add_argument(
        "--output-path",
        help="Output file name (default: stdout).",
        default=None)

    compare_parser = subparsers.add_parser("compare", help="Compare two races")
    compare_parser.add_argument(
        "--baseline",
        required=True,
        help=f"Race ID of the baseline (see {PROGRAM_NAME} list races).")
    compare_parser.add_argument(
        "--contender",
        required=True,
        help=f"Race ID of the contender (see {PROGRAM_NAME} list races).")
    compare_parser.add_argument(
        "--report-format",
        help="Define the output format for the command line report (default: markdown).",
        choices=["markdown", "csv"],
        default="markdown")
    compare_parser.add_argument(
        "--report-numbers-align",
        help="Define the output column number alignment for the command line report (default: right).",
        choices=["right", "center", "left", "decimal"],
        default="right")
    compare_parser.add_argument(
        "--report-file",
        help="Write the command line report also to the provided file.",
        default="")
    compare_parser.add_argument(
        "--show-in-report",
        help="Whether to include the comparison in the results file.",
        default=True)

    download_parser = subparsers.add_parser("download", help="Downloads an artifact")
    download_parser.add_argument(
        "--team-repository",
        help="Define the repository from where Rally will load teams and cars (default: default).",
        default="default")
    download_parser.add_argument(
        "--team-revision",
        help="Define a specific revision in the team repository that Rally should use.",
        default=None)
    download_parser.add_argument(
        "--team-path",
        help="Define the path to the car and plugin configurations to use.")
    download_parser.add_argument(
        "--distribution-version",
        type=supported_es_version,
        help="Define the version of the Elasticsearch distribution to download. "
             "Check https://www.elastic.co/downloads/elasticsearch for released versions.",
        default="")
    download_parser.add_argument(
        "--distribution-repository",
        help="Define the repository from where the Elasticsearch distribution should be downloaded (default: release).",
        default="release")
    download_parser.add_argument(
        "--car",
        help=f"Define the car to use. List possible cars with `{PROGRAM_NAME} list cars` (default: defaults).",
        default="defaults")  # optimized for local usage
    download_parser.add_argument(
        "--car-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim as variables for the car.",
        default=""
    )
    download_parser.add_argument(
        "--target-os",
        help="The name of the target operating system for which an artifact should be downloaded (default: current OS)",
    )
    download_parser.add_argument(
        "--target-arch",
        help="The name of the CPU architecture for which an artifact should be downloaded (default: current architecture)",
    )

    install_parser = subparsers.add_parser("install", help="Installs an Elasticsearch node locally")
    install_parser.add_argument(
        "--revision",
        help="Define the source code revision for building the benchmark candidate. 'current' uses the source tree as is,"
             " 'latest' fetches the latest version on master. It is also possible to specify a commit id or a timestamp."
             " The timestamp must be specified as: \"@ts\" where \"ts\" must be a valid ISO 8601 timestamp, "
             "e.g. \"@2013-07-27T10:37:00Z\" (default: current).",
        default="current")  # optimized for local usage, don't fetch sources
    # Intentionally undocumented as we do not consider Docker a fully supported option.
    install_parser.add_argument(
        "--build-type",
        help=argparse.SUPPRESS,
        choices=["tar", "docker"],
        default="tar")
    install_parser.add_argument(
        "--team-repository",
        help="Define the repository from where Rally will load teams and cars (default: default).",
        default="default")
    install_parser.add_argument(
        "--team-revision",
        help="Define a specific revision in the team repository that Rally should use.",
        default=None)
    install_parser.add_argument(
        "--team-path",
        help="Define the path to the car and plugin configurations to use.")
    install_parser.add_argument(
        "--runtime-jdk",
        type=runtime_jdk,
        help="The major version of the runtime JDK to use during installation.",
        default=None)
    install_parser.add_argument(
        "--distribution-repository",
        help="Define the repository from where the Elasticsearch distribution should be downloaded (default: release).",
        default="release")
    install_parser.add_argument(
        "--distribution-version",
        type=supported_es_version,
        help="Define the version of the Elasticsearch distribution to download. "
             "Check https://www.elastic.co/downloads/elasticsearch for released versions.",
        default="")
    install_parser.add_argument(
        "--car",
        help=f"Define the car to use. List possible cars with `{PROGRAM_NAME} list cars` (default: defaults).",
        default="defaults")  # optimized for local usage
    install_parser.add_argument(
        "--car-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim as variables for the car.",
        default=""
    )
    install_parser.add_argument(
        "--elasticsearch-plugins",
        help="Define the Elasticsearch plugins to install. (default: install no plugins).",
        default="")
    install_parser.add_argument(
        "--plugin-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to all plugins as variables.",
        default=""
    )
    install_parser.add_argument(
        "--network-host",
        help="The IP address to bind to and publish",
        default="127.0.0.1"
    )
    install_parser.add_argument(
        "--http-port",
        help="The port to expose for HTTP traffic",
        default="39200"
    )
    install_parser.add_argument(
        "--node-name",
        help="The name of this Elasticsearch node",
        default="rally-node-0"
    )
    install_parser.add_argument(
        "--master-nodes",
        help="A comma-separated list of the initial master node names",
        default=""
    )
    install_parser.add_argument(
        "--seed-hosts",
        help="A comma-separated list of the initial seed host IPs",
        default=""
    )

    start_parser = subparsers.add_parser("start", help="Starts an Elasticsearch node locally")
    start_parser.add_argument(
        "--installation-id",
        required=True,
        help="The id of the installation to start",
        # the default will be dynamically derived by racecontrol based on the presence / absence of other command line options
        default="")
    start_parser.add_argument(
        "--race-id",
        required=True,
        help="Define a unique id for this race.",
        default="")
    start_parser.add_argument(
        "--runtime-jdk",
        type=runtime_jdk,
        help="The major version of the runtime JDK to use.",
        default=None)
    start_parser.add_argument(
        "--telemetry",
        help=f"Enable the provided telemetry devices, provided as a comma-separated list. List possible telemetry "
             f"devices with `{PROGRAM_NAME} list telemetry`.",
        default="")
    start_parser.add_argument(
        "--telemetry-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to the telemetry devices as parameters.",
        default=""
    )

    stop_parser = subparsers.add_parser("stop", help="Stops an Elasticsearch node locally")
    stop_parser.add_argument(
        "--installation-id",
        required=True,
        help="The id of the installation to stop",
        # the default will be dynamically derived by racecontrol based on the presence / absence of other command line options
        default="")
    stop_parser.add_argument(
        "--preserve-install",
        help=f"Keep the benchmark candidate and its index. (default: {str(preserve_install).lower()}).",
        default=preserve_install,
        action="store_true")

    for p in [list_parser, race_parser]:
        p.add_argument(
            "--distribution-version",
            type=supported_es_version,
            help="Define the version of the Elasticsearch distribution to download. "
                 "Check https://www.elastic.co/downloads/elasticsearch for released versions.",
            default="")
        p.add_argument(
            "--team-path",
            help="Define the path to the car and plugin configurations to use.")
        p.add_argument(
            "--team-repository",
            help="Define the repository from where Rally will load teams and cars (default: default).",
            default="default")
        p.add_argument(
            "--team-revision",
            help="Define a specific revision in the team repository that Rally should use.",
            default=None)

    race_parser.add_argument(
        "--race-id",
        help="Define a unique id for this race.",
        default=str(uuid.uuid4()))
    race_parser.add_argument(
        "--pipeline",
        help="Select the pipeline to run.",
        # the default will be dynamically derived by racecontrol based on the presence / absence of other command line options
        default="")
    race_parser.add_argument(
        "--revision",
        help="Define the source code revision for building the benchmark candidate. 'current' uses the source tree as is,"
             " 'latest' fetches the latest version on master. It is also possible to specify a commit id or a timestamp."
             " The timestamp must be specified as: \"@ts\" where \"ts\" must be a valid ISO 8601 timestamp, "
             "e.g. \"@2013-07-27T10:37:00Z\" (default: current).",
        default="current")  # optimized for local usage, don't fetch sources
    add_track_source(race_parser)
    race_parser.add_argument(
        "--track",
        help=f"Define the track to use. List possible tracks with `{PROGRAM_NAME} list tracks`."
    )
    race_parser.add_argument(
        "--track-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to the track as variables.",
        default=""
    )
    race_parser.add_argument(
        "--challenge",
        help=f"Define the challenge to use. List possible challenges for tracks with `{PROGRAM_NAME} list tracks`.")
    race_parser.add_argument(
        "--car",
        help=f"Define the car to use. List possible cars with `{PROGRAM_NAME} list cars` (default: defaults).",
        default="defaults")  # optimized for local usage
    race_parser.add_argument(
        "--car-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim as variables for the car.",
        default=""
    )
    race_parser.add_argument(
        "--runtime-jdk",
        type=runtime_jdk,
        help="The major version of the runtime JDK to use.",
        default=None)
    race_parser.add_argument(
        "--elasticsearch-plugins",
        help="Define the Elasticsearch plugins to install. (default: install no plugins).",
        default="")
    race_parser.add_argument(
        "--plugin-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to all plugins as variables.",
        default=""
    )
    race_parser.add_argument(
        "--target-hosts",
        help="Define a comma-separated list of host:port pairs which should be targeted if using the pipeline 'benchmark-only' "
             "(default: localhost:9200).",
        default="")  # actually the default is pipeline specific and it is set later
    race_parser.add_argument(
        "--load-driver-hosts",
        help="Define a comma-separated list of hosts which should generate load (default: localhost).",
        default="localhost")
    race_parser.add_argument(
        "--client-options",
        help=f"Define a comma-separated list of client options to use. The options will be passed to the Elasticsearch "
             f"Python client (default: {opts.ClientOptions.DEFAULT_CLIENT_OPTIONS}).",
        default=opts.ClientOptions.DEFAULT_CLIENT_OPTIONS)
    race_parser.add_argument("--on-error",
                             choices=["continue", "abort"],
                             help="Controls how Rally behaves on response errors (default: continue).",
                             default="continue")
    race_parser.add_argument(
        "--telemetry",
        help=f"Enable the provided telemetry devices, provided as a comma-separated list. List possible telemetry "
             f"devices with `{PROGRAM_NAME} list telemetry`.",
        default="")
    race_parser.add_argument(
        "--telemetry-params",
        help="Define a comma-separated list of key:value pairs that are injected verbatim to the telemetry devices as parameters.",
        default=""
    )
    race_parser.add_argument(
        "--distribution-repository",
        help="Define the repository from where the Elasticsearch distribution should be downloaded (default: release).",
        default="release")

    task_filter_group = race_parser.add_mutually_exclusive_group()
    task_filter_group.add_argument(
        "--include-tasks",
        help="Defines a comma-separated list of tasks to run. By default all tasks of a challenge are run.")
    task_filter_group.add_argument(
        "--exclude-tasks",
        help="Defines a comma-separated list of tasks not to run. By default all tasks of a challenge are run.")
    race_parser.add_argument(
        "--user-tag",
        help="Define a user-specific key-value pair (separated by ':'). It is added to each metric record as meta info. "
             "Example: intention:baseline-ticket-12345",
        default="")
    race_parser.add_argument(
        "--report-format",
        help="Define the output format for the command line report (default: markdown).",
        choices=["markdown", "csv"],
        default="markdown")
    race_parser.add_argument(
        "--report-numbers-align",
        help="Define the output column number alignment for the command line report (default: right).",
        choices=["right", "center", "left", "decimal"],
        default="right")
    race_parser.add_argument(
        "--show-in-report",
        help="Define which values are shown in the summary report (default: available).",
        choices=["available", "all-percentiles", "all"],
        default="available")
    race_parser.add_argument(
        "--report-file",
        help="Write the command line report also to the provided file.",
        default="")
    race_parser.add_argument(
        "--preserve-install",
        help=f"Keep the benchmark candidate and its index. (default: {str(preserve_install).lower()}).",
        default=preserve_install,
        action="store_true")
    race_parser.add_argument(
        "--test-mode",
        help="Runs the given track in 'test mode'. Meant to check a track for errors but not for real benchmarks (default: false).",
        default=False,
        action="store_true")
    race_parser.add_argument(
        "--enable-driver-profiling",
        help="Enables a profiler for analyzing the performance of calls in Rally's driver (default: false).",
        default=False,
        action="store_true")
    race_parser.add_argument(
        "--enable-assertions",
        help="Enables assertion checks for tasks (default: false).",
        default=False,
        action="store_true")
    race_parser.add_argument(
        "--kill-running-processes",
        action="store_true",
        default=False,
        help="If any processes is running, it is going to kill them and allow Rally to continue to run."
    )

    ###############################################################################
    #
    # The options below are undocumented and can be removed or changed at any time.
    #
    ###############################################################################
    # This option is intended to tell Rally to assume a different start date than 'now'. This is effectively just useful for things like
    # backtesting or a benchmark run across environments (think: comparison of EC2 and bare metal) but never for the typical user.
    race_parser.add_argument(
        "--effective-start-date",
        help=argparse.SUPPRESS,
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        default=None)
    # Skips checking that the REST API is available before proceeding with the benchmark
    race_parser.add_argument(
        "--skip-rest-api-check",
        help=argparse.SUPPRESS,
        action="store_true",
        default=False)

    for p in [list_parser, race_parser, compare_parser, download_parser, install_parser,
              start_parser, stop_parser, info_parser, generate_parser, create_track_parser]:
        # This option is needed to support a separate configuration for the integration tests on the same machine
        p.add_argument(
            "--configuration-name",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--quiet",
            help="Suppress as much as output as possible (default: false).",
            default=False,
            action="store_true")
        p.add_argument(
            "--offline",
            help="Assume that Rally has no connection to the Internet (default: false).",
            default=False,
            action="store_true")

    return parser


def dispatch_list(cfg):
    what = cfg.opts("system", "list.config.option")
    if what == "telemetry":
        telemetry.list_telemetry()
    elif what == "tracks":
        track.list_tracks(cfg)
    elif what == "pipelines":
        racecontrol.list_pipelines()
    elif what == "races":
        metrics.list_races(cfg)
    elif what == "cars":
        team.list_cars(cfg)
    elif what == "elasticsearch-plugins":
        team.list_plugins(cfg)
    else:
        raise exceptions.SystemSetupError("Cannot list unknown configuration option [%s]" % what)


def print_help_on_errors():
    heading = "Getting further help:"
    console.println(console.format.bold(heading))
    console.println(console.format.underline_for(heading))
    console.println(f"* Check the log files in {paths.logs()} for errors.")
    console.println(f"* Read the documentation at {console.format.link(doc_link())}.")
    console.println(f"* Ask a question on the forum at {console.format.link(FORUM_LINK)}.")
    console.println(f"* Raise an issue at {console.format.link('https://github.com/elastic/rally/issues')} "
                    f"and include the log files in {paths.logs()}.")


def race(cfg, kill_running_processes=False):
    logger = logging.getLogger(__name__)

    if kill_running_processes:
        logger.info("Killing running Rally processes")

        # Kill any lingering Rally processes before attempting to continue - the actor system needs to be a singleton on this machine
        # noinspection PyBroadException
        try:
            process.kill_running_rally_instances()
        except BaseException:
            logger.exception(
                "Could not terminate potentially running Rally instances correctly. Attempting to go on anyway.")
    else:
        other_rally_processes = process.find_all_other_rally_processes()
        if other_rally_processes:
            pids = [p.pid for p in other_rally_processes]

            msg = f"There are other Rally processes running on this machine (PIDs: {pids}) but only one Rally " \
                  f"benchmark is allowed to run at the same time.\n\nYou can use --kill-running-processes flag " \
                  f"to kill running processes automatically and allow Rally to continue to run a new benchmark. " \
                  f"Otherwise, you need to manually kill them."
            raise exceptions.RallyError(msg)

    with_actor_system(racecontrol.run, cfg)


def with_actor_system(runnable, cfg):
    logger = logging.getLogger(__name__)
    already_running = actor.actor_system_already_running()
    logger.info("Actor system already running locally? [%s]", str(already_running))
    try:
        actors = actor.bootstrap_actor_system(try_join=already_running, prefer_local_only=not already_running)
        # We can only support remote benchmarks if we have a dedicated daemon that is not only bound to 127.0.0.1
        cfg.add(config.Scope.application, "system", "remote.benchmarking.supported", already_running)
    # This happens when the admin process could not be started, e.g. because it could not open a socket.
    except thespian.actors.InvalidActorAddress:
        logger.info("Falling back to offline actor system.")
        actor.use_offline_actor_system()
        actors = actor.bootstrap_actor_system(try_join=True)
    except Exception as e:
        logger.exception("Could not bootstrap actor system.")
        if str(e) == "Unable to determine valid external socket address.":
            console.warn("Could not determine a socket address. Are you running without any network? Switching to degraded mode.",
                         logger=logger)
            logger.info("Falling back to offline actor system.")
            actor.use_offline_actor_system()
            actors = actor.bootstrap_actor_system(try_join=True)
        else:
            raise
    try:
        runnable(cfg)
    finally:
        # We only shutdown the actor system if it was not already running before
        if not already_running:
            shutdown_complete = False
            times_interrupted = 0
            # give some time for any outstanding messages to be delivered to the actor system
            time.sleep(3)
            while not shutdown_complete and times_interrupted < 2:
                try:
                    logger.info("Attempting to shutdown internal actor system.")
                    actors.shutdown()
                    # note that this check will only evaluate to True for a TCP-based actor system.
                    timeout = 15
                    while actor.actor_system_already_running() and timeout > 0:
                        logger.info("Actor system is still running. Waiting...")
                        time.sleep(1)
                        timeout -= 1
                    if timeout > 0:
                        shutdown_complete = True
                        logger.info("Shutdown completed.")
                    else:
                        logger.warning("Shutdown timed out. Actor system is still running.")
                        break
                except KeyboardInterrupt:
                    times_interrupted += 1
                    logger.warning("User interrupted shutdown of internal actor system.")
                    console.info("Please wait a moment for Rally's internal components to shutdown.")
            if not shutdown_complete and times_interrupted > 0:
                logger.warning("Terminating after user has interrupted actor system shutdown explicitly for [%d] times.",
                               times_interrupted)
                console.println("")
                console.warn("Terminating now at the risk of leaving child processes behind.")
                console.println("")
                console.warn("The next race may fail due to an unclean shutdown.")
                console.println("")
                console.println(SKULL)
                console.println("")
            elif not shutdown_complete:
                console.warn("Could not terminate all internal processes within timeout. Please check and force-terminate "
                             "all Rally processes.")


def generate(cfg):
    chart_generator.generate(cfg)


def configure_telemetry_params(args, cfg):
    cfg.add(config.Scope.applicationOverride, "telemetry", "devices", opts.csv_to_list(args.telemetry))
    cfg.add(config.Scope.applicationOverride, "telemetry", "params", opts.to_dict(args.telemetry_params))


def configure_track_params(arg_parser, args, cfg, command_requires_track=True):
    cfg.add(config.Scope.applicationOverride, "track", "repository.revision", args.track_revision)
    # We can assume here that if a track-path is given, the user did not specify a repository either (although argparse sets it to
    # its default value)
    if args.track_path:
        cfg.add(config.Scope.applicationOverride, "track", "track.path", os.path.abspath(io.normalize_path(args.track_path)))
        cfg.add(config.Scope.applicationOverride, "track", "repository.name", None)
        if args.track_revision:
            # stay as close as possible to argparse errors although we have a custom validation.
            arg_parser.error("argument --track-revision not allowed with argument --track-path")
        if command_requires_track and args.track:
            # stay as close as possible to argparse errors although we have a custom validation.
            arg_parser.error("argument --track not allowed with argument --track-path")
    else:
        cfg.add(config.Scope.applicationOverride, "track", "repository.name", args.track_repository)
        if command_requires_track:
            if not args.track:
                raise arg_parser.error("argument --track is required")
            cfg.add(config.Scope.applicationOverride, "track", "track.name", args.track)

    if command_requires_track:
        cfg.add(config.Scope.applicationOverride, "track", "params", opts.to_dict(args.track_params))
        cfg.add(config.Scope.applicationOverride, "track", "challenge.name", args.challenge)
        cfg.add(config.Scope.applicationOverride, "track", "include.tasks", opts.csv_to_list(args.include_tasks))
        cfg.add(config.Scope.applicationOverride, "track", "exclude.tasks", opts.csv_to_list(args.exclude_tasks))


def configure_mechanic_params(args, cfg, command_requires_car=True):
    if args.team_path:
        cfg.add(config.Scope.applicationOverride, "mechanic", "team.path", os.path.abspath(io.normalize_path(args.team_path)))
        cfg.add(config.Scope.applicationOverride, "mechanic", "repository.name", None)
        cfg.add(config.Scope.applicationOverride, "mechanic", "repository.revision", None)
    else:
        cfg.add(config.Scope.applicationOverride, "mechanic", "repository.name", args.team_repository)
        cfg.add(config.Scope.applicationOverride, "mechanic", "repository.revision", args.team_revision)

    if command_requires_car:
        if args.distribution_version:
            cfg.add(config.Scope.applicationOverride, "mechanic", "distribution.version", args.distribution_version)
        cfg.add(config.Scope.applicationOverride, "mechanic", "distribution.repository", args.distribution_repository)
        cfg.add(config.Scope.applicationOverride, "mechanic", "car.names", opts.csv_to_list(args.car))
        cfg.add(config.Scope.applicationOverride, "mechanic", "car.params", opts.to_dict(args.car_params))


def configure_connection_params(arg_parser, args, cfg):
    # Also needed by mechanic (-> telemetry) - duplicate by module?
    target_hosts = opts.TargetHosts(args.target_hosts)
    cfg.add(config.Scope.applicationOverride, "client", "hosts", target_hosts)
    client_options = opts.ClientOptions(args.client_options, target_hosts=target_hosts)
    cfg.add(config.Scope.applicationOverride, "client", "options", client_options)
    if "timeout" not in client_options.default:
        console.info("You did not provide an explicit timeout in the client options. Assuming default of 10 seconds.")
    if list(target_hosts.all_hosts) != list(client_options.all_client_options):
        arg_parser.error("--target-hosts and --client-options must define the same keys for multi cluster setups.")


def configure_reporting_params(args, cfg):
    cfg.add(config.Scope.applicationOverride, "reporting", "format", args.report_format)
    cfg.add(config.Scope.applicationOverride, "reporting", "values", args.show_in_report)
    cfg.add(config.Scope.applicationOverride, "reporting", "output.path", args.report_file)
    cfg.add(config.Scope.applicationOverride, "reporting", "numbers.align", args.report_numbers_align)


def dispatch_sub_command(arg_parser, args, cfg):
    sub_command = args.subcommand

    cfg.add(config.Scope.application, "system", "quiet.mode", args.quiet)
    cfg.add(config.Scope.application, "system", "offline.mode", args.offline)

    try:
        if sub_command == "compare":
            configure_reporting_params(args, cfg)
            reporter.compare(cfg, args.baseline, args.contender)
        elif sub_command == "list":
            cfg.add(config.Scope.applicationOverride, "system", "list.config.option", args.configuration)
            cfg.add(config.Scope.applicationOverride, "system", "list.races.max_results", args.limit)
            configure_mechanic_params(args, cfg, command_requires_car=False)
            configure_track_params(arg_parser, args, cfg, command_requires_track=False)
            dispatch_list(cfg)
        elif sub_command == "download":
            cfg.add(config.Scope.applicationOverride, "mechanic", "target.os", args.target_os)
            cfg.add(config.Scope.applicationOverride, "mechanic", "target.arch", args.target_arch)
            configure_mechanic_params(args, cfg)
            mechanic.download(cfg)
        elif sub_command == "install":
            cfg.add(config.Scope.applicationOverride, "system", "install.id", str(uuid.uuid4()))
            cfg.add(config.Scope.applicationOverride, "mechanic", "network.host", args.network_host)
            cfg.add(config.Scope.applicationOverride, "mechanic", "network.http.port", args.http_port)
            cfg.add(config.Scope.applicationOverride, "mechanic", "source.revision", args.revision)
            cfg.add(config.Scope.applicationOverride, "mechanic", "build.type", args.build_type)
            cfg.add(config.Scope.applicationOverride, "mechanic", "runtime.jdk", args.runtime_jdk)
            cfg.add(config.Scope.applicationOverride, "mechanic", "node.name", args.node_name)
            cfg.add(config.Scope.applicationOverride, "mechanic", "master.nodes", opts.csv_to_list(args.master_nodes))
            cfg.add(config.Scope.applicationOverride, "mechanic", "seed.hosts", opts.csv_to_list(args.seed_hosts))
            cfg.add(config.Scope.applicationOverride, "mechanic", "car.plugins", opts.csv_to_list(args.elasticsearch_plugins))
            cfg.add(config.Scope.applicationOverride, "mechanic", "plugin.params", opts.to_dict(args.plugin_params))
            configure_mechanic_params(args, cfg)
            mechanic.install(cfg)
        elif sub_command == "start":
            cfg.add(config.Scope.applicationOverride, "system", "race.id", args.race_id)
            cfg.add(config.Scope.applicationOverride, "system", "install.id", args.installation_id)
            cfg.add(config.Scope.applicationOverride, "mechanic", "runtime.jdk", args.runtime_jdk)
            configure_telemetry_params(args, cfg)
            mechanic.start(cfg)
        elif sub_command == "stop":
            cfg.add(config.Scope.applicationOverride, "mechanic", "preserve.install", convert.to_bool(args.preserve_install))
            cfg.add(config.Scope.applicationOverride, "system", "install.id", args.installation_id)
            mechanic.stop(cfg)
        elif sub_command == "race":
            # As the race command is doing more work than necessary at the moment, we duplicate several parameters
            # in this section that actually belong to dedicated subcommands (like install, start or stop). Over time
            # these duplicated parameters will vanish as we move towards dedicated subcommands and use "race" only
            # to run the actual benchmark (i.e. generating load).
            if args.effective_start_date:
                cfg.add(config.Scope.applicationOverride, "system", "time.start", args.effective_start_date)
            cfg.add(config.Scope.applicationOverride, "system", "race.id", args.race_id)
            # use the race id implicitly also as the install id.
            cfg.add(config.Scope.applicationOverride, "system", "install.id", args.race_id)
            cfg.add(config.Scope.applicationOverride, "race", "pipeline", args.pipeline)
            cfg.add(config.Scope.applicationOverride, "race", "user.tag", args.user_tag)
            cfg.add(config.Scope.applicationOverride, "driver", "profiling", args.enable_driver_profiling)
            cfg.add(config.Scope.applicationOverride, "driver", "assertions", args.enable_assertions)
            cfg.add(config.Scope.applicationOverride, "driver", "on.error", args.on_error)
            cfg.add(config.Scope.applicationOverride, "driver", "load_driver_hosts", opts.csv_to_list(args.load_driver_hosts))
            cfg.add(config.Scope.applicationOverride, "track", "test.mode.enabled", args.test_mode)
            configure_track_params(arg_parser, args, cfg)
            configure_connection_params(arg_parser, args, cfg)
            configure_telemetry_params(args, cfg)
            configure_mechanic_params(args, cfg)
            cfg.add(config.Scope.applicationOverride, "mechanic", "runtime.jdk", args.runtime_jdk)
            cfg.add(config.Scope.applicationOverride, "mechanic", "source.revision", args.revision)
            cfg.add(config.Scope.applicationOverride, "mechanic", "car.plugins", opts.csv_to_list(args.elasticsearch_plugins))
            cfg.add(config.Scope.applicationOverride, "mechanic", "plugin.params", opts.to_dict(args.plugin_params))
            cfg.add(config.Scope.applicationOverride, "mechanic", "preserve.install", convert.to_bool(args.preserve_install))
            cfg.add(config.Scope.applicationOverride, "mechanic", "skip.rest.api.check", convert.to_bool(args.skip_rest_api_check))

            configure_reporting_params(args, cfg)

            race(cfg, args.kill_running_processes)
        elif sub_command == "generate":
            cfg.add(config.Scope.applicationOverride, "generator", "chart.spec.path", args.chart_spec_path)
            cfg.add(config.Scope.applicationOverride, "generator", "chart.type", args.chart_type)
            cfg.add(config.Scope.applicationOverride, "generator", "output.path", args.output_path)
            generate(cfg)
        elif sub_command == "create-track":
            cfg.add(config.Scope.applicationOverride, "generator", "indices", args.indices)
            cfg.add(config.Scope.applicationOverride, "generator", "output.path", args.output_path)
            cfg.add(config.Scope.applicationOverride, "track", "track.name", args.track)
            configure_connection_params(arg_parser, args, cfg)

            tracker.create_track(cfg)
        elif sub_command == "info":
            configure_track_params(arg_parser, args, cfg)
            track.track_info(cfg)
        else:
            raise exceptions.SystemSetupError(f"Unknown subcommand [{sub_command}]")
        return True
    except exceptions.RallyError as e:
        logging.getLogger(__name__).exception("Cannot run subcommand [%s].", sub_command)
        msg = str(e.message)
        nesting = 0
        while hasattr(e, "cause") and e.cause:
            nesting += 1
            e = e.cause
            if hasattr(e, "message"):
                msg += "\n%s%s" % ("\t" * nesting, e.message)
            else:
                msg += "\n%s%s" % ("\t" * nesting, str(e))

        console.error("Cannot %s. %s" % (sub_command, msg))
        console.println("")
        print_help_on_errors()
        return False
    except BaseException as e:
        logging.getLogger(__name__).exception("A fatal error occurred while running subcommand [%s].", sub_command)
        console.error("Cannot %s. %s." % (sub_command, e))
        console.println("")
        print_help_on_errors()
        return False


def main():
    check_python_version()
    log.install_default_log_config()
    log.configure_logging()
    logger = logging.getLogger(__name__)
    start = time.time()

    # Early init of console output so we start to show everything consistently.
    console.init(quiet=False)

    arg_parser = create_arg_parser()
    args = arg_parser.parse_args()

    console.init(quiet=args.quiet)
    console.println(BANNER)

    cfg = config.Config(config_name=args.configuration_name)
    if not cfg.config_present():
        cfg.install_default_config()
    cfg.load_config(auto_upgrade=True)
    cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.utcnow())
    # Local config per node
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    cfg.add(config.Scope.application, "node", "rally.cwd", os.getcwd())

    logger.info("OS [%s]", str(platform.uname()))
    logger.info("Python [%s]", str(sys.implementation))
    logger.info("Rally version [%s]", version.version())
    logger.debug("Command line arguments: %s", args)
    # Configure networking
    net.init()
    if not args.offline:
        probing_url = cfg.opts("system", "probing.url", default_value="https://github.com", mandatory=False)
        if not net.has_internet_connection(probing_url):
            console.warn("No Internet connection detected. Automatic download of track data sets etc. is disabled.",
                         logger=logger)
            cfg.add(config.Scope.applicationOverride, "system", "offline.mode", True)
        else:
            logger.info("Detected a working Internet connection.")

    success = dispatch_sub_command(arg_parser, args, cfg)

    end = time.time()
    if success:
        console.println("")
        console.info("SUCCESS (took %d seconds)" % (end - start), overline="-", underline="-")
    else:
        console.println("")
        console.info("FAILURE (took %d seconds)" % (end - start), overline="-", underline="-")
        sys.exit(64)


if __name__ == "__main__":
    main()
