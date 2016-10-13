import argparse
import datetime
import logging
import os
import shutil
import sys
import time

import pkg_resources
import thespian.actors
from esrally import config, paths, racecontrol, reporter, metrics, track, exceptions, PROGRAM_NAME
from esrally.utils import io, convert, git, process, console, net
from esrally.mechanic import car, telemetry

__version__ = pkg_resources.require("esrally")[0].version

BANNER = """
    ____        ____
   / __ \____ _/ / /_  __
  / /_/ / __ `/ / / / / /
 / _, _/ /_/ / / / /_/ /
/_/ |_|\__,_/_/_/\__, /
                /____/
"""

logger = logging.getLogger("rally.main")

SKULL = '''
                 uuuuuuu
             uu$$$$$$$$$$$uu
          uu$$$$$$$$$$$$$$$$$uu
         u$$$$$$$$$$$$$$$$$$$$$u
        u$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$"   "$$$"   "$$$$$$u
       "$$$$"      u$u       $$$$"
        $$$u       u$u       u$$$
        $$$u      u$$$u      u$$$
         "$$$$uu$$$   $$$uu$$$$"
          "$$$$$$$"   "$$$$$$$"
            u$$$$$$$u$$$$$$$u
             u$"$"$"$"$"$"$u
  uuu        $$u$ $ $ $ $u$$       uuu
 u$$$$        $$$$$u$u$u$$$       u$$$$
  $$$$$uu      "$$$$$$$$$"     uu$$$$$$
u$$$$$$$$$$$uu    """""    uuuu$$$$$$$$$$
$$$$"""$$$$$$$$$$uuu   uu$$$$$$$$$"""$$$"
"""      ""$$$$$$$$$$$uu ""$"""
uuuu ""$$$$$$$$$$uuu
u$$$uuu$$$$$$$$$uu ""$$$$$$$$$$$uuu$$$
$$$$$$$$$$""""           ""$$$$$$$$$$$"
   "$$$$$"                      ""$$$$""
     $$$"                         $$$$"
'''


def rally_root_path():
    return os.path.dirname(os.path.realpath(__file__))


def version():
    release = __version__
    try:
        if git.is_working_copy(io.normalize_path("%s/.." % rally_root_path())):
            revision = git.head_revision(rally_root_path())
            return "%s (git revision: %s)" % (release, revision.strip())
    except BaseException:
        pass
    # cannot determine head revision so user has probably installed Rally via pip instead of git clone
    return release


# we want to use some basic logging even before the output to log file is configured
def pre_configure_logging():
    logging.basicConfig(level=logging.INFO)


def log_file_path(cfg):
    log_dir = paths.Paths(cfg).log_root()
    node_name = cfg.opts("system", "node.name", mandatory=False)
    if node_name:
        return "%s/rally_out_%s.log" % (log_dir, node_name)
    else:
        return "%s/rally_out.log" % log_dir


def configure_logging(cfg):
    # Even if we don't log to a file, other parts of the application rely on this path to exist -> enforce
    log_file = log_file_path(cfg)
    log_dir = os.path.dirname(log_file)
    io.ensure_dir(log_dir)
    cfg.add(config.Scope.application, "system", "log.dir", log_dir)

    logging_output = cfg.opts("system", "logging.output")

    if logging_output == "file":
        console.println("\nWriting additional logs to %s\n" % log_file)
        # there is an old log file lying around -> backup
        if os.path.exists(log_file):
            os.rename(log_file, "%s-bak-%d.log" % (log_file, int(os.path.getctime(log_file))))
        ch = logging.FileHandler(filename=log_file, mode="a")
    else:
        ch = logging.StreamHandler(stream=sys.stdout)

    log_level = logging.INFO
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime
    ch.setFormatter(formatter)

    # Remove all handlers associated with the root logger object so we can start over with an entirely fresh log configuration
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.root.addHandler(ch)
    logging.getLogger("elasticsearch").setLevel(logging.WARN)


def configure_actor_logging(cfg):
    class ActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" in logrecord.__dict__

    class NotActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" not in logrecord.__dict__

    log_dir = paths.Paths(cfg).log_root()

    logging_output = cfg.opts("system", "logging.output")

    if logging_output == "file":
        actor_log_handler = {"class": "logging.FileHandler", "filename": "%s/rally-actors.log" % log_dir}
        actor_messages_handler = {"class": "logging.FileHandler", "filename": "%s/rally-actor-messages.log" % log_dir}
    else:
        actor_log_handler = {"class": "logging.StreamHandler", "stream": sys.stdout}
        actor_messages_handler = {"class": "logging.StreamHandler", "stream": sys.stdout}

    actor_log_handler["formatter"] = "normal"
    actor_log_handler["filters"] = ["notActorLog"]
    actor_log_handler["level"] = logging.INFO

    actor_messages_handler["formatter"] = "actor"
    actor_messages_handler["filters"] = ["isActorLog"]
    actor_messages_handler["level"] = logging.INFO

    return {
        "version": 1,
        "formatters": {
            "normal": {
                "format": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s"
            },
            "actor": {
                "format": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(actorAddress)s => %(message)s"
            }
        },
        "filters": {
            "isActorLog": {
                "()": ActorLogFilter
            },
            "notActorLog": {
                "()": NotActorLogFilter
            }
        },
        "handlers": {
            "h1": actor_log_handler,
            "h2": actor_messages_handler
        },
        "loggers": {
            "": {
                "handlers": ["h1", "h2"], "level": logging.INFO
            }
        }
    }


def parse_args():
    # try to preload configurable defaults, but this does not work together with `--configuration-name` (which is undocumented anyway)
    cfg = config.Config()
    if cfg.config_present():
        cfg.load_config()
        preserve_install = cfg.opts("defaults", "preserve_benchmark_candidate", default_value=False, mandatory=False)
    else:
        preserve_install = False

    # workaround for http://bugs.python.org/issue13041
    #
    # Set a proper width (see argparse.HelpFormatter)
    try:
        int(os.environ["COLUMNS"])
    except (KeyError, ValueError):
        # noinspection PyBroadException
        try:
            os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)
        except BaseException:
            # don't fail if anything goes wrong here
            pass

    parser = argparse.ArgumentParser(prog=PROGRAM_NAME,
                                     description=BANNER + "\n\n You know for benchmarking Elasticsearch.",
                                     epilog="Find out more about Rally at %s" % console.format.link("https://esrally.readthedocs.io"),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version="%(prog)s " + version())

    subparsers = parser.add_subparsers(
        title="subcommands",
        dest="subcommand",
        help="")

    race_parser = subparsers.add_parser("race", help="Run the benchmarking pipeline. This sub-command should typically be used.")
    # change in favor of "list telemetry", "list tracks", "list pipelines"
    list_parser = subparsers.add_parser("list", help="List configuration options")
    list_parser.add_argument(
        "configuration",
        metavar="configuration",
        help="The configuration for which Rally should show the available options. "
             "Possible values are: telemetry, tracks, pipelines, races, cars",
        choices=["telemetry", "tracks", "pipelines", "races", "cars"])
    list_parser.add_argument(
        "--limit",
        help="Limit the number of search results for recent races (default: 10).",
        default=10,
    )

    compare_parser = subparsers.add_parser("compare", help="Compare two races")
    compare_parser.add_argument(
        "--baseline",
        help="Race timestamp of the baseline (see %s list races)" % PROGRAM_NAME,
        default="")
    compare_parser.add_argument(
        "--contender",
        help="Race timestamp of the contender (see %s list races)" % PROGRAM_NAME,
        default="")

    config_parser = subparsers.add_parser("configure", help="Write the configuration file or reconfigure Rally")
    for p in [parser, config_parser]:
        p.add_argument(
            "--advanced-config",
            help="show additional configuration options (default: false)",
            default=False,
            action="store_true")

    for p in [parser, race_parser]:
        p.add_argument(
            "--pipeline",
            help="Selects a specific pipeline to run. A pipeline defines the steps that are executed (default: from-sources-complete).",
            default="from-sources-complete")
        p.add_argument(
            "--preserve-install",
            help="preserves the Elasticsearch benchmark candidate installation including all data. Caution: This will take lots of disk "
                 "space! (default: %s)" % str(preserve_install).lower(),
            default=preserve_install)
        p.add_argument(
            "--telemetry",
            help="enables the provided telemetry devices (i.e. profilers). Multiple telemetry devices have to be "
                 "provided as a comma-separated list. List possible telemetry devices with `%s list telemetry`" % PROGRAM_NAME,
            default="")
        p.add_argument(
            "--revision",
            help="defines which sources to use when building the benchmark candidate. 'current' uses the source tree as is,"
                 " 'latest' fetches the latest version on master. It is also possible to specify a commit id or a timestamp."
                 " The timestamp must be specified as: \"@ts\" where \"ts\" must be a valid ISO 8601 timestamp, "
                 "e.g. \"@2013-07-27T10:37:00Z\" (default: current).",
            default="current")  # optimized for local usage, don't fetch sources
        p.add_argument(
            "--track",
            help="defines which track should be run. List possible tracks with `%s list tracks` (default: geonames)." % PROGRAM_NAME,
            default="geonames")
        p.add_argument(
            "--challenge",
            help="defines which challenge should be run. List possible challenges for tracks with `%s list tracks`"
                 " (default: append-no-conflicts)." % PROGRAM_NAME,
            default="append-no-conflicts")  # optimized for local usage
        p.add_argument(
            "--car",
            help="defines which car should drive on a track. List possible cars with `%s list cars` (default: defaults)." % PROGRAM_NAME,
            default="defaults")  # optimized for local usage

        p.add_argument(
            "--target-hosts",
            help="defines a comma-separated list of host:port pairs which should be targeted iff using the pipeline 'benchmark-only' "
                 "(default: localhost:9200).",
            default="localhost:9200")
        p.add_argument(
            "--client-options",
            help="defines a comma-separated list of client options to use. The options will be passed to the Elasticsearch Python client "
                 "(default: timeout:60000,request_timeout:60000).",
            default="timeout:60000,request_timeout:60000")
        p.add_argument(
            "--user-tag",
            help="defines a user-specific key-value pair that is separated by a ':' and added to each metric record as meta info. "
                 "Example: intention:baseline-ticket-12345",
            default="")
        p.add_argument(
            "--report-format",
            help="The output format for the command line report. Possible values are: markdown, csv (default: markdown)",
            choices=["markdown", "csv"],
            default="markdown")
        p.add_argument(
            "--report-file",
            help="If provided, Rally writes the report also to this file (default: only write to stdout)",
            default="")
        p.add_argument(
            "--quiet",
            help="Suppresses as much as output as possible. Activate it unless you want to see what's happening during the "
                 "benchmark (default: false)",
            default=False,
            action="store_true")

    for p in [parser, list_parser, race_parser]:
        p.add_argument(
            "--distribution-version",
            help="defines the version of the Elasticsearch distribution to download. "
                 "Check https://www.elastic.co/downloads/elasticsearch for released versions.",
            default="")
        p.add_argument(
            "--distribution-repository",
            help="defines the repository from where the Elasticsearch distribution should be downloaded (default: release).",
            choices=["snapshot", "release"],
            default="release")
        p.add_argument(
            "--track-repository",
            help="defines the repository from where Rally will load tracks (default: default).",
            default="default")
        p.add_argument(
            "--offline",
            help="Assume that Rally has no connection to the Internet (default: false)",
            default=False,
            action="store_true")

    ###############################################################################
    #
    # The options below are undocumented and can be removed or changed at any time.
    #
    ###############################################################################
    for p in [parser, race_parser]:
        # This option is intended to tell Rally to assume a different start date than 'now'. This is effectively just useful for things like
        # backtesting or a benchmark run across environments (think: comparison of EC2 and bare metal) but never for the typical user.
        p.add_argument(
            "--effective-start-date",
            help=argparse.SUPPRESS,
            type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
            default=datetime.datetime.utcnow())
        # This is a highly experimental option and will likely be removed
        p.add_argument(
            "--data-paths",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--override-src-dir",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--cluster-health",
            choices=["red", "yellow", "green"],
            help=argparse.SUPPRESS,
            default="green")

    for p in [parser, config_parser, list_parser, race_parser]:
        # This option is needed to support a separate configuration for the integration tests on the same machine
        p.add_argument(
            "--configuration-name",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--logging",
            choices=["file", "console"],
            help=argparse.SUPPRESS,
            default="file"
        )

    return parser.parse_args()


def derive_sub_command(args, cfg):
    sub_command = args.subcommand
    # first, trust the user...
    if sub_command is not None:
        return sub_command
    # we apply some smarts in case the user did not specify a sub-command
    if cfg.config_present():
        return "race"
    else:
        return "configure"


def ensure_configuration_present(cfg, args, sub_command):
    if sub_command == "configure":
        config.ConfigFactory().create_config(cfg.config_file, advanced_config=args.advanced_config)
        exit(0)
    else:
        if cfg.config_present():
            cfg.load_config()
            if not cfg.config_compatible():
                cfg.migrate_config()
                # Reload config after upgrading
                cfg.load_config()
        else:
            console.println("Error: No config present. Please run '%s configure' first." % PROGRAM_NAME)
            exit(64)


def list(cfg):
    what = cfg.opts("system", "list.config.option")
    if what == "telemetry":
        telemetry.list_telemetry(cfg)
    elif what == "tracks":
        track.list_tracks(cfg)
    elif what == "pipelines":
        racecontrol.list_pipelines()
    elif what == "races":
        metrics.list_races(cfg)
    elif what == "cars":
        car.list_cars()
    else:
        raise exceptions.SystemSetupError("Cannot list unknown configuration option [%s]" % what)


def print_help_on_errors(cfg):
    console.println("Please check the log file [%s] for further details first." % log_file_path(cfg))
    console.println("")
    console.println("If you need further help, please check Rally's docs at %s or ask a question in the forum at %s."
                    % (console.format.link("https://esrally.readthedocs.io"), console.format.link("https://discuss.elastic.co/c/rally")))
    console.println("")
    console.println("If you think this is a bug, please file a report at %s and include the log file for this race (%s)." %
                    (console.format.link("https://github.com/elastic/rally/issues"), log_file_path(cfg)))


def dispatch_sub_command(cfg, sub_command):
    try:
        if sub_command == "compare":
            reporter.compare(cfg)
        elif sub_command == "list":
            list(cfg)
        elif sub_command == "race":
            racecontrol.run(cfg)
        else:
            raise exceptions.SystemSetupError("Unknown subcommand [%s]" % sub_command)
        return True
    except exceptions.RallyError as e:
        logging.exception("Cannot run subcommand [%s]." % sub_command)
        console.println("\nERROR: Cannot %s\n\nReason: %s" % (sub_command, e))
        console.println("")
        print_help_on_errors(cfg)
        return False
    except BaseException as e:
        logging.exception("A fatal error occurred while running subcommand [%s]." % sub_command)
        console.println("\nFATAL: Cannot %s\n\nReason: %s" % (sub_command, e))
        console.println("")
        print_help_on_errors(cfg)
        return False


def csv_to_list(csv):
    if csv is None:
        return None
    elif len(csv.strip()) == 0:
        return []
    else:
        return [e.strip() for e in csv.split(",")]


def kv_to_map(kvs):
    def convert(v):
        # string
        if v.startswith("'"):
            return v[1:-1]

        # int
        try:
            return int(v)
        except ValueError:
            pass

        # float
        try:
            return float(v)
        except ValueError:
            pass

        # boolean
        if v.lower() == "false":
            return False
        elif v.lower() == "true":
            return True
        else:
            raise ValueError("Could not convert value '%s'" % v)

    result = {}
    for kv in kvs:
        k, v = kv.split(":")
        # key is always considered a string, value needs to be converted
        result[k.strip()] = convert(v.strip())
    return result


def convert_hosts(configured_host_list):
    hosts = []
    try:
        for authority in configured_host_list:
            host, port = authority.split(":")
            hosts.append({"host": host, "port": port})
        return hosts
    except ValueError:
        msg = "Could not convert hosts. Invalid format for %s. Expected a comma-separated list of host:port pairs, " \
              "e.g. host1:9200,host2:9200." % configured_host_list
        logger.exception(msg)
        raise exceptions.SystemSetupError(msg)


def main():
    # Early init of console output so we start to show everything consistently.
    console.init(quiet=False)

    pre_configure_logging()
    args = parse_args()

    console.init(quiet=args.quiet)
    console.println(BANNER)

    cfg = config.Config(config_name=args.configuration_name)
    sub_command = derive_sub_command(args, cfg)
    ensure_configuration_present(cfg, args, sub_command)
    # Add global meta info derived by rally itself
    cfg.add(config.Scope.application, "meta", "time.start", args.effective_start_date)
    cfg.add(config.Scope.application, "system", "rally.root", rally_root_path())
    cfg.add(config.Scope.application, "system", "invocation.root.dir", paths.Paths(cfg).invocation_root())
    # Add command line config
    cfg.add(config.Scope.applicationOverride, "source", "revision", args.revision)
    cfg.add(config.Scope.applicationOverride, "source", "distribution.version", args.distribution_version)
    cfg.add(config.Scope.applicationOverride, "source", "distribution.repository", args.distribution_repository)
    cfg.add(config.Scope.applicationOverride, "system", "pipeline", args.pipeline)
    cfg.add(config.Scope.applicationOverride, "system", "track.repository", args.track_repository)
    cfg.add(config.Scope.applicationOverride, "system", "quiet.mode", args.quiet)
    cfg.add(config.Scope.applicationOverride, "system", "offline.mode", args.offline)
    cfg.add(config.Scope.applicationOverride, "system", "user.tag", args.user_tag)
    cfg.add(config.Scope.applicationOverride, "system", "logging.output", args.logging)
    cfg.add(config.Scope.applicationOverride, "telemetry", "devices", csv_to_list(args.telemetry))
    cfg.add(config.Scope.applicationOverride, "benchmarks", "track", args.track)
    cfg.add(config.Scope.applicationOverride, "benchmarks", "challenge", args.challenge)
    cfg.add(config.Scope.applicationOverride, "benchmarks", "car", args.car)
    cfg.add(config.Scope.applicationOverride, "benchmarks", "cluster.health", args.cluster_health)
    cfg.add(config.Scope.applicationOverride, "provisioning", "datapaths", csv_to_list(args.data_paths))
    cfg.add(config.Scope.applicationOverride, "provisioning", "install.preserve", convert.to_bool(args.preserve_install))
    cfg.add(config.Scope.applicationOverride, "launcher", "external.target.hosts", convert_hosts(csv_to_list(args.target_hosts)))
    cfg.add(config.Scope.applicationOverride, "launcher", "client.options", kv_to_map(csv_to_list(args.client_options)))
    cfg.add(config.Scope.applicationOverride, "report", "reportformat", args.report_format)
    cfg.add(config.Scope.applicationOverride, "report", "reportfile", args.report_file)
    if args.override_src_dir is not None:
        cfg.add(config.Scope.applicationOverride, "source", "local.src.dir", args.override_src_dir)

    if sub_command == "list":
        cfg.add(config.Scope.applicationOverride, "system", "list.config.option", args.configuration)
        cfg.add(config.Scope.applicationOverride, "system", "list.races.max_results", args.limit)
    if sub_command == "compare":
        cfg.add(config.Scope.applicationOverride, "report", "comparison.baseline.timestamp", args.baseline)
        cfg.add(config.Scope.applicationOverride, "report", "comparison.contender.timestamp", args.contender)

    configure_logging(cfg)
    logger.info("Rally version [%s]" % version())
    logger.info("Command line arguments: %s" % args)
    # Configure networking
    net.init()

    # Kill any lingering Rally processes before attempting to continue - the actor system needs to a singleton on this machine
    # noinspection PyBroadException
    try:
        process.kill_running_rally_instances()
    except BaseException:
        logger.exception("Could not terminate potentially running Rally instances correctly. Attempting to go on anyway.")

    # bootstrap Rally's Actor system
    try:
        actors = thespian.actors.ActorSystem("multiprocTCPBase", logDefs=configure_actor_logging(cfg))
    except thespian.actors.ActorSystemException:
        logger.exception("Could not initialize internal actor system. Terminating.")
        console.println("ERROR: Could not initialize successfully.")
        console.println("")
        console.println("The most likely cause is that there are still processes running from a previous race.")
        console.println("Please check for running Python processes and terminate them before running Rally again.")
        console.println("")
        print_help_on_errors(cfg)
        sys.exit(70)

    success = False
    try:
        success = dispatch_sub_command(cfg, sub_command)
    finally:
        shutdown_complete = False
        times_interrupted = 0
        while not shutdown_complete and times_interrupted < 2:
            try:
                logger.info("Attempting to shutdown internal actor system.")
                actors.shutdown()
                shutdown_complete = True
                logger.info("Shutdown completed.")
            except KeyboardInterrupt:
                times_interrupted += 1
                logger.warn("User interrupted shutdown of internal actor system.")
                console.println("Please wait a moment for Rally's internal components to shutdown.")
        if not shutdown_complete and times_interrupted > 0:
            logger.warn("Terminating after user has interrupted actor system shutdown explicitly for [%d] times." % times_interrupted)
            console.println("**********************************************************************")
            console.println("")
            console.println("WARN: Terminating now at the risk of leaving child processes behind.")
            console.println("")
            console.println("The next race may fail due to an unclean shutdown.")
            console.println("")
            console.println(SKULL)
            console.println("")
            console.println("**********************************************************************")

    if not success:
        sys.exit(64)


if __name__ == "__main__":
    main()
