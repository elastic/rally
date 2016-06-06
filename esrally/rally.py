import datetime
import time
import os
import sys
import logging
import argparse
import pkg_resources

from esrally import config, paths, racecontrol, PROGRAM_NAME
from esrally.utils import io, format

__version__ = pkg_resources.require("esrally")[0].version

BANNER = """
    ____        ____
   / __ \____ _/ / /_  __
  / /_/ / __ `/ / / / / /
 / _, _/ /_/ / / / /_/ /
/_/ |_|\__,_/_/_/\__, /
                /____/
"""


# we want to use some basic logging even before the output to log file is configured
def pre_configure_logging():
    logging.basicConfig(level=logging.INFO)


def configure_logging(cfg):
    log_dir = paths.Paths(cfg).log_root()
    io.ensure_dir(log_dir)
    cfg.add(config.Scope.application, "system", "log.dir", log_dir)
    log_file = "%s/rally_out.log" % log_dir

    print("\nWriting additional logs to %s\n" % log_file)

    # Remove all handlers associated with the root logger object so we can start over with an entirely fresh log configuration
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_level = logging.INFO
    ch = logging.FileHandler(filename=log_file, mode="a")
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime
    ch.setFormatter(formatter)
    logging.root.addHandler(ch)


def parse_args():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME,
                                     description=BANNER + "\n\n You know for benchmarking Elasticsearch.",
                                     epilog="Find out more about Rally at %s" % format.link("https://esrally.readthedocs.io"),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version="%(prog)s " + __version__)

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
        help="defines the race timestamp of the baseline for the comparison (see output %s list races)" % PROGRAM_NAME,
        default="")
    compare_parser.add_argument(
        "--contender",
        help="defines the race timestamp of the contender for the comparison (see output %s list races)" % PROGRAM_NAME,
        default="")

    config_parser = subparsers.add_parser("configure", help="Write the configuration file or reconfigure Rally")
    for p in [parser, config_parser]:
        p.add_argument(
            "--advanced-config",
            help="show additional configuration options when creating the config file (default: false)",
            default=False,
            action="store_true")

    for p in [parser, race_parser]:
        p.add_argument(
            "--pipeline",
            help="Selects a specific pipeline to run. A pipeline defines the steps that are executed (default: from-sources-complete).",
            default="from-sources-complete")
        p.add_argument(
            "--quiet",
            help="Suppresses as much as output as possible. Activate it unless you want to see what's happening during the "
                 "benchmark (default: false)",
            default=False,
            action="store_true")
        p.add_argument(
            "--offline",
            help="Assume that Rally has no connection to the Internet (default: false)",
            default=False,
            action="store_true")
        p.add_argument(
            "--preserve-install",
            help="preserves the Elasticsearch benchmark candidate installation including all data. Caution: This will take lots of disk "
                 "space! (default: false)",
            default=False,
            action="store_true")
        # Add this as a hidden parameter for now, we'll enable support in #92
        p.add_argument(
            "--rounds",
            # help="Number of times each benchmark is run (default: 3)",
            help=argparse.SUPPRESS,
            default=1,
        )
        p.add_argument(
            "--telemetry",
            help="Rally will enable all of the provided telemetry devices (i.e. profilers). Multiple telemetry devices have to be "
                 "provided as a comma-separated list.",
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
            "--user-tag",
            help="defines a user-specific key-value pair that is separated by a ':' and added to each metric record as meta info. "
                 "Example: intention:baseline-ticket-12345",
            default="")

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

    for p in [parser, config_parser, list_parser, race_parser]:
        # This option is needed to support a separate configuration for the integration tests on the same machine
        p.add_argument(
            "--configuration-name",
            help=argparse.SUPPRESS,
            default=None)

    for p in [parser, list_parser, race_parser]:
        p.add_argument(
                "--distribution-version",
                help="defines the version of the Elasticsearch distribution to download. Check https://www.elastic.co/downloads/elasticsearch "
                     "for released versions.",
                default="")

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
        # TODO dm: Consider creating a simple function
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
            print("Error: No config present. Please run '%s configure' first." % PROGRAM_NAME)
            exit(64)


def dispatch_sub_command(cfg, sub_command):
    race_control = racecontrol.RaceControl(cfg)
    return race_control.start(sub_command)


def csv_to_list(csv):
    if csv is None:
        return None
    else:
        return [e.strip() for e in csv.split(",")]


def main():
    pre_configure_logging()
    args = parse_args()
    print(BANNER)

    cfg = config.Config(config_name=args.configuration_name)
    sub_command = derive_sub_command(args, cfg)
    ensure_configuration_present(cfg, args, sub_command)
    # Add global meta info derived by rally itself
    cfg.add(config.Scope.application, "meta", "time.start", args.effective_start_date)
    cfg.add(config.Scope.application, "system", "rally.root", os.path.dirname(os.path.realpath(__file__)))
    cfg.add(config.Scope.application, "system", "invocation.root.dir", paths.Paths(cfg).invocation_root())
    # Add command line config
    cfg.add(config.Scope.applicationOverride, "source", "revision", args.revision)
    cfg.add(config.Scope.applicationOverride, "source", "distribution.version", args.distribution_version)
    cfg.add(config.Scope.applicationOverride, "system", "pipeline", args.pipeline)
    # Don't expose the ability to define different repositories for now
    cfg.add(config.Scope.applicationOverride, "system", "track.repository", "default")
    cfg.add(config.Scope.applicationOverride, "system", "track", args.track)
    cfg.add(config.Scope.applicationOverride, "system", "quiet.mode", args.quiet)
    cfg.add(config.Scope.applicationOverride, "system", "offline.mode", args.offline)
    cfg.add(config.Scope.applicationOverride, "system", "user.tag", args.user_tag)
    cfg.add(config.Scope.applicationOverride, "telemetry", "devices", csv_to_list(args.telemetry))
    cfg.add(config.Scope.applicationOverride, "benchmarks", "challenge", args.challenge)
    cfg.add(config.Scope.applicationOverride, "benchmarks", "car", args.car)
    cfg.add(config.Scope.applicationOverride, "benchmarks", "rounds", args.rounds)
    cfg.add(config.Scope.applicationOverride, "provisioning", "datapaths", csv_to_list(args.data_paths))
    cfg.add(config.Scope.applicationOverride, "provisioning", "install.preserve", args.preserve_install)
    cfg.add(config.Scope.applicationOverride, "launcher", "external.target.hosts", csv_to_list(args.target_hosts))
    if sub_command == "list":
        cfg.add(config.Scope.applicationOverride, "system", "list.config.option", args.configuration)
        cfg.add(config.Scope.applicationOverride, "system", "list.races.max_results", args.limit)
    if sub_command == "compare":
        cfg.add(config.Scope.applicationOverride, "report", "comparison.baseline.timestamp", args.baseline)
        cfg.add(config.Scope.applicationOverride, "report", "comparison.contender.timestamp", args.contender)

    configure_logging(cfg)

    success = dispatch_sub_command(cfg, sub_command)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
