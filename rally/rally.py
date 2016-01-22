import datetime
import os
import logging
import argparse

import rally.racecontrol
import rally.telemetry

import rally.config
import rally.utils.io
import rally.paths


# we want to use some basic logging even before the output to log file is configured
def preconfigure_logging():
  logging.basicConfig(level=logging.INFO)


def configure_logging(cfg):
  log_dir = rally.paths.Paths(cfg).log_root()
  rally.utils.io.ensure_dir(log_dir)
  cfg.add(rally.config.Scope.application, "system", "log.dir", log_dir)
  log_file = "%s/rally_out.log" % log_dir

  print("\nWriting additional logs to %s\n" % log_file)

  # Remove all handlers associated with the root logger object so we can start over with an entirely fresh log configuration
  for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

  logging.basicConfig(filename=log_file,
                      filemode='a',
                      format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                      datefmt='%H:%M:%S',
                      level=logging.INFO)


def parse_args():
  parser = argparse.ArgumentParser(prog='esrally', description='Benchmark Elasticsearch')

  # TODO dm: Come up with a more descriptive help message
  subparsers = parser.add_subparsers(
      title='subcommands',
      dest='subcommand',
      help='Subcommands define what Rally will do')

  all_parser = subparsers.add_parser('all', help="Run the whole benchmarking pipeline. This subcommand should typically be used.")
  race_parser = subparsers.add_parser('race', help="Run only the benchmarks (without generating reports)")
  report_parser = subparsers.add_parser('report', help="Generate only reports based on existing data")
  # TODO dm: Should we define a generic "list" command which allows us to list all sorts of things and have just options, e.g.
  # "esrally list --telemetry"?
  subparsers.add_parser('list-telemetry', help='Lists all of the available telemetry devices')

  config_parser = subparsers.add_parser('configure', help='Write the configuration file or reconfigure Rally')
  for p in [parser, config_parser]:
    p.add_argument(
        '--advanced-config',
        help='show additional configuration options when creating the config file (intended for CI runs) (default: false)',
        default=False,
        action="store_true")

  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--skip-build',
        help='assumes an Elasticsearch zip file is already built and skips the build phase (default: false)',
        default=False,
        action="store_true")

  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--preserve-install',
        help='preserves the Elasticsearch benchmark candidate installation including all data. Caution: This will take lots of disk space! (default: false)',
        default=False,
        action="store_true")

  # range: intended for backtesting, can provide two values, lower, upper (each can have the same values as for single)
  # tournament: provide two revisions to compare (similar to backtesting but only two revisions are checked, not all between them)
  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--benchmark-mode',
        help="defines how to run benchmarks. 'single' runs the single revision given by '--revision'. 'range' allows for backtesting across "
             "a range of versions (intended for CI). Currently only 'single' is supported (default: single).",
        choices=["single", "range"],  # later also 'tournament'
        default="single")

  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--telemetry',
        help='Rally will enable all of the provided telemetry devices (i.e. profilers). Multiple telemetry devices have to be '
             'provided as a comma-separated list.',
        default="")

  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--revision',
        help="defines which sources to use for 'single' benchmark mode. 'current' uses the source tree as is, 'latest' fetches the latest "
             "version on master. It is also possible to specify a commit id or a timestamp. The timestamp must be"
             "specified as: \"@ts\" where ts is any valid timestamp understood by git, e.g. \"@2013-07-27 10:37\" (default: current).",
        default="current")  # optimized for local usage, don't fetch sources

  for p in [parser, all_parser, race_parser]:
    p.add_argument(
        '--track-setup',
        help="defines which track-setups should be run. Multiple track setups can be specified as a comma-separated list.",
        default="defaults")  # optimized for local usage

  # This option is intended to tell Rally to assume a different start date than 'now'. This is effectively just useful for things like
  # backtesting or a benchmark run across environments (think: comparison of EC2 and bare metal) but never for the typical user.
  #
  # That's why we add this just as an undocumented option.
  for p in [parser, all_parser, race_parser, report_parser]:
    p.add_argument(
        '--effective-start-date',
        help=argparse.SUPPRESS,
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        default=datetime.datetime.now())

  # This is a highly experimental option and will likely be removed
  for p in [parser, all_parser, race_parser, report_parser]:
    p.add_argument(
        '--data-paths',
        help=argparse.SUPPRESS,
        default=None)

  return parser.parse_args()


def print_banner():
  print("    ____        ____     ")
  print("   / __ \____ _/ / /_  __")
  print("  / /_/ / __ `/ / / / / /")
  print(" / _, _/ /_/ / / / /_/ / ")
  print("/_/ |_|\__,_/_/_/\__, /  ")
  print("                /____/   ")


def derive_subcommand(args, cfg):
  subcommand = args.subcommand
  # first, trust the user...
  if subcommand is not None:
    return subcommand
  # we apply some smarts in case the user did not specify a subcommand
  if cfg.config_present():
    return "all"
  else:
    return "configure"


def csv_to_list(csv):
  if csv is None:
    return None
  else:
    return [e.strip() for e in csv.split(",")]


def main():
  preconfigure_logging()
  print_banner()
  args = parse_args()
  cfg = rally.config.Config()
  subcommand = derive_subcommand(args, cfg)

  if subcommand == "configure":
    cfg.create_config(advanced_config=args.advanced_config)
    exit(0)
  else:
    if cfg.config_present():
      cfg.load_config()
      if not cfg.config_compatible():
        # logger.info("Detected incompatible configuration file. Trying to upgrade.")
        cfg.migrate_config()
        # Reload config after upgrading
        cfg.load_config()
    else:
      print("Error: No config present. Please run 'esrally configure' first.")
      exit(64)

  # Add global meta info derived by rally itself
  cfg.add(rally.config.Scope.application, "meta", "time.start", args.effective_start_date)
  cfg.add(rally.config.Scope.application, "system", "rally.root", os.path.dirname(os.path.realpath(__file__)))
  cfg.add(rally.config.Scope.application, "system", "invocation.root.dir", rally.paths.Paths(cfg).invocation_root())
  # Add command line config
  cfg.add(rally.config.Scope.applicationOverride, "source", "revision", args.revision)
  cfg.add(rally.config.Scope.applicationOverride, "build", "skip", args.skip_build)
  cfg.add(rally.config.Scope.applicationOverride, "telemetry", "devices", csv_to_list(args.telemetry))
  cfg.add(rally.config.Scope.applicationOverride, "benchmarks", "tracksetups.selected", csv_to_list(args.track_setup))
  cfg.add(rally.config.Scope.applicationOverride, "provisioning", "datapaths", csv_to_list(args.data_paths))
  cfg.add(rally.config.Scope.applicationOverride, "provisioning", "install.preserve", args.preserve_install)

  configure_logging(cfg)

  # TODO dm [Refactoring]: I am not too happy with dispatching commands on such a high-level. Can we push this down?
  if subcommand == "list-telemetry":
    telemetry = rally.telemetry.Telemetry(cfg, None)
    telemetry.list()
    exit(0)
  else:
    race_control = rally.racecontrol.RaceControl(cfg)
    race_control.start(subcommand)


if __name__ == '__main__':
  main()
