import datetime
import os
import logging
import argparse

import rally.racecontrol as rc
import rally.config
import rally.utils.io


def configure_logging(cfg):
  log_root_dir = cfg.opts("system", "log.root.dir")
  start = cfg.opts("meta", "time.start")

  ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)

  log_dir = "%s/%s" % (log_root_dir, ts)
  rally.utils.io.ensure_dir(log_dir)
  cfg.add(rally.config.Scope.globalScope, "system", "log.dir", log_dir)

  log_file = "%s/rally_out.log" % log_dir

  print("\nWriting additional logs to %s\n" % log_file)

  # console logging
  # logging.basicConfig(level=logging.INFO)

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

  subparsers.add_parser('all', help="Run the whole benchmarking pipeline. This subcommand should typically be used.")
  subparsers.add_parser('race', help="Run only the benchmarks (without generating reports)")
  subparsers.add_parser('report', help="Generate only reports based on existing data")
  config_parser = subparsers.add_parser('configure', help='Write the configuration file or reconfigure Rally')

  parser.add_argument(
    '--skip-build',
    help='assumes an Elasticsearch zip file is already built and skips the build phase (default: false)',
    default=False,
    action="store_true")

  # range: intended for backtesting, can provide two values, lower, upper (each can have the same values as for single)
  # tournament: provide two revisions to compare (similar to backtesting but only two revisions are checked, not all between them)
  parser.add_argument(
    '--benchmark-mode',
    help="defines how to run benchmarks. 'single' runs the single revision given by '--revision'. 'range' allows for backtesting across "
         "a range of versions (intended for CI). Currently only 'single' is supported (default: single).",
    choices=["single", "range"],  # later also 'tournament'
    default="single")

  parser.add_argument(
    '--revision',
    help="defines which sources to use for 'single' benchmark mode. 'current' uses the source tree as is, 'latest' fetches the latest "
         "version on master. It is also possible to specify a commit id or a timestamp. The timestamp must be"
         "specified as: \"@ts\" where ts is any valid timestamp understood by git, e.g. \"@2013-07-27 10:37\" (default: current).",
    default="current")  # optimized for local usage, don't fetch sources

  # This option is intended to tell Rally to assume a different start date than 'now'. This is effectively just useful for things like
  # backtesting or a benchmark run across environments (think: comparison of EC2 and bare metal) but never for the typical user.
  #
  # That's why we add this just as an undocumented option.
  parser.add_argument(
    '--effective-start-date',
    help=argparse.SUPPRESS,
    type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
    default=datetime.datetime.now())

  config_parser.add_argument(
    '--advanced-config',
    help='show additional configuration options when creating the config file (intended for CI runs) (default: false)',
    default=False,
    action="store_true")

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


def main():
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
    else:
      print("Error: No config present. Please run 'esrally configure' first.")
      exit(64)

  # Add global meta info derived by rally itself
  cfg.add(rally.config.Scope.globalScope, "meta", "time.start", args.effective_start_date)
  cfg.add(rally.config.Scope.globalScope, "system", "rally.root", os.path.dirname(os.path.realpath(__file__)))
  # Add command line config
  cfg.add(rally.config.Scope.globalOverrideScope, "source", "revision", args.revision)
  cfg.add(rally.config.Scope.globalOverrideScope, "build", "skip", args.skip_build)

  configure_logging(cfg)

  race_control = rc.RaceControl(cfg)
  race_control.start(subcommand)

if __name__ == '__main__':
  main()
