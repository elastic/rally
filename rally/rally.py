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

  # console logging
  # logging.basicConfig(level=logging.INFO)

  logging.basicConfig(filename="%s/rally_out.log" % log_dir,
                      filemode='a',
                      format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                      datefmt='%H:%M:%S',
                      level=logging.INFO)


def parse_args():
  parser = argparse.ArgumentParser(prog='esrally', description='Benchmark Elasticsearch')

  parser.add_argument(
    '--skip-build',
    help='assumes an Elasticsearch zip file is already built and skips the build phase (default: false)',
    default=False,
    action="store_true")

  # range: intended for backtesting, can provide two values, lower, upper (each can have the same values as for single)
  # tournament: provide two revisions to compare (similar to backtesting but only two revisions are checked, not all between them)
  parser.add_argument(
    '--benchmark-mode',
    help="defines how to run benchmarks. 'single' runs the single revision given by '--revision'. 'range' allows for backtesting across a range of versions (intended for CI). Currently only 'single' is supported (default: single).",
    choices=["single", "range"],  # later also 'tournament'
    default="single")

  parser.add_argument(
    '--revision',
    help="defines which sources to use for 'single'_benchmark mode. 'current' uses the source tree as is, 'latest' fetches the latest version on master (default: current).",
    choices=["current", "latest"],
    default="current")  # optimized for local usage, don't fetch sources

  parser.add_argument(
    '--advanced-config',
    help='show additional configuration options when creating the config file (intended for CI runs) (default: false)',
    default=False,
    action="store_true")

  return parser.parse_args()


def main():
  args = parse_args()
  cfg = rally.config.Config()
  if cfg.config_present():
    cfg.load_config()
  else:
    cfg.create_config(advanced_config=args.advanced_config)
    exit(0)
  # Add global meta info derived by rally itself
  cfg.add(rally.config.Scope.globalScope, "meta", "time.start", datetime.datetime.now())
  cfg.add(rally.config.Scope.globalScope, "system", "rally.root", os.path.dirname(os.path.realpath(__file__)))
  # Add command line config
  cfg.add(rally.config.Scope.globalOverrideScope, "source", "revision", args.revision)
  cfg.add(rally.config.Scope.globalOverrideScope, "build", "skip", args.skip_build)

  configure_logging(cfg)

  race_control = rc.RaceControl(cfg)
  race_control.start()


if __name__ == '__main__':
  main()
