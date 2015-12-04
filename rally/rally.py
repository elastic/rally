import datetime
import sys
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

  # FIXME dm: This is actually already a benchmark mode!!!
  parser.add_argument(
    '--update-sources',
    help='force a remote fetch and rebase on master (intended for CI runs) (default: false)',
    default=False,
    action="store_true")

  parser.add_argument(
    '--advanced-config',
    help='show additional configuration options when creating the config file (intended for CI runs) (default: false)',
    default=False,
    action="store_true")

  # TODO dm: Add benchmark mode (https://docs.python.org/3.5/library/argparse.html#sub-commands)
  #
  # The idea is:
  #
  # single: can provide
  # * a specific revision
  # * a timestamp
  # * the meta-revision "current" (i.e. assume source tree is already at the right version, which is handy for locally running benchmarks)
  # * the meta-revision "latest" (fetches latest master, typically for CI / nightly benchmarks)
  #
  # range: intended for backtesting, can provide two values, lower, upper (each can have the same values as for single)
  # tournament: provide two revisions to compare (similar to backtesting but only two revisions are checked, not all between them)
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
  cfg.add(rally.config.Scope.globalOverrideScope, "source", "force.update", args.update_sources)
  cfg.add(rally.config.Scope.globalOverrideScope, "build", "skip", args.skip_build)

  configure_logging(cfg)

  race_control = rc.RaceControl(cfg)
  race_control.start()


if __name__ == '__main__':
  main()
