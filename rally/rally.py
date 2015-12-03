import datetime
import sys
import os

import logging

import racecontrol as rc
import config
import utils.io


def print_help():
  print("Usage: %s [options]\n" % sys.argv[0])

  print("Supported options:\n")
  print("--help\t\tShows this help")
  # Don't advertise this yet, it is not fully working (only for build)
  # print("--dry-run\tDry run of the whole benchmark (useful for checking the configuration)")
  # TODO dm: This is not yet supported
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

  # print("--benchmark-mode\tSupported values are: single (default), range, tournament")


def configure_logging(cfg):
  log_root_dir = cfg.opts("system", "log.root.dir")
  start = cfg.opts("meta", "time.start")

  ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)

  log_dir = "%s/%s" % (log_root_dir, ts)
  utils.io.ensure_dir(log_dir)
  cfg.add(config.Scope.globalScope, "system", "log.dir", log_dir)

  # console logging
  # logging.basicConfig(level=logging.INFO)

  logging.basicConfig(filename="%s/rally_out.log" % log_dir,
                      filemode='a',
                      format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                      datefmt='%H:%M:%S',
                      level=logging.INFO)


def main():
  if "--help" in sys.argv:
    print_help()
  else:
    cfg = config.Config()
    cfg.add(config.Scope.globalScope, "meta", "time.start", datetime.datetime.now())
    cfg.add(config.Scope.globalScope, "system", "dryrun", "--dry-run" in sys.argv)
    cfg.add(config.Scope.globalScope, "system", "rally.root", os.path.dirname(os.path.realpath(__file__)))

    configure_logging(cfg)

    race_control = rc.RaceControl(cfg)
    race_control.start()


if __name__ == '__main__':
  main()
