# mode:
# latest (i.e. git fetch origin && git checkout master && git rebase origin/master,
# current (just use source tree as is)
# replay (for backtesting), can get a range of dates or commits

# later:
# tournament: checking two revisions against each other

import mechanic.mechanic as m
# TODO dm: we need to autodiscover all tracks later. For now, we import the sole track explicitly
import track.logging_track as t
import driver.driver as d
import reporter.reporter as r

import utils.io


class RaceControl:
  def __init__(self, config):
    self._config = config

  def start(self):
    # TODO dm: Iterate over benchmarks here (later when we have more than one)
    utils.io.kill_java()
    config = self._config
    mechanic = m.Mechanic(config)
    mechanic.setup_for_series()
    driver = d.Driver(config)
    reporter = r.Reporter(config)

    t.loggingSeries.setup(config)

    for track in t.loggingSeries.tracks():
      track.setup(config)
      mechanic.setup()
      cluster = mechanic.start_engine()
      driver.setup(cluster, track)
      driver.go(cluster, track)
      mechanic.stop_engine(cluster)

    reporter.report(t.loggingSeries.tracks())
