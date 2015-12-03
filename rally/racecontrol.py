# mode:
# latest (i.e. git fetch origin && git checkout master && git rebase origin/master,
# current (just use source tree as is)
# replay (for backtesting), can get a range of dates or commits

# later:
# tournament: checking two revisions against each other

import rally.mechanic.mechanic as m
# TODO dm: we need to autodiscover all tracks later. For now, we import the sole track explicitly
import rally.track.logging_track as t
import rally.driver as d
import rally.reporter as r

import rally.utils.io


class RaceControl:
  def __init__(self, config):
    self._config = config

  def start(self):
    # This is one of the few occasions where we directly log to console in order to give at least some feedback to users.
    # Do not log relevant output, just feedback, that we're still progressing...
    print("Preparing benchmark...")
    # TODO dm: Iterate over benchmarks here (later when we have more than one)
    rally.utils.io.kill_java()
    config = self._config
    mechanic = m.Mechanic(config)
    mechanic.setup_for_series()
    driver = d.Driver(config)
    reporter = r.Reporter(config)

    t.loggingSeries.setup(config)

    print("Running benchmark...")
    for track in t.loggingSeries.tracks():
      track.setup(config)
      mechanic.setup()
      cluster = mechanic.start_engine()
      driver.setup(cluster, track)
      driver.go(cluster, track)
      mechanic.stop_engine(cluster)

    reporter.report(t.loggingSeries.tracks())
