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
    config = self._config
    mechanic = m.Mechanic(config)
    driver = d.Driver(config)
    reporter = r.Reporter(config)

    mechanic.pre_setup()
    for series in self._all_series():
      print("Running '%s'..." % series.name())
      rally.utils.io.kill_java()
      series.setup(config)
      for track in series.tracks():
        track.setup(config)
        # TODO dm: We propably need the track here to perform proper track-specific setup (later)
        mechanic.setup_for_track()
        cluster = mechanic.start_engine()
        driver.setup(cluster, track)
        driver.go(cluster, track)
        mechanic.stop_engine(cluster)

      reporter.report(series.tracks())

  def _all_series(self):
    # just one series for now
    return [t.loggingSeries]
