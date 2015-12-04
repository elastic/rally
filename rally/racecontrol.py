# mode:
# latest (i.e. git fetch origin && git checkout master && git rebase origin/master,
# current (just use source tree as is)
# replay (for backtesting), can get a range of dates or commits

# later:
# tournament: checking two revisions against each other

import rally.mechanic.mechanic as m
import rally.driver as d
import rally.reporter as r
# TODO dm: we need to autodiscover all tracks later. For now, we import the sole track explicitly
import rally.track.countries_track as ct
import rally.track.logging_track as lt

import rally.utils.io


class RaceControl:
  def __init__(self, config):
    self._config = config

  def start(self):
    # This is one of the few occasions where we directly log to console in order to give at least some feedback to users.
    # Do not log relevant output, just feedback, that we're still progressing...
    print("Preparing for race ...")
    config = self._config
    mechanic = m.Mechanic(config)
    driver = d.Driver(config)
    reporter = r.Reporter(config)

    mechanic.pre_setup()
    for track in self._all_tracks():
      print("Running on track '%s' ..." % track.name())
      rally.utils.io.kill_java()
      track.setup(config)
      for track_setup in track.track_setups():
        track_setup.setup(config)
        # TODO dm: We probably need the track here to perform proper track-setup-specific setup (later)
        cluster = mechanic.start_engine()
        driver.setup(cluster, track_setup)
        driver.go(cluster, track_setup)
        mechanic.stop_engine(cluster)

      reporter.report(track)

  def _all_tracks(self):
    # just one track for now
    # return [lt.loggingSeries]
    return [ct.countriesTrack]
