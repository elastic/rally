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
import rally.track.track

import rally.utils.process


class RaceControl:
  def __init__(self, config):
    self._config = config

  def start(self):
    # This is one of the few occasions where we directly log to console in order to give at least some feedback to users.
    # Do not log relevant output, just feedback, that we're still progressing...
    print("Preparing for race on %d track(s) ..." % len(self._all_tracks()))
    config = self._config
    mechanic = m.Mechanic(config)
    driver = d.Driver(config)
    reporter = r.Reporter(config)

    mechanic.prepare_candidate()

    marshal = rally.track.track.Marshal(config)

    for index, track in enumerate(self._all_tracks(), start=1):
      print("Current track: '%s' [%d/%d]. ETA: %d minutes (depending on your hardware)\n" %
            (track.name, index, len(self._all_tracks()), track.estimated_benchmark_time_in_minutes))
      rally.utils.process.kill_java()
      marshal.setup(track)
      for track_setup in track.track_setups:
        print("Running on track '%s' with setup '%s'" % (track.name, track_setup.name))
        # TODO dm [Refactoring] Don't call this method at all, just have the appropriate class interpret the specification
        track_setup.setup(config)
        # TODO dm: We probably need the track here to perform proper track-setup-specific setup (later)
        cluster = mechanic.start_engine()
        driver.setup(cluster, track, track_setup)
        driver.go(cluster, track, track_setup)
        mechanic.stop_engine(cluster)
        driver.tear_down(track, track_setup)
        mechanic.revise_candidate()

      print("\nAll tracks done. Generating reports...")
      reporter.report(track)

  def _all_tracks(self):
    # just one track for now
    return [ct.countriesTrackSpec]
