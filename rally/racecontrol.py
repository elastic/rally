# mode:
# latest (i.e. git fetch origin && git checkout master && git rebase origin/master,
# current (just use source tree as is)
# replay (for backtesting), can get a range of dates or commits

# later:
# tournament: checking two revisions against each other

import rally.mechanic.mechanic as m
import rally.driver as d
import rally.reporter
import rally.summary_reporter
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
    print("Preparing for race (might take a few moments) ...")
    config = self._config
    mechanic = m.Mechanic(config)
    driver = d.Driver(config)
    summary_reporter = rally.summary_reporter.SummaryReporter(config)
    reporter = rally.reporter.Reporter(config)

    mechanic.prepare_candidate()

    marshal = rally.track.track.Marshal(config)
    print("Racing on %d track(s). Overall ETA: %d minutes (depending on your hardware)\n" % (len(self._all_tracks()), self._eta()))
    for index, track in enumerate(self._all_tracks(), start=1):
      rally.utils.process.kill_running_es_instances()
      marshal.setup(track)
      for track_setup in track.track_setups:
        print("Racing on track '%s' with setup '%s'" % (track.name, track_setup.name))
        cluster = mechanic.start_engine(track_setup)
        driver.setup(cluster, track, track_setup)
        driver.go(cluster, track, track_setup)
        mechanic.stop_engine(cluster)
        driver.tear_down(track, track_setup)
        mechanic.revise_candidate()

      print("\nAll tracks done.")
      summary_reporter.report(track)
      # also write the HTML reports
      reporter.report(track)

  def _all_tracks(self):
    # just one track for now
    return [ct.countriesTrackSpec]

  def _eta(self):
    eta = 0
    for track in self._all_tracks():
      eta += track.estimated_benchmark_time_in_minutes
    return eta
