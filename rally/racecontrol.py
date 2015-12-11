# mode:
# latest (i.e. git fetch origin && git checkout master && git rebase origin/master,
# current (just use source tree as is)
# replay (for backtesting), can get a range of dates or commits

# later:
# tournament: checking two revisions against each other
import logging

import rally.mechanic.mechanic as m
import rally.driver as d
import rally.reporter
import rally.summary_reporter
# TODO dm: we need to autodiscover all tracks later. For now, we import the sole track explicitly
import rally.track.countries_track as ct
import rally.track.logging_track as lt
import rally.track.track

import rally.utils.process
import rally.exceptions

logger = logging.getLogger("rally.racecontrol")


class RaceControl:
  def __init__(self, config):
    self._config = config

  def start(self, command):
    participants = self.choose_participants(command)

    for p in participants:
      p.prepare(self._all_tracks(), self._config)

    for track in self._all_tracks():
      for p in participants:
        p.do(track)

    print("\nAll tracks done.")

  def choose_participants(self, command):
    logger.info("Executing command [%s]" % command)
    if command == "all":
      return [RacingTeam(), Press(report_only=False)]
    elif command == "race":
      return [RacingTeam()]
    elif command == "report":
      return [Press(report_only=True)]
    else:
      raise rally.exceptions.ImproperlyConfigured("Unknown command [%s]" % command)

  def _all_tracks(self):
    # just one track for now
    return [ct.countriesTrackSpec]


class RacingTeam:
  def __init__(self):
    self._mechanic = None
    self._driver = None
    self._marshal = None

  def prepare(self, tracks, config):
    # This is one of the few occasions where we directly log to console in order to give at least some feedback to users.
    # Do not log relevant output, just feedback, that we're still progressing...
    print("Preparing for race (might take a few moments) ...")
    self._mechanic = m.Mechanic(config)
    self._driver = d.Driver(config)
    self._marshal = rally.track.track.Marshal(config)
    self._mechanic.prepare_candidate()
    print("Racing on %d track(s). Overall ETA: %d minutes (depending on your hardware)\n" % (len(tracks), self._eta(tracks)))

  def do(self, track):
    rally.utils.process.kill_running_es_instances()
    self._marshal.setup(track)
    for track_setup in track.track_setups:
      print("Racing on track '%s' with setup '%s'" % (track.name, track_setup.name))
      cluster = self._mechanic.start_engine(track_setup)
      self._driver.setup(cluster, track, track_setup)
      self._driver.go(cluster, track, track_setup)
      self._mechanic.stop_engine(cluster)
      self._driver.tear_down(track, track_setup)
      self._mechanic.revise_candidate()

  def _eta(self, tracks):
    eta = 0
    for track in tracks:
      eta += track.estimated_benchmark_time_in_minutes
    return eta


class Press:
  def __init__(self, report_only):
    self._reporter = None
    self._summary_reporter = None
    self.report_only = report_only

  def prepare(self, tracks, config):
    self._reporter = rally.reporter.Reporter(config)
    self._summary_reporter = rally.summary_reporter.SummaryReporter(config)

  def do(self, track):
    # always write the HTML reports
    self._reporter.report(track)
    # Producing a summary report only makes sense if we have current metrics
    if not self.report_only:
      self._summary_reporter.report(track)
