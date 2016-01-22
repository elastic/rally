from unittest import TestCase

import rally.telemetry
import rally.config
import rally.track.track


class TelemetryTests(TestCase):
  def test_instrument_candidate_env(self):
    config = rally.config.Config()
    config.add(rally.config.Scope.application, "telemetry", "devices", "jfr")
    config.add(rally.config.Scope.application, "system", "track.setup.root.dir", "track-setup-root")
    config.add(rally.config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

    t = rally.telemetry.Telemetry(config, None)

    track_setup = rally.track.track.TrackSetup(name="test-track", description="Test Track")
    opts = t.instrument_candidate_env(track_setup)

    self.assertTrue(opts)
