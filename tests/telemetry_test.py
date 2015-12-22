from unittest import TestCase

import rally.telemetry
import rally.config
import rally.track.track


class TelemetryTests(TestCase):
  def test_install(self):
    config = rally.config.Config()
    config.add(rally.config.Scope.globalScope, "telemetry", "devices", "jfr")
    config.add(rally.config.Scope.globalScope, "system", "track.setup.root.dir", "track-setup-root")
    config.add(rally.config.Scope.globalScope, "benchmarks", "metrics.log.dir", "telemetry")

    t = rally.telemetry.Telemetry(config)

    track_setup = rally.track.track.TrackSetup(name="test-track", description="Test Track")
    opts = t.install(track_setup)

    self.assertTrue(opts)
