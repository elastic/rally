import unittest.mock as mock
from unittest import TestCase

from esrally import racecontrol, config, exceptions


class RaceControlTests(TestCase):
    def test_finds_available_pipelines(self):
        expected = [
            ["from-sources-complete", "Builds and provisions Elasticsearch, runs a benchmark and reports results."],
            ["from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results."],
            ["from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results."],
            ["benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results"]
        ]
        self.assertEqual(expected, racecontrol.available_pipelines())

    def test_prevents_running_an_unknown_pipeline(self):
        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "invalid")
        cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "5.0.0")

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            racecontrol.run(cfg)
        self.assertEqual("Unknown pipeline [invalid]. List the available pipelines with esrally list pipelines.", ctx.exception.args[0])

    def test_runs_a_known_pipeline(self):
        mock_pipeline = mock.Mock()

        p = racecontrol.Pipeline("unit-test-pipeline", "Pipeline intended for unit-testing", mock_pipeline)

        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "unit-test-pipeline")
        cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "")

        racecontrol.run(cfg)

        mock_pipeline.assert_called_once_with(cfg)

        # ensure we remove it again from the list of registered pipelines to avoid unwanted side effects
        del p


