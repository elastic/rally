# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest.mock as mock
import os
import random
from unittest import TestCase

from esrally import racecontrol, config, exceptions, DOC_LINK
from esrally.utils import console


class RaceControlTests(TestCase):
    def test_finds_available_pipelines(self):
        expected = [
            ["from-sources-complete", "Builds and provisions Elasticsearch, runs a benchmark and reports results."],
            ["from-sources-skip-build", "Provisions Elasticsearch (skips the build), runs a benchmark and reports results."],
            ["from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results."],
            ["benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results"],
        ]

        self.assertEqual(expected, racecontrol.available_pipelines())

    def test_prevents_running_an_unknown_pipeline(self):
        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "invalid")
        cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "5.0.0")

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            racecontrol.run(cfg)
        self.assertRegex(ctx.exception.args[0], r"Unknown pipeline \[invalid\]. List the available pipelines with [\S]+? list pipelines.")

    @mock.patch.dict(os.environ, {"RALLY_RUNNING_IN_DOCKER": "true"})
    def test_passes_benchmark_only_pipeline_in_docker(self):
        mock_pipeline = mock.Mock()
        test_pipeline_name = "benchmark-only"
        racecontrol.Pipeline("benchmark-only", "Mocked benchmark-only pipeline for unittest", mock_pipeline)
        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "benchmark-only")

        racecontrol.run(cfg)

        mock_pipeline.assert_called_once_with(cfg)

        del racecontrol.pipelines[test_pipeline_name]

    @mock.patch.dict(os.environ, {"RALLY_RUNNING_IN_DOCKER": "true"})
    def test_fails_without_benchmark_only_pipeline_in_docker(self):
        mock_pipeline = mock.Mock()
        test_pipeline_name = "unit-test-pipeline"
        racecontrol.Pipeline("unit-test-pipeline", "Pipeline intended for unit-testing", mock_pipeline)
        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "unit-test-pipeline")

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            racecontrol.run(cfg)

        self.assertEqual(
            "Only the [benchmark-only] pipeline is supported by the Rally Docker image.\n"
            "Add --pipeline=benchmark-only in your Rally arguments and try again.\n"
            "For more details read the docs for the benchmark-only pipeline in https://esrally.readthedocs.io/en/latest/pipelines.html#benchmark-only\n",
            ctx.exception.args[0])
        del racecontrol.pipelines[test_pipeline_name]

    def test_runs_a_known_pipeline(self):
        mock_pipeline = mock.Mock()
        test_pipeline_name = "unit-test-pipeline"

        racecontrol.Pipeline("unit-test-pipeline", "Pipeline intended for unit-testing", mock_pipeline)

        cfg = config.Config()
        cfg.add(config.Scope.benchmark, "race", "pipeline", "unit-test-pipeline")
        cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "")

        racecontrol.run(cfg)

        mock_pipeline.assert_called_once_with(cfg)

        # ensure we remove it again from the list of registered pipelines to avoid unwanted side effects
        del racecontrol.pipelines[test_pipeline_name]
