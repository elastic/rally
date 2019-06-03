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

import random
import pytest
import unittest.mock as mock

from unittest import TestCase
from esrally.utils import console


class ConsoleFunctionTests(TestCase):
    @mock.patch("os.environ.get")
    def test_global_rally_running_in_docker_is_false(self, patched_env_get):
        patched_env_get.return_value = random.choice(["false", "False", "FALSE"])

        _ = console.init()
        self.assertEqual(False, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch("os.environ.get")
    def test_global_rally_running_in_docker_is_false_if_unset(self, patched_env_get):
        patched_env_get.return_value = ""

        _ = console.init()
        self.assertEqual(False, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch("os.environ.get")
    def test_global_rally_running_in_docker_is_true(self, patched_env_get):
        patched_env_get.return_value = random.choice(["true", "True", "TRUE"])

        _ = console.init()
        self.assertEqual(True, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_println_randomized_dockertrue_or_istty_and_isnotquiet(self, patched_print, patched_isatty):
        console.QUIET = False
        random_boolean = random.choice([True, False])
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random_boolean, not random_boolean

        console.println(msg="Unittest message")
        patched_print.assert_called_once_with(
            "Unittest message", end="\n", flush=False
        )

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_println_isquiet_and_randomized_docker_or_istty(self, patched_print, patched_isatty):
        console.QUIET = True
        random_boolean = random.choice([True, False])
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random_boolean, not random_boolean

        console.println(msg="Unittest message")
        patched_print.assert_not_called()


# pytest style class names need to start with Test and don't need to subclass
class TestCmdLineProgressReporter:
    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    # monkey patching builtins might break pytest's internals
    # (https://docs.pytest.org/en/latest/monkeypatch.html#global-patch-example-preventing-requests-from-remote-operations)
    # but hopefully print doesn't fall in this category
    @mock.patch("builtins.print")
    @pytest.mark.parametrize("seed", range(20))
    def test_print_when_isquiet_and_any_docker_or_istty(self, patched_print, patched_isatty, patched_flush, seed):
        console.QUIET = True
        random.seed(seed)
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random.choice([True, False]), random.choice([True, False])

        message = "Unit test message"
        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.print(message=message, progress=".")
        patched_print.assert_not_called()


    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_prints_when_isnotquiet_and_nodocker_and_isnotty(self, patched_print, patched_isatty):
        console.QUIET = False
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = False, False

        message = "Unit test message"
        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.print(message=message, progress=".")
        patched_print.assert_not_called()

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    @pytest.mark.parametrize("seed", range(20))
    def test_prints_when_isnotquiet_and_randomized_docker_or_istty(self, patched_print, patched_isatty, patched_flush, seed):
        console.QUIET = False
        random.seed(seed)
        random_boolean = random.choice([True, False])
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random_boolean, not random_boolean

        message = "Unit test message"
        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.print(message=message, progress=".")
        patched_print.assert_has_calls([
            mock.call(" " * width, end=""),
            mock.call("\x1b[{}D{}{}.".format(width, message, " "*(width-len(message)-1)), end="")
        ])

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_noprint_when_isnotquiet_and_nodocker_and_noistty(self, patched_print, patched_isatty, patched_flush):
        patched_isatty.return_value = False
        console.QUIET = False
        console.RALLY_RUNNING_IN_DOCKER = False

        message = "Unit test message"
        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.print(message=message, progress=".")
        patched_print.assert_not_called()

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    @pytest.mark.parametrize("seed", range(10))
    def test_finish_noprint_when_isquiet_and_randomized_docker_or_istty(self, patched_print, patched_isatty, seed):
        console.QUIET = True
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random.choice([True, False]), random.choice([True, False])

        random.seed(seed)
        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.finish()
        patched_print.assert_not_called()

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    @pytest.mark.parametrize("seed", range(30))
    def test_finish_prints_when_isnotquiet_and_randomized_docker_or_istty(self, patched_print, patched_isatty, seed):
        random.seed(seed)
        random_boolean = random.choice([True, False])
        console.QUIET = False
        patched_isatty.return_value, console.RALLY_RUNNING_IN_DOCKER = random_boolean, not random_boolean

        width = random.randint(20, 140)
        progress_reporter = console.CmdLineProgressReporter(width=width)
        progress_reporter.finish()
        patched_print.assert_called_once_with("")
