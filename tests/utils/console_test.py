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
import os

from unittest import TestCase
from esrally.utils import console


class ConsoleFunctionTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.oldconsole_quiet = console.QUIET
        cls.oldconsole_rally_running_in_docker = console.RALLY_RUNNING_IN_DOCKER

    @classmethod
    def tearDownClass(cls):
        console.QUIET = cls.oldconsole_quiet
        console.RALLY_RUNNING_IN_DOCKER = cls.oldconsole_rally_running_in_docker

    @mock.patch.dict(os.environ, {"RALLY_RUNNING_IN_DOCKER": random.choice(["false", "False", "FALSE", ""])})
    def test_global_rally_running_in_docker_is_false(self):
        console.init()
        self.assertEqual(False, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch.dict(os.environ, {"RALLY_RUNNING_IN_DOCKER": ""})
    def test_global_rally_running_in_docker_is_false_if_unset(self):
        console.init()
        self.assertEqual(False, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch.dict(os.environ, {"RALLY_RUNNING_IN_DOCKER": random.choice(["True", "true", "TRUE"])})
    def test_global_rally_running_in_docker_is_true(self):
        console.init()
        self.assertEqual(True, console.RALLY_RUNNING_IN_DOCKER)

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_println_randomized_dockertrue_or_istty_and_isnotquiet(self, patched_print, patched_isatty):
        console.init()
        console.QUIET = False
        random_boolean = random.choice([True, False])
        patched_isatty.return_value = random_boolean
        console.RALLY_RUNNING_IN_DOCKER = not random_boolean

        console.println(msg="Unittest message")
        patched_print.assert_called_once_with(
            "Unittest message", end="\n", flush=False
        )

    @mock.patch("sys.stdout.isatty")
    @mock.patch("builtins.print")
    def test_println_isquiet_and_randomized_docker_or_istty(self, patched_print, patched_isatty):
        console.init()
        console.QUIET = True
        random_boolean = random.choice([True, False])
        patched_isatty.return_value = random_boolean
        console.RALLY_RUNNING_IN_DOCKER = not random_boolean
        console.println(msg="Unittest message")
        patched_print.assert_not_called()


# pytest style class names need to start with Test and don't need to subclass
class TestCmdLineProgressReporter:
    @classmethod
    def setup_class(cls):
        cls.oldconsole_quiet = console.QUIET
        cls.oldconsole_rally_running_in_docker = console.RALLY_RUNNING_IN_DOCKER

    @classmethod
    def teardown_class(cls):
        console.QUIET = cls.oldconsole_quiet
        console.RALLY_RUNNING_IN_DOCKER = cls.oldconsole_rally_running_in_docker

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    @pytest.mark.parametrize("seed", range(20))
    def test_print_when_isquiet_and_any_docker_or_istty(self, patched_isatty, patched_flush, seed):
        console.QUIET = True
        random.seed(seed)
        patched_isatty.return_value = random.choice([True, False])
        console.RALLY_RUNNING_IN_DOCKER = random.choice([True, False])

        message = "Unit test message"
        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)

        progress_reporter.print(message=message, progress=".")
        mock_printer.assert_not_called()
        patched_flush.assert_not_called()

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    def test_prints_when_isnotquiet_and_nodocker_and_isnotty(self, patched_isatty, patched_flush):
        console.QUIET = False
        patched_isatty.return_value = False
        console.RALLY_RUNNING_IN_DOCKER = False

        message = "Unit test message"
        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)
        progress_reporter.print(message=message, progress=".")
        mock_printer.assert_not_called()
        patched_flush.assert_not_called()

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    @pytest.mark.parametrize("seed", range(20))
    def test_prints_when_isnotquiet_and_randomized_docker_or_istty(self, patched_isatty, patched_flush, seed):
        console.QUIET = False
        random.seed(seed)
        random_boolean = random.choice([True, False])
        patched_isatty.return_value = random_boolean
        console.RALLY_RUNNING_IN_DOCKER = not random_boolean

        message = "Unit test message"
        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)
        progress_reporter.print(message=message, progress=".")
        mock_printer.assert_has_calls([
            mock.call(" " * width, end=""),
            mock.call("\x1b[{}D{}{}.".format(width, message, " "*(width-len(message)-1)), end="")
        ])
        patched_flush.assert_called_once_with()

    @mock.patch("sys.stdout.flush")
    @mock.patch("sys.stdout.isatty")
    def test_noprint_when_isnotquiet_and_nodocker_and_noistty(self, patched_isatty, patched_flush):
        patched_isatty.return_value = False
        console.QUIET = False
        console.RALLY_RUNNING_IN_DOCKER = False

        message = "Unit test message"
        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)
        progress_reporter.print(message=message, progress=".")
        mock_printer.assert_not_called()
        patched_flush.assert_not_called()

    @mock.patch("sys.stdout.isatty")
    @pytest.mark.parametrize("seed", range(10))
    def test_finish_noprint_when_isquiet_and_randomized_docker_or_istty(self, patched_isatty, seed):
        console.QUIET = True
        random.seed(seed)
        patched_isatty.return_value = random.choice([True, False])
        console.RALLY_RUNNING_IN_DOCKER = random.choice([True, False])

        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)
        progress_reporter.finish()
        mock_printer.assert_not_called()

    @mock.patch("sys.stdout.isatty")
    @pytest.mark.parametrize("seed", range(30))
    def test_finish_prints_when_isnotquiet_and_randomized_docker_or_istty(self, patched_isatty, seed):
        console.QUIET = False
        random.seed(seed)
        random_boolean = random.choice([True, False])
        patched_isatty.return_value = random_boolean
        console.RALLY_RUNNING_IN_DOCKER = not random_boolean

        width = random.randint(20, 140)
        mock_printer = mock.Mock()
        progress_reporter = console.CmdLineProgressReporter(width=width, custom_print=mock_printer)
        progress_reporter.finish()
        mock_printer.assert_called_once_with("")
