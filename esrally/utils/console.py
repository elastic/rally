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

import os
import shutil
import sys

PLAIN = False
QUIET = False
RALLY_RUNNING_IN_DOCKER = False
ASSUME_TTY = True


class PlainFormat:
    @classmethod
    def bold(cls, message):
        return message

    @classmethod
    def link(cls, href):
        return href

    @classmethod
    def red(cls, message):
        return message

    @classmethod
    def green(cls, message):
        return message

    @classmethod
    def yellow(cls, message):
        return message

    @classmethod
    def neutral(cls, message):
        return message

    @classmethod
    def underline_for(cls, message, underline_symbol="*"):
        return underline_symbol * len(message)


class RichFormat:
    @classmethod
    def bold(cls, message):
        return "\033[1m%s\033[0m" % message

    @classmethod
    def link(cls, href):
        return "\033[4m%s\033[0m" % href

    @classmethod
    def red(cls, message):
        return "\033[31;1m%s\033[0m" % message

    @classmethod
    def green(cls, message):
        return "\033[32;1m%s\033[0m" % message

    @classmethod
    def yellow(cls, message):
        return "\033[33;1m%s\033[0m" % message

    @classmethod
    def neutral(cls, message):
        return "\033[39;1m%s\033[0m" % message

    @classmethod
    def underline_for(cls, message, underline_symbol="*"):
        return underline_symbol * len(message)


format = PlainFormat


def init(quiet=False, assume_tty=True):
    """
    Initialize console out.

    :param quiet: Flag indicating whether Rally should not print anything except when forced explicitly. Default: False.
    :param assume_tty: Flag indicating whether to assume a tty is attached without checking. Default: True.
    """
    global QUIET, ASSUME_TTY, RALLY_RUNNING_IN_DOCKER, PLAIN, format

    QUIET = quiet
    ASSUME_TTY = assume_tty
    RALLY_RUNNING_IN_DOCKER = os.environ.get("RALLY_RUNNING_IN_DOCKER", "").upper() == "TRUE"
    if os.environ.get("TERM") == "dumb" or sys.platform == "win32":
        PLAIN = True
        format = PlainFormat
    else:
        PLAIN = False
        format = RichFormat
    # workaround for http://bugs.python.org/issue13041
    #
    # Set a proper width (see argparse.HelpFormatter)
    try:
        int(os.environ["COLUMNS"])
    except (KeyError, ValueError):
        # noinspection PyBroadException
        try:
            os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)
        except BaseException:
            # don't fail if anything goes wrong here
            pass


def set_assume_tty(assume_tty):
    """
    Change whether Rally should assume a tty. If ``True`` is provided, output will be printed. If ``False`` is provided,
    Rally will explicitly check whether it is attached to a tty before attempting to print anything.

    :param assume_tty: Flag indicating whether to assume a tty is attached without checking.
    """
    global ASSUME_TTY
    ASSUME_TTY = assume_tty


def info(msg, end="\n", flush=False, force=False, logger=None, overline=None, underline=None):
    println(msg, console_prefix="[INFO]", end=end, flush=flush, force=force, overline=overline, underline=underline,
            logger=logger.info if logger else None)


def warn(msg, end="\n", flush=False, force=False, logger=None, overline=None, underline=None):
    println(msg, console_prefix="[WARNING]", end=end, flush=flush, force=force, overline=overline, underline=underline
            , logger=logger.warning if logger else None)


def error(msg, end="\n", flush=False, force=False, logger=None, overline=None, underline=None):
    println(msg, console_prefix="[ERROR]", end=end, flush=flush, force=force, overline=overline, underline=underline
            , logger=logger.error if logger else None)


def println(msg, console_prefix=None, end="\n", flush=False, force=False, logger=None, overline=None, underline=None):
    allow_print = force or (not QUIET and (RALLY_RUNNING_IN_DOCKER or ASSUME_TTY or sys.stdout.isatty()))
    if allow_print:
        complete_msg = "%s %s" % (console_prefix, msg) if console_prefix else msg
        if overline:
            print(format.underline_for(complete_msg, underline_symbol=overline), flush=flush)
        print(complete_msg, end=end, flush=flush)
        if underline:
            print(format.underline_for(complete_msg, underline_symbol=underline), flush=flush)
    if logger:
        logger(msg)


def progress(width=90):
    return CmdLineProgressReporter(width, plain_output=PLAIN)


class CmdLineProgressReporter:
    """
    CmdLineProgressReporter supports displaying an updating progress indication together with an information message.

    :param printer: allow use of a different print method to assist with patching in unittests
    """

    # pylint: disable=E0601
    def __init__(self, width, plain_output=False, printer=print):
        self._width = width
        self._first_print = True
        self._plain_output = plain_output
        self._printer = printer

    def print(self, message, progress):
        """
        Prints/updates a line. The message will be left aligned, and the progress message will be right aligned.

        Typically, the message is static and progress changes over time (it could show a progress indication as
         percentage).

        :param message: A message to display (will be left-aligned)
        :param progress: A progress indication (will be right-aligned)
        """
        if QUIET or (not RALLY_RUNNING_IN_DOCKER and not ASSUME_TTY and not sys.stdout.isatty()):
            return
        w = self._width
        if self._first_print:
            self._printer(" " * w, end="")
            self._first_print = False

        final_message = self._truncate(message, self._width - len(progress))

        formatted_progress = progress.rjust(w - len(final_message))
        if self._plain_output:
            self._printer("\n{0}{1}".format(final_message, formatted_progress), end="")
        else:
            self._printer("\033[{0}D{1}{2}".format(w, final_message, formatted_progress), end="")
        sys.stdout.flush()

    def _truncate(self, text, max_length, omission="..."):
        if len(text) <= max_length:
            return text
        else:
            return "%s%s" % (text[0:max_length - len(omission) - 5], omission)

    def finish(self):
        if QUIET or (not RALLY_RUNNING_IN_DOCKER and not ASSUME_TTY and not sys.stdout.isatty()):
            return
        # print a final statement in order to end the progress line
        self._printer("")
