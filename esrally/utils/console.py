import os
import sys

QUIET = False
PLAIN = False


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


format = PlainFormat


def init(quiet=False):
    global QUIET, PLAIN, format

    QUIET = quiet
    if os.environ.get("TERM") == "dumb":
        PLAIN = True
        format = PlainFormat
    else:
        PLAIN = False
        format = RichFormat


def println(msg, end="\n", flush=False, logger=lambda m: m):
    if not QUIET:
        print(msg, end=end, flush=flush)
    logger(msg)


def progress(width=90):
    return CmdLineProgressReporter(width, plain_output=PLAIN)


class CmdLineProgressReporter:
    """
    CmdLineProgressReporter supports displaying an updating progress indication together with an information message.
    """

    def __init__(self, width, plain_output=False):
        self._width = width
        self._first_print = True
        self._plain_output = plain_output

    def print(self, message, progress):
        """
        Prints/updates a line. The message will be left aligned, and the progress message will be right aligned.

        Typically, the message is static and progress changes over time (it could show a progress indication as
         percentage).

        :param message: A message to display (will be left-aligned)
        :param progress: A progress indication (will be right-aligned)
        """
        w = self._width
        if self._first_print:
            print(" " * w, end="")
            self._first_print = False

        final_message = self._truncate(message, self._width - len(progress))

        formatted_progress = progress.rjust(w - len(final_message))
        if self._plain_output:
            print("\n{0}{1}".format(final_message, formatted_progress), end="")
        else:
            print("\033[{0}D{1}{2}".format(w, final_message, formatted_progress), end="")
        sys.stdout.flush()

    def _truncate(self, text, max_length, omission="..."):
        if len(text) <= max_length:
            return text
        else:
            return "%s%s" % (text[0:max_length - len(omission) - 5], omission)

    def finish(self):
        # print a final statement in order to end the progress line
        print("")
