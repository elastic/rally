import sys


class CmdLineProgressReporter:
    """
    CmdLineProgressReporter supports displaying an updating progress indication together with an information message.
    """

    def __init__(self, width=80):
        self._width = width
        self._first_print = True

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

        formatted_progress = progress.rjust(w - len(message))
        print("\033[{0}D{1}{2}".format(w, message, formatted_progress), end="")
        sys.stdout.flush()

    def finish(self):
        # print a final statement in order to end the progress line
        print("")
