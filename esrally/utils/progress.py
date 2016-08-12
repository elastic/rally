import sys


class CmdLineProgressReporter:
    """
    CmdLineProgressReporter supports displaying an updating progress indication together with an information message.
    """

    def __init__(self, width=90):
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

        final_message = self._truncate(message, self._width - len(progress))

        formatted_progress = progress.rjust(w - len(final_message))
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
