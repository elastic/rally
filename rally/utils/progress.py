import sys


class CmdLineProgressReporter:
  def __init__(self, width=80):
    self._width = width
    self._first_print = True

  def print(self, message, progress):
    w = self._width
    if self._first_print:
      print(' ' * w, end='')
      self._first_print = False

    formatted_progress = progress.rjust(w - len(message))
    print('\033[{0}D{1}{2}'.format(w, message, formatted_progress), end='')
    sys.stdout.flush()
