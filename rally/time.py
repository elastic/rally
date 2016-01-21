import time


def to_unix_timestamp(t):
  """
  Convert a time instance retrieved via time.time() to a unix timestamp.
  :param t: a time instance
  :return: the corresponding unix timestamp (as int)
  """
  return int(round(t))


def to_iso8601(dt):
  """
  Convert a datetime instance to a ISO-8601 compliant string.
  :param dt: A datetime instance
  :return: The corresponding ISO-8601 formatted string
  """
  return '%04d%02d%02dT%02d%02d%02dZ' % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def sleep(seconds):
  time.sleep(seconds)

class Clock:
  """
  A clock abstracts time measurements. Its main purpose is to ease testing
  """

  @staticmethod
  def now():
    """
    :return: The current time.
    """
    return time.time()

  @staticmethod
  def stop_watch():
    return StopWatch()


class StopWatch:
  def __init__(self):
    self._start = None
    self._stop = None

  def start(self):
    self._start = self._now()

  def stop(self):
    self._stop = self._now()

  def split_time(self):
    return self._interval(self._start, self._now())

  def total_time(self):
    return self._interval(self._start, self._stop)

  def _interval(self, t0, t1):
    if t0 is None:
      raise RuntimeError("start time is None")
    if t1 is None:
      raise RuntimeError("end time is None")
    return t1 - t0

  def _now(self):
    return time.perf_counter()


