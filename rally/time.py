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
