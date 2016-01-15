import datetime
import re

timestamp_pattern = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)$')


def all_invocations_root_dir(root):
  return "%s/races" % root


def invocation_root_dir(root, start):
  ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  return "%s/%s" % (all_invocations_root_dir(root), ts)


def to_timestamp(dir):
  m = timestamp_pattern.match(dir)
  if len(m.groups()) == 6:
    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    hour = int(m.group(4))
    minute = int(m.group(5))
    second = int(m.group(6))
    return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
  else:
    raise RuntimeError("[%s] is an invalid timestamp (expected to match against [%s])" % (invocation_root_dir, timestamp_pattern))


def all_tracks_root_root_dir(invocation_root):
  return "%s/tracks" % invocation_root


# we place tracks in their own sub directory, otherwise track names might clash with Rally internal directories (like "logs")
def track_root_dir(invocation_root, track_name):
  return "%s/%s" % (all_tracks_root_root_dir(invocation_root), track_name.lower())


def track_setup_root_dir(track_root, track_setup_name):
  return "%s/%s" % (track_root, track_setup_name)
