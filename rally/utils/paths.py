def invocation_root_dir(root, start):
  ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  return "%s/races/%s" % (root, ts)


def track_root_dir(invocation_root, track_name):
  return "%s/%s" % (invocation_root, track_name.lower())


def track_setup_root_dir(track_root, track_setup_name):
  return "%s/%s" % (track_root, track_setup_name)
