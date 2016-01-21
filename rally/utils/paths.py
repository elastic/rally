def all_invocations_root_dir(root):
  return "%s/races" % root


def invocation_root_dir(root, start, env):
  ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  return "%s/%s/%s" % (all_invocations_root_dir(root), ts, env)


def all_tracks_root_root_dir(invocation_root):
  return "%s/tracks" % invocation_root


# we place tracks in their own sub directory, otherwise track names might clash with Rally internal directories (like "logs")
def track_root_dir(invocation_root, track_name):
  return "%s/%s" % (all_tracks_root_root_dir(invocation_root), track_name.lower())


def track_setup_root_dir(track_root, track_setup_name):
  return "%s/%s" % (track_root, track_setup_name)
