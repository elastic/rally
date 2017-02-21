import os


def rally_root():
    return os.path.dirname(os.path.realpath(__file__))


def race_root(cfg):
    root = cfg.opts("node", "root.dir")
    start = cfg.opts("system", "time.start")
    env = cfg.opts("system", "env.name")
    ts = "%04d-%02d-%02d-%02d-%02d-%02d" % (start.year, start.month, start.day, start.hour, start.minute, start.second)
    return "%s/races/%s/%s" % (root, ts, env)
