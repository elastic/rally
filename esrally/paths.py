import os


def rally_root():
    return os.path.dirname(os.path.realpath(__file__))


def races_root(cfg):
    return "%s/races" % cfg.opts("node", "root.dir")


def race_root(cfg=None, start=None):
    if not start:
        start = cfg.opts("system", "time.start")
    ts = "%04d-%02d-%02d-%02d-%02d-%02d" % (start.year, start.month, start.day, start.hour, start.minute, start.second)
    return "%s/%s" % (races_root(cfg), ts)

