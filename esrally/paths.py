class Paths:
    def __init__(self, config):
        self._config = config

    def invocation_root(self):
        root = self._config.opts("system", "root.dir")
        start = self._config.opts("meta", "time.start")
        env = self._config.opts("system", "env.name")
        ts = "%04d-%02d-%02d-%02d-%02d-%02d" % (start.year, start.month, start.day, start.hour, start.minute, start.second)
        return "%s/races/%s/%s" % (root, ts, env)

    def log_root(self):
        root_dir = self._config.opts("system", "invocation.root.dir")
        log_root_dir = self._config.opts("system", "log.root.dir")
        return "%s/%s" % (root_dir, log_root_dir)

    def track_root(self, track_name):
        invocation_root = self._config.opts("system", "invocation.root.dir")
        # we place tracks in their own sub directory, otherwise track names might clash with Rally internal directories (like "logs")
        return "%s/tracks/%s" % (invocation_root, track_name.lower())

    def challenge_root(self, track_name, challenge_name):
        return "%s/%s" % (self.track_root(track_name), challenge_name)

    def challenge_logs(self, track_name, challenge_name):
        invocation_root = self._config.opts("system", "invocation.root.dir")
        log_dir_name = self._config.opts("system", "log.root.dir")
        return "%s/%s/%s/%s" % (invocation_root, log_dir_name, track_name.lower(), challenge_name)
