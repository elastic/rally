import logging

from esrally import metrics, paths, config
from esrally.utils import console
from esrally.mechanic import builder, supplier, provisioner, launcher

logger = logging.getLogger("rally.mechanic")


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, cfg):
        self._config = cfg
        self._supplier = supplier.Supplier(cfg)
        self._builder = builder.Builder(cfg)
        self._provisioner = provisioner.Provisioner(cfg)
        self._launcher = launcher.InProcessLauncher(cfg)
        self._metrics_store = None

        # TODO dm module refactoring: just moved it to the right place. Simplify (this should actually not be needed at all. It's just there
        # to ensure we don't mix ES installs)
        track_name = self._config.opts("system", "track")
        challenge_name = self._config.opts("benchmarks", "challenge")
        race_paths = paths.Paths(self._config)
        self._config.add(config.Scope.challenge, "system", "challenge.root.dir",
                         race_paths.challenge_root(track_name, challenge_name))
        self._config.add(config.Scope.challenge, "system", "challenge.log.dir",
                                race_paths.challenge_logs(track_name, challenge_name))


    # This is the one-time setup the mechanic performs (once for all benchmarks run)
    def prepare_candidate(self):
        console.println("Preparing for race (might take a few moments) ...")
        self._supplier.fetch()
        self._builder.build()

    def find_candidate(self):
        self._builder.add_binary_to_config()

    def start_metrics(self, track, challenge, car):
        invocation = self._config.opts("meta", "time.start")
        self._metrics_store = metrics.metrics_store(self._config)
        self._metrics_store.open(invocation, track, challenge, car, create=True)

    def start_engine(self, car, client, http_port):
        self._provisioner.prepare(car, http_port)
        return self._launcher.start(car, client, self._metrics_store)

    def start_engine_external(self, client):
        external_launcher = launcher.ExternalLauncher(self._config)
        return external_launcher.start(client, self._metrics_store)

    def stop_engine(self, cluster):
        self._launcher.stop(cluster)

    def stop_metrics(self):
        self._metrics_store.close()

    def revise_candidate(self):
        self._provisioner.cleanup()

