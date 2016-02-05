import logging

from rally import metrics
from rally.mechanic import builder, supplier, provisioner, launcher


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, cfg):
        self._config = cfg
        logger = logging.getLogger("rally.mechanic")
        self._supplier = supplier.Supplier(cfg, logger, supplier.GitRepository(cfg))
        self._builder = builder.Builder(cfg, logger)
        self._provisioner = provisioner.Provisioner(cfg, logger)
        self._launcher = launcher.Launcher(cfg, logger)
        self._metrics_store = None

    # This is the one-time setup the mechanic performs (once for all benchmarks run)
    def prepare_candidate(self):
        print("Preparing for race (might take a few moments) ...")
        self._supplier.fetch()
        self._builder.build()

    def start_engine(self, track, setup):
        self._provisioner.prepare(setup)
        invocation = self._config.opts("meta", "time.start")
        self._metrics_store = metrics.EsMetricsStore(self._config)
        self._metrics_store.open(invocation, track.name, setup.name, create=True)
        return self._launcher.start(track, setup, self._metrics_store)

    def stop_engine(self, cluster):
        self._launcher.stop(cluster)

    def revise_candidate(self):
        self._provisioner.cleanup()
        self._metrics_store.close()
