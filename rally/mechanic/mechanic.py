from rally import metrics
from rally.mechanic import builder, supplier, provisioner, launcher


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, cfg):
        self._config = cfg
        self._supplier = supplier.Supplier(cfg, supplier.GitRepository(cfg))
        self._builder = builder.Builder(cfg)
        self._provisioner = provisioner.Provisioner(cfg)
        self._launcher = launcher.Launcher(cfg)
        self._metrics_store = None

    # This is the one-time setup the mechanic performs (once for all benchmarks run)
    def prepare_candidate(self):
        print("Preparing for race (might take a few moments) ...")
        self._supplier.fetch()
        self._builder.build()

    def start_engine(self, track, setup):
        # It's not the best place but it ensures for now that we add the binary path for the provisioner even when the build step is skipped
        self._builder.add_binary_to_config()
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
