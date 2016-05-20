from esrally import metrics
from esrally.mechanic import builder, supplier, provisioner, launcher


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
        self._launcher = launcher.InProcessLauncher(cfg)
        self._metrics_store = None

    # This is the one-time setup the mechanic performs (once for all benchmarks run)
    def prepare_candidate(self):
        print("Preparing for race (might take a few moments) ...")
        self._supplier.fetch()
        self._builder.build()

    def find_candidate(self):
        self._builder.add_binary_to_config()

    def start_metrics(self, track, challenge, car):
        invocation = self._config.opts("meta", "time.start")
        self._metrics_store = metrics.metrics_store(self._config)
        self._metrics_store.open(invocation, track.name, challenge.name, car.name, create=True)

    def start_engine(self, track, challenge, car):
        self._provisioner.prepare(car)
        return self._launcher.start(track, challenge, car, self._metrics_store)

    def start_engine_external(self, track, challenge, car):
        external_launcher = launcher.ExternalLauncher(self._config)
        return external_launcher.start(track, challenge, self._metrics_store)

    def stop_engine(self, cluster):
        self._launcher.stop(cluster)

    def stop_metrics(self):
        self._metrics_store.close()

    def revise_candidate(self):
        self._provisioner.cleanup()

