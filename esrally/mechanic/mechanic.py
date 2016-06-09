import logging
import json

from esrally import metrics, track
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
        self._metrics_store.open(invocation, str(track), str(challenge), str(car), create=True)

    def start_engine(self, car):
        self._provisioner.prepare(car)
        return self._launcher.start(car, self._metrics_store)

    def start_engine_external(self, car):
        external_launcher = launcher.ExternalLauncher(self._config)
        return external_launcher.start(self._metrics_store)

    def setup_index(self, cluster, t, challenge):
        if track.BenchmarkPhase.index in challenge.benchmark:
            index_settings = challenge.benchmark[track.BenchmarkPhase.index].index_settings
            for index in t.indices:
                if cluster.client.indices.exists(index=index.name):
                    logger.warn("Index [%s] already exists. Deleting it." % index.name)
                    cluster.client.indices.delete(index=index.name)
                logger.info("Creating index [%s]" % index.name)
                cluster.client.indices.create(index=index.name, body=index_settings)
                for type in index.types:
                    mappings = open(type.mapping_file).read()
                    logger.debug("create mapping for type [%s] in index [%s]" % (type.name, index.name))
                    logger.debug(mappings)
                    cluster.client.indices.put_mapping(index=index.name,
                                                       doc_type=type.name,
                                                       body=json.loads(mappings))
        cluster.wait_for_status_green()

    def stop_engine(self, cluster):
        self._launcher.stop(cluster)

    def stop_metrics(self):
        self._metrics_store.close()

    def revise_candidate(self):
        self._provisioner.cleanup()

