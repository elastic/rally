import logging

import rally.mechanic.supplier as supplier
import rally.mechanic.builder as builder
import rally.mechanic.provisioner as provisioner
import rally.mechanic.launcher as launcher
import rally.metrics


class Mechanic:
  """
  Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
  running the benchmark).
  """

  def __init__(self, config):
    self._config = config
    logger = logging.getLogger("rally.mechanic")
    self._supplier = supplier.Supplier(config, logger, supplier.GitRepository(config))
    self._builder = builder.Builder(config, logger)
    self._provisioner = provisioner.Provisioner(config, logger)
    self._launcher = launcher.Launcher(config, logger)
    self._metrics_store = None

  # This is the one-time setup the mechanic performs (once for all benchmarks run)
  def prepare_candidate(self):
    print("Preparing for race (might take a few moments) ...")
    self._supplier.fetch()
    self._builder.build()

  def start_engine(self, track, setup):
    self._provisioner.prepare(setup)
    invocation = self._config.opts("meta", "time.start")
    self._metrics_store = rally.metrics.EsMetricsStore(self._config)
    self._metrics_store.open(invocation, track, setup.name, create=True)
    return self._launcher.start(track, setup, self._metrics_store)

  def stop_engine(self, cluster):
    self._launcher.stop(cluster)

  def revise_candidate(self):
    self._provisioner.cleanup()
    self._metrics_store.close()
