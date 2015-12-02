import logging

import mechanic.supplier as supplier
import mechanic.builder as builder
import mechanic.provisioner as provisioner
import mechanic.launcher as launcher
import metrics


class Mechanic:
  def __init__(self, config):
    self._config = config
    logger = logging.getLogger("rally.mechanic")
    self._supplier = supplier.Supplier(config, logger)
    self._builder = builder.Builder(config, logger)
    self._provisioner = provisioner.Provisioner(config, logger)
    #self._launcher = launcher.Launcher(config, logger, metrics.MetricsCollector(config, ""))
    #TODO dm: Remove metrics collector here (maybe)
    self._launcher = launcher.Launcher(config, logger, None)

  def setup_for_series(self):
    # When we iterate over individual benchmark configurations we must not fetch sources or rebuild
    self._supplier.fetch()
    self._builder.build()

  def setup(self):
    self._provisioner.prepare()

  def start_engine(self):
    return self._launcher.start()

  def stop_engine(self, cluster):
    self._launcher.stop(cluster)
