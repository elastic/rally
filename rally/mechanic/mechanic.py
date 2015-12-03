import logging

import rally.mechanic.supplier as supplier
import rally.mechanic.builder as builder
import rally.mechanic.provisioner as provisioner
import rally.mechanic.launcher as launcher


class Mechanic:
  def __init__(self, config):
    self._config = config
    logger = logging.getLogger("rally.mechanic")
    self._supplier = supplier.Supplier(config, logger)
    self._builder = builder.Builder(config, logger)
    self._provisioner = provisioner.Provisioner(config, logger)
    self._launcher = launcher.Launcher(config, logger)

  # This is the one-time setup the mechanic performs (once for all benchmarks run)
  def pre_setup(self):
    self._supplier.fetch()
    self._builder.build()

  def setup_for_track(self):
    self._provisioner.prepare()

  def start_engine(self):
    return self._launcher.start()

  def stop_engine(self, cluster):
    self._launcher.stop(cluster)
