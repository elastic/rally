import logging

import rally.mechanic.supplier as supplier
import rally.mechanic.builder as builder
import rally.mechanic.provisioner as provisioner
import rally.mechanic.launcher as launcher


class Mechanic:
  """
  Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
  running the benchmark).
  """
  def __init__(self, config):
    self._config = config
    logger = logging.getLogger("rally.mechanic")
    self._supplier = supplier.Supplier(config, logger)
    self._builder = builder.Builder(config, logger)
    self._provisioner = provisioner.Provisioner(config, logger)
    self._launcher = launcher.Launcher(config, logger)

  # This is the one-time setup the mechanic performs (once for all benchmarks run)
  def prepare_candidate(self):
    self._supplier.fetch()
    self._builder.build()

  def start_engine(self):
    self._provisioner.prepare()
    return self._launcher.start()

  def stop_engine(self, cluster):
    self._launcher.stop(cluster)
    self._provisioner.cleanup()
