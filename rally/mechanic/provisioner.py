import socket
import os
import glob
import shutil
import config as cfg

import utils.io

# Prepares the runtime environment for execution
# Supported modes:
# local
# ec2 (not yet - to be implemented)
#
# Input: binary distribution files
# Output: Distribution set up for benchmarking (binary in place + config is done)
class Provisioner:
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger

  def prepare(self):
    self._install_binary()
    self._configure()

  def _install_binary(self):
    binary = self._config.opts("builder", "candidate.bin.path")
    install_dir = self._install_dir()
    self._logger.info("Preparing candidate locally in %s." % install_dir)
    if not self._dry_run():
      # Clean any old configs first
      if os.path.exists(install_dir):
        shutil.rmtree(install_dir)
      utils.io.ensure_dir(install_dir)
      self._logger.info("Unzipping %s to %s" % (binary, install_dir))
      utils.io.unzip(binary, install_dir)
      binary_path = glob.glob("%s/elasticsearch*" % install_dir)[0]
      # config may be different for each track so we have to reinitialize every time, hence track scope
      self._config.add(cfg.Scope.trackScope, "provisioning", "local.binary.path", binary_path)

  # TODO: needs also to know about the benchmark scenario (e.g. for proper runtime configuration of Elasticsearch)
  # TODO: Do we need to be somehow aware about changing config settings over time, i.e. when options become deprecated / removed that have
  # been used before.
  def _configure(self):
    dataPaths = None
    # From the original code
    # TODO dm: Check with Mike
    #dataPaths = ['%s/data/%s' % (install_dir)]
    #dataPaths.extend('/ssd%d' % x for x in range(1, dataPathCount))
    #self._logger.info('Using data paths: %s' % dataPaths)

    binary_path = self._config.opts("provisioning", "local.binary.path")

    # So we see IW infoStream messages:
    #s = open('config/logging.yml').read()
    #s = s.replace('es.logger.level: INFO', 'es.logger.level: %s' % logLevel)
    #if verboseIW:
    #  s = open('config/logging.yml').read()
    #  s = s.replace('additivity:', '  index.engine.lucene.iw: TRACE\n\nadditivity:')
    #open('config/logging.yml', 'w').write(s)

    additional_config = self._config.opts("provisioning", "es.config", mandatory=False)

    s = open(binary_path + "/config/elasticsearch.yml", 'r').read()
    # TODO dm: Maybe the cluster name should change to something more generic than 'nightlybench'. Also consider multi-node setups
    s += '\ncluster.name: %s\n' % 'nightlybench.%s' % socket.gethostname()
    s += '\nindex.optimize_auto_generated_id: false\n'
    if dataPaths is None:
      dataPaths = ['%s/data' % binary_path]

    self._config.add(cfg.Scope.trackScope, "provisioning", "local.data.paths", dataPaths)
    s += '\npath.data: %s' % ', '.join(dataPaths)
    if additional_config:
      s += '\n%s' % additional_config
    s = open(binary_path + "/config/elasticsearch.yml", 'w').write(s)

  def _install_dir(self):
    return self._config.opts("provisioning", "local.install.dir")

  def _dry_run(self):
    return self._config.opts("system", "dryrun")
