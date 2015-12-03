import socket
import os
import glob
import shutil
import rally.config as cfg

import rally.utils.io


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
    # Clean any old configs first
    if os.path.exists(install_dir):
      shutil.rmtree(install_dir)
    rally.utils.io.ensure_dir(install_dir)
    self._logger.info("Unzipping %s to %s" % (binary, install_dir))
    rally.utils.io.unzip(binary, install_dir)
    binary_path = glob.glob("%s/elasticsearch*" % install_dir)[0]
    # config may be different for each track so we have to reinitialize every time, hence track scope
    self._config.add(cfg.Scope.trackScope, "provisioning", "local.binary.path", binary_path)

  # TODO: needs also to know about the benchmark scenario (e.g. for proper runtime configuration of Elasticsearch)
  # TODO: Do we need to be somehow aware about changing config settings over time, i.e. when options become deprecated / removed that have
  # been used before? -> Check with Mike
  def _configure(self):
    self._configure_logging()
    self._configure_cluster()

  def _configure_logging(self):
    # TODO dm: The larger idea behind this seems to be that we want sometimes to modify the (log) configuration. -> Check with Mike
    # So we see IW infoStream messages:
    # s = open('config/logging.yml').read()
    # s = s.replace('es.logger.level: INFO', 'es.logger.level: %s' % logLevel)
    # if verboseIW:
    #  s = open('config/logging.yml').read()
    #  s = s.replace('additivity:', '  index.engine.lucene.iw: TRACE\n\nadditivity:')
    # open('config/logging.yml', 'w').write(s)
    pass

  def _configure_cluster(self):
    binary_path = self._config.opts("provisioning", "local.binary.path")
    additional_config = self._config.opts("provisioning", "es.config", mandatory=False)
    data_paths = self._data_paths()
    self._config.add(cfg.Scope.trackScope, "provisioning", "local.data.paths", data_paths)
    # TODO dm: We should probably have the cluster log also to our common log directory, otherwise the logs are gone as the install dir is constantly wiped
    s = open(binary_path + "/config/elasticsearch.yml", 'r').read()
    # TODO dm: Maybe the cluster name should change to something more generic than 'nightlybench'. Also consider multi-node setups
    s += '\ncluster.name: %s\n' % 'nightlybench.%s' % socket.gethostname()
    s += '\nindex.optimize_auto_generated_id: false\n'
    s += '\npath.data: %s' % ', '.join(data_paths)
    if additional_config:
      s += '\n%s' % additional_config
    s = open(binary_path + "/config/elasticsearch.yml", 'w').write(s)

  def _data_paths(self):
    data_paths = None
    binary_path = self._config.opts("provisioning", "local.binary.path")
    # From the original code
    # TODO dm: Check with Mike
    # data_paths = ['%s/data/%s' % (install_dir)]
    # data_paths.extend('/ssd%d' % x for x in range(1, dataPathCount))
    # self._logger.info('Using data paths: %s' % data_paths)
    if data_paths is None:
      return ['%s/data' % binary_path]
    else:
      return data_paths

  def _install_dir(self):
    return self._config.opts("provisioning", "local.install.dir")
