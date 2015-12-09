import os
import time
import threading
import subprocess
import signal

import rally.mechanic.gear as gear
import rally.cluster as c


class Launcher:
  """
  Launcher is responsible for starting and stopping the benchmark candidate.

  Currently, only local launching is supported.
  """
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger
    self._servers = []

  def start(self):
    num_nodes = self._config.opts("provisioning", "es.nodes")
    # TODO dm: Should we warn here if _servers is not empty? Might have missed shutdown?
    return c.Cluster([self._start_node(node) for node in range(num_nodes)])

  def _start_node(self, node):
    node_name = self._node_name(node)
    install_dir = self._config.opts("provisioning", "local.binary.path")
    heap = self._config.opts("provisioning", "es.heap", mandatory=False)
    processor_count = self._config.opts("provisioning", "es.processors", mandatory=False)
    server_log_dir = "%s/server" % self._config.opts("system", "log.dir")

    os.chdir(install_dir)
    startup_event = threading.Event()
    env = {}
    env.update(os.environ)

    if heap is not None:
      env['ES_HEAP_SIZE'] = heap
      # self._logger.info('ES_HEAP_SIZE=%s' % heap)
    # TODO dm: Reenable
    # if verboseGC:
    #   #env['ES_JAVA_OPTS'] = '-verbose:gc -agentlib:hprof=heap=sites,depth=30'
    #  env['ES_JAVA_OPTS'] = '-verbose:gc'

    # env['ES_GC_OPTS'] = '-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC'
    java_home = gear.Gear(self._config).capability(gear.Capability.java)
    # Unix specific!:
    env['PATH'] = '%s/bin' % java_home + ':' + env['PATH']
    env['JAVA_HOME'] = java_home
    self._logger.debug('ENV: %s' % str(env))
    cmd = ['bin/elasticsearch', '-Des.node.name=%s' % node_name]
    if processor_count is not None and processor_count > 1:
      cmd.append('-Des.processors=%s' % processor_count)
    cmd.append('-Dpath.logs=%s' % server_log_dir)
    self._logger.info('ES launch: %s' % str(cmd))
    server = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env)
    t = threading.Thread(target=self._read_output, args=(node_name, server, startup_event))
    t.setDaemon(True)
    t.start()
    startup_event.wait()
    # self._logger.info('Started node=%s on pid=%s' % (node_name, server.pid))

    return server

  def _node_name(self, node):
    return "node%d" % node

  def _read_output(self, node_name, server, startup_event):
    """
    Reads the output from the ES (node) subprocess.
    """
    while True:
      l = server.stdout.readline().decode('utf-8')
      if len(l) == 0:
        break
      l = l.rstrip()

      if l.find('Initialization Failed') != -1:
        startup_event.set()

      self._logger.info('%s: %s' % (node_name, l.replace('\n', '\n%s (stdout): ' % node_name)))
      if l.endswith('started') and not startup_event.isSet():
        startup_event.set()
        self._logger.info('%s: **started**' % node_name)

  def stop(self, cluster):
    self._logger.info('Shutting down ES cluster')

    # Ask all nodes to shutdown:
    t0 = time.time()
    for idx, server in enumerate(cluster.servers()):
      node = self._node_name(idx)
      os.kill(server.pid, signal.SIGINT)

      try:
        server.wait(10.0)
        self._logger.info('Done shutdown server (%.1f sec)' % (time.time() - t0))
      except subprocess.TimeoutExpired:
        # kill -9
        self._logger.warn('Server %s did not shut down itself after 10 seconds; now kill -QUIT server, to see threads:' % node)
        try:
          os.kill(server.pid, signal.SIGQUIT)
        except OSError:
          self._logger.warn('  no such process')
          return

        try:
          server.wait(120.0)
          self._logger.info('Done shutdown server (%.1f sec)' % (time.time() - t0))
          return
        except subprocess.TimeoutExpired:
          pass

        self._logger.info('kill -KILL server')
        try:
          server.kill()
        except ProcessLookupError:
          self._logger.warn('No such process')
