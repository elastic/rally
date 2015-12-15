import os
import time
import threading
import subprocess
import signal

import rally.mechanic.gear
import rally.cluster
import rally.telemetry


class Launcher:
  """
  Launcher is responsible for starting and stopping the benchmark candidate.

  Currently, only local launching is supported.
  """

  def __init__(self, config, logger):
    self._config = config
    self._logger = logger
    self._servers = []

  def start(self, setup):
    if self._servers:
      self._logger.warn("There are still referenced servers on startup. Did the previous shutdown succeed?")
    num_nodes = setup.candidate_settings.nodes
    return rally.cluster.Cluster([self._start_node(node, setup) for node in range(num_nodes)])

  def _start_node(self, node, setup):
    install_dir = self._config.opts("provisioning", "local.binary.path")
    server_log_dir = "%s/server" % self._config.opts("system", "log.dir")

    node_name = self._node_name(node)
    processor_count = setup.candidate_settings.processors

    os.chdir(install_dir)
    startup_event = threading.Event()
    env = {}
    env.update(os.environ)

    # For now we keep telemetry quite constrained to the launcher. It is expected that we move it elsewhere later but keep it simple now
    telemetry = rally.telemetry.Telemetry(self._config)
    # we just blindly trust telemetry here...
    for k, v in telemetry.install(setup).items():
      self._set_env(env, k, v)

    self._set_env(env, 'ES_HEAP_SIZE', setup.candidate_settings.heap)
    self._set_env(env, 'ES_JAVA_OPTS', setup.candidate_settings.java_opts)
    self._set_env(env, 'ES_GC_OPTS', setup.candidate_settings.gc_opts)

    java_home = rally.mechanic.gear.Gear(self._config).capability(rally.mechanic.gear.Capability.java)
    # Unix specific!:
    self._set_env(env, 'PATH', '%s/bin' % java_home, separator=':')
    # Don't merge here!
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
    self._logger.info('Started node=%s with pid=%s' % (node_name, server.pid))

    return server

  def _set_env(self, env, k, v, separator=' '):
    if v is not None:
      if k not in env:
        env[k] = v
      else:  # merge
        env[k] = v + separator + env[k]


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
        self._logger.info('%s: started' % node_name)

  def stop(self, cluster):
    self._logger.info('Shutting down ES cluster')

    # Ask all nodes to shutdown:
    t0 = time.time()
    for idx, server in enumerate(cluster.servers):
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
    self._servers = []
