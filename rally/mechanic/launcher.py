import os
import time
import threading
import subprocess
import signal

import mechanic.gear as gear
import metrics
import cluster as c
import config


# Currently only local launch is supported
class Launcher:
  def __init__(self, config, logger, metrics):
    self._config = config
    self._logger = logger
    self._servers = []
    self._metrics = metrics

  def start(self):
    num_nodes = self._config.opts("provisioning", "es.nodes")
    # TODO dm: Should we warn here if _servers is not empty? Might have missed shutdown?
    return c.Cluster([self._start_node(node) for node in range(num_nodes)])

  def _start_node(self, node):
    node_name = self._node_name(node)
    install_dir = self._config.opts("provisioning", "local.binary.path")
    os.chdir(install_dir)
    startup_event = threading.Event()
    env = {}
    env.update(os.environ)

    heap = self._config.opts("provisioning", "es.heap", mandatory=False)
    processor_count = self._config.opts("provisioning", "es.processors", mandatory=False)

    if heap is not None:
      env['ES_HEAP_SIZE'] = heap
      # print('ES_HEAP_SIZE=%s' % heap)
    # TODO dm: Reenable
    # if verboseGC:
    #   #env['ES_JAVA_OPTS'] = '-verbose:gc -agentlib:hprof=heap=sites,depth=30'
    #  env['ES_JAVA_OPTS'] = '-verbose:gc'

    # env['ES_GC_OPTS'] = '-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC'
    java_home = gear.Gear(self._config).capability(gear.Capability.java)
    # Unix specific!:
    env['PATH'] = '%s/bin' % java_home + ':' + env['PATH']
    env['JAVA_HOME'] = java_home
    # print('ENV: %s' % str(env))
    cmd = ['bin/elasticsearch', '-Des.node.name=%s' % node_name]
    if processor_count is not None:
      cmd.append('-Des.processors=%s' % processor_count)
    # print('ES launch: %s' % str(cmd))
    server = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env)
    t = threading.Thread(target=self._read_output, args=(node_name, server, startup_event))
    t.setDaemon(True)
    t.start()
    startup_event.wait()
    # print('TEST: started node=%s on pid=%s' % (node_name, server.pid))

    return server

  def _node_name(self, node):
    return "node%d" % node

  def _read_output(self, nodeName, server, startupEvent):
    """
    Reads the output from the ES (node) subprocess.
    """
    while True:
      l = server.stdout.readline().decode('utf-8')
      if len(l) == 0:
        break
      l = l.rstrip()

      if l.find('Initialization Failed') != -1:
        startupEvent.set()

      print('%s: %s' % (nodeName, l.replace('\n', '\n%s: ' % nodeName)))
      if l.endswith('started') and not startupEvent.isSet():
        startupEvent.set()
        print('%s: **started**' % nodeName)


  def stop(self, cluster):
    # print('shutdown server nodes=%s' % nodes)

    # Ask all nodes to shutdown:
    t0 = time.time()
    for idx, server in enumerate(cluster.servers()):
      node = self._node_name(idx)
      os.kill(server.pid, signal.SIGINT)

      try:
        server.wait(10.0)
        print('done shutdown server (%.1f sec)' % (time.time() - t0))
      except subprocess.TimeoutExpired:
        # kill -9
        print('\n WARNING: server %s did not shut down itself after 10 seconds; now kill -QUIT server, to see threads:' % node)
        try:
          os.kill(server.pid, signal.SIGQUIT)
        except OSError:
          print('  no such process')
          return

        try:
          server.wait(120.0)
          print('TEST: done shutdown server (%.1f sec)' % (time.time() - t0))
          return
        except subprocess.TimeoutExpired:
          pass

        print('TEST: kill -KILL server')
        try:
          server.kill()
        except ProcessLookupError:
          print('TEST: no such process')
