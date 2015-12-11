import time
import socket
import elasticsearch
import logging

from enum import Enum


class ClusterStatus(Enum):
  red = 1
  yellow = 2
  green = 3


logger = logging.getLogger("rally.cluster")


class Cluster:
  """
  Cluster exposes APIs of the running benchmark candidate.
  """

  def __init__(self, servers):
    self._es = elasticsearch.Elasticsearch()
    self._servers = servers

  @property
  def servers(self):
    return self._servers

  # Just expose the client API directly (for now)
  def client(self):
    return self._es

  def wait_for_status(self, cluster_status):
    cluster_status_name = cluster_status.name
    logger.info('\nWait for %s cluster...' % cluster_status_name)
    es = self._es
    t0 = time.time()
    while True:
      try:
        result = es.cluster.health(wait_for_status=cluster_status_name, wait_for_relocating_shards=0, timeout='1s')
      except (socket.timeout, elasticsearch.exceptions.ConnectionError, elasticsearch.exceptions.TransportError):
        pass
      else:
        logger.info('GOT: %s' % str(result))
        logger.info('ALLOC:\n%s' % es.cat.allocation(v=True))
        logger.info('RECOVERY:\n%s' % es.cat.recovery(v=True))
        logger.info('SHARDS:\n%s' % es.cat.shards(v=True))
        if result['status'] == cluster_status_name and result['relocating_shards'] == 0:
          break
        else:
          time.sleep(0.5)

    logger.info('%s cluster done (%.1f sec)' % (cluster_status_name, time.time() - t0))
    logger.info('Cluster health: %s' % str(es.cluster.health()))
    logger.info('SHARDS:\n%s' % es.cat.shards(v=True))
