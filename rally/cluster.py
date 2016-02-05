import socket
import elasticsearch
import logging

from rally import exceptions, time


logger = logging.getLogger("rally.cluster")


class Server:
    def __init__(self, process, telemetry):
        self.process = process
        self.telemetry = telemetry

    def on_benchmark_start(self):
        self.telemetry.on_benchmark_start()

    def on_benchmark_stop(self):
        self.telemetry.on_benchmark_stop()


class Cluster:
    EXPECTED_CLUSTER_STATUS = "green"
    """
    Cluster exposes APIs of the running benchmark candidate.
    """

    def __init__(self, servers, metrics_store, clock=time.Clock):
        self.client = elasticsearch.Elasticsearch(timeout=60, request_timeout=60)
        self.servers = servers
        self.metrics_store = metrics_store
        self.clock = clock

    def wait_for_status_green(self):
        logger.info("\nWait for %s cluster..." % Cluster.EXPECTED_CLUSTER_STATUS)
        stop_watch = self.clock.stop_watch()
        stop_watch.start()
        reached_cluster_status, relocating_shards = self._do_wait(Cluster.EXPECTED_CLUSTER_STATUS)
        stop_watch.stop()
        logger.info("Cluster reached status [%s] within [%.1f] sec." % (reached_cluster_status, stop_watch.total_time()))
        logger.info("Cluster health: %s" % str(self.client.cluster.health()))
        logger.info("SHARDS:\n%s" % self.client.cat.shards(v=True))

    def _do_wait(self, expected_cluster_status):
        reached_cluster_status = None
        for attempt in range(10):
            try:
                result = self.client.cluster.health(wait_for_status=expected_cluster_status, wait_for_relocating_shards=0, timeout="3s")
            except (socket.timeout, elasticsearch.exceptions.ConnectionError, elasticsearch.exceptions.TransportError):
                pass
            else:
                reached_cluster_status = result["status"]
                relocating_shards = result["relocating_shards"]
                logger.info("GOT: %s" % str(result))
                logger.info("ALLOC:\n%s" % self.client.cat.allocation(v=True))
                logger.info("RECOVERY:\n%s" % self.client.cat.recovery(v=True))
                logger.info("SHARDS:\n%s" % self.client.cat.shards(v=True))
                if reached_cluster_status == expected_cluster_status and relocating_shards == 0:
                    return reached_cluster_status, relocating_shards
                else:
                    time.sleep(0.5)
        msg = "Cluster did not reach status [%s]. Last reached status: [%s]" % (expected_cluster_status, reached_cluster_status)
        logger.error(msg)
        raise exceptions.LaunchError(msg)

    def on_benchmark_start(self):
        for server in self.servers:
            server.on_benchmark_start()

    def on_benchmark_stop(self):
        for server in self.servers:
            server.on_benchmark_stop()
