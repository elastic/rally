import logging

logger = logging.getLogger("rally.cluster")


class Node:
    """
    Represents an Elasticsearch cluster node.
    """

    def __init__(self, process, host_name, node_name, telemetry):
        """
        Creates a new node.

        :param process: Process handle for this node.
        :param host_name: The name of the host where this node is running.
        :param node_name: The name of this node.
        :param telemetry: The attached telemetry.
        """
        self.process = process
        self.host_name = host_name
        self.node_name = node_name
        self.telemetry = telemetry

    def on_benchmark_start(self):
        """
        Callback method when a benchmark is about to start.
        """
        self.telemetry.on_benchmark_start()

    def on_benchmark_stop(self):
        """
        Callback method when a benchmark is about to stop.
        """
        self.telemetry.on_benchmark_stop()


class Cluster:
    """
    Cluster exposes APIs of the running benchmark candidate.
    """
    def __init__(self, nodes, telemetry):
        """
        Creates a new Elasticsearch cluster.

        :param nodes: The nodes of which this cluster consists of. Mandatory.
        :param telemetry Telemetry attached to this cluster. Mandatory.
        """
        self.nodes = nodes
        self.telemetry = telemetry

    def on_benchmark_start(self):
        """
        Callback method when a benchmark is about to start.
        """
        self.telemetry.on_benchmark_start()
        for node in self.nodes:
            node.on_benchmark_start()

    def on_benchmark_stop(self):
        """
        Callback method when a benchmark is about to stop.
        """
        self.telemetry.on_benchmark_stop()
        for node in self.nodes:
            node.on_benchmark_stop()
