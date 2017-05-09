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
        self.ip = None
        self.telemetry = telemetry
        # populated by telemetry
        self.os = {}
        self.jvm = {}
        self.cpu = {}
        self.memory = {}
        self.fs = []

    def on_benchmark_start(self):
        """
        Callback method when a benchmark is about to start.
        """
        if self.telemetry:
            self.telemetry.on_benchmark_start()

    def on_benchmark_stop(self):
        """
        Callback method when a benchmark is about to stop.
        """
        if self.telemetry:
            self.telemetry.on_benchmark_stop()


class Cluster:
    """
    Cluster exposes APIs of the running benchmark candidate.
    """
    def __init__(self, hosts, nodes, telemetry):
        """
        Creates a new Elasticsearch cluster.

        :param hosts: An array of host/port pairs.
        :param nodes: The nodes of which this cluster consists of. Mandatory.
        :param telemetry Telemetry attached to this cluster. Mandatory.
        """
        self.hosts = hosts
        self.nodes = nodes
        self.telemetry = telemetry
        self.distribution_version = None
        self.source_revision = None

    def node(self, name):
        """
        Finds a cluster node by name.

        :param name: The node's name.
        :return: The corresponding node or ``None`` if the cluster does not contain a node with this name.
        """
        for n in self.nodes:
            if n.node_name == name:
                return n
        return None

    def has_node(self, name):
        """
        :param name: The node's name.
        :return: True iff the cluster contains such a node.
        """
        return self.node(name) is not None

    def add_node(self, host_name, node_name):
        new_node = Node(process=None, host_name=host_name, node_name=node_name, telemetry=None)
        self.nodes.append(new_node)
        return new_node

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
