# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
        self.plugins = []

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

    def __repr__(self):
        return self.node_name


class Cluster:
    """
    Cluster exposes APIs of the running benchmark candidate.
    """
    def __init__(self, hosts, nodes, telemetry, preserve=False):
        """
        Creates a new Elasticsearch cluster.

        :param hosts: An array of host/port pairs.
        :param nodes: The nodes of which this cluster consists of. Mandatory.
        :param telemetry Telemetry attached to this cluster. Mandatory.
        :param preserve Whether the cluster will be kept around after the benchmark. Optional.

        """
        self.hosts = hosts
        self.nodes = nodes
        self.telemetry = telemetry
        self.distribution_version = None
        self.source_revision = None
        self.preserve = preserve

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
