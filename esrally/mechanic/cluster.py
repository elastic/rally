# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
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

    def __init__(self, pid, binary_path, host_name, node_name, telemetry):
        """
        Creates a new node.

        :param pid: PID for this node.
        :param binary_path: The local path to the binaries for this node.
        :param host_name: The name of the host where this node is running.
        :param node_name: The name of this node.
        :param telemetry: The attached telemetry.
        """
        self.pid = pid
        self.binary_path = binary_path
        self.host_name = host_name
        self.node_name = node_name
        self.telemetry = telemetry

    def __repr__(self):
        return self.node_name
