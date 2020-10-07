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

import unittest.mock as mock
from unittest import TestCase

from esrally import config, exceptions
from esrally.mechanic import mechanic


class HostHandlingTests(TestCase):
    @mock.patch("esrally.utils.net.resolve")
    def test_converts_valid_hosts(self, resolver):
        resolver.side_effect = ["127.0.0.1", "10.16.23.5", "11.22.33.44"]

        hosts = [
            {"host": "127.0.0.1", "port": 9200},
            # also applies default port if none given
            {"host": "10.16.23.5"},
            {"host": "site.example.com", "port": 9200},
        ]

        self.assertEqual([
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
        ], mechanic.to_ip_port(hosts))

    @mock.patch("esrally.utils.net.resolve")
    def test_rejects_hosts_with_unexpected_properties(self, resolver):
        resolver.side_effect = ["127.0.0.1", "10.16.23.5", "11.22.33.44"]

        hosts = [
            {"host": "127.0.0.1", "port": 9200, "ssl": True},
            {"host": "10.16.23.5", "port": 10200},
            {"host": "site.example.com", "port": 9200},
        ]

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            mechanic.to_ip_port(hosts)
        self.assertEqual("When specifying nodes to be managed by Rally you can only supply hostname:port pairs (e.g. 'localhost:9200'), "
                         "any additional options cannot be supported.", ctx.exception.args[0])

    def test_groups_nodes_by_host(self):
        ip_port = [
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
            ("11.22.33.44", 9200),
        ]

        self.assertDictEqual(
            {
                ("127.0.0.1", 9200): [0, 1, 2],
                ("10.16.23.5", 9200): [3],
                ("11.22.33.44", 9200): [4, 5],

            }, mechanic.nodes_by_host(ip_port)
        )

    def test_extract_all_node_ips(self):
        ip_port = [
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("127.0.0.1", 9200),
            ("10.16.23.5", 9200),
            ("11.22.33.44", 9200),
            ("11.22.33.44", 9200),
        ]
        self.assertSetEqual({"127.0.0.1", "10.16.23.5", "11.22.33.44"},
                            mechanic.extract_all_node_ips(ip_port))


class MechanicTests(TestCase):
    class Node:
        def __init__(self, node_name):
            self.node_name = node_name

    class TestLauncher:
        def __init__(self):
            self.started = False

        def start(self, node_configs):
            self.started = True
            return [MechanicTests.Node("rally-node-{}".format(n)) for n in range(len(node_configs))]

        def stop(self, nodes, metrics_store):
            self.started = False

    # We stub irrelevant methods for the test
    class TestMechanic(mechanic.Mechanic):
        def _current_race(self):
            return "race 17"

        def _add_results(self, current_race, node):
            pass

    @mock.patch("esrally.mechanic.provisioner.cleanup")
    def test_start_stop_nodes(self, cleanup):
        supplier = lambda: "/home/user/src/elasticsearch/es.tar.gz"
        provisioners = [mock.Mock(), mock.Mock()]
        launcher = MechanicTests.TestLauncher()
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "race.id", "17")
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        metrics_store = mock.Mock()
        m = MechanicTests.TestMechanic(cfg, metrics_store, supplier, provisioners, launcher)
        m.start_engine()
        self.assertTrue(launcher.started)
        for p in provisioners:
            self.assertTrue(p.prepare.called)

        m.stop_engine()
        self.assertFalse(launcher.started)
        self.assertEqual(cleanup.call_count, 2)
