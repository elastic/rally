from unittest import TestCase

import unittest.mock as mock

from esrally import exceptions
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



