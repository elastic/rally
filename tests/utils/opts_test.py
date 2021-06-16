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

import os
from unittest import TestCase

from esrally.utils import opts


class ConfigHelperFunctionTests(TestCase):
    def test_csv_to_list(self):
        self.assertEqual([], opts.csv_to_list(""))
        self.assertEqual(["a", "b", "c", "d"], opts.csv_to_list("    a,b,c   , d"))
        self.assertEqual(["a-;d", "b", "c", "d"], opts.csv_to_list("    a-;d    ,b,c   , d"))

    def test_kv_to_map(self):
        self.assertEqual({}, opts.kv_to_map([]))
        # explicit treatment as string
        self.assertEqual({"k": "3"}, opts.kv_to_map(["k:'3'"]))
        self.assertEqual({"k": 3}, opts.kv_to_map(["k:3"]))
        # implicit treatment as string
        self.assertEqual({"k": "v"}, opts.kv_to_map(["k:v"]))
        self.assertEqual({"k": "v", "size": 4, "empty": False, "temperature": 0.5},
                         opts.kv_to_map(["k:'v'", "size:4", "empty:false", "temperature:0.5"]))


class GenericHelperFunctionTests(TestCase):
    def test_list_as_bulleted_list(self):
        src_list = ["param-1", "param-2", "a_longer-parameter"]

        self.assertEqual(
            ["- param-1", "- param-2", "- a_longer-parameter"],
            opts.bulleted_list_of(src_list)
        )

    def test_list_as_double_quoted_list(self):
        src_list = ["oneitem", "_another-weird_item", "param-3"]

        self.assertEqual(
            opts.double_quoted_list_of(src_list),
            ['"oneitem"', '"_another-weird_item"', '"param-3"']
        )

    def test_make_list_of_close_matches(self):
        word_list = [
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "target_throughput",
            "bulk_size",
            "number_of-shards",
            "number_of_replicas",
            "index_refresh_interval"]

        available_word_list = [
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "bulk_size",
            "cluster_health",
            "disk_type",
            "duration",
            "forcemerge",
            "index_alias",
            "index_refresh_interval",
            "indices_delete_pattern",
            "joiner",
            "max_rolledover_indices",
            "number_of_replicas",
            "number_of_shards",
            "ops_per_25_gb",
            "p1_bulk_indexing_clients",
            "p1_bulk_size",
            "p1_duration_secs",
            "p2_bulk_indexing_clients",
            "p2_bulk_size",
            "p2_duration_secs",
            "p2_ops",
            "p2_query1_target_interval",
            "p2_query2_target_interval",
            "p2_query3_target_interval",
            "p2_query4_target_interval",
            "phase_duration_secs",
            "pre_filter_shard_size",
            "query_iterations",
            "range",
            "rate_limit_duration_secs",
            "rate_limit_max",
            "rate_limit_step",
            "rolledover_indices_suffix_separator",
            "rollover_max_age",
            "rollover_max_size",
            "shard_sizing_iterations",
            "shard_sizing_queries",
            "source_enabled",
            "target_throughput",
            "translog_sync"]

        self.assertEqual(
            ['bulk_indexing_clients',
             'bulk_indexing_iterations',
             'target_throughput',
             'bulk_size',
             # number_of-shards had a typo
             'number_of_shards',
             'number_of_replicas',
             'index_refresh_interval'],
            opts.make_list_of_close_matches(word_list, available_word_list)
        )

    def test_make_list_of_close_matches_returns_with_empty_word_list(self):
        self.assertEqual(
            [],
            opts.make_list_of_close_matches([], ["number_of_shards"])
        )

    def test_make_list_of_close_matches_returns_empty_list_with_no_close_matches(self):
        self.assertEqual(
            [],
            opts.make_list_of_close_matches(
                ["number_of_shards", "number_of-replicas"],
                [])
        )


class TestTargetHosts(TestCase):
    def test_empty_arg_parses_as_empty_list(self):
        self.assertEqual([], opts.TargetHosts('').default)
        self.assertEqual({'default': []}, opts.TargetHosts('').all_hosts)

    def test_csv_hosts_parses(self):
        target_hosts = '127.0.0.1:9200,10.17.0.5:19200'

        self.assertEqual(
            {'default': [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}]},
            opts.TargetHosts(target_hosts).all_hosts
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            opts.TargetHosts(target_hosts).default
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            opts.TargetHosts(target_hosts).default)

    def test_jsonstring_parses_as_dict_of_clusters(self):
        target_hosts = ('{"default": ["127.0.0.1:9200","10.17.0.5:19200"],'
                        ' "remote_1": ["88.33.22.15:19200"],'
                        ' "remote_2": ["10.18.0.6:19200","10.18.0.7:19201"]}')

        self.assertEqual(
            {'default': ['127.0.0.1:9200','10.17.0.5:19200'],
             'remote_1': ['88.33.22.15:19200'],
             'remote_2': ['10.18.0.6:19200','10.18.0.7:19201']},
            opts.TargetHosts(target_hosts).all_hosts)

    def test_json_file_parameter_parses(self):
        self.assertEqual(
            {"default": ["127.0.0.1:9200","10.127.0.3:19200"] },
            opts.TargetHosts(os.path.join(os.path.dirname(__file__), "resources", "target_hosts_1.json")).all_hosts)

        self.assertEqual(
            {
              "default": [
                {"host": "127.0.0.1", "port": 9200},
                {"host": "127.0.0.1", "port": 19200}
              ],
              "remote_1":[
                {"host": "10.127.0.3", "port": 9200},
                {"host": "10.127.0.8", "port": 9201}
              ],
              "remote_2":[
                {"host": "88.33.27.15", "port": 39200}
              ]
            },
            opts.TargetHosts(os.path.join(os.path.dirname(__file__), "resources", "target_hosts_2.json")).all_hosts)


class TestClientOptions(TestCase):
    def test_csv_client_options_parses(self):
        client_options_string = "use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem'"

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            opts.ClientOptions(client_options_string).default)

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            opts.ClientOptions(client_options_string).default
        )

        self.assertEqual(
            {'default': {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'}},
            opts.ClientOptions(client_options_string).all_client_options
        )

    def test_jsonstring_client_options_parses(self):
        client_options_string = '{"default": {"timeout": 60},' \
            '"remote_1": {"use_ssl":true,"verify_certs":true,"basic_auth_user": "elastic", "basic_auth_password": "changeme"},'\
            '"remote_2": {"use_ssl":true,"verify_certs":true,"ca_certs":"/path/to/cacert.pem"}}'

        self.assertEqual(
            {'timeout': 60},
            opts.ClientOptions(client_options_string).default)

        self.assertEqual(
            {'timeout': 60},
            opts.ClientOptions(client_options_string).default)

        self.assertEqual(
            {'default': {'timeout':60},
             'remote_1': {'use_ssl': True,'verify_certs': True,'basic_auth_user':'elastic','basic_auth_password':'changeme'},
             'remote_2': {'use_ssl': True,'verify_certs': True, 'ca_certs':'/path/to/cacert.pem'}},
            opts.ClientOptions(client_options_string).all_client_options)

    def test_json_file_parameter_parses(self):
        self.assertEqual(
            {'default': {'timeout':60},
             'remote_1': {'use_ssl': True,'verify_certs': True,'basic_auth_user':'elastic','basic_auth_password':'changeme'},
             'remote_2': {'use_ssl': True,'verify_certs': True, 'ca_certs':'/path/to/cacert.pem'}},
            opts.ClientOptions(os.path.join(os.path.dirname(__file__), "resources", "client_options_1.json")).all_client_options)

        self.assertEqual(
            {'default': {'timeout':60}},
            opts.ClientOptions(os.path.join(os.path.dirname(__file__), "resources", "client_options_2.json")).all_client_options)

    def test_no_client_option_parses_to_default(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = None

        self.assertEqual(
            {"timeout": 60},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).default)

        self.assertEqual(
            {"default": {"timeout": 60}},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).all_client_options)

        self.assertEqual(
            {"timeout": 60},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).default)

    def test_no_client_option_parses_to_default_with_multicluster(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = opts.TargetHosts('{"default": ["127.0.0.1:9200,10.17.0.5:19200"], "remote": ["88.33.22.15:19200"]}')

        self.assertEqual(
            {"timeout": 60},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).default)

        self.assertEqual(
            {"default": {"timeout": 60}, "remote": {"timeout": 60}},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).all_client_options)

        self.assertEqual(
            {"timeout": 60},
            opts.ClientOptions(client_options_string,
                                 target_hosts=target_hosts).default)

    def test_default_client_ops_with_max_connections(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        self.assertEqual(
            {"default": {"timeout": 60, "max_connections": 256}, "remote": {"timeout": 60, "max_connections": 256}},
            opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(256))

    def test_sets_minimum_max_connections(self):
        client_options_string = '{"default": {"timeout":60,"max_connections":5}, "remote": {"timeout":60}}'
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        self.assertEqual(
            {"default": {"timeout": 60, "max_connections": 256}, "remote": {"timeout": 60, "max_connections": 256}},
            opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(5))

    def test_keeps_greater_than_minimum_max_connections(self):
        client_options_string = '{"default": {"timeout":60,"max_connections":512}, "remote": {"timeout":60,"max_connections":1024}}'
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        self.assertEqual(
            {"default": {"timeout": 60, "max_connections": 512}, "remote": {"timeout": 60, "max_connections": 1024}},
            opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(32))
