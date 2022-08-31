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

import os
from unittest import mock

import pytest

from esrally.utils import opts


class TestConfigHelperFunction:
    def test_csv_to_list_inline(self):
        assert opts.csv_to_list("") == []
        assert opts.csv_to_list("    a,b,c   , d") == ["a", "b", "c", "d"]
        assert opts.csv_to_list("    a-;d    ,b,c   , d") == ["a-;d", "b", "c", "d"]
        assert opts.csv_to_list("node-stats,recovery-stats") == ["node-stats", "recovery-stats"]
        # stringified inline json tests
        assert opts.csv_to_list('["node-stats", "recovery-stats"]') == ["node-stats", "recovery-stats"]
        assert opts.csv_to_list(' [     "node-stats", "recovery-stats"]') == ["node-stats", "recovery-stats"]
        assert opts.csv_to_list(' [ "node-stats", "recovery-stats"]') == ["node-stats", "recovery-stats"]
        assert opts.csv_to_list("[]") == []
        assert opts.csv_to_list(" [ ]    ") == []

    def test_csv_to_line_from_json_file(self):
        # used to mock contents of "./telemetry_params.json"
        config_data = mock.mock_open(read_data='["v1", "v2"]')

        with mock.patch("esrally.utils.opts.open", config_data):
            assert opts.csv_to_list("telemetry_params.json") == ["v1", "v2"]

    def test_csv_to_line_raises_with_non_acceptable_json_file(self):
        # used to mock contents of "./telemetry_params.json"
        config_data = mock.mock_open(read_data="{}")

        with mock.patch("esrally.utils.opts.open", config_data):
            with pytest.raises(ValueError, match=r"csv args only support arrays in json but you supplied"):
                opts.csv_to_list("telemetry_params.json")

    def test_kv_to_map(self):
        assert opts.kv_to_map([]) == {}
        # explicit treatment as string
        assert opts.kv_to_map(["k:'3'"]) == {"k": "3"}
        assert opts.kv_to_map(["k:3"]) == {"k": 3}
        # implicit treatment as string
        assert opts.kv_to_map(["k:v"]) == {"k": "v"}
        assert opts.kv_to_map(["k:'v'", "size:4", "empty:false", "temperature:0.5", "timeout:None"]) == {
            "k": "v",
            "size": 4,
            "empty": False,
            "temperature": 0.5,
            "timeout": None,
        }
        # When passing 'None' with quotes, treat it as string
        assert opts.kv_to_map(["k:'None'"]) == {"k": "None"}
        # When passing None without quotes, treat it as pythonic None
        assert opts.kv_to_map(["k:None"]) == {"k": None}
        assert opts.kv_to_map(["k:none"]) == {"k": None}
        assert opts.kv_to_map(["k:NONE"]) == {"k": None}

    def test_to_dict_with_inline_json(self):
        assert opts.to_dict('{"default": ["127.0.0.1:9200","10.17.0.5:19200"]}') == {"default": ["127.0.0.1:9200", "10.17.0.5:19200"]}

        # valid json may also start with an array
        assert opts.to_dict('["node-stats", "recovery-stats"]') == ["node-stats", "recovery-stats"]

        assert opts.to_dict("[]") == []

    def test_to_dict_with_inline_kv(self):
        assert opts.to_dict("k1:v1,k2:v2") == {"k1": "v1", "k2": "v2"}


class TestGenericHelperFunction:
    def test_list_as_bulleted_list(self):
        src_list = ["param-1", "param-2", "a_longer-parameter"]

        assert opts.bulleted_list_of(src_list) == ["- param-1", "- param-2", "- a_longer-parameter"]

    def test_list_as_double_quoted_list(self):
        src_list = ["oneitem", "_another-weird_item", "param-3"]

        assert opts.double_quoted_list_of(src_list) == ['"oneitem"', '"_another-weird_item"', '"param-3"']

    def test_make_list_of_close_matches(self):
        word_list = [
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "target_throughput",
            "bulk_size",
            "number_of-shards",
            "number_of_replicas",
            "index_refresh_interval",
        ]

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
            "translog_sync",
        ]

        assert opts.make_list_of_close_matches(word_list, available_word_list) == [
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "target_throughput",
            "bulk_size",
            # number_of-shards had a typo
            "number_of_shards",
            "number_of_replicas",
            "index_refresh_interval",
        ]

    def test_make_list_of_close_matches_returns_with_empty_word_list(self):
        assert opts.make_list_of_close_matches([], ["number_of_shards"]) == []

    def test_make_list_of_close_matches_returns_empty_list_with_no_close_matches(self):
        assert opts.make_list_of_close_matches(["number_of_shards", "number_of-replicas"], []) == []


class TestTargetHosts:
    def test_empty_arg_parses_as_empty_list(self):
        assert opts.TargetHosts("").default == []
        assert opts.TargetHosts("").all_hosts == {"default": []}

    def test_csv_hosts_parses(self):
        target_hosts = "127.0.0.1:9200,10.17.0.5:19200"

        assert opts.TargetHosts(target_hosts).all_hosts == {
            "default": [{"host": "127.0.0.1", "port": 9200}, {"host": "10.17.0.5", "port": 19200}]
        }

        assert opts.TargetHosts(target_hosts).default == [{"host": "127.0.0.1", "port": 9200}, {"host": "10.17.0.5", "port": 19200}]

        assert opts.TargetHosts(target_hosts).default == [{"host": "127.0.0.1", "port": 9200}, {"host": "10.17.0.5", "port": 19200}]

    def test_jsonstring_parses_as_dict_of_clusters(self):
        target_hosts = (
            '{"default": ["127.0.0.1:9200","10.17.0.5:19200"],'
            ' "remote_1": ["88.33.22.15:19200"],'
            ' "remote_2": ["10.18.0.6:19200","10.18.0.7:19201"]}'
        )

        assert opts.TargetHosts(target_hosts).all_hosts == {
            "default": ["127.0.0.1:9200", "10.17.0.5:19200"],
            "remote_1": ["88.33.22.15:19200"],
            "remote_2": ["10.18.0.6:19200", "10.18.0.7:19201"],
        }

    def test_json_file_parameter_parses(self):
        assert opts.TargetHosts(os.path.join(os.path.dirname(__file__), "resources", "target_hosts_1.json")).all_hosts == {
            "default": ["127.0.0.1:9200", "10.127.0.3:19200"]
        }

        assert opts.TargetHosts(os.path.join(os.path.dirname(__file__), "resources", "target_hosts_2.json")).all_hosts == {
            "default": [{"host": "127.0.0.1", "port": 9200}, {"host": "127.0.0.1", "port": 19200}],
            "remote_1": [{"host": "10.127.0.3", "port": 9200}, {"host": "10.127.0.8", "port": 9201}],
            "remote_2": [{"host": "88.33.27.15", "port": 39200}],
        }


class TestClientOptions:
    def test_csv_client_options_parses(self):
        # "timeout": 60 should automatically get added to each configuration unless overridden
        client_options_string = "use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem'"

        assert opts.ClientOptions(client_options_string).default == {
            "use_ssl": True,
            "verify_certs": True,
            "ca_certs": "/path/to/cacert.pem",
            "timeout": 60,
        }

        assert opts.ClientOptions(client_options_string).default == {
            "use_ssl": True,
            "verify_certs": True,
            "ca_certs": "/path/to/cacert.pem",
            "timeout": 60,
        }

        assert opts.ClientOptions(client_options_string).all_client_options == {
            "default": {
                "use_ssl": True,
                "verify_certs": True,
                "ca_certs": "/path/to/cacert.pem",
                "timeout": 60,
            }
        }

    def test_jsonstring_client_options_parses(self):
        client_options_string = (
            '{"default": {"timeout": 60},'
            '"remote_1": {"use_ssl":true,"verify_certs":true,"basic_auth_user": "elastic", "basic_auth_password": "changeme"},'
            '"remote_2": {"use_ssl":true,"verify_certs":true,"ca_certs":"/path/to/cacert.pem"}}'
        )

        assert opts.ClientOptions(client_options_string).default == {"timeout": 60}

        assert opts.ClientOptions(client_options_string).default == {"timeout": 60}

        assert opts.ClientOptions(client_options_string).all_client_options == {
            "default": {"timeout": 60},
            "remote_1": {"use_ssl": True, "verify_certs": True, "basic_auth_user": "elastic", "basic_auth_password": "changeme"},
            "remote_2": {"use_ssl": True, "verify_certs": True, "ca_certs": "/path/to/cacert.pem"},
        }

    def test_json_file_parameter_parses(self):
        assert opts.ClientOptions(os.path.join(os.path.dirname(__file__), "resources", "client_options_1.json")).all_client_options == {
            "default": {"timeout": 60},
            "remote_1": {"use_ssl": True, "verify_certs": True, "basic_auth_user": "elastic", "basic_auth_password": "changeme"},
            "remote_2": {"use_ssl": True, "verify_certs": True, "ca_certs": "/path/to/cacert.pem"},
        }

        assert opts.ClientOptions(os.path.join(os.path.dirname(__file__), "resources", "client_options_2.json")).all_client_options == {
            "default": {"timeout": 60}
        }

    def test_no_client_option_parses_to_default(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = None

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).default == {"timeout": 60}

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).all_client_options == {"default": {"timeout": 60}}

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).default == {"timeout": 60}

    def test_no_client_option_parses_to_default_with_multicluster(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = opts.TargetHosts('{"default": ["127.0.0.1:9200,10.17.0.5:19200"], "remote": ["88.33.22.15:19200"]}')

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).default == {"timeout": 60}

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).all_client_options == {
            "default": {"timeout": 60},
            "remote": {"timeout": 60},
        }

        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).default == {"timeout": 60}

    def test_default_client_ops_with_max_connections(self):
        client_options_string = opts.ClientOptions.DEFAULT_CLIENT_OPTIONS
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(256) == {
            "default": {"timeout": 60, "max_connections": 256},
            "remote": {"timeout": 60, "max_connections": 256},
        }

    def test_sets_minimum_max_connections(self):
        client_options_string = '{"default": {"timeout":60,"max_connections":5}, "remote": {"timeout":60}}'
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(5) == {
            "default": {"timeout": 60, "max_connections": 256},
            "remote": {"timeout": 60, "max_connections": 256},
        }

    def test_keeps_greater_than_minimum_max_connections(self):
        client_options_string = '{"default": {"timeout":60,"max_connections":512}, "remote": {"timeout":60,"max_connections":1024}}'
        target_hosts = opts.TargetHosts('{"default": ["10.17.0.5:9200"], "remote": ["88.33.22.15:9200"]}')
        assert opts.ClientOptions(client_options_string, target_hosts=target_hosts).with_max_connections(32) == {
            "default": {"timeout": 60, "max_connections": 512},
            "remote": {"timeout": 60, "max_connections": 1024},
        }
