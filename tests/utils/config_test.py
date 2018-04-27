import os

from unittest import TestCase

from esrally import exceptions
from esrally.utils import config

class ConfigHelperFunctionTests(TestCase):
    def test_csv_to_list(self):
        self.assertEqual([], config.csv_to_list(""))
        self.assertEqual(["a", "b", "c", "d"], config.csv_to_list("    a,b,c   , d"))
        self.assertEqual(["a-;d", "b", "c", "d"], config.csv_to_list("    a-;d    ,b,c   , d"))

    def test_kv_to_map(self):
        self.assertEqual({}, config.kv_to_map([]))
        self.assertEqual({"k": "v"}, config.kv_to_map(["k:'v'"]))
        self.assertEqual({"k": "v", "size": 4, "empty": False, "temperature": 0.5},
                         config.kv_to_map(["k:'v'", "size:4", "empty:false", "temperature:0.5"]))


class TestTargetHosts(TestCase):
    def test_empty_arg_parses_as_empty_list(self):
        self.assertEqual([], config.TargetHosts('')())
        self.assertEqual([], config.TargetHosts('').default)
        self.assertEqual({'default': []}, config.TargetHosts('').all_hosts)

    def test_csv_hosts_parses(self):
        target_hosts = '127.0.0.1:9200,10.17.0.5:19200'

        self.assertEqual(
            {'default': [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}]},
            config.TargetHosts(target_hosts).all_hosts
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            config.TargetHosts(target_hosts).default
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            config.TargetHosts(target_hosts)()
        )

    def test_jsonstring_parses_as_dict_of_clusters(self):
        target_hosts = '{"default": ["127.0.0.1:9200,10.17.0.5:19200"], "remote_1": ["88.33.22.15:19200"], "remote_2": ["10.18.0.6:19200,10.18.0.7:19201"]}'

        self.assertEqual(
            {'default': ['127.0.0.1:9200,10.17.0.5:19200'],
             'remote_1': ['88.33.22.15:19200'],
             'remote_2': ['10.18.0.6:19200,10.18.0.7:19201']},
            config.TargetHosts(target_hosts).all_hosts)

    def test_json_file_parameter_parses(self):
        self.assertEqual(
            {"default": ["127.0.0.1:9200","10.127.0.3:19200"] },
            config.TargetHosts(os.path.join(os.path.dirname(__file__), "resources/target_hosts_1.json")).all_hosts)

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
            config.TargetHosts(os.path.join(os.path.dirname(__file__), "resources/target_hosts_2.json")).all_hosts)


class TestClientOptions(TestCase):
    def test_csv_client_options_parses(self):
        client_options_string = "use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem'"

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            config.ClientOptions(client_options_string)()
        )

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            config.ClientOptions(client_options_string).default
        )

        self.assertEqual(
            {'default': {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'}},
            config.ClientOptions(client_options_string).all_client_options
        )


    def test_jsonstring_client_options_parses(self):
        client_options_string = '{"default": {"timeout": 60},' \
            '"remote_1": {"use_ssl":true,"verify_certs":true,"basic_auth_user": "elastic", "basic_auth_password": "changeme"},'\
            '"remote_2": {"use_ssl":true,"verify_certs":true,"ca_certs":"/path/to/cacert.pem"}}'

        self.assertEqual(
            {'timeout': 60},
            config.ClientOptions(client_options_string).default)

        self.assertEqual(
            {'timeout': 60},
            config.ClientOptions(client_options_string)())

        self.assertEqual(
            {'default': {'timeout':60},
             'remote_1': {'use_ssl': True,'verify_certs': True,'basic_auth_user':'elastic','basic_auth_password':'changeme'},
             'remote_2': {'use_ssl': True,'verify_certs': True, 'ca_certs':'/path/to/cacert.pem'}},
            config.ClientOptions(client_options_string).all_client_options)


    def test_json_file_parameter_parses(self):
        self.assertEqual(
            {'default': {'timeout':60},
             'remote_1': {'use_ssl': True,'verify_certs': True,'basic_auth_user':'elastic','basic_auth_password':'changeme'},
             'remote_2': {'use_ssl': True,'verify_certs': True, 'ca_certs':'/path/to/cacert.pem'}},
            config.TargetHosts(os.path.join(os.path.dirname(__file__), "resources/client_options_1.json")).all_hosts)

        self.assertEqual(
            {'default': {'timeout':60}},
            config.TargetHosts(os.path.join(os.path.dirname(__file__), "resources/client_options_2.json")).all_hosts)
