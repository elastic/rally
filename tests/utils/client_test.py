from unittest import TestCase

from esrally import exceptions
from esrally.utils import config


class TestTargetHosts(TestCase):
    def test_empty_arg_parses_as_empty_list(self):
        self.assertEqual([], config.TargetHosts('--target-hosts', '')())
        self.assertEqual([], config.TargetHosts('--target-hosts', '').default)
        self.assertEqual({'default': []}, config.TargetHosts('--target-hosts', '').all_hosts)

    def test_csv_hosts_parses(self):
        target_hosts = '127.0.0.1:9200,10.17.0.5:19200'

        self.assertEqual(
            {'default': [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}]},
            config.TargetHosts('--target-hosts', target_hosts).all_hosts
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            config.TargetHosts('--target-hosts', target_hosts).default
        )

        self.assertEqual(
            [{'host': '127.0.0.1', 'port': 9200},{'host': '10.17.0.5', 'port': 19200}],
            config.TargetHosts('--target-hosts', target_hosts)()
        )

    def test_jsonstring_parses_as_dict_of_clusters(self):
        target_hosts = '{"default": "127.0.0.1:9200,10.17.0.5:19200", "remote_1": "88.33.22.15:19200", "remote_2": "10.18.0.6:19200,10.18.0.7:19201"}'

        self.assertEqual(
            {'default': [{'host': '127.0.0.1', 'port': 9200},
                         {'host': '10.17.0.5', 'port': 19200}],
             'remote_1': [{'host': '88.33.22.15', 'port': 19200}],
             'remote_2': [{'host': '10.18.0.6', 'port': 19200},
                          {'host': '10.18.0.7', 'port': 19201}]},
            config.TargetHosts('--target-hosts', target_hosts).all_hosts)

class TestClientOptions(TestCase):
    def test_csv_client_options_parses(self):
        client_options_string = "use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem'"

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            config.ClientOptions('--client-options', client_options_string)()
        )

        self.assertEqual(
            {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'},
            config.ClientOptions('--client-options', client_options_string).default
        )

        self.assertEqual(
            {'default': {'use_ssl': True, 'verify_certs': True, 'ca_certs': '/path/to/cacert.pem'}},
            config.ClientOptions('--client-options', client_options_string).all_client_options
        )


    def test_jsonstring_client_options_parses(self):
        client_options_string = '{"default": "timeout:60",' \
            '"remote_1": "use_ssl:true,verify_certs:true,basic_auth_user:\'elastic\',basic_auth_password:\'changeme\'",'\
            '"remote_2": "use_ssl:true,verify_certs:true,ca_certs:\'/path/to/cacert.pem\'"}'

        self.assertEqual(
            {'timeout': 60},
            config.ClientOptions('--client-options', client_options_string).default)

        self.assertEqual(
            {'timeout': 60},
            config.ClientOptions('--client-options', client_options_string)())

        self.assertEqual(
            {'default': {'timeout':60},
             'remote_1': {'use_ssl': True,'verify_certs': True,'basic_auth_user':'elastic','basic_auth_password':'changeme'},
             'remote_2': {'use_ssl': True,'verify_certs': True, 'ca_certs':'/path/to/cacert.pem'}},
            config.ClientOptions('--client-options', client_options_string).all_client_options)


#     def test_jsonstring_error_messages(self):
#         wrong_client_options = r'{"default1": "timeout:60", "remote": "use_ssl:true"}'

#         self.assertEqual(
#             r'''\
# You have specified a number of clusters with --client-options but none has the "default" key. Please adjust your --client-options parameter using the following example:
# --client-options="{\"default\": \"timeout:60\",\"remote_1\": \"use_ssl:true,verify_certs:true,basic_auth_user:\'elastic\',basic_auth_password:\'changeme\'\",\"remote_2\": \"use_ssl:true,verify_certs:true,ca_certs:\'/path/to/cacert.pem\'\"}"
# and try again.''',
#             config.ClientOptions('--client-options', wrong_client_options).all_client_options
#         )
