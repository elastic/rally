import os
import ssl
import random
import logging

from copy import deepcopy
from unittest import TestCase, mock

from esrally import client, exceptions


class EsClientFactoryTests(TestCase):
    cwd = os.path.dirname(__file__)

    def test_create_http_connection(self):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {}
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = dict(client_options)

        f = client.EsClientFactory(hosts, client_options)

        self.assertEqual(hosts, f.hosts)
        self.assertIsNone(f.ssl_context)
        self.assertEqual("http", f.client_options["scheme"])
        self.assertFalse("http_auth" in f.client_options)

        self.assertDictEqual(original_client_options, client_options)

    def test_create_https_connection_verify_server(self):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = deepcopy(client_options)

        f = client.EsClientFactory(hosts, client_options)

        self.assertEqual(hosts, f.hosts)
        self.assertTrue(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)
        self.assertNotIn("ca_certs", f.client_options)

        self.assertDictEqual(original_client_options, client_options)

    @mock.patch.object(ssl.SSLContext, "load_cert_chain")
    def test_create_https_connection_verify_self_signed_server_and_client_certificate(self, mocked_load_cert_chain):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password"),
            "ca_certs": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/ca.crt"),
            "client_cert": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/client.crt"),
            "client_key": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/client.key")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = deepcopy(client_options)

        f = client.EsClientFactory(hosts, client_options)

        mocked_load_cert_chain.assert_called_with(
            certfile=client_options["client_cert"],
            keyfile=client_options["client_key"]
        )

        self.assertEqual(hosts, f.hosts)
        self.assertTrue(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)
        self.assertNotIn("ca_certs", f.client_options)
        self.assertNotIn("client_cert", f.client_options)
        self.assertNotIn("client_key", f.client_options)

        self.assertDictEqual(original_client_options, client_options)

    @mock.patch.object(ssl.SSLContext, "load_cert_chain")
    def test_create_https_connection_only_verify_self_signed_server_certificate(self, mocked_load_cert_chain):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password"),
            "ca_certs": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/ca.crt")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = deepcopy(client_options)

        f = client.EsClientFactory(hosts, client_options)

        assert not mocked_load_cert_chain.called, "ssl_context.load_cert_chain should not have been called as we have not supplied client " \
            "certs"
        self.assertEqual(hosts, f.hosts)
        self.assertTrue(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)
        self.assertNotIn("ca_certs", f.client_options)

        self.assertDictEqual(original_client_options, client_options)

    def test_raises_error_when_only_one_of_client_cert_and_client_key_defined(self):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password"),
            "ca_certs": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/ca.crt")
        }

        client_ssl_options = {
            "client_cert": "utils/resources/certs/client.crt",
            "client_key": "utils/resources/certs/client.key"
        }

        random_client_ssl_option = random.choice(list(client_ssl_options.keys()))
        missing_client_ssl_option = list(set(client_ssl_options)-set([random_client_ssl_option]))[0]
        client_options.update(
            {random_client_ssl_option: client_ssl_options[random_client_ssl_option]}
        )

        logger = logging.getLogger("esrally.client")
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            with mock.patch.object(logger, "error") as mocked_error_logger:
                f = client.EsClientFactory(hosts, client_options)
            mocked_error_logger.assert_called_once_with(
                "Supplied client-options contain only one of client_cert/client_key. "
                "If your Elasticsearch setup requires client certificate verification both need to be supplied. "
                "See https://esrally.readthedocs.io/en/stable/command_line_reference.html?highlight=client_options#id2"
            )
        self.assertEqual(
            "'{}' is missing from client-options but '{}' has been specified".format(
                random_client_ssl_option,
                missing_client_ssl_option
            ),
            ctx.exception.args[0]
        )

    def test_create_https_connection_unverified_certificate(self):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": False,
            "basic_auth_user": "user",
            "basic_auth_password": "password"
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = dict(client_options)

        f = client.EsClientFactory(hosts, client_options)

        self.assertEqual(hosts, f.hosts)
        self.assertFalse(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_NONE, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)
        self.assertNotIn("basic_auth_user", f.client_options)
        self.assertNotIn("basic_auth_password", f.client_options)

        self.assertDictEqual(original_client_options, client_options)
