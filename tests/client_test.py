import ssl
from unittest import TestCase

from esrally import client


class EsClientFactoryTests(TestCase):
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

    def test_create_https_connection_verify_certificate(self):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = dict(client_options)

        f = client.EsClientFactory(hosts, client_options)

        self.assertEqual(hosts, f.hosts)
        self.assertTrue(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)

        self.assertDictEqual(original_client_options, client_options)

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
