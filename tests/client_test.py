import logging
import os
import random
import ssl

from copy import deepcopy
from unittest import TestCase, mock

from elasticsearch.connection_pool import DummyConnectionPool
from elasticsearch.transport import Transport

from esrally import client, exceptions, DOC_LINK
from esrally.utils import console

from urllib3 import ProxyManager


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

    @mock.patch.object(ssl.SSLContext, "load_cert_chain")
    def test_create_https_connection_verify_server(self, mocked_load_cert_chain):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": True,
            "http_auth": ("user", "password")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = deepcopy(client_options)

        logger = logging.getLogger("esrally.client")
        with mock.patch.object(logger, "info") as mocked_info_logger:
            f = client.EsClientFactory(hosts, client_options)
        mocked_info_logger.assert_has_calls([
            mock.call("SSL support: on"),
            mock.call("SSL certificate verification: on"),
            mock.call("SSL client authentication: off")
        ])

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

        logger = logging.getLogger("esrally.client")
        with mock.patch.object(logger, "info") as mocked_info_logger:
            f = client.EsClientFactory(hosts, client_options)
        mocked_info_logger.assert_has_calls([
            mock.call("SSL support: on"),
            mock.call("SSL certificate verification: on"),
            mock.call("SSL client authentication: on")
        ])

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

        logger = logging.getLogger("esrally.client")
        with mock.patch.object(logger, "info") as mocked_info_logger:
            f = client.EsClientFactory(hosts, client_options)
        mocked_info_logger.assert_has_calls([
            mock.call("SSL support: on"),
            mock.call("SSL certificate verification: on"),
            mock.call("SSL client authentication: off")
        ])

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

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            with mock.patch.object(console, "println") as mocked_console_println:
                f = client.EsClientFactory(hosts, client_options)
        mocked_console_println.assert_called_once_with(
            "'{}' is missing from client-options but '{}' has been specified.\n"
            "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
            "Read the documentation at {}/command_line_reference.html#client-options\n".format(
                missing_client_ssl_option,
                random_client_ssl_option,
                console.format.link(DOC_LINK)
            )
        )
        self.assertEqual(
            "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                random_client_ssl_option,
                missing_client_ssl_option
            ),
            ctx.exception.args[0]
        )

    @mock.patch.object(ssl.SSLContext, "load_cert_chain")
    def test_create_https_connection_unverified_certificate(self, mocked_load_cert_chain):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": False,
            "basic_auth_user": "user",
            "basic_auth_password": "password"
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = dict(client_options)

        logger = logging.getLogger("esrally.client")
        with mock.patch.object(logger, "info") as mocked_info_logger:
            f = client.EsClientFactory(hosts, client_options)
        mocked_info_logger.assert_has_calls([
            mock.call("SSL support: on"),
            mock.call("SSL certificate verification: off"),
            mock.call("SSL client authentication: off")
        ])

        assert not mocked_load_cert_chain.called, "ssl_context.load_cert_chain should not have been called as we have not supplied client " \
                                                  "certs"

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

    @mock.patch.object(ssl.SSLContext, "load_cert_chain")
    def test_create_https_connection_unverified_certificate_present_client_certificates(self, mocked_load_cert_chain):
        hosts = [{"host": "127.0.0.1", "port": 9200}]
        client_options = {
            "use_ssl": True,
            "verify_certs": False,
            "http_auth": ("user", "password"),
            "client_cert": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/client.crt"),
            "client_key": os.path.join(EsClientFactoryTests.cwd, "utils/resources/certs/client.key")
        }
        # make a copy so we can verify later that the factory did not modify it
        original_client_options = deepcopy(client_options)

        logger = logging.getLogger("esrally.client")
        with mock.patch.object(logger, "info") as mocked_info_logger:
            f = client.EsClientFactory(hosts, client_options)
        mocked_info_logger.assert_has_calls([
            mock.call("SSL certificate verification: off"),
            mock.call("SSL client authentication: on")
        ])

        mocked_load_cert_chain.assert_called_with(
            certfile=client_options["client_cert"],
            keyfile=client_options["client_key"]
        )

        self.assertEqual(hosts, f.hosts)
        self.assertFalse(f.ssl_context.check_hostname)
        self.assertEqual(ssl.CERT_NONE, f.ssl_context.verify_mode)

        self.assertEqual("https", f.client_options["scheme"])
        self.assertEqual(("user", "password"), f.client_options["http_auth"])
        self.assertNotIn("use_ssl", f.client_options)
        self.assertNotIn("verify_certs", f.client_options)
        self.assertNotIn("basic_auth_user", f.client_options)
        self.assertNotIn("basic_auth_password", f.client_options)
        self.assertNotIn("ca_certs", f.client_options)
        self.assertNotIn("client_cert", f.client_options)
        self.assertNotIn("client_key", f.client_options)

        self.assertDictEqual(original_client_options, client_options)


class ProxyEnabledEsClientFactoryTests(TestCase):

    class DummyResponse(object):
        """
        A dummy response used as a return value in mock testing
        """
        
        def __init__(self):
            self.status = 200
            self.data = b'{}'
            self.headers = {}
    
        def getheaders(self):
            return self.headers

    def setUp(self):
        super(ProxyEnabledEsClientFactoryTests, self).setUp()
        os.environ['http_proxy'] = 'http://localhost:3128'
        os.environ.pop('esrally_benchmark_no_proxy', None)
        self.hosts = [{"host": "192.168.50.1", "port": 9200}]
        self.client_options = {}
        self.base_headers = {'connection': 'keep-alive', 'content-type': 'application/json'}

    def tearDown(self):
        os.environ.pop('http_proxy', None)
        super(ProxyEnabledEsClientFactoryTests, self).tearDown()

    def test_http_proxy_not_present(self):
        os.environ.pop('http_proxy', None)
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        pool = es.transport.connection_pool.get_connection().pool
        self.assertFalse(isinstance(pool, ProxyManager))
        
    def test_proxy_manager_configured(self):
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        pool = es.transport.connection_pool.get_connection().pool
        self.assertTrue(isinstance(pool, ProxyManager))
        self.assertEqual("%s://%s:%i" % (pool.proxy.scheme, pool.proxy.host, pool.proxy.port), os.environ['http_proxy'])

    def test_proxy_manager_single_host(self):
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        with mock.patch.object(ProxyManager, "urlopen", return_value=__class__.DummyResponse()) as mocked_urlopen:
            for i in range(3):
                es.cluster.health()
            mocked_urlopen.assert_has_calls([
                mock.call('GET', 'http://%(host)s:%(port)s/_cluster/health' % self.hosts[0],
                    None, headers=self.base_headers, retries=False)
            ] * 3)

    def test_proxy_manager_round_robin(self):
        self.hosts = [
            {"host": "192.168.50.1", "port": 9200},
            {"host": "192.168.50.2", "port": 9200},
        ]
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        self.assertEqual(len(es.transport.connection_pool.connections), 2)
        with mock.patch.object(ProxyManager, "urlopen", return_value=__class__.DummyResponse()) as mocked_urlopen:
            for i in range(6):
                es.cluster.health()
            # two pooled objects which each round-robin
            # n.b. the objects in the pool are randomly shuffled by default..
            mocked_urlopen.assert_has_calls([
                mock.call('GET', 'http://%(host)s:%(port)s/_cluster/health' % self.hosts[x],
                    None, headers=self.base_headers, retries=False)
                for x in [0, 0, 1, 1, 0, 0]
            ])

    def test_proxy_manager_ssl(self):
        self.client_options = {
            "use_ssl": True,
            "verify_certs": False,
            "basic_auth_user": "user",
            "basic_auth_password": "password"
        }
        headers = deepcopy(self.base_headers)
        headers['authorization'] = 'Basic dXNlcjpwYXNzd29yZA=='
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        with mock.patch.object(ProxyManager, "urlopen", return_value=__class__.DummyResponse()) as mocked_urlopen:
            es.cluster.health()
            mocked_urlopen.assert_has_calls([
                mock.call('GET', 'https://%(host)s:%(port)s/_cluster/health' % self.hosts[0],
                    None, headers=headers, retries=False)
            ])

    def test_no_proxy_var_set_true(self):
        for truthy in ['true', 'True', 'tRuE']:
            os.environ['esrally_benchmark_no_proxy'] = truthy
            es = client.EsClientFactory(self.hosts, self.client_options).create()
            pool = es.transport.connection_pool.get_connection().pool
            self.assertFalse(isinstance(pool, ProxyManager))

    def test_no_proxy_var_set_not_true(self):
        for not_truthy in ['false', 'False', 'etc']:
            os.environ['esrally_benchmark_no_proxy'] = not_truthy
            es = client.EsClientFactory(self.hosts, self.client_options).create()
            pool = es.transport.connection_pool.get_connection().pool
            self.assertTrue(isinstance(pool, ProxyManager))

    def test_no_proxy_var_not_set(self):
        os.environ.pop('esrally_benchmark_no_proxy', None)
        es = client.EsClientFactory(self.hosts, self.client_options).create()
        pool = es.transport.connection_pool.get_connection().pool
        self.assertTrue(isinstance(pool, ProxyManager))

