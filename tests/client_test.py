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

import asyncio
import logging
import os
import random
import ssl
from copy import deepcopy
from unittest import TestCase, mock

import elasticsearch
import urllib3.exceptions

from esrally import client, exceptions, doc_link
from esrally.utils import console
from tests import run_async


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

        assert not mocked_load_cert_chain.called, "ssl_context.load_cert_chain should not have been called as we have not supplied " \
                                                  "client certs"

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

        assert not mocked_load_cert_chain.called, "ssl_context.load_cert_chain should not have been called as we have not supplied " \
            "client certs"
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
                client.EsClientFactory(hosts, client_options)
        mocked_console_println.assert_called_once_with(
            "'{}' is missing from client-options but '{}' has been specified.\n"
            "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
            "Read the documentation at {}\n".format(
                missing_client_ssl_option,
                random_client_ssl_option,
                console.format.link(doc_link("command_line_reference.html#client-options"))
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

        assert not mocked_load_cert_chain.called, "ssl_context.load_cert_chain should not have been called as we have not supplied " \
                                                  "client certs"

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


class RequestContextManagerTests(TestCase):
    @run_async
    async def test_propagates_nested_context(self):
        test_client = client.RequestContextHolder()
        async with test_client.new_request_context() as top_level_ctx:
            test_client.on_request_start()
            await asyncio.sleep(0.1)
            async with test_client.new_request_context() as nested_ctx:
                test_client.on_request_start()
                await asyncio.sleep(0.1)
                test_client.on_request_end()
                nested_duration = nested_ctx.request_end - nested_ctx.request_start
            test_client.on_request_end()
            top_level_duration = top_level_ctx.request_end - top_level_ctx.request_start

        # top level request should cover total duration
        self.assertAlmostEqual(top_level_duration, 0.2, delta=0.05)
        # nested request should only cover nested duration
        self.assertAlmostEqual(nested_duration, 0.1, delta=0.05)


class RestLayerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_successfully_waits_for_rest_layer(self, es):
        es.transport.hosts = [
            {"host": "node-a.example.org", "port": 9200},
            {"host": "node-b.example.org", "port": 9200}
        ]

        self.assertTrue(client.wait_for_rest_layer(es, max_attempts=3))

        es.cluster.health.assert_has_calls([
            mock.call(wait_for_nodes=">=2"),
        ])

    # don't sleep in realtime
    @mock.patch("time.sleep")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_retries_on_transport_errors(self, es, sleep):
        es.cluster.health.side_effect = [
            elasticsearch.TransportError(503, "Service Unavailable"),
            elasticsearch.TransportError(401, "Unauthorized"),
            elasticsearch.TransportError(408, "Timed Out"),
            elasticsearch.TransportError(408, "Timed Out"),
            {
                "version": {
                    "number": "5.0.0",
                    "build_hash": "abc123"
                }
            }
        ]
        self.assertTrue(client.wait_for_rest_layer(es, max_attempts=5))

    # don't sleep in realtime
    @mock.patch("time.sleep")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_dont_retry_eternally_on_transport_errors(self, es, sleep):
        es.cluster.health.side_effect = elasticsearch.TransportError(401, "Unauthorized")
        self.assertFalse(client.wait_for_rest_layer(es, max_attempts=3))

    @mock.patch("elasticsearch.Elasticsearch")
    def test_ssl_error(self, es):
        es.cluster.health.side_effect = elasticsearch.ConnectionError("N/A",
                                                            "[SSL: UNKNOWN_PROTOCOL] unknown protocol (_ssl.c:719)",
                                                            urllib3.exceptions.SSLError(
                                                                "[SSL: UNKNOWN_PROTOCOL] unknown protocol (_ssl.c:719)"))
        with self.assertRaisesRegex(expected_exception=exceptions.SystemSetupError,
                                    expected_regex="Could not connect to cluster via https. Is this an https endpoint?"):
            client.wait_for_rest_layer(es, max_attempts=3)
