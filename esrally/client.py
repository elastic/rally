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

import contextvars
import logging
import time

import certifi
import urllib3

from esrally import exceptions, doc_link
from esrally.utils import console, convert


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created. Intended for testing.
    """
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.ssl_context = None
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        self.logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            import ssl
            self.logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            # ssl.Purpose.CLIENT_AUTH allows presenting client certs and can only be enabled during instantiation
            # but can be disabled via the verify_mode property later on.
            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH,
                                                          cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.verify_mode = ssl.CERT_NONE
                self.ssl_context.check_hostname = False

                self.logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                self.ssl_context.verify_mode=ssl.CERT_REQUIRED
                self.ssl_context.check_hostname = True
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error(
                    "Supplied client-options contain only one of client_cert/client_key. "
                )
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")))
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option,
                        missing_client_ssl_option))
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert,
                                                 keyfile=client_key)
        else:
            self.logger.info("SSL support: off")
            self.client_options["scheme"] = "http"

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed‘. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        import elasticsearch
        return elasticsearch.Elasticsearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)

    def create_async(self):
        # keep imports confined as we do some temporary patching to work around unsolved issues in the async ES connector
        import elasticsearch.transport
        import elasticsearch.compat
        import elasticsearch_async
        from aiohttp.client import ClientTimeout
        import esrally.async_connection
        import io
        import aiohttp

        from elasticsearch.serializer import JSONSerializer

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, s):
                meta = RallyAsyncElasticsearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(s)
                else:
                    return super().loads(s)

        async def on_request_start(session, trace_config_ctx, params):
            meta = RallyAsyncElasticsearch.request_context.get()
            # this can happen if multiple requests are sent on the wire for one logical request (e.g. scrolls)
            if "request_start" not in meta:
                meta["request_start"] = time.perf_counter()

        async def on_request_end(session, trace_config_ctx, params):
            meta = RallyAsyncElasticsearch.request_context.get()
            meta["request_end"] = time.perf_counter()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # needs patching as https://github.com/elastic/elasticsearch-py-async/pull/68 is not merged yet
        class RallyAsyncTransport(elasticsearch_async.transport.AsyncTransport):
            def __init__(self, hosts, connection_class=esrally.async_connection.AIOHttpConnection, loop=None,
                         connection_pool_class=elasticsearch_async.connection_pool.AsyncConnectionPool,
                         sniff_on_start=False, raise_on_sniff_error=True, **kwargs):
                super().__init__(hosts, connection_class, loop, connection_pool_class, sniff_on_start, raise_on_sniff_error, **kwargs)

        if "timeout" in self.client_options and not isinstance(self.client_options["timeout"], ClientTimeout):
            self.client_options["timeout"] = ClientTimeout(total=self.client_options["timeout"])
        else:
            # 10 seconds is the Elasticsearch default, ensure we always set a ClientTimeout object here
            self.client_options["timeout"] = ClientTimeout(total=10)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()
        self.client_options["trace_config"] = trace_config

        # copy of AsyncElasticsearch as https://github.com/elastic/elasticsearch-py-async/pull/49 is not yet released.
        # That PR (also) fixes the behavior reported in https://github.com/elastic/elasticsearch-py-async/issues/43.
        class RallyAsyncElasticsearch(elasticsearch.Elasticsearch):
            request_context = contextvars.ContextVar("rally_request_context")

            def __init__(self, hosts=None, transport_class=RallyAsyncTransport, **kwargs):
                super().__init__(hosts, transport_class=transport_class, **kwargs)

            def init_request_context(self):
                ctx = {}
                RallyAsyncElasticsearch.request_context.set(ctx)
                return ctx

            def return_raw_response(self):
                ctx = RallyAsyncElasticsearch.request_context.get()
                ctx["raw_response"] = True

            # workaround for https://github.com/elastic/elasticsearch-py/issues/919
            @elasticsearch.client.utils.query_params(
                "_source",
                "_source_excludes",
                "_source_includes",
                "pipeline",
                "refresh",
                "routing",
                "timeout",
                "wait_for_active_shards",
            )
            def bulk(self, body, index=None, doc_type=None, params=None, headers=None):
                if body in elasticsearch.client.utils.SKIP_IN_PATH:
                    raise ValueError("Empty value passed for a required argument 'body'.")

                body = self._bulk_body(body)
                return self.transport.perform_request(
                    "POST",
                    elasticsearch.client.utils._make_path(index, doc_type, "_bulk"),
                    params=params,
                    headers=headers,
                    body=body,
                )

            def _bulk_body(self, body):
                # if not passed in a string, serialize items and join by newline
                if not isinstance(body, elasticsearch.compat.string_types):
                    body = "\n".join(map(self.transport.serializer.dumps, body))

                # bulk body must end with a newline
                if isinstance(body, bytes) and not body.endswith(b"\n"):
                    body += b"\n"
                elif isinstance(body, str) and not body.endswith("\n"):
                    body += "\n"

                return body

            async def __aenter__(self):
                return self

            async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
                yield self.transport.close()

        return RallyAsyncElasticsearch(hosts=self.hosts,
                                       transport_class=RallyAsyncTransport,
                                       ssl_context=self.ssl_context,
                                       **self.client_options)


def wait_for_rest_layer(es, max_attempts=40):
    """
    Waits for ``max_attempts`` until Elasticsearch's REST API is available.

    :param es: Elasticsearch client to use for connecting.
    :param max_attempts: The maximum number of attempts to check whether the REST API is available.
    :return: True iff Elasticsearch's REST API is available.
    """
    # assume that at least the hosts that we expect to contact should be available. Note that this is not 100%
    # bullet-proof as a cluster could have e.g. dedicated masters which are not contained in our list of target hosts
    # but this is still better than just checking for any random node's REST API being reachable.
    expected_node_count = len(es.transport.hosts)
    logger = logging.getLogger(__name__)
    for attempt in range(max_attempts):
        logger.debug("REST API is available after %s attempts", attempt)
        import elasticsearch
        try:
            # see also WaitForHttpResource in Elasticsearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            es.cluster.health(wait_for_nodes=">={}".format(expected_node_count))
            logger.info("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except elasticsearch.ConnectionError as e:
            if "SSL: UNKNOWN_PROTOCOL" in str(e):
                raise exceptions.SystemSetupError("Could not connect to cluster via https. Is this an https endpoint?", e)
            else:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
        except elasticsearch.TransportError as e:
            # cluster block, x-pack not initialized yet, our wait condition is not reached
            if e.status_code in (503, 401, 408):
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.status_code, attempt)
                time.sleep(3)
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.status_code, attempt)
                raise e
    return False
