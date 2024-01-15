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

import logging
import time

import certifi
from urllib3.connection import is_ipaddress

from esrally import doc_link, exceptions
from esrally.utils import console, convert, versions


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created and customizes the client for backwards
    compatibility guarantees that are broader than the library's defaults.
    """

    def __init__(self, hosts, client_options, distribution_version=None, distribution_flavor=None):
        def host_string(host):
            # protocol can be set at either host or client opts level
            protocol = "https" if client_options.get("use_ssl") or host.get("use_ssl") else "http"
            return f"{protocol}://{host['host']}:{host['port']}"

        self.hosts = [host_string(h) for h in hosts]
        self.client_options = dict(client_options)
        self.ssl_context = None
        # This attribute is necessary for the backwards-compatibility logic contained in
        # RallySyncElasticsearch.perform_request() and RallyAsyncElasticsearch.perform_request(), and also for
        # identification of whether or not a client is 'serverless'.
        self.distribution_version = distribution_version
        self.distribution_flavor = distribution_flavor
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        if "api_key" in masked_client_options:
            masked_client_options["api_key"] = "*****"
        self.logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl

            self.logger.debug("SSL support: on")

            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH, cafile=self.client_options.pop("ca_certs", certifi.where())
            )

            # We call get() here instead of pop() in order to pass verify_certs through as a kwarg
            # to the elasticsearch.Elasticsearch constructor. Setting the ssl_context's verify_mode to
            # ssl.CERT_NONE is insufficient with version 8.0+ of elasticsearch-py.
            if not self.client_options.get("verify_certs", True):
                self.logger.debug("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE
                self.client_options["ssl_show_warn"] = False

                self.logger.warning(
                    "User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a benchmark."
                )
            else:
                # check_hostname should not be set when host is an IP address
                self.ssl_context.check_hostname = self._only_hostnames(hosts)
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
                self.logger.debug("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.debug("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error("Supplied client-options contain only one of client_cert/client_key. ")
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")),
                    )
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option, missing_client_ssl_option
                    )
                )
            elif client_cert and client_key:
                self.logger.debug("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert, keyfile=client_key)
        else:
            self.logger.debug("SSL support: off")

        if self._is_set(self.client_options, "create_api_key_per_client"):
            self.client_options.pop("create_api_key_per_client")
            basic_auth_user = self.client_options.get("basic_auth_user", False)
            basic_auth_password = self.client_options.get("basic_auth_password", False)
            provided_auth = {"basic_auth_user": basic_auth_user, "basic_auth_password": basic_auth_password}
            missing_auth = [k for k, v in provided_auth.items() if not v]
            if missing_auth:
                console.println(
                    "Basic auth credentials are required in order to create API keys.\n"
                    f"Missing basic auth client options are: {missing_auth}\n"
                    f"Read the documentation at {console.format.link(doc_link('command_line_reference.html#client-options'))}"
                )
                raise exceptions.SystemSetupError(
                    "You must provide the 'basic_auth_user' and 'basic_auth_password' client options in addition "
                    "to 'create_api_key_per_client' in order to create client API keys."
                )
            self.logger.debug("Automatic creation of client API keys: on")
        else:
            self.logger.debug("Automatic creation of client API keys: off")

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.client_options["basic_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
            self.logger.debug("HTTP basic authentication: on")
        else:
            self.logger.debug("HTTP basic authentication: off")

        if self._is_set(self.client_options, "api_key"):
            self.logger.debug("API key authentication: on")
        else:
            self.logger.debug("API key authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed‘. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.debug("HTTP compression: on")
        else:
            self.logger.debug("HTTP compression: off")

        self.enable_cleanup_closed = convert.to_bool(self.client_options.pop("enable_cleanup_closed", True))
        self.max_connections = max(256, self.client_options.pop("max_connections", 0))
        self.static_responses = self.client_options.pop("static_responses", None)

        if self._is_set(self.client_options, "timeout"):
            self.client_options["request_timeout"] = self.client_options.pop("timeout")

    @staticmethod
    def _only_hostnames(hosts):
        has_ip = False
        has_hostname = False
        for host in hosts:
            is_ip = is_ipaddress(host["host"])
            if is_ip:
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            raise exceptions.SystemSetupError("Cannot verify certs with mixed IP addresses and hostnames")

        return has_hostname

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        # pylint: disable=import-outside-toplevel
        from esrally.client.synchronous import RallySyncElasticsearch

        return RallySyncElasticsearch(
            distribution_version=self.distribution_version,
            distribution_flavor=self.distribution_flavor,
            hosts=self.hosts,
            ssl_context=self.ssl_context,
            **self.client_options,
        )

    def create_async(self, api_key=None, client_id=None):
        # pylint: disable=import-outside-toplevel
        import io

        import aiohttp
        from elasticsearch.serializer import JSONSerializer

        from esrally.client.asynchronous import (
            RallyAsyncElasticsearch,
            RallyAsyncTransport,
        )

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, data):
                meta = RallyAsyncElasticsearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(data)
                else:
                    return super().loads(data)

        async def on_request_start(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_start()

        async def on_request_end(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_end()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()

        if api_key is not None:
            self.client_options.pop("http_auth", None)
            self.client_options.pop("basic_auth", None)
            self.client_options["api_key"] = api_key

        async_client = RallyAsyncElasticsearch(
            distribution_version=self.distribution_version,
            distribution_flavor=self.distribution_flavor,
            hosts=self.hosts,
            transport_class=RallyAsyncTransport,
            ssl_context=self.ssl_context,
            maxsize=self.max_connections,
            **self.client_options,
        )

        # the AsyncElasticsearch constructor automatically creates the corresponding NodeConfig objects, so we set
        # their instance attributes after they've been instantiated
        for node_connection in async_client.transport.node_pool.all():
            node_connection.trace_configs = [trace_config]
            node_connection.enable_cleanup_closed = self.enable_cleanup_closed
            node_connection.static_responses = self.static_responses
            node_connection.client_id = client_id

        return async_client


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
    expected_node_count = len(es.transport.node_pool)
    logger = logging.getLogger(__name__)
    attempt = 0
    while attempt <= max_attempts:
        attempt += 1
        # pylint: disable=import-outside-toplevel
        from elastic_transport import (
            ApiError,
            ConnectionError,
            SerializationError,
            TlsError,
            TransportError,
        )

        try:
            # see also WaitForHttpResource in Elasticsearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            es.cluster.health(wait_for_nodes=f">={expected_node_count}")
            logger.debug("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except SerializationError as e:
            if "Client sent an HTTP request to an HTTPS server" in str(e):
                raise exceptions.SystemSetupError(
                    "Rally sent an HTTP request to an HTTPS server. Are you sure this is an HTTP endpoint?", e
                )

            if attempt <= max_attempts:
                logger.debug("Got serialization error [%s] on attempt [%s]. Sleeping...", e, attempt)
                time.sleep(3)
            else:
                raise
        except TlsError as e:
            raise exceptions.SystemSetupError("Could not connect to cluster via HTTPS. Are you sure this is an HTTPS endpoint?", e)
        except ConnectionError as e:
            if "ProtocolError" in str(e):
                raise exceptions.SystemSetupError(
                    "Received a protocol error. Are you sure you're using the correct scheme (HTTP or HTTPS)?", e
                )

            if attempt <= max_attempts:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
            else:
                raise
        except TransportError as e:
            if attempt <= max_attempts:
                logger.debug("Got transport error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
            else:
                raise
        except ApiError as e:
            # cluster block, x-pack not initialized yet, our wait condition is not reached
            if e.status_code in (503, 401, 408) and attempt <= max_attempts:
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.message, attempt)
                time.sleep(3)
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.message, attempt)
                raise
    return False


def cluster_distribution_version(hosts, client_options, client_factory=EsClientFactory):
    """
    Attempt to get the target cluster's distribution version, build flavor, and build hash by creating and using
    a 'sync' Elasticsearch client.

    :param hosts: The host(s) to connect to.
    :param client_options: The client options to customize the Elasticsearch client.
    :param client_factory: Factory class that creates the Elasticsearch client.
    :return: The cluster's build flavor, version number, and build hash. For Serverless Elasticsearch these may all be
      the build flavor value. Also returns the operator status (always False for stateful).
    """
    # no way for us to know whether we're talking to a serverless elasticsearch or not, so we default to the sync client
    es = client_factory(hosts, client_options).create()

    # wait_for_rest_layer  calls the Cluster Health API, which is not available for unprivileged users on Serverless
    # As a result, we need to call the info API first to know if we can call wait_for_rest_layer().
    version = es.info()["version"]

    version_build_flavor = version.get("build_flavor", "oss")
    # if build hash is not available default to build flavor
    version_build_hash = version.get("build_hash", version_build_flavor)
    # if version number is not available default to build flavor
    version_number = version.get("number", version_build_flavor)

    serverless_operator = False
    if versions.is_serverless(version_build_flavor):
        # overwrite static serverless version number
        version_number = "serverless"
        authentication_info = es.perform_request(method="GET", path="/_security/_authenticate")
        serverless_operator = authentication_info.body.get("operator", False)

    if not versions.is_serverless(version_build_flavor) or serverless_operator is True:
        # if available, unconditionally wait for the REST layer - if it's not up, we'll intentionally raise the original error
        wait_for_rest_layer(es)

    return version_build_flavor, version_number, version_build_hash, serverless_operator


def create_api_key(es, client_id, max_attempts=5):
    """
    Creates an API key for the provided ``client_id``.

    :param es: Elasticsearch client to use for connecting.
    :param client_id: ID of the client for which the API key is being created.
    :param max_attempts: The maximum number of attempts to create the API key.
    :return: A dict with at least the following keys: ``id``, ``name``, ``api_key``.
    """
    logger = logging.getLogger(__name__)

    for attempt in range(1, max_attempts + 1):
        # pylint: disable=import-outside-toplevel
        import elasticsearch

        try:
            logger.debug("Creating ES API key for client ID [%s]", client_id)
            return es.security.create_api_key(name=f"rally-client-{client_id}")
        except elasticsearch.TransportError as e:
            logger.debug("Got transport error [%s] on attempt [%s]. Sleeping...", str(e), attempt)
            time.sleep(1)
        except elasticsearch.ApiError as e:
            if e.meta.status == 405:
                # We don't retry on 405 since it indicates a misconfigured benchmark candidate and isn't recoverable
                raise exceptions.SystemSetupError(
                    "Got status code 405 when attempting to create API keys. Is Elasticsearch Security enabled?", e
                )
            logger.debug("Got status code [%s] on attempt [%s] of [%s]. Sleeping...", e.status_code, attempt, max_attempts)
            time.sleep(1)


def delete_api_keys(es, ids, max_attempts=5):
    """
    Deletes the provided list of API key IDs.

    :param es: Elasticsearch client to use for connecting.
    :param ids: List of API key IDs to delete.
    :param max_attempts: The maximum number of attempts to delete the API keys.
    :return: True iff all provided key IDs were successfully deleted.
    """
    logger = logging.getLogger(__name__)

    def raise_exception(failed_ids, cause=None):
        msg = f"Could not delete API keys with the following IDs: {failed_ids}"
        if cause is not None:
            raise exceptions.RallyError(msg) from cause
        raise exceptions.RallyError(msg)

    # Before ES 7.10, deleting API keys by ID had to be done individually.
    # After ES 7.10, a list of API key IDs can be deleted in one request.
    version = es.info()["version"]
    current_version = versions.Version.from_string(version.get("number", "7.10.0"))
    minimum_version = versions.Version.from_string("7.10.0")

    deleted = []
    remaining = ids

    for attempt in range(1, max_attempts + 1):
        # pylint: disable=import-outside-toplevel
        import elasticsearch

        try:
            if current_version >= minimum_version or es.is_serverless:
                resp = es.security.invalidate_api_key(ids=remaining)
                deleted += resp["invalidated_api_keys"]
                remaining = [i for i in ids if i not in deleted]
                # Like bulk indexing requests, we can get an HTTP 200, but the
                # response body could still contain an array of individual errors.
                # So, we have to handle the case were some keys weren't deleted, but
                # the request overall succeeded (i.e. we didn't encounter an exception)
                if attempt < max_attempts:
                    if resp["error_count"] > 0:
                        logger.debug(
                            "Got the following errors on attempt [%s] of [%s]: [%s]. Sleeping...",
                            attempt,
                            max_attempts,
                            resp["error_details"],
                        )
                else:
                    if remaining:
                        logger.warning(
                            "Got the following errors on final attempt to delete API keys: [%s]",
                            resp["error_details"],
                        )
                        raise_exception(remaining)
            else:
                remaining = [i for i in ids if i not in deleted]
                if attempt < max_attempts:
                    for i in remaining:
                        es.security.invalidate_api_key(id=i)
                        deleted.append(i)
                else:
                    if remaining:
                        raise_exception(remaining)
            return True

        except elasticsearch.ApiError as e:
            if attempt < max_attempts:
                logger.debug("Got status code [%s] on attempt [%s] of [%s]. Sleeping...", e.meta.status, attempt, max_attempts)
                time.sleep(1)
            else:
                raise_exception(remaining, cause=e)
        except Exception as e:
            if attempt < max_attempts:
                logger.debug("Got error on attempt [%s] of [%s]. Sleeping...", attempt, max_attempts)
                time.sleep(1)
            else:
                raise_exception(remaining, cause=e)
