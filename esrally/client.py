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

import logging
import certifi
import urllib3

from esrally import exceptions, DOC_LINK
from esrally.utils import console


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
            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=self.client_options.pop("ca_certs", certifi.where()))

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
                    "Read the documentation at {}/command_line_reference.html#client-options\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(DOC_LINK))
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option,
                        missing_client_ssl_option,
                        DOC_LINK))
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

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        import elasticsearch
        return elasticsearch.Elasticsearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)
