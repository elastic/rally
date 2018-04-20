import gzip
import logging

import certifi
import urllib3

logger = logging.getLogger("rally.client")


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created. Intended for testing.
    """
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.ssl_context = None

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            import ssl
            from elasticsearch.connection import create_ssl_context
            logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            self.ssl_context = create_ssl_context(cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                logger.info("SSL certificate verification: off")
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                               "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                               "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                logger.info("SSL certificate verification: on")
        else:
            logger.info("SSL support: off")
            self.client_options["scheme"] = "http"

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            logger.info("HTTP basic authentication: off")

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        class PoolWrap(object):
            def __init__(self, pool, compressed=False, **kwargs):
                self.pool = pool
                self.compressed = compressed

            def urlopen(self, method, url, body, retries, headers, **kw):
                if body is not None and self.compressed:
                    body = gzip.compress(body)
                return self.pool.urlopen(method, url, body=body, retries=retries, headers=headers, **kw)

            def __getattr__(self, attr_name):
                return getattr(self.pool, attr_name)

        import elasticsearch

        class ConfigurableHttpConnection(elasticsearch.Urllib3HttpConnection):
            def __init__(self, compressed=False, **kwargs):
                super(ConfigurableHttpConnection, self).__init__(**kwargs)
                if compressed:
                    logger.info("HTTP compression: on")
                    self.headers.update(urllib3.make_headers(accept_encoding=True))
                    self.headers.update({"Content-Encoding": "gzip"})
                else:
                    logger.info("HTTP compression: off")
                self.pool = PoolWrap(self.pool, **kwargs)

        return elasticsearch.Elasticsearch(hosts=self.hosts, connection_class=ConfigurableHttpConnection,
                                           ssl_context=self.ssl_context, **self.client_options)
