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
        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (client_options["http_auth"][0], "*****")
        logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)
        self.hosts = hosts
        self.client_options = client_options

        if self._is_set(client_options, "use_ssl") and self._is_set(client_options, "verify_certs") and "ca_certs" not in client_options:
            self.client_options["ca_certs"] = certifi.where()
        elif self._is_set(client_options, "use_ssl") and not self._is_set(client_options, "verify_certs"):
            logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a benchmark. "
                           "Disabling urllib warnings now to avoid a logging storm. "
                           "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
            # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
            # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
            urllib3.disable_warnings()
        if self._is_set(client_options, "basic_auth_user") and self._is_set(client_options, "basic_auth_password"):
            # Maybe we should remove these keys from the dict?
            self.client_options["http_auth"] = (client_options["basic_auth_user"], client_options["basic_auth_password"])

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
                    self.headers.update(urllib3.make_headers(accept_encoding=True))
                    self.headers.update({"Content-Encoding": "gzip"})
                self.pool = PoolWrap(self.pool, **kwargs)

        return elasticsearch.Elasticsearch(hosts=self.hosts, connection_class=ConfigurableHttpConnection, **self.client_options)
