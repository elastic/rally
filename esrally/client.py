import gzip
import urllib3
import logging
import elasticsearch
import certifi

logger = logging.getLogger("rally.client")


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


class ConfigurableHttpConnection(elasticsearch.Urllib3HttpConnection):
    def __init__(self, compressed=False, **kwargs):
        super(ConfigurableHttpConnection, self).__init__(**kwargs)
        if compressed:
            self.headers.update(urllib3.make_headers(accept_encoding=True))
            self.headers.update({"Content-Encoding": "gzip"})
        self.pool = PoolWrap(self.pool, **kwargs)


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created. Intended for testing.
    """

    def __init__(self, hosts, client_options):
        logger.info("Creating ES client connected to %s with options [%s]" % (hosts, client_options))
        if self._is_set(client_options, "use_ssl") and self._is_set(client_options, "verify_certs") and "ca_certs" not in client_options:
            client_options["ca_certs"] = certifi.where()
        if self._is_set(client_options, "basic_auth_user") and self._is_set(client_options, "basic_auth_password"):
            # Maybe we should remove these keys from the dict?
            client_options["http_auth"] = (client_options["basic_auth_user"], client_options["basic_auth_password"])
        self.client = elasticsearch.Elasticsearch(hosts=hosts, connection_class=ConfigurableHttpConnection, **client_options)

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        return self.client
