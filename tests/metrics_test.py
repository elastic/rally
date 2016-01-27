import datetime
from unittest import TestCase
import unittest.mock as mock

import rally.metrics
import rally.config
import rally.track.track


class MockClientFactory:
    def __init__(self, config):
        self._es = mock.create_autospec(rally.metrics.EsClient)

    def create(self):
        return self._es


class DummyIndexTemplateProvider:
    def __init__(self, config):
        pass

    def template(self):
        return "test-template"


class StaticClock:
    NOW = 1453362707

    @staticmethod
    def now():
        return StaticClock.NOW


class MetricsTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)

    def setUp(self):
        config = rally.config.Config()
        config.add(rally.config.Scope.application, "system", "env.name", "unittest")
        self.metrics_store = rally.metrics.EsMetricsStore(config,
                                                          client_factory_class=MockClientFactory,
                                                          index_template_provider_class=DummyIndexTemplateProvider,
                                                          clock=StaticClock)
        # get hold of the mocked client...
        self.es_mock = self.metrics_store._client

    def test_put_value(self):
        throughput = 5000
        self.metrics_store.open(MetricsTests.TRIAL_TIMESTAMP, "test", "defaults", create=True)

        self.metrics_store.put_count("indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW,
            "trial-timestamp": "20160131T000000Z",
            "environment": "unittest",
            "track": "test",
            "track-setup": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s"
        }
        self.metrics_store.close()
        self.es_mock.create_index.assert_called_with(index="rally-2016")
        self.es_mock.bulk_index.assert_called_with(index="rally-2016", doc_type="metrics", items=[expected_doc])

    def test_get_value(self):
        throughput = 5000
        search_result = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "value": throughput
                        }
                    }
                ]
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(MetricsTests.TRIAL_TIMESTAMP, "test", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "trial-timestamp": "20160131T000000Z"
                            }
                        },
                        {
                            "term": {
                                "environment": "unittest"
                            }
                        },
                        {
                            "term": {
                                "track": "test"
                            }
                        },
                        {
                            "term": {
                                "track-setup": "defaults"
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        }
                    ]
                }
            }
        }

        actual_throughput = self.metrics_store.get_one("indexing_throughput")

        self.es_mock.search.assert_called_with(index="rally-2016", doc_type="metrics", body=expected_query)

        self.assertEqual(throughput, actual_throughput)
