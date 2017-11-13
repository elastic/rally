import os
import datetime
import unittest.mock as mock
from unittest import TestCase
import elasticsearch.exceptions

from esrally import config, metrics, track, exceptions


class MockClientFactory:
    def __init__(self, cfg):
        self._es = mock.create_autospec(metrics.EsClient)

    def create(self):
        return self._es


class DummyIndexTemplateProvider:
    def __init__(self, cfg):
        pass

    def metrics_template(self):
        return "metrics-test-template"

    def races_template(self):
        return "races-test-template"

    def results_template(self):
        return "results-test-template"


class StaticClock:
    NOW = 1453362707

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return StaticStopWatch()


class StaticStopWatch:
    def start(self):
        pass

    def stop(self):
        pass

    def split_time(self):
        return 0

    def total_time(self):
        return 0


class EsClientTests(TestCase):
    class TransportMock:
        def __init__(self, hosts):
            self.hosts = hosts

    class ClientMock:
        def __init__(self, hosts):
            self.transport = EsClientTests.TransportMock(hosts)

    def test_raises_sytem_setup_error_on_connection_problems(self):
        def raise_connection_error():
            raise elasticsearch.exceptions.ConnectionError("unit-test")

        client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9200"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_connection_error)
        self.assertEqual("Could not connect to your Elasticsearch metrics store. Please check that it is running on host [127.0.0.1] at "
                         "port [9200] or fix the configuration in [%s/.rally/rally.ini]." % os.path.expanduser("~"),
                         ctx.exception.args[0])

    def test_raises_sytem_setup_error_on_authentication_problems(self):
        def raise_authentication_error():
            raise elasticsearch.exceptions.AuthenticationException("unit-test")

        client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authentication_error)
        self.assertEqual("The configured user could not authenticate against your Elasticsearch metrics store running on host [127.0.0.1] "
                         "at port [9243] (wrong password?). Please fix the configuration in [%s/.rally/rally.ini]."
                         % os.path.expanduser("~"), ctx.exception.args[0])

    def test_raises_sytem_setup_error_on_authorization_problems(self):
        def raise_authorization_error():
            raise elasticsearch.exceptions.AuthorizationException("unit-test")

        client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authorization_error)
        self.assertEqual("The configured user does not have enough privileges to run the operation [raise_authorization_error] against "
                         "your Elasticsearch metrics store running on host [127.0.0.1] at port [9243]. Please adjust your x-pack "
                         "configuration or specify a user with enough privileges in the configuration in [%s/.rally/rally.ini]."
                         % os.path.expanduser("~"), ctx.exception.args[0])

    def test_raises_rally_error_on_unknown_problems(self):
        def raise_unknown_error():
            raise elasticsearch.exceptions.SerializationError("unit-test")

        client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.RallyError) as ctx:
            client.guarded(raise_unknown_error)
        self.assertEqual("An unknown error occurred while running the operation [raise_unknown_error] against your Elasticsearch metrics "
                         "store on host [127.0.0.1] at port [9243].", ctx.exception.args[0])


class EsMetricsTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {"shard-count": 3})
        self.metrics_store = metrics.EsMetricsStore(self.cfg,
                                                    client_factory_class=MockClientFactory,
                                                    index_template_provider_class=DummyIndexTemplateProvider,
                                                    clock=StaticClock)
        # get hold of the mocked client...
        self.es_mock = self.metrics_store._client
        self.es_mock.exists.return_value = False

    def test_put_value_without_meta_info(self):
        throughput = 5000
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1

        self.metrics_store.put_count_cluster_level("indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append-no-conflicts",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", items=[expected_doc])

    def test_put_value_with_explicit_timestamps(self):
        throughput = 5000
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1

        self.metrics_store.put_count_cluster_level(name="indexing_throughput", count=throughput, unit="docs/s",
                                                   absolute_time=0, relative_time=10)
        expected_doc = {
            "@timestamp": 0,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 10000000,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append-no-conflicts",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", items=[expected_doc])

    def test_put_value_with_meta_info(self):
        throughput = 5000
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tag", "intention:testing,disk_type:hdd")
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_value_node_level("node0", "indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append-no-conflicts",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0"
            }
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", items=[expected_doc])

    def test_get_value(self):
        throughput = 5000
        search_result = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": StaticClock.NOW * 1000,
                            "value": throughput
                        }
                    }
                ]
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

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
                                "challenge": "append-no-conflicts"
                            }
                        },
                        {
                            "term": {
                                "car": "defaults"
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        },
                        {
                            "term": {
                                "lap": 3
                            }
                        }
                    ]
                }
            }
        }

        actual_throughput = self.metrics_store.get_one("indexing_throughput", lap=3)

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", body=expected_query)

        self.assertEqual(throughput, actual_throughput)

    def test_get_median(self):
        median_throughput = 30535
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "percentile_stats": {
                    "values": {
                        "50.0": median_throughput
                    }
                }
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

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
                                "challenge": "append-no-conflicts"
                            }
                        },
                        {
                            "term": {
                                "car": "defaults"
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        },
                        {
                            "term": {
                                "lap": 3
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "percentile_stats": {
                    "percentiles": {
                        "field": "value",
                        "percents": ["50.0"]
                    }
                }
            }
        }

        actual_median_throughput = self.metrics_store.get_median("indexing_throughput", lap=3)

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", body=expected_query)

        self.assertEqual(median_throughput, actual_median_throughput)

    def test_get_error_rate_implicit_zero(self):
        self.assertEqual(0.0, self._get_error_rate(buckets=[
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 0

            }
        ]))

    def test_get_error_rate_explicit_zero(self):
        self.assertEqual(0.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 0
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 500
            }
        ]))

    def test_get_error_rate_implicit_one(self):
        self.assertEqual(1.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 123
            }
        ]))

    def test_get_error_rate_explicit_one(self):
        self.assertEqual(1.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 123
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 0
            }
        ]))

    def test_get_error_rate_mixed(self):
        self.assertEqual(0.5, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 500
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 500
            }
        ]))

    def test_get_error_rate_additional_unknown_key(self):
        self.assertEqual(0.25, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 500
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 1500
            },
            {
                "key": 2,
                "key_as_string": "undefined_for_test",
                "doc_count": 13700
            }
        ]))

    def _get_error_rate(self, buckets):
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "error_rate": {
                    "buckets": buckets
                }
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

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
                                "challenge": "append-no-conflicts"
                            }
                        },
                        {
                            "term": {
                                "car": "defaults"
                            }
                        },
                        {
                            "term": {
                                "name": "service_time"
                            }
                        },
                        {
                            "term": {
                                "operation": "scroll_query"
                            }
                        },
                        {
                            "term": {
                                "lap": 3
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "error_rate": {
                    "terms": {
                        "field": "meta.success"
                    }
                }
            }
        }

        actual_error_rate = self.metrics_store.get_error_rate("scroll_query", lap=3)
        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="metrics", body=expected_query)
        return actual_error_rate


class EsRaceStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "time.start", EsRaceStoreTests.TRIAL_TIMESTAMP)
        self.race_store = metrics.EsRaceStore(self.cfg,
                                              client_factory_class=MockClientFactory,
                                              index_template_provider_class=DummyIndexTemplateProvider,
                                              )
        # get hold of the mocked client...
        self.es_mock = self.race_store.client

    def test_store_race(self):
        schedule = [
            track.Task("index #1", track.Operation("index", track.OperationType.Index))
        ]

        t = track.Track(name="unittest", description="unittest track",
                        source_root_url="http://example.org",
                        indices=[track.Index(name="tests", auto_managed=True, types=[track.Type(name="test-type", mapping={})])],
                        challenges=[track.Challenge(name="index", default=True, index_settings=None, schedule=schedule)])

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_timestamp=EsRaceStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tag="let-me-test", track=t, track_params={"shard-count": 3},
                            challenge=t.default_challenge, car="4gheap",
                            total_laps=12,
                            cluster=EsRaceStoreTests.DictHolder(
                                {
                                    "distribution-version": "5.0.0",
                                    "nodes": [
                                        {"node_name": "node0", "ip": "127.0.0.1", "plugins": ["analysis-icu", "x-pack"]}
                                    ]
                                }),
                            lap_results=[],
                            results=EsRaceStoreTests.DictHolder(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "operation": "index #1",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.race_store.store_race(race)

        expected_doc = {
            "rally-version": "0.4.4",
            "environment": "unittest",
            "trial-timestamp": "20160131T000000Z",
            "pipeline": "from-sources",
            "user-tag": "let-me-test",
            "track": "unittest",
            "track-params": {
                "shard-count": 3
            },
            "challenge": "index",
            "car": "4gheap",
            "total-laps": 12,
            "cluster": {
                "distribution-version": "5.0.0",
                "nodes": [
                    {
                        "node_name": "node0",
                        "ip": "127.0.0.1",
                        "plugins": ["analysis-icu", "x-pack"]
                    }
                ]
            },
            "results": {
                "young_gc_time": 100,
                "old_gc_time": 5,
                "op_metrics": [
                    {
                        "operation": "index #1",
                        "throughput": {
                            "min": 1000,
                            "median": 1250,
                            "max": 1500,
                            "unit": "docs/s"
                        }
                    }
                ]
            }
        }
        self.es_mock.index.assert_called_with(index="rally-races-2016-01", doc_type="races", item=expected_doc)


class EsResultsStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "system", "time.start", EsRaceStoreTests.TRIAL_TIMESTAMP)
        self.race_store = metrics.EsResultsStore(self.cfg,
                                                 client_factory_class=MockClientFactory,
                                                 index_template_provider_class=DummyIndexTemplateProvider,
                                                 )
        # get hold of the mocked client...
        self.es_mock = self.race_store.client

    def test_store_results(self):
        # here we need the real thing
        from esrally import reporter
        from esrally.mechanic import cluster

        schedule = [
            track.Task("index #1", track.Operation("index", track.OperationType.Index))
        ]

        t = track.Track(name="unittest-track", description="unittest track",
                        source_root_url="http://example.org",
                        indices=[track.Index(name="tests", auto_managed=True, types=[track.Type(name="test-type", mapping={})])],
                        challenges=[track.Challenge(name="index", default=True, index_settings=None, schedule=schedule)])

        c = cluster.Cluster([], [], None)
        c.distribution_version = "5.0.0"
        node = c.add_node("localhost", "rally-node-0")
        node.plugins.append("x-pack")

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_timestamp=EsResultsStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tag="let-me-test", track=t, track_params=None,
                            challenge=t.default_challenge, car="4gheap",
                            total_laps=12,
                            cluster=c,
                            lap_results=[],
                            results=reporter.Stats(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "operation": "index #1",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.race_store.store_results(race)

        expected_docs = [
            {
                "environment": "unittest",
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tag": "let-me-test",
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "old_gc_time",
                "value": {
                    "single": 5
                }
            },
            {
                "environment": "unittest",
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tag": "let-me-test",
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "throughput",
                "operation": "index #1",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s"
                }
            },
            {
                "environment": "unittest",
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tag": "let-me-test",
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "young_gc_time",
                "value": {
                    "single": 100
                }
            }
        ]
        self.es_mock.bulk_index.assert_called_with(index="rally-results-2016-01", doc_type="results", items=expected_docs)


class InMemoryMetricsStoreTests(TestCase):
    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {})
        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)

    def tearDown(self):
        del self.metrics_store
        del self.cfg

    def test_get_value(self):
        throughput = 5000
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_count_cluster_level("indexing_throughput", 1, "docs/s", sample_type=metrics.SampleType.Warmup)
        self.metrics_store.put_count_cluster_level("indexing_throughput", throughput, "docs/s")
        self.metrics_store.put_count_cluster_level("final_index_size", 1000, "GB")

        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assertEqual(1, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(throughput, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Normal))

    def test_get_percentile(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assert_equal_percentiles("query_latency", [100.0], {100.0: 1000.0})
        self.assert_equal_percentiles("query_latency", [99.0], {99.0: 990.0})
        self.assert_equal_percentiles("query_latency", [99.9], {99.9: 999.0})
        self.assert_equal_percentiles("query_latency", [0.0], {0.0: 1.0})

        self.assert_equal_percentiles("query_latency", [99, 99.9, 100], {99: 990.0, 99.9: 999.0, 100: 1000.0})

    def test_get_median(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assertAlmostEqual(500.5, self.metrics_store.get_median("query_latency", lap=1))

    def assert_equal_percentiles(self, name, percentiles, expected_percentiles):
        actual_percentiles = self.metrics_store.get_percentiles(name, percentiles=percentiles)
        self.assertEqual(len(expected_percentiles), len(actual_percentiles))
        for percentile, actual_percentile_value in actual_percentiles.items():
            self.assertAlmostEqual(expected_percentiles[percentile], actual_percentile_value, places=1,
                                   msg=str(percentile) + "th percentile differs")

    def test_externalize_and_bulk_add(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_count_cluster_level("final_index_size", 1000, "GB")

        self.assertEqual(1, len(self.metrics_store.docs))
        memento = self.metrics_store.to_externalizable()

        self.metrics_store.close()
        del self.metrics_store

        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)
        self.assertEqual(0, len(self.metrics_store.docs))

        self.metrics_store.bulk_add(memento)
        self.assertEqual(1, len(self.metrics_store.docs))
        self.assertEqual(1000, self.metrics_store.get_one("final_index_size"))

    def test_meta_data_per_document(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "cluster-name", "test")

        self.metrics_store.put_count_cluster_level("final_index_size", 1000, "GB", meta_data={
            "fs-block-size-bytes": 512
        })
        self.metrics_store.put_count_cluster_level("final_bytes_written", 1, "TB", meta_data={
            "io-batch-size-kb": 4
        })

        self.assertEqual(2, len(self.metrics_store.docs))
        self.assertEqual({
            "cluster-name": "test",
            "fs-block-size-bytes": 512
        }, self.metrics_store.docs[0]["meta"])

        self.assertEqual({
            "cluster-name": "test",
            "io-batch-size-kb": 4
        }, self.metrics_store.docs[1]["meta"])

    def test_get_error_rate_zero_without_samples(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_by_sample_type(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Warmup,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assertEqual(1.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_mixed(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", operation="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.2, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))


class FileRaceStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setUp(self):
        import tempfile
        import uuid
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.races.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", FileRaceStoreTests.TRIAL_TIMESTAMP)
        self.race_store = metrics.FileRaceStore(self.cfg)

    def test_store_race(self):
        from esrally import time
        schedule = [
            track.Task("index #1", track.Operation("index", track.OperationType.Index))
        ]

        t = track.Track(name="unittest", description="unittest track",
                        source_root_url="http://example.org",
                        indices=[track.Index(name="tests", auto_managed=True, types=[track.Type(name="test-type", mapping={})])],
                        challenges=[track.Challenge(name="index", default=True, index_settings=None, schedule=schedule)])

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_timestamp=FileRaceStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tag="let-me-test", track=t, track_params={"clients": 12},
                            challenge=t.default_challenge, car="4gheap",
                            total_laps=12,
                            cluster=FileRaceStoreTests.DictHolder(
                                {
                                    "distribution-version": "5.0.0",
                                    "nodes": [
                                        {"node_name": "node0", "ip": "127.0.0.1"}
                                    ]
                                }),
                            lap_results=[],
                            results=FileRaceStoreTests.DictHolder(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "operation": "index #1",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.race_store.store_race(race)

        retrieved_race = self.race_store.find_by_timestamp(timestamp=time.to_iso8601(FileRaceStoreTests.TRIAL_TIMESTAMP))
        self.assertEqual(race.trial_timestamp, retrieved_race.trial_timestamp)
        self.assertEqual(1, len(self.race_store.list()))
