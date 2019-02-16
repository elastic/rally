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

import os
import datetime
import logging
import unittest.mock as mock
import random
import string
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


class TransportErrors:
    err_return_codes = {502: "Bad Gateway",
                        503: "Service Unavailable",
                        504: "Gateway Timeout",
                        429: "Too Many Requests"}

    def __init__(self, max_err_responses=10):
        self.max_err_responses = max_err_responses
        # allow duplicates in list of error codes
        self.rnd_err_codes = [
            random.choice(list(TransportErrors.err_return_codes))
            for _ in range(self.max_err_responses)
        ]

    @property
    def code_list(self):
        return self.rnd_err_codes

    @property
    def side_effects(self):
        side_effect_list = [
            elasticsearch.exceptions.TransportError(rnd_code, TransportErrors.err_return_codes[rnd_code])
            for rnd_code in self.rnd_err_codes
        ]
        side_effect_list.append("success")

        return side_effect_list


class ExtractUserTagsTests(TestCase):
    def test_no_tags_returns_empty_dict(self):
        cfg = config.Config()
        self.assertEqual(0, len(metrics.extract_user_tags_from_config(cfg)))

    def test_missing_comma_raises_error(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "race", "user.tag", "invalid")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            metrics.extract_user_tags_from_config(cfg)
        self.assertEqual("User tag keys and values have to separated by a ':'. Invalid value [invalid]", ctx.exception.args[0])

    def test_missing_value_raises_error(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "race", "user.tag", "invalid1,invalid2")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            metrics.extract_user_tags_from_config(cfg)
        self.assertEqual("User tag keys and values have to separated by a ':'. Invalid value [invalid1,invalid2]", ctx.exception.args[0])

    def test_extracts_proper_user_tags(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "race", "user.tag", "os:Linux,cpu:ARM")
        self.assertDictEqual({"os": "Linux", "cpu": "ARM"}, metrics.extract_user_tags_from_config(cfg))


class EsClientTests(TestCase):
    class TransportMock:
        def __init__(self, hosts):
            self.hosts = hosts

    class ClientMock:
        def __init__(self, hosts):
            self.transport = EsClientTests.TransportMock(hosts)

    @mock.patch("esrally.client.EsClientFactory")
    def test_config_opts_parsing(self, client_esclientfactory):
        cfg = config.Config()

        _datastore_host = ".".join([str(random.randint(1,254)) for _ in range(4)])
        _datastore_port = random.randint(1024,65535)
        _datastore_secure = random.choice(["True", "true"])
        _datastore_user = "".join([random.choice(string.ascii_letters) for _ in range(8)])
        _datastore_password = "".join([random.choice(string.ascii_letters + string.digits + "_-@#$/") for _ in range(12)])
        _datastore_verify_certs = random.choice([True, False])

        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.user", _datastore_user)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.password", _datastore_password)
        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.ssl.verification_mode", "none")

        f = metrics.EsClientFactory(cfg)
        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "basic_auth_user": _datastore_user,
            "basic_auth_password": _datastore_password,
            "verify_certs": _datastore_verify_certs
        }

        client_esclientfactory.assert_called_with(
            hosts=[{"host": _datastore_host, "port": _datastore_port}],
            client_options=expected_client_options
        )

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

    def test_retries_on_various_transport_errors(self):
        @mock.patch("random.random")
        @mock.patch("esrally.time.sleep")
        def test_transport_error_retries(side_effect, expected_logging_calls, expected_sleep_calls, mocked_sleep, mocked_random):
            # should return on first success
            operation = mock.Mock(side_effect=side_effect)

            # Disable additional randomization time in exponential backoff calls
            mocked_random.return_value = 0

            client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

            logger = logging.getLogger("esrally.metrics")
            with mock.patch.object(logger, "debug") as mocked_debug_logger:
                test_result = client.guarded(operation)
                mocked_sleep.assert_has_calls(expected_sleep_calls)
                mocked_debug_logger.assert_has_calls(
                    expected_logging_calls,
                    any_order=True
                )
                self.assertEqual("success", test_result)

        max_retry = 10
        all_err_codes = TransportErrors.err_return_codes
        transport_errors = TransportErrors(max_err_responses=max_retry)
        rnd_err_codes = transport_errors.code_list
        rnd_side_effects = transport_errors.side_effects
        rnd_mocked_logger_calls = []

        # The sec to sleep for 10 transport errors is
        # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] ~> 17.05min in total
        sleep_slots = [float(2**i) for i in range(0, max_retry)]
        mocked_sleep_calls = [mock.call(sleep_slots[i]) for i in range(0, max_retry)]

        for rnd_err_idx, rnd_err_code in enumerate(rnd_err_codes):
            # List of logger.debug calls to expect
            rnd_mocked_logger_calls.append(
                mock.call("%s (code: %d) in attempt [%d/%d]. Sleeping for [%f] seconds.",
                          all_err_codes[rnd_err_code], rnd_err_code,
                          rnd_err_idx+1, max_retry+1, sleep_slots[rnd_err_idx])
            )

        test_transport_error_retries(rnd_side_effects,
                                     rnd_mocked_logger_calls,
                                     mocked_sleep_calls)

    @mock.patch("esrally.time.sleep")
    def test_fails_after_too_many_errors(self, mocked_sleep):
        def random_transport_error(rnd_resp_code):
            raise elasticsearch.exceptions.TransportError(rnd_resp_code, TransportErrors.err_return_codes[rnd_resp_code])

        client = metrics.EsClient(EsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))
        rnd_code = random.choice(list(TransportErrors.err_return_codes))

        with self.assertRaises(exceptions.RallyError) as ctx:
            client.guarded(random_transport_error, rnd_code)

        self.assertEqual("A transport error occurred while running the operation "
                         "[random_transport_error] against your Elasticsearch metrics "
                         "store on host [127.0.0.1] at port [9243].",
                         ctx.exception.args[0])


class EsMetricsTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TRIAL_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

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
        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append", "defaults", create=True)
        self.metrics_store.lap = 1

        self.metrics_store.put_count_cluster_level("indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "trial-id": EsMetricsTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_value_with_explicit_timestamps(self):
        throughput = 5000
        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append", "defaults", create=True)
        self.metrics_store.lap = 1

        self.metrics_store.put_count_cluster_level(name="indexing_throughput", count=throughput, unit="docs/s",
                                                   absolute_time=0, relative_time=10)
        expected_doc = {
            "@timestamp": 0,
            "trial-id": EsMetricsTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 10000000,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_value_with_meta_info(self):
        throughput = 5000
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tag", "intention:testing,disk_type:hdd")
        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append", "defaults", create=True)
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
            "trial-id": EsMetricsTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append",
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
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_doc_no_meta_data(self):
        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append", "defaults", create=True)
        self.metrics_store.lap = 1

        self.metrics_store.put_doc(doc={
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        })
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "trial-id": EsMetricsTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append",
            "car": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_doc_with_metadata(self):
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tag", "intention:testing,disk_type:hdd")
        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append", "defaults", create=True)
        self.metrics_store.lap = 1

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_doc(doc={
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        }, level=metrics.MetaInfoScope.node,
            node_name="node0",
            meta_data={
                "node_type": "hot"
            })
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "trial-id": EsMetricsTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "track": "test",
            "track-params": {
                "shard-count": 3
            },
            "lap": 1,
            "challenge": "append",
            "car": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0",
                "node_type": "hot"
            }
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", items=[expected_doc])

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

        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "trial-id": EsMetricsTests.TRIAL_ID
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

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", body=expected_query)

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

        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "trial-id": EsMetricsTests.TRIAL_ID
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

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", body=expected_query)

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

        self.metrics_store.open(EsMetricsTests.TRIAL_ID, EsMetricsTests.TRIAL_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "trial-id": EsMetricsTests.TRIAL_ID
                            }
                        },
                        {
                            "term": {
                                "name": "service_time"
                            }
                        },
                        {
                            "term": {
                                "task": "scroll_query"
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
        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", doc_type="_doc", body=expected_query)
        return actual_error_rate


class EsRaceStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TRIAL_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

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
            track.Task("index #1", track.Operation("index", track.OperationType.Bulk))
        ]

        t = track.Track(name="unittest",
                        indices=[track.Index(name="tests", types=["test-type"])],
                        challenges=[track.Challenge(name="index", default=True, schedule=schedule)])

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_id=EsRaceStoreTests.TRIAL_ID,
                            trial_timestamp=EsRaceStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, track=t, track_params={"shard-count": 3},
                            challenge=t.default_challenge, car="defaults", car_params={"heap_size": "512mb"}, plugin_params=None,
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
                                            "task": "index #1",
                                            "operation": "index",
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
            "trial-id": EsRaceStoreTests.TRIAL_ID,
            "trial-timestamp": "20160131T000000Z",
            "pipeline": "from-sources",
            "user-tags": {
                "os": "Linux"
            },
            "track": "unittest",
            "track-params": {
                "shard-count": 3
            },
            "challenge": "index",
            "car": "defaults",
            "car-params": {
                "heap_size": "512mb"
            },
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
                        "task": "index #1",
                        "operation": "index",
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
        self.es_mock.index.assert_called_with(index="rally-races-2016-01", doc_type="_doc", item=expected_doc)


class EsResultsStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TRIAL_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

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
            track.Task("index #1", track.Operation("index", track.OperationType.Bulk))
        ]

        t = track.Track(name="unittest-track",
                        indices=[track.Index(name="tests", types=["test-type"])],
                        challenges=[track.Challenge(name="index", default=True, schedule=schedule)])

        c = cluster.Cluster([], [], None)
        c.distribution_version = "5.0.0"
        node = c.add_node("localhost", "rally-node-0")
        node.plugins.append("x-pack")

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_id=EsResultsStoreTests.TRIAL_ID,
                            trial_timestamp=EsResultsStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, track=t, track_params=None,
                            challenge=t.default_challenge, car="4gheap", car_params=None, plugin_params={"some-param": True},
                            total_laps=12,
                            cluster=c,
                            lap_results=[],
                            results=reporter.Stats(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "task": "index #1",
                                            "operation": "index",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ],
                                    "node_metrics": [
                                        {
                                            "node": "rally-node-0",
                                            "startup_time": 3.4
                                        }
                                    ]
                                })
                            )

        self.race_store.store_results(race)

        expected_docs = [
            {
                "rally-version": "0.4.4",
                "environment": "unittest",
                "trial-id": EsResultsStoreTests.TRIAL_ID,
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "old_gc_time",
                "value": {
                    "single": 5
                }
            },
            {
                "rally-version": "0.4.4",
                "environment": "unittest",
                "trial-id": EsResultsStoreTests.TRIAL_ID,
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "node": "rally-node-0",
                "name": "startup_time",
                "value": {
                    "single": 3.4
                },
            },
            {
                "rally-version": "0.4.4",
                "environment": "unittest",
                "trial-id": EsResultsStoreTests.TRIAL_ID,
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "throughput",
                "task": "index #1",
                "operation": "index",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s"
                }
            },
            {
                "rally-version": "0.4.4",
                "environment": "unittest",
                "trial-id": EsResultsStoreTests.TRIAL_ID,
                "trial-timestamp": "20160131T000000Z",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "track": "unittest-track",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "node-count": 1,
                "plugins": ["x-pack"],
                "active": True,
                "name": "young_gc_time",
                "value": {
                    "single": 100
                }
            }
        ]
        self.es_mock.bulk_index.assert_called_with(index="rally-results-2016-01", doc_type="_doc", items=expected_docs)


class InMemoryMetricsStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TRIAL_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

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
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_count_cluster_level("indexing_throughput", 1, "docs/s", sample_type=metrics.SampleType.Warmup)
        self.metrics_store.put_count_cluster_level("indexing_throughput", throughput, "docs/s")
        self.metrics_store.put_count_cluster_level("final_index_size", 1000, "GB")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(1, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(throughput, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Normal))

    def test_get_percentile(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assert_equal_percentiles("query_latency", [100.0], {100.0: 1000.0})
        self.assert_equal_percentiles("query_latency", [99.0], {99.0: 990.0})
        self.assert_equal_percentiles("query_latency", [99.9], {99.9: 999.0})
        self.assert_equal_percentiles("query_latency", [0.0], {0.0: 1.0})

        self.assert_equal_percentiles("query_latency", [99, 99.9, 100], {99: 990.0, 99.9: 999.0, 100: 1000.0})

    def test_get_median(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertAlmostEqual(500.5, self.metrics_store.get_median("query_latency", lap=1))

    def assert_equal_percentiles(self, name, percentiles, expected_percentiles):
        actual_percentiles = self.metrics_store.get_percentiles(name, percentiles=percentiles)
        self.assertEqual(len(expected_percentiles), len(actual_percentiles))
        for percentile, actual_percentile_value in actual_percentiles.items():
            self.assertAlmostEqual(expected_percentiles[percentile], actual_percentile_value, places=1,
                                   msg=str(percentile) + "th percentile differs")

    def test_externalize_and_bulk_add(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
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
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
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
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_by_sample_type(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Warmup,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(1.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_mixed(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.lap = 1
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TRIAL_ID, InMemoryMetricsStoreTests.TRIAL_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.2, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))


class FileRaceStoreTests(TestCase):
    TRIAL_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TRIAL_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

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
            track.Task("index #1", track.Operation("index", track.OperationType.Bulk))
        ]

        t = track.Track(name="unittest",
                        indices=[track.Index(name="tests", types=["test-type"])],
                        challenges=[track.Challenge(name="index", default=True, schedule=schedule)])

        race = metrics.Race(rally_version="0.4.4", environment_name="unittest", trial_id=FileRaceStoreTests.TRIAL_ID,
                            trial_timestamp=FileRaceStoreTests.TRIAL_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, track=t, track_params={"clients": 12},
                            challenge=t.default_challenge, car="4gheap", car_params=None, plugin_params=None,
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
                                            "task": "index #1",
                                            "operation": "index",
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
