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

import dataclasses
import json
import math
import random
from unittest import mock

import pytest
from elasticsearch import AsyncElasticsearch

from esrally.client.asynchronous import (
    RallyAsyncElasticsearch,
    RallyTCPConnector,
    ResponseMatcher,
)
from esrally.utils.cases import cases


class TestResponseMatcher:
    def test_matches(self):
        matcher = ResponseMatcher(
            responses=[
                {
                    "path": "*/_bulk",
                    "body": {
                        "response-type": "bulk",
                    },
                },
                {
                    "path": "/_cluster/health*",
                    "body": {
                        "response-type": "cluster-health",
                    },
                },
                {"path": "*", "body": {"response-type": "default"}},
            ]
        )

        self.assert_response_type(matcher, "/_cluster/health", "cluster-health")
        self.assert_response_type(matcher, "/_cluster/health/geonames", "cluster-health")
        self.assert_response_type(matcher, "/geonames/_bulk", "bulk")
        self.assert_response_type(matcher, "/geonames", "default")
        self.assert_response_type(matcher, "/geonames/force_merge", "default")

    def assert_response_type(self, matcher, path, expected_response_type):
        response = json.loads(matcher.response(path))
        assert response["response-type"] == expected_response_type


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_of_dns_resp, num_clients",
    [(1, 256), (2, 128), (3, 1), (3, 64), (4, 16), (3, 2000)],
)
@mock.patch("esrally.client.asynchronous.aiohttp.ThreadedResolver.resolve")
async def test_resolve_host_even_client_allocation(
    mocked_resolver,
    num_of_dns_resp,
    num_clients,
):
    def generate_dns_response(num_of_dns_resp):
        dns_resp = []
        for _ in range(num_of_dns_resp):
            dns_resp.append(
                {
                    "hostname": "rally-dns-test.es.us-east-1.aws.found.io",
                    "host": ".".join([str(random.randint(0, 255)) for _ in range(4)]),
                    "port": 443,
                    "family": "test-family",
                    "proto": 6,
                    "flags": "test-flag",
                },
            )
        return dns_resp

    dns_responses = generate_dns_response(num_of_dns_resp)
    mocked_resolver.return_value = dns_responses

    hostinfo = []
    for i in range(num_clients):
        hostinfo.append(
            # pylint: disable=protected-access
            await RallyTCPConnector(limit_per_host=256, use_dns_cache=True, enable_cleanup_closed=True, client_id=i)._resolve_host(
                "rally-dns-test.es.us-east-1.aws.found.io", 443
            )
        )

    first_host_per_client = []
    for host in hostinfo:
        # for each host extract the first 'host' resolved as that's what will be used to establish a connection
        first_host_per_client.append(host[0]["host"])

    # count the distribution of the allocation
    ip_alloc = {}
    for ip in first_host_per_client:
        if ip in ip_alloc:
            ip_alloc[ip] += 1
        else:
            ip_alloc[ip] = 1

    # maximum and minimum number of clients a single ip should have
    upper_bound = math.ceil(num_clients / num_of_dns_resp)
    lower_bound = math.floor(num_clients / num_of_dns_resp)

    for num_of_clients in ip_alloc.values():
        assert num_of_clients == upper_bound or lower_bound

    assert sum(ip_alloc.values()) == num_clients


def _make_response():
    return mock.MagicMock()


@dataclasses.dataclass
class PerformRequestCase:
    distribution_version: str | None
    distribution_flavor: str | None
    method: str
    path: str
    body: str | None
    headers: dict[str, str] | None
    compatibility_mode: int | None
    want_content_type: str | None = None
    want_accept: str | None = None


@cases(
    distribution_8_sets_compat_headers=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor=None,
        method="GET",
        path="/_cluster/health",
        body="{}",
        headers=None,
        compatibility_mode=None,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    distribution_9_bulk_sets_ndjson_compat=PerformRequestCase(
        distribution_version="9.1.0",
        distribution_flavor=None,
        method="POST",
        path="/_bulk",
        body='{"index":{}}\n',
        headers=None,
        compatibility_mode=None,
        want_content_type="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
        want_accept="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
    ),
    serverless_no_compat_rewrite=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor="serverless",
        method="GET",
        path="/",
        body="{}",
        headers=None,
        compatibility_mode=None,
        want_content_type="application/json",
        want_accept="application/json",
    ),
    explicit_compatibility_mode_overrides=PerformRequestCase(
        distribution_version="9.0.0",
        distribution_flavor=None,
        method="GET",
        path="/",
        body="{}",
        headers=None,
        compatibility_mode=8,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    no_body_no_default_headers=PerformRequestCase(
        distribution_version=None,
        distribution_flavor=None,
        method="HEAD",
        path="/my_index/_doc/1",
        body=None,
        headers=None,
        compatibility_mode=None,
        want_content_type=None,
        want_accept=None,
    ),
    passthrough_method_path_params=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor=None,
        method="POST",
        path="/my_index/_doc",
        body='{"a":1}',
        headers={"x-custom": "value"},
        compatibility_mode=None,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
)
@pytest.mark.asyncio
async def test_perform_request(
    case: PerformRequestCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # instance=True is not allowed for async functions in create_autospec (Python 3.13)
    mock_super = mock.create_autospec(AsyncElasticsearch.perform_request, instance=False)
    mock_super.return_value = mock.AsyncMock(return_value=_make_response())
    monkeypatch.setattr(
        "esrally.client.asynchronous.AsyncElasticsearch.perform_request",
        mock_super,
    )
    client = RallyAsyncElasticsearch(
        hosts=["http://localhost:9200"],
        distribution_version=case.distribution_version,
        distribution_flavor=case.distribution_flavor,
    )
    await client.perform_request(
        case.method,
        case.path,
        body=case.body,
        headers=case.headers,
        compatibility_mode=case.compatibility_mode,
    )
    mock_super.assert_called_once_with(
        client,
        case.method,
        case.path,
        params=None,
        headers=mock.ANY,
        body=case.body,
        endpoint_id=None,
        path_parts=None,
    )
    passed_headers = mock_super.call_args[1]["headers"]
    if case.want_content_type is not None:
        assert passed_headers.get("content-type") == case.want_content_type
    if case.want_accept is not None:
        assert passed_headers.get("accept") == case.want_accept
    if case.headers:
        for key, value in case.headers.items():
            if key.lower() not in ("content-type", "accept"):
                assert passed_headers.get(key) == value
