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

import elastic_transport
import pytest

from esrally.client import asynchronous
from esrally.utils.cases import cases


class TestResponseMatcher:
    def test_matches(self):
        matcher = asynchronous.ResponseMatcher(
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
            await asynchronous.RallyTCPConnector(
                limit_per_host=256, use_dns_cache=True, enable_cleanup_closed=True, client_id=i
            )._resolve_host("rally-dns-test.es.us-east-1.aws.found.io", 443)
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


@dataclasses.dataclass
class PerformRequestCase:
    distribution_version: str | None
    distribution_flavor: str | None
    method: str
    path: str
    body: str | None = None
    headers: dict[str, str] | None = None
    compatibility_mode: int | None = None
    want_headers: dict = dataclasses.field(default_factory=dict)


@cases(
    distribution_8_sets_compat_headers=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor=None,
        method="GET",
        path="/_cluster/health",
        body="{}",
        want_headers={
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
            "accept": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    ),
    distribution_9_bulk_sets_ndjson_compat=PerformRequestCase(
        distribution_version="9.1.0",
        distribution_flavor=None,
        method="POST",
        path="/_bulk",
        body='{"index":{}}\n',
        want_headers={
            "content-type": "application/vnd.elasticsearch+x-ndjson; compatible-with=9",
            "accept": "application/vnd.elasticsearch+x-ndjson; compatible-with=9",
        },
    ),
    serverless_uses_default_compat_mode=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor="serverless",
        method="GET",
        path="/",
        body="{}",
        want_headers={
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
            "accept": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    ),
    explicit_compatibility_mode_overrides=PerformRequestCase(
        distribution_version="9.0.0",
        distribution_flavor=None,
        method="GET",
        path="/",
        body="{}",
        compatibility_mode=8,
        want_headers={
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
            "accept": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    ),
    no_body_no_default_headers=PerformRequestCase(
        distribution_version=None,
        distribution_flavor=None,
        method="HEAD",
        path="/my_index/_doc/1",
    ),
    passthrough_method_path_params=PerformRequestCase(
        distribution_version="8.0.0",
        distribution_flavor=None,
        method="POST",
        path="/my_index/_doc",
        body='{"a":1}',
        headers={"x-custom": "value"},
        want_headers={
            "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
            "accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "x-custom": "value",
        },
    ),
)
@pytest.mark.asyncio
async def test_perform_request(
    case: PerformRequestCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_transport = mock.create_autospec(
        asynchronous.RallyAsyncTransport.perform_request,
        return_value=elastic_transport.TransportApiResponse(
            meta=elastic_transport.ApiResponseMeta(
                status=200,
                http_version="1.1",
                headers=elastic_transport.HttpHeaders(),
                node=elastic_transport.NodeConfig(scheme="http", host="localhost", port=9200),
                duration=0.0,
            ),
            body={},
        ),
    )
    monkeypatch.setattr(
        "esrally.client.asynchronous.RallyAsyncTransport.perform_request",
        mock_transport,
    )
    client = asynchronous.RallyAsyncElasticsearch(
        hosts=["http://localhost:9200"],
        distribution_version=case.distribution_version,
        distribution_flavor=case.distribution_flavor,
        transport_class=asynchronous.RallyAsyncTransport,
    )
    await client.perform_request(
        case.method,
        case.path,
        body=case.body,
        headers=case.headers,
        compatibility_mode=case.compatibility_mode,
    )

    mock_transport.assert_called_once()
    got_headers = mock_transport.call_args_list[0].kwargs.get("headers")
    assert got_headers == case.want_headers
