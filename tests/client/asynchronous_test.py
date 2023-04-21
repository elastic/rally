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

import json
import math
import random
from unittest import mock

import pytest

from esrally.client.asynchronous import RallyTCPConnector, ResponseMatcher


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


class TestResolveHost:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "num_of_dns_resp, num_clients",
        [(1, 256), (2, 128), (3, 1), (3, 64), (4, 16), (3, 2000)],
    )
    @mock.patch("esrally.client.asynchronous.aiohttp.ThreadedResolver.resolve")
    async def test_resolve_host_even_client_allocation(
        self,
        mocked_resolver,
        num_of_dns_resp,
        num_clients,
    ):
        """
        TCPConnector._resolve_host() returns all the IPs a given name resolves to, but the underlying connection but the
        logic in TCPConnector._create_direct_connection() only ever selects the first succesful host from this list.

        So, we use the factory assigned client_id to deterministically return an IP and then place it at the beginning
        of the list to evenly distributes connections across _all_ clients.

        https://github.com/elastic/rally/issues/1598
        """

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
