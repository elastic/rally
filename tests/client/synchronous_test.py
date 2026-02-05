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
from unittest import mock

import elastic_transport
import pytest

from esrally.client import synchronous
from esrally.utils.cases import cases


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
        elastic_transport.Transport.perform_request,
        return_value=elastic_transport.TransportApiResponse(
            meta=elastic_transport.ApiResponseMeta(
                status=200,
                http_version="1.1",
                headers=elastic_transport.HttpHeaders(
                    {
                        "X-Elastic-Product": "Elasticsearch",
                    }
                ),
                node=elastic_transport.NodeConfig(scheme="http", host="localhost", port=9200),
                duration=0.0,
            ),
            body={},
        ),
    )
    monkeypatch.setattr(elastic_transport.Transport, "perform_request", mock_transport)
    client = synchronous.RallySyncElasticsearch(
        hosts=["http://localhost:9200"],
        distribution_version=case.distribution_version,
        distribution_flavor=case.distribution_flavor,
    )
    client.perform_request(
        case.method,
        case.path,
        body=case.body,
        headers=case.headers,
        compatibility_mode=case.compatibility_mode,
    )

    mock_transport.assert_called_once()
    got_headers = mock_transport.call_args_list[0].kwargs.get("headers")
    assert got_headers == case.want_headers
