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

import pytest
from elasticsearch import Elasticsearch

from esrally.client.synchronous import RallySyncElasticsearch
from esrally.utils.cases import cases


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
def test_perform_request(
    case: PerformRequestCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_super = mock.create_autospec(Elasticsearch.perform_request, instance=True)
    mock_super.return_value = _make_response()
    monkeypatch.setattr(
        "esrally.client.synchronous.Elasticsearch.perform_request",
        mock_super,
    )
    client = RallySyncElasticsearch(
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
