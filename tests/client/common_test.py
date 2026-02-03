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

from esrally.client import common
from esrally.utils.cases import cases


@dataclasses.dataclass
class CompatibilityModeCase:
    version: str | None
    flavour: str | None
    want: int | None


@cases(
    serverless_flavor=CompatibilityModeCase(version="8.0.0", flavour="serverless", want=None),
    version_8=CompatibilityModeCase(version="8.0.0", flavour=None, want=8),
    version_9=CompatibilityModeCase(version="9.1.0", flavour=None, want=9),
    version_7=CompatibilityModeCase(version="7.17.0", flavour=None, want=7),
    no_version=CompatibilityModeCase(version=None, flavour=None, want=None),
    empty_version=CompatibilityModeCase(version="", flavour=None, want=None),
    invalid_version=CompatibilityModeCase(version="invalid", flavour=None, want=None),
    default_flavor=CompatibilityModeCase(version="8.2.0", flavour="default", want=8),
)
def test_compatibility_mode_from_distribution(case: CompatibilityModeCase) -> None:
    got = common.compatibility_mode_from_distribution(version=case.version, flavour=case.flavour)
    assert got == case.want


@dataclasses.dataclass
class EnsureMimetypeHeadersCase:
    headers: dict[str, str] | None
    path: str | None
    body: str | None
    compatibility_mode: int | None
    want_content_type: str | None = None
    want_accept: str | None = None


@cases(
    no_body_empty_headers=EnsureMimetypeHeadersCase(
        headers=None,
        path=None,
        body=None,
        compatibility_mode=None,
        want_content_type=None,
        want_accept=None,
    ),
    body_sets_json=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body="{}",
        compatibility_mode=None,
        want_content_type="application/json",
        want_accept="application/json",
    ),
    body_bulk_sets_ndjson=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_bulk",
        body='{"index":{}}\n',
        compatibility_mode=None,
        want_content_type="application/x-ndjson",
        want_accept="application/x-ndjson",
    ),
    body_bulk_path_suffix=EnsureMimetypeHeadersCase(
        headers=None,
        path="/my_index/_bulk",
        body="{}",
        compatibility_mode=None,
        want_content_type="application/x-ndjson",
        want_accept="application/x-ndjson",
    ),
    compatibility_mode_rewrites=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body="{}",
        compatibility_mode=8,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_bulk=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_bulk",
        body="{}",
        compatibility_mode=9,
        want_content_type="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
        want_accept="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
    ),
    existing_headers_preserved=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json", "accept": "application/json"},
        path="/_cluster/health",
        body="{}",
        compatibility_mode=None,
        want_content_type="application/json",
        want_accept="application/json",
    ),
    case_insensitive_headers=EnsureMimetypeHeadersCase(
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        path="/_cluster/health",
        body="{}",
        compatibility_mode=8,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_skips_missing_headers=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body=None,
        compatibility_mode=8,
        want_content_type=None,
        want_accept=None,
    ),
    compatibility_mode_rewrites_only_present_accept=EnsureMimetypeHeadersCase(
        headers={"accept": "application/json"},
        path="/_cluster/health",
        body=None,
        compatibility_mode=8,
        want_content_type=None,
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_rewrites_only_present_content_type=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json"},
        path="/_cluster/health",
        body=None,
        compatibility_mode=8,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept=None,
    ),
)
def test_ensure_mimetype_headers(case: EnsureMimetypeHeadersCase) -> None:
    result = common.ensure_mimetype_headers(
        headers=case.headers,
        path=case.path,
        body=case.body,
        compatibility_mode=case.compatibility_mode,
    )
    if case.want_content_type is not None:
        assert result.get("content-type") == case.want_content_type
    if case.want_accept is not None:
        assert result.get("accept") == case.want_accept
