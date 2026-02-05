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
import contextlib
import dataclasses

import pytest

from esrally.client import common
from esrally.utils.cases import cases


@dataclasses.dataclass
class CompatibilityModeCase:
    version: str | int | None
    want: int | None = None
    want_error: type[Exception] | None = None


@cases(
    version_8=CompatibilityModeCase(version="8.0.0", want=8),
    version_9=CompatibilityModeCase(version="9.1.0", want=9),
    version_7_raises=CompatibilityModeCase(version="7.17.0", want_error=ValueError),
    version_int_7_raises=CompatibilityModeCase(version=7, want_error=ValueError),
    version_int_8=CompatibilityModeCase(version=8, want=8),
    version_int_9=CompatibilityModeCase(version=9, want=9),
    version_int_10=CompatibilityModeCase(version=10, want=10, want_error=ValueError),
    no_version=CompatibilityModeCase(version=None, want=8),
    empty_version_raises=CompatibilityModeCase(version="", want_error=ValueError),
    invalid_version_raises=CompatibilityModeCase(version="invalid", want_error=ValueError),
)
def test_get_compatibility_mode(case: CompatibilityModeCase) -> None:
    if case.want_error is not None:
        with pytest.raises(case.want_error):
            common.get_compatibility_mode(version=case.version)
    else:
        got = common.get_compatibility_mode(version=case.version)
        assert got == case.want


@dataclasses.dataclass
class EnsureMimetypeHeadersCase:
    headers: dict[str, str] | None
    path: str | None
    body: str | None
    version: str | int | None
    want_content_type: str | None = None
    want_accept: str | None = None
    want_warning_message: str | None = None


@cases(
    no_body_empty_headers=EnsureMimetypeHeadersCase(
        headers=None,
        path=None,
        body=None,
        version=None,
    ),
    body_sets_json=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body="{}",
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    body_bulk_sets_ndjson=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_bulk",
        body='{"index":{}}\n',
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+x-ndjson; compatible-with=8",
        want_accept="application/vnd.elasticsearch+x-ndjson; compatible-with=8",
    ),
    body_bulk_path_suffix=EnsureMimetypeHeadersCase(
        headers=None,
        path="/my_index/_bulk",
        body="{}",
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+x-ndjson; compatible-with=8",
        want_accept="application/vnd.elasticsearch+x-ndjson; compatible-with=8",
    ),
    compatibility_mode_rewrites=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body="{}",
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_bulk=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_bulk",
        body="{}",
        version="9.1.0",
        want_content_type="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
        want_accept="application/vnd.elasticsearch+x-ndjson; compatible-with=9",
    ),
    existing_headers_preserved=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json", "accept": "application/json"},
        path="/_cluster/health",
        body="{}",
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    case_insensitive_headers=EnsureMimetypeHeadersCase(
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        path="/_cluster/health",
        body="{}",
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_skips_missing_headers=EnsureMimetypeHeadersCase(
        headers=None,
        path="/_cluster/health",
        body=None,
        version=None,
    ),
    compatibility_mode_rewrites_only_present_accept=EnsureMimetypeHeadersCase(
        headers={"accept": "application/json"},
        path="/_cluster/health",
        body=None,
        version="8.0.0",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    compatibility_mode_rewrites_only_present_content_type=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json"},
        path="/_cluster/health",
        body=None,
        version="8.0.0",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    unsupported_version_int=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json"},
        path="/",
        body="{}",
        version=7,
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    unsupported_version_str=EnsureMimetypeHeadersCase(
        headers={"accept": "application/json"},
        path="/",
        body=None,
        version="7.0.0",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
    ),
    valid_version_no_warning=EnsureMimetypeHeadersCase(
        headers={"content-type": "application/json"},
        path="/",
        body="{}",
        version="8.0.0",
        want_accept="application/vnd.elasticsearch+json; compatible-with=8",
        want_content_type="application/vnd.elasticsearch+json; compatible-with=8",
    ),
)
def test_ensure_mimetype_headers(case: EnsureMimetypeHeadersCase) -> None:
    if case.want_warning_message is not None:
        catch_warnings = pytest.warns(Warning, match=case.want_warning_message)
    else:
        catch_warnings = contextlib.nullcontext()

    with catch_warnings as warnings:
        got = common.ensure_mimetype_headers(
            headers=case.headers,
            path=case.path,
            body=case.body,
            version=case.version,
        )

    assert got.get("content-type") == case.want_content_type
    assert got.get("accept") == case.want_accept
    if case.want_warning_message is not None:
        assert len(warnings) == 1
        assert str(warnings[0].message) == case.want_warning_message
    else:
        assert warnings is None
