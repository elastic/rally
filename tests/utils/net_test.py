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
import random
from unittest import mock

import pytest
import urllib3.exceptions
from werkzeug.wrappers import Response

from esrally.utils import net


class TestNetUtils:
    # Mocking boto3 objects directly is too complex so we keep all code in a helper function and mock this instead
    @pytest.mark.parametrize("seed", range(1))
    @mock.patch("esrally.utils.net._download_from_s3_bucket")
    def test_download_from_s3_bucket(self, download, seed):
        random.seed(seed)
        expected_size = random.choice([None, random.randint(0, 1000)])
        progress_indicator = random.choice([None, "some progress indicator"])

        net.download_from_bucket(
            "s3", "s3://mybucket.elasticsearch.org/data/documents.json.bz2", "/tmp/documents.json.bz2", expected_size, progress_indicator
        )
        download.assert_called_once_with(
            "mybucket.elasticsearch.org", "data/documents.json.bz2", "/tmp/documents.json.bz2", expected_size, progress_indicator
        )

    @mock.patch("esrally.utils.console.error")
    @mock.patch("esrally.utils.net._fake_import_boto3")
    def test_missing_boto3(self, import_boto3, console_error):
        import_boto3.side_effect = ImportError("no module named 'boto3'")
        with pytest.raises(ImportError, match="no module named 'boto3'"):
            net.download_from_bucket("s3", "s3://mybucket/data", "/tmp/data", None, None)
        console_error.assert_called_once_with("S3 support is optional. Install it with `python -m pip install esrally[s3]`")

    @pytest.mark.parametrize("seed", range(1))
    @mock.patch("esrally.utils.net._download_from_gcs_bucket")
    def test_download_from_gs_bucket(self, download, seed):
        random.seed(seed)
        expected_size = random.choice([None, random.randint(0, 1000)])
        progress_indicator = random.choice([None, "some progress indicator"])

        net.download_from_bucket(
            "gs", "gs://unittest-gcp-bucket.test.org/data/documents.json.bz2", "/tmp/documents.json.bz2", expected_size, progress_indicator
        )
        download.assert_called_once_with(
            "unittest-gcp-bucket.test.org", "data/documents.json.bz2", "/tmp/documents.json.bz2", expected_size, progress_indicator
        )

    @pytest.mark.parametrize("seed", range(40))
    def test_gcs_object_url(self, seed):
        random.seed(seed)
        bucket_name = random.choice(
            ["unittest-bucket.test.me", "/unittest-bucket.test.me", "/unittest-bucket.test.me/", "unittest-bucket.test.me/"]
        )
        bucket_path = random.choice(["path/to/object", "/path/to/object", "/path/to/object/", "path/to/object/"])

        # pylint: disable=protected-access
        assert (
            net._build_gcs_object_url(bucket_name, bucket_path)
            == "https://storage.googleapis.com/storage/v1/b/unittest-bucket.test.me/o/path%2Fto%2Fobject?alt=media"
        )

    def test_add_url_param_elastic_no_kpi(self):
        url = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.2.0.tar.gz"
        assert (
            net.add_url_param_elastic_no_kpi(url)
            == "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.2.0.tar.gz?x-elastic-no-kpi=true"
        )

    def test_add_url_param_elastic_no_kpi_no_http(self):
        url = "gs://my_bucket/builds/elasticsearch/elasticsearch-8.0.0-SNAPSHOT.tar.gz"
        assert net.add_url_param_elastic_no_kpi(url) == "gs://my_bucket/builds/elasticsearch/elasticsearch-8.0.0-SNAPSHOT.tar.gz"

    def test_add_url_param_encoding_and_update(self):
        url = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.2.0.tar.gz?flag1=true"
        params = {"flag1": "test me", "flag2": "test@me"}
        # pylint: disable=protected-access
        assert (
            net._add_url_param(url, params)
            == "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.2.0.tar.gz?flag1=test+me&flag2=test%40me"
        )

    def test_progress(self):
        progress = net.Progress("test")
        mock_progress = mock.Mock()
        progress.p = mock_progress
        progress(42, 100)
        assert mock_progress.print.called
        mock_progress.reset_mock()
        progress(42, None)
        assert mock_progress.print.called

    def test_s3_dependency(self):
        # pylint: disable=import-outside-toplevel,unused-import
        import boto3

        assert True

    def test_gcs_dependency(self):
        # pylint: disable=import-outside-toplevel,unused-import
        import google.auth

        assert True


def test_download_http(httpserver, tmp_path):
    data = b"x" * 10
    httpserver.expect_request("/foobar").respond_with_data(data)
    local_path = str(tmp_path / "file.txt")

    def raise_error(seconds):
        # Make sure we don't sleep in the success case
        raise ValueError()

    net.download_http(httpserver.url_for("/foobar"), local_path=local_path, sleep=raise_error)
    with open(local_path, "rb") as f:
        assert f.read() == data


def test_download_http_retry_incomplete_read_retry_failure(httpserver, tmp_path):
    data = b"x" * 10
    short_resp = Response(headers={"Content-Length": 100, "foo": "bar"})
    short_resp.automatically_set_content_length = False
    short_resp.set_data(data)

    httpserver.expect_request("/foobar").respond_with_response(short_resp)
    local_path = str(tmp_path / "file.txt")
    retries = 0

    def sleep(seconds):
        nonlocal retries
        retries += 1

    with pytest.raises(urllib3.exceptions.ProtocolError):
        net.download_http(httpserver.url_for("/foobar"), local_path=local_path, sleep=sleep)
    assert retries == 10


def test_download_http_retry_incomplete_read_retry_success(httpserver, tmp_path):
    data = b"x" * 10
    short_resp = Response(headers={"Content-Length": 100, "foo": "bar"})
    short_resp.automatically_set_content_length = False
    short_resp.set_data(data)

    # missing data for the first 10 tries
    for _i in range(10):
        httpserver.expect_ordered_request("/foobar").respond_with_response(short_resp)
    # finally, good content-length
    httpserver.expect_ordered_request("/foobar").respond_with_data(data)

    local_path = str(tmp_path / "file.txt")
    retries = 0

    def sleep(seconds):
        nonlocal retries
        retries += 1

    net.download_http(httpserver.url_for("/foobar"), local_path=local_path, sleep=sleep)
    with open(local_path, "rb") as f:
        assert f.read() == data
    assert retries == 10
