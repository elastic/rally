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
from unittest import mock
from unittest.mock import call

from esrally.tracker import corpus


def serialize_doc(doc):
    return (json.dumps(doc, separators=(",", ":")) + "\n").encode("utf-8")


@mock.patch("builtins.open", new_callable=mock.mock_open)
@mock.patch("elasticsearch.Elasticsearch")
def test_extract(client, mo):
    doc = {"field1": "stuff", "field2": "things"}
    doc_data = serialize_doc(doc)
    client.count.return_value = {"count": 1001}
    # the scan helper calls client.options(), which returns a new client instance
    # we override this behavior here to facilitate mocking
    client.options.return_value = client
    client.search.return_value = {
        "_scroll_id": "uohialjrknf",
        "_shards": {
            "successful": 1,
            "total": 1,
            "skipped": 0,
        },
        "hits": {
            "hits": [
                {
                    "_index": "test",
                    "_id": "0",
                    "_score": 0,
                    "_source": doc,
                },
            ],
        },
    }

    def set_corp_size(*args, **kwargs):
        path = args[0]
        mockstat = mock.Mock()
        if ".bz2" in path:
            mockstat.st_size = 500
        else:
            mockstat.st_size = 1000
        return mockstat

    client.scroll.return_value = {}

    index = "test"
    outdir = "/abs/outpath/to/tracks/"

    with mock.patch("os.stat") as osstat:
        osstat.side_effect = set_corp_size
        res = corpus.extract(client, outdir, index)
    assert mo.call_count == 4
    mo.assert_has_calls(
        [
            call("/abs/outpath/to/tracks/test-documents.json", "wb"),
            call("/abs/outpath/to/tracks/test-documents.json.bz2", "wb"),
            call("/abs/outpath/to/tracks/test-documents-1k.json", "wb"),
            call("/abs/outpath/to/tracks/test-documents-1k.json.bz2", "wb"),
        ],
        any_order=True,
    )

    assert res == {
        "filename": "test-documents.json.bz2",
        "path": "/abs/outpath/to/tracks/test-documents.json.bz2",
        "compressed_bytes": 500,
        "index_name": "test",
        "doc_count": 1001,
        "uncompressed_bytes": 1000,
    }

    file_mock = mo.return_value
    file_mock.assert_has_calls([call.write(doc_data)])
