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
import json
from unittest import mock
from unittest.mock import call

from estracker import corpus


def serialize_doc(doc):
    return (json.dumps(doc, separators=(',', ':')) + "\n").encode("utf-8")


@mock.patch("os.stat")
@mock.patch("builtins.open", new_callable=mock.mock_open)
@mock.patch("elasticsearch.Elasticsearch")
def test_extract(client, mo, osstat):
    doc = {
        "field1": "stuff",
        "field2": "things"
    }
    doc_data = serialize_doc(doc)
    client.count.return_value = {
        "count": 1
    }
    client.search.return_value = {
        "_scroll_id": "uohialjrknf",
        "_shards": {
            "successful": 1,
            "total": 1
        },
        "hits": {
            "hits": [
                {
                    "_index": "test",
                    "_id": "0",
                    "_score": 0,
                    "_source": doc
                }
            ]
        }
    }
    client.scroll.return_value = {}

    index = "test"
    outdir = "/tracks/"
    res = corpus.extract(client, outdir, index)
    assert mo.call_count == 2
    mo.assert_has_calls([call("/tracks/test-documents.json", "wb"),
                         call("/tracks/test-documents.json.bz2", "wb")], any_order=True)

    file_mock = mo.return_value
    file_mock.assert_has_calls([call.write(doc_data)])
