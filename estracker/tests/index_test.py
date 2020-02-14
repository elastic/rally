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

from unittest import mock
from estracker.index import filter_ephemeral_index_settings, extract_index_mapping_and_settings


def test_index_setting_filter():
    unfiltered_index_settings = {
        "number_of_shards": "5",
        "provided_name": "queries",
        "creation_date": "1579230289084",
        "requests": {
            "cache": {
                "enable": "false"
            }
        },
        "number_of_replicas": "0",
        "queries": {
            "cache": {
                "enabled": "false"
            }
        },
        "uuid": "jdzVt-dDS1aRlqdZWK4pdA",
        "version": {
            "created": "7050099"
        },
        "store": {
            "type": "fs"
        }
    }
    settings = filter_ephemeral_index_settings(unfiltered_index_settings)
    assert settings.keys() == {"number_of_shards", "number_of_replicas", "requests", "queries"}


@mock.patch("elasticsearch.Elasticsearch")
def test_extract_index_create(client):
    client.indices.get.return_value = {
        "osmgeopoints": {
            "aliases": {},
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "location": {
                        "type": "geo_point"
                    }
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": "3",
                    "provided_name": "osmgeopoints",
                    "creation_date": "1579210032233",
                    "requests": {
                        "cache": {
                            "enable": "false"
                        }
                    },
                    "number_of_replicas": "2",
                    "uuid": "vOOsPNfxTJyQekkIo9TjPA",
                    "version": {
                        "created": "7050099"
                    },
                    "store": {
                        "type": "fs"
                    }
                }
            }
        }
    }
    expected = {
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "location": {
                    "type": "geo_point"
                }
            }
        },
        "settings": {
            "index": {
                "number_of_replicas": "{{number_of_replicas | default(2)}}",
                "number_of_shards": "{{number_of_shards | default(3)}}",
                "requests": {
                    "cache": {
                        "enable": "false"
                    }
                }
            }
        }
    }
    res = extract_index_mapping_and_settings(client, "osmgeopoints")
    assert res == expected
