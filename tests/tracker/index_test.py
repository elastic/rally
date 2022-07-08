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

from unittest import mock

from esrally.tracker.index import (
    extract_index_mapping_and_settings,
    extract_indices_from_data_stream,
    filter_ephemeral_index_settings,
    update_index_setting_parameters,
)


def test_index_setting_filter():
    unfiltered_index_settings = {
        "number_of_shards": "5",
        "provided_name": "queries",
        "creation_date": "1579230289084",
        "requests": {
            "cache": {"enable": "false"},
        },
        "number_of_replicas": "0",
        "queries": {
            "cache": {"enabled": "false"},
        },
        "uuid": "jdzVt-dDS1aRlqdZWK4pdA",
        "version": {"created": "7050099"},
        "store": {"type": "fs"},
    }
    settings = filter_ephemeral_index_settings(unfiltered_index_settings)
    assert settings.keys() == {"number_of_shards", "number_of_replicas", "requests", "queries"}


def test_index_setting_parameters():
    settings = {
        "number_of_shards": "5",
        "provided_name": "queries",
        "creation_date": "1579230289084",
        "requests": {
            "cache": {"enable": "false"},
        },
        "number_of_replicas": "0",
    }
    update_index_setting_parameters(settings)
    assert settings == {
        "number_of_shards": "{{number_of_shards | default(5)}}",
        "provided_name": "queries",
        "creation_date": "1579230289084",
        "requests": {
            "cache": {"enable": "false"},
        },
        "number_of_replicas": "{{number_of_replicas | default(0)}}",
    }
    # make sure we don't explode if the parameterized settings aren't present for some reason
    settings.pop("number_of_shards")
    settings.pop("number_of_replicas")
    update_index_setting_parameters(settings)


@mock.patch("elasticsearch.Elasticsearch")
def test_extract_index_create(client):
    client.indices.get.return_value = {
        "osmgeopoints": {
            "aliases": {},
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "location": {
                        "type": "geo_point",
                    },
                },
            },
            "settings": {
                "index": {
                    "number_of_shards": "3",
                    "provided_name": "osmgeopoints",
                    "creation_date": "1579210032233",
                    "requests": {
                        "cache": {"enable": "false"},
                    },
                    "number_of_replicas": "2",
                    "uuid": "vOOsPNfxTJyQekkIo9TjPA",
                    "version": {"created": "7050099"},
                    "store": {"type": "fs"},
                }
            },
        },
        # should be filtered
        ".security": {
            "mappings": {},
            "settings": {
                "index": {
                    "number_of_shards": "1",
                },
            },
        },
        "geodata": {
            "mappings": {},
            "settings": {
                "index": {
                    "number_of_shards": "1",
                },
            },
        },
    }

    res = extract_index_mapping_and_settings(client, "_all")
    assert res == {
        "osmgeopoints": {
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "location": {
                        "type": "geo_point",
                    },
                },
            },
            "settings": {
                "index": {
                    "number_of_replicas": "{{number_of_replicas | default(2)}}",
                    "number_of_shards": "{{number_of_shards | default(3)}}",
                    "requests": {
                        "cache": {"enable": "false"},
                    },
                }
            },
        },
        "geodata": {
            "mappings": {},
            "settings": {
                "index": {
                    "number_of_shards": "{{number_of_shards | default(1)}}",
                },
            },
        },
    }


@mock.patch("elasticsearch.Elasticsearch")
def test_extract_indices_from_data_stream(client):
    data_streams_filter = ["metrics-kubernetes-*"]
    client.indices.get_data_stream.return_value = {
        "data_streams": [
            {
                "name": "metrics-kubernetes.event-default",
                "timestamp_field": {"name": "@timestamp"},
                "indices": [
                    {"index_name": ".ds-metrics-kubernetes.event-default-2022.06.20-000001", "index_uuid": "0W8L56dKQoGXjkGQc8mfzg"}
                ],
                "generation": 1,
                "_meta": {"description": "default metrics template installed by x-pack", "managed": "true"},
                "status": "GREEN",
                "template": "metrics",
                "ilm_policy": "metrics",
                "hidden": "false",
                "system": "false",
                "allow_custom_routing": "false",
                "replicated": "false",
            }
        ]
    }

    res = extract_indices_from_data_stream(client, data_streams_filter)
    assert res == ["metrics-kubernetes.event-default"]
