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
import os


INDEX_SETTINGS_EPHEMERAL_KEYS = ["uuid",
                                 "creation_date",
                                 "version",
                                 "provided_name"]


def filter_ephemeral_index_settings(settings):
    """
    Some of the 'settings' reported by Elasticsearch for an index are
    ephemeral values, not useful for re-creating the index.
    :param settings: Index settings reported by index.get()
    :return: settings with ephemeral keys removed
    """
    return {k: v for k, v in settings.items() if k not in INDEX_SETTINGS_EPHEMERAL_KEYS}


def extract_index_create(client, index):
    """
    Calls index GET to retrieve mapping + settings, filtering settings
    so they can be used to re-create this index
    :param client: Elasticsearch client
    :param index: name of index
    :return: index creation dictionary
    """
    response = client.indices.get(index)
    details = response[index]

    mappings = details["mappings"]
    index_settings = filter_ephemeral_index_settings(details["settings"]["index"])
    return {"mappings": mappings, "settings": {"index": index_settings}}


def extract(client, outdir, index):
    """
    Request index information to format in "index.json" for Rally
    :param client: Elasticsearch client
    :param outdir: destination directory
    :param index: name of index
    :return: None
    """
    index_obj = extract_index_create(client, index)
    outpath = os.path.join(outdir, "index.json")
    with open(outpath, "w") as outfile:
        json.dump(index_obj, outfile, indent=4, sort_keys=True)
        outfile.write('\n')
