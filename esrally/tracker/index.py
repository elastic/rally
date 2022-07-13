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
import logging
import os

INDEX_SETTINGS_EPHEMERAL_KEYS = ["uuid", "creation_date", "version", "provided_name", "store"]
INDEX_SETTINGS_PARAMETERS = {
    "number_of_replicas": "{{{{number_of_replicas | default({orig})}}}}",
    "number_of_shards": "{{{{number_of_shards | default({orig})}}}}",
}


def filter_ephemeral_index_settings(settings):
    """
    Some of the 'settings' reported by Elasticsearch for an index are
    ephemeral values, not useful for re-creating the index.
    :param settings: Index settings reported by index.get()
    :return: settings with ephemeral keys removed
    """
    filtered = dict(settings)
    for s in INDEX_SETTINGS_EPHEMERAL_KEYS:
        filtered.pop(s, None)
    return filtered


def update_index_setting_parameters(settings):
    for s, param in INDEX_SETTINGS_PARAMETERS.items():
        if s in settings:
            orig_value = settings[s]
            settings[s] = param.format(orig=orig_value)


def is_valid(index_name, index_pattern):
    if len(index_name) == 0:
        return False, "Index name is empty"
    # When the indices are requested directly (with --data-streams or --indices) then we honor the
    # request, even if it includes hidden indices. But when asking for all indices we skip hidden
    # indices as they could be system indices and restoring them to another cluster would break it.
    if index_pattern in ("_all", "*") and index_name.startswith("."):
        return False, f"Index [{index_name}] is hidden"
    return True, None


def extract_index_mapping_and_settings(client, index_pattern):
    """
    Calls index GET to retrieve mapping + settings, filtering settings
    so they can be used to re-create this index
    :param client: Elasticsearch client
    :param index_pattern: name of index
    :return: index creation dictionary
    """
    results = {}
    logger = logging.getLogger(__name__)
    # the response might contain multiple indices if a wildcard was provided
    response = client.indices.get(index=index_pattern, params={"expand_wildcards": "all"})
    for index, details in response.items():
        valid, reason = is_valid(index, index_pattern)
        if valid:
            mappings = details["mappings"]
            index_settings = filter_ephemeral_index_settings(details["settings"]["index"])
            update_index_setting_parameters(index_settings)
            results[index] = {"mappings": mappings, "settings": {"index": index_settings}}
        else:
            logger.info("Skipping index [%s] (reason: %s).", index, reason)

    return results


def extract(client, outdir, index_pattern):
    """
    Request index information to format in "index.json" for Rally
    :param client: Elasticsearch client
    :param outdir: destination directory
    :param index_pattern: name of index
    :return: Dict of template variables representing the index for use in track
    """
    results = []

    index_obj = extract_index_mapping_and_settings(client, index_pattern)
    for index, details in index_obj.items():
        filename = f"{index}.json"
        outpath = os.path.join(outdir, filename)
        with open(outpath, "w") as outfile:
            json.dump(details, outfile, indent=4, sort_keys=True)
            outfile.write("\n")
        results.append(
            {
                "name": index,
                "path": outpath,
                "filename": filename,
            }
        )
    return results


def extract_indices_from_data_stream(client, data_stream_pattern):
    """
    Calls Elasticsearch client get_data_stream function to retrieve list of indices
    :param client: Elasticsearch client
    :param data_stream_pattern: name of data stream
    :return: list of index names
    """
    results = []
    # the response might contain multiple indices if a wildcard was provided
    params_defined = {"expand_wildcards": "all", "filter_path": "data_streams.name"}
    results_data_streams = client.indices.get_data_stream(name=data_stream_pattern, params=params_defined)

    for indices in results_data_streams["data_streams"]:
        results.append(indices.get("name"))
    return results
