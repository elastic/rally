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

import difflib
import json
import re

from esrally.utils import io

# detect (very simplistically) json that starts with an empty array or array of strings
RE_JSON_ARRAY_START = re.compile(r'^(\s*\[\s*\])|(\s*\[\s*".*)')


def csv_to_list(csv):
    if csv is None:
        return None
    if io.has_extension(csv, ".json"):
        with open(io.normalize_path(csv), encoding="utf-8") as f:
            content = f.read()
            if not RE_JSON_ARRAY_START.match(content):
                raise ValueError(f"csv args only support arrays in json but you supplied [{csv}]")
            return json.loads(content)
    elif RE_JSON_ARRAY_START.match(csv):
        return json.loads(csv)
    elif len(csv.strip()) == 0:
        return []
    else:
        return [e.strip() for e in csv.split(",")]


def to_bool(v):
    if v is None:
        return None
    elif v.lower() == "false":
        return False
    elif v.lower() == "true":
        return True
    else:
        raise ValueError("Could not convert value '%s'" % v)


def to_none(v):
    if v is None:
        return None
    elif v.lower() == "none":
        return None
    else:
        raise ValueError("Could not convert value '%s'" % v)


def kv_to_map(kvs):
    def convert(v):
        # string (specified explicitly)
        if v.startswith("'"):
            return v[1:-1]

        # int
        try:
            return int(v)
        except ValueError:
            pass

        # float
        try:
            return float(v)
        except ValueError:
            pass

        # boolean
        try:
            return to_bool(v)
        except ValueError:
            pass

        try:
            return to_none(v)
        except ValueError:
            pass

        # treat it as string by default
        return v

    result = {}
    for kv in kvs:
        k, v = kv.split(":")
        # key is always considered a string, value needs to be converted
        result[k.strip()] = convert(v.strip())
    return result


def to_dict(arg, default_parser=kv_to_map):
    if io.has_extension(arg, ".json"):
        with open(io.normalize_path(arg), encoding="utf-8") as f:
            return json.load(f)
    try:
        return json.loads(arg)
    except json.decoder.JSONDecodeError:
        return default_parser(csv_to_list(arg))


def bulleted_list_of(src_list):
    return [f"- {param}" for param in src_list]


def double_quoted_list_of(src_list):
    return [f'"{param}"' for param in src_list]


def make_list_of_close_matches(word_list, all_possibilities):
    """
    Returns list of closest matches for `word_list` from `all_possibilities`.
    e.g. [num_of-shards] will return [num_of_shards] when all_possibilities=["num_of_shards", "num_of_replicas"]

    :param word_list: A list of strings that we want to find closest matches for.
    :param all_possibilities: List of strings that the algorithm will calculate the closest match from.
    :return:
    """
    close_matches = []
    for param in word_list:
        matched_word = difflib.get_close_matches(param, all_possibilities, n=1)
        if matched_word:
            close_matches.append(matched_word[0])

    return close_matches


class ConnectOptions:
    """
    Base Class to help either parsing --target-hosts or --client-options
    """

    def __getitem__(self, key):
        """
        Race expects the cfg object to be subscriptable
        Just return 'default'
        """
        return self.default

    @property
    def default(self):
        """Return a list with the options assigned to the 'default' key"""
        return self.parsed_options["default"]

    @property
    def all_options(self):
        """Return a dict with all parsed options"""
        return self.parsed_options


class TargetHosts(ConnectOptions):
    DEFAULT = "default"

    def __init__(self, argvalue):
        self.argname = "--target-hosts"
        self.argvalue = argvalue
        self.parsed_options = []

        self.parse_options()

    def parse_options(self):
        def normalize_to_dict(arg):
            """
            Return parsed comma separated host string as dict with "default" key.
            This is needed to support backwards compatible --target-hosts for single clusters that are not
            defined as a json string or file.
            """
            # pylint: disable=import-outside-toplevel
            from elasticsearch.client import _normalize_hosts

            return {TargetHosts.DEFAULT: _normalize_hosts(arg)}

        self.parsed_options = to_dict(self.argvalue, default_parser=normalize_to_dict)

    @property
    def all_hosts(self):
        """Return a dict with all parsed options"""
        return self.all_options


class ClientOptions(ConnectOptions):
    DEFAULT_CLIENT_OPTIONS = "timeout:60"

    """
    Convert --client-options arg to a dict.
    When no --client-options have been specified but multi-cluster --target-hosts are used,
    apply options defaults for all cluster names.
    """

    def __init__(self, argvalue, target_hosts=None):
        self.argname = "--client-options"
        self.argvalue = argvalue
        self.target_hosts = target_hosts
        self.parsed_options = []

        self.parse_options()

    def parse_options(self):
        default_client_map = kv_to_map([ClientOptions.DEFAULT_CLIENT_OPTIONS])
        if self.argvalue == ClientOptions.DEFAULT_CLIENT_OPTIONS and self.target_hosts is not None:
            # --client-options unset but multi-clusters used in --target-hosts? apply options defaults for all cluster names.
            self.parsed_options = {cluster_name: default_client_map for cluster_name in self.target_hosts.all_hosts.keys()}
        else:
            self.parsed_options = to_dict(self.argvalue, default_parser=ClientOptions.normalize_to_dict)

    @staticmethod
    def normalize_to_dict(arg):

        """
        When --client-options is a non-json csv string (single cluster mode),
        return parsed client options as dict with "default" key
        This is needed to support single cluster use of --client-options when not
        defined as a json string or file.
        """
        default_client_map = kv_to_map([ClientOptions.DEFAULT_CLIENT_OPTIONS])

        return {TargetHosts.DEFAULT: {**default_client_map, **kv_to_map(arg)}}

    @property
    def all_client_options(self):
        """Return a dict with all client options"""
        return self.all_options

    @property
    def uses_static_responses(self):
        return self.default.get("static_responses", False)

    def with_max_connections(self, max_connections):
        final_client_options = {}
        for cluster, original_opts in self.all_client_options.items():
            amended_opts = dict(original_opts)
            amended_opts["max_connections"] = max(256, amended_opts.get("max_connections", max_connections))
            final_client_options[cluster] = amended_opts
        return final_client_options
