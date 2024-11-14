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
import copy
from typing import Any, Generator, Mapping


def merge_dicts(d1: Mapping[str, Any], d2: Mapping[str, Any]) -> Generator[Any, None, Any]:
    """
    Merges two dictionaries which may contain nested dictionaries or lists. Conflicting keys in d2 override keys in d1.

    :param d1: A dictionary. May be empty.
    :param d2: A dictionary. May be empty.
    :return: A generator that contains a merged view of both dictionaries.
    """
    for k in set(d1) | set(d2):
        if k in d1 and k in d2:
            if isinstance(d1[k], dict) and isinstance(d2[k], dict):
                yield k, dict(merge_dicts(d1[k], d2[k]))
            elif isinstance(d1[k], list) and isinstance(d2[k], list):
                yield k, list(set(d1[k] + d2[k]))
            else:
                yield k, d2[k]
        elif k in d1:
            yield k, d1[k]
        else:
            yield k, d2[k]


def deep_update(orig_dict: Mapping[str, Any], *updates: Mapping[str, Any]) -> None:
    """
    Recursively updates a `dict` with other dicts in place.

    NOTE: This function has different semantics than `merge_dicts` as it does not merge lists.

    For example:
        ```python
        d1 = {"foo": [1, 2, 3]}
        d2 = {"foo": [3, 4, 5]}
        merged = merge_dicts(d1, d2))

        updated = copy.deepcopy(d1)
        deep_update(updated, d2)

        assert merged == {"foo": [1, 2, 3, 4, 5]}
        assert updated == {"foo": [3, 4, 5]}
        ```

    :param orig_dict: The original dict. May be empty.
    :param updates: The dicts to update originale dict with. May be empty.
    """
    for update in updates:
        if update is None:
            continue
        for k, v in update.items():
            if k in orig_dict and isinstance(orig_dict[k], Mapping) and isinstance(v, Mapping):
                deep_update(orig_dict[k], v)
            else:
                orig_dict[k] = copy.deepcopy(v)
