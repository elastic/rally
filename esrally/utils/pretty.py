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
from __future__ import annotations

import difflib
import json
import typing
from collections import abc

O = typing.Union[abc.Mapping, abc.Sequence, str, int, float, bool, None]


_diff_methods = []


def diff(old, new: O, flat_dict=False) -> str:
    return "\n".join(difflib.ndiff(_dump(old, flat_dict=flat_dict), _dump(new, flat_dict=flat_dict)))


def dump(o: O, flat_dict=False):
    return "\n".join(_dump(o, flat_dict=flat_dict))


def _dump(o: O, flat_dict=False) -> abc.Sequence[str]:
    if flat_dict:
        o = _flat_dict(o)
    return json.dumps(o, indent=2, sort_keys=True).splitlines()


def _flat_dict(o: O) -> dict[str, str]:
    """Given a JSON like object, it produces a key value flat dictionary of strings easy to read and compare.
    :param o: a JSON like object
    :return: a flat dictionary
    """
    return dict(_visit_nested(o))


def _visit_nested(o: O) -> abc.Generator[tuple[str, str], None, None]:
    if isinstance(o, (str, bytes)):
        yield "", str(o)
    elif isinstance(o, abc.Mapping):
        for k1, v1 in o.items():
            for k2, v2 in _visit_nested(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield k1, v2
    elif isinstance(o, abc.Sequence):
        for k1, v1 in enumerate(o):
            for k2, v2 in _visit_nested(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield str(k1), v2
    else:
        yield "", json.dumps(o)
