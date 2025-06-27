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
import enum
import json
import re
from collections import abc
from typing import Any


class Flag(enum.Flag):
    FLAT_DICT = enum.auto()
    DUMP_EQUALS = enum.auto()


def dump(o: Any, flags: Flag = Flag(0)) -> str:
    """dump creates a human-readable multiline text to make easy to visualize the content of a JSON like object.

    :param o: the object the dump has to be obtained from.
    :param flags:
        flags & FLAT_DICT != 0: it will squash nested objects to make simple reading them.
    :return: JSON human-readable multiline text representation of the input object.
    """
    lines: abc.Sequence[str] = _dump(o, flags)
    return "\n".join(lines)


_HAS_DIFF = re.compile(r"^\+ ", flags=re.MULTILINE)


def diff(old: Any, new: Any, flags: Flag = Flag(0)) -> str:
    """diff creates a human-readable multiline text to make easy to visualize the difference of content between two JSON like object.

    :param old: the old object the diff dump has to be obtained from.
    :param new: the new object the diff dump has to be obtained from.
    :param flags:
        flags & Flags.FLAT_DICT: it squashes nested objects to make simple reading them;
        flags & Flags.DUMP_EQUALS: in case there is no difference it will print the same as dump function.
    :return: JSON human-readable multiline text representation of the difference between input objects, if any, or '' otherwise.
    """
    if Flag.DUMP_EQUALS not in flags and old == new:
        return ""
    ret = "\n".join(difflib.ndiff(_dump(old, flags), _dump(new, flags)))
    if Flag.DUMP_EQUALS not in flags and _HAS_DIFF.search(ret) is None:
        return ""
    return ret


def _dump(o: Any, flags: Flag) -> abc.Sequence[str]:
    """Lower level wrapper to json.dump method"""
    if Flag.FLAT_DICT in flags:
        # It reduces nested dictionary to a flat one to improve readability.
        o = flat(o)
    return json.dumps(o, indent=2, sort_keys=True).splitlines()


def flat(o: Any) -> dict[str, str]:
    """Given a JSON like object, it produces a key value flat dictionary of strings easy to read and compare.
    :param o: a JSON like object
    :return: a flat dictionary
    """
    return dict(_flat(o))


def _flat(o: Any) -> abc.Generator[tuple[str, str], None, None]:
    """Recursive helper function generating the content for the flat dictionary.

    :param o: a JSON like object
    :return: a generator of (key, value) pairs.
    """
    if isinstance(o, (str, bytes)):
        yield "", str(o)
    elif isinstance(o, abc.Mapping):
        for k1, v1 in o.items():
            for k2, v2 in _flat(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield k1, v2
    elif isinstance(o, abc.Sequence):
        for k1, v1 in enumerate(o):
            for k2, v2 in _flat(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield str(k1), v2
    else:
        yield "", json.dumps(o)


def seconds(s: float | int) -> str:
    if s < 0:
        raise ValueError("duration must be positive")
    ms = int(s * 1000) % 1000
    s = int(s)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d > 0:
        return f"{d}d{h}h{m}m{s}s"
    if h > 0:
        return f"{h}h{m}m{s}s"
    if m > 0:
        return f"{m}m{s}s"
    if ms > 0:
        return f"{s}.{ms}s"
    return f"{s}s"


def size(value: int | float | None) -> str:
    if value is None:
        return "?"
    value = float(value)
    if value < 100.0:
        return f"{value:.0f}B"
    value /= 1024.0
    if value < 100.0:
        return f"{value:.2f}KB"
    value /= 1024.0
    if value < 100.0:
        return f"{value:.2f}MB"
    value /= 1024.0
    if value < 100.0:
        return f"{value:.2f}GB"
    value /= 1024.0
    return f"{value:.2f}TB"
