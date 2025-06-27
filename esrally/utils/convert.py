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

from collections.abc import Callable
from typing import TypeVar, overload

FROM_KB = 1024.0
FROM_MB = 1024 * 1024
FROM_GB = 1024 * 1024 * 1024
TO_KB = 1.0 / FROM_KB
TO_MB = 1.0 / FROM_MB
TO_GB = 1.0 / FROM_GB


@overload
def bytes_to_kb(b: None) -> None:
    pass


@overload
def bytes_to_kb(b: int | float) -> float:
    pass


def bytes_to_kb(b):
    if b is None:
        return None
    return b * TO_KB


@overload
def bytes_to_mb(b: None) -> None:
    pass


@overload
def bytes_to_mb(b: int | float) -> float:
    pass


def bytes_to_mb(b):
    if b is None:
        return None
    return b * TO_MB


@overload
def bytes_to_gb(b: None) -> None:
    pass


@overload
def bytes_to_gb(b: int | float) -> float:
    pass


def bytes_to_gb(b):
    if b is None:
        return None
    return b * TO_GB


def bytes_to_human_string(b: float | int | None) -> str:
    if b is None:
        return "N/A"

    value, unit = _bytes_to_human(b)
    if unit == "bytes":
        return f"{value} bytes"
    return f"{value:.1f} {unit}"


@overload
def bytes_to_human_value(b: None) -> None:
    pass


@overload
def bytes_to_human_value(b: int | float) -> float:
    pass


def bytes_to_human_value(b):
    return _bytes_to_human(b)[0]


def bytes_to_human_unit(b: float | int | None) -> str:
    return _bytes_to_human(b)[1]


@overload
def bytes_to_unit(unit: str, b: None) -> None:
    pass


@overload
def bytes_to_unit(unit: str, b: int) -> int | float:
    pass


@overload
def bytes_to_unit(unit: str, b: float) -> float:
    pass


def bytes_to_unit(unit, b):
    if unit == "N/A":
        return b
    elif unit == "GB":
        return bytes_to_gb(b)
    elif unit == "MB":
        return bytes_to_mb(b)
    elif unit == "kB":
        return bytes_to_kb(b)
    else:
        return b


@overload
def _bytes_to_human(b: None) -> tuple[None, str]:
    pass


@overload
def _bytes_to_human(b: int) -> tuple[float | int, str]:
    pass


@overload
def _bytes_to_human(b: float) -> tuple[float, str]:
    pass


def _bytes_to_human(b):
    if b is None:
        return b, "N/A"
    gb = bytes_to_gb(b)
    if gb > 1.0 or gb < -1.0:
        return gb, "GB"
    mb = bytes_to_mb(b)
    if mb > 1.0 or mb < -1.0:
        return mb, "MB"
    kb = bytes_to_kb(b)
    if kb > 1.0 or kb < -1.0:
        return kb, "kB"
    return b, "bytes"


def number_to_human_string(number: int | float) -> str:
    return f"{number:,}"


@overload
def mb_to_bytes(mb: None) -> None:
    pass


@overload
def mb_to_bytes(mb: float | int) -> int:
    pass


def mb_to_bytes(mb):
    if mb is None:
        return None
    return int(mb * FROM_MB)


@overload
def gb_to_bytes(gb: None) -> None:
    pass


@overload
def gb_to_bytes(gb: int) -> int:
    pass


@overload
def gb_to_bytes(gb: float) -> float:
    pass


def gb_to_bytes(gb):
    if gb is None:
        return None
    return gb * FROM_GB


T = TypeVar("T", float, int, None)

TO_MS = 1000
TO_M = 1.0 / 60
FROM_MS = 0.001


def seconds_to_ms(s: T) -> T:
    if s is None:
        return None
    return s * TO_MS


def seconds_to_hour_minute_seconds(s: T) -> tuple[T, T, T]:
    if not s:
        return s, s, s

    hours = s // 3600
    minutes = (s - 3600 * hours) // 60
    seconds = s - 3600 * hours - 60 * minutes
    return hours, minutes, seconds


def ms_to_seconds(ms: T) -> float | None:
    if ms is None:
        return None
    return ms * FROM_MS


def ms_to_minutes(ms: T) -> float | None:
    if ms is None:
        return None
    return ms * FROM_MS * TO_M


N = TypeVar("N", float, int)


def factor(n: N) -> Callable[[N], N]:
    return lambda v: v * n


def to_bool(value: str | bool) -> bool:
    if value in ["True", "true", "Yes", "yes", "t", "y", "1", True]:
        return True
    elif value in ["False", "false", "No", "no", "f", "n", "0", False]:
        return False
    raise ValueError(f"Cannot convert [{value}] to bool.")
