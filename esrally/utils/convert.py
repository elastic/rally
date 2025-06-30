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

import enum
from collections.abc import Callable
from typing import TypeVar


class SizeUnit(enum.IntEnum):
    B = 1
    KB = 1024 * B
    MB = 1024 * KB
    GB = 1024 * MB
    TB = 1024 * GB

    @classmethod
    def parse(cls, s: str) -> SizeUnit | None:
        s = s.strip().upper()
        for unit in sorted(cls, key=lambda u: len(u.name), reverse=True):
            if s.endswith(unit.name):
                return unit
        return None

    def __str__(self) -> str:
        return self.name.upper()


class Size(int):

    @classmethod
    def parse(cls, text: str, unit: SizeUnit | None = None) -> Size:
        text = text.strip()
        if unit is None:
            unit = SizeUnit.parse(text)
            if unit is None:
                unit = SizeUnit.B
            else:
                text = text[: -len(unit.name)].rstrip()
        x = float(text)
        if unit != SizeUnit.B:
            x *= unit
        return Size(x)

    @property
    def unit(self) -> SizeUnit:
        it = iter(SizeUnit)
        last = next(it)
        for unit in it:
            if self < unit:
                return last
            last = unit
        return last

    def to_unit(self, unit: SizeUnit) -> float:
        return float(self / unit)

    def kb(self):
        return self.to_unit(SizeUnit.KB)

    def mb(self):
        return self.to_unit(SizeUnit.MB)

    def gb(self):
        return self.to_unit(SizeUnit.GB)

    def tb(self):
        return self.to_unit(SizeUnit.TB)

    def __str__(self):
        unit = self.unit
        if unit == SizeUnit.B:
            return f"{int(self)}B"
        x = self.to_unit(unit)
        return f"{x:.1f}{unit}"


def size(x: int | float, unit: SizeUnit = SizeUnit.B) -> Size:
    if isinstance(x, Size):
        return x
    if x < 0:
        raise TypeError("negative size")
    return Size(x * unit)


def bytes_to_kb(x: int | float | None) -> float | None:
    if x is None:
        return None
    return size(x).to_unit(SizeUnit.KB)


def bytes_to_mb(x: int | float | None) -> float | None:
    if x is None:
        return None
    return size(x).to_unit(SizeUnit.MB)


def bytes_to_gb(x: int | float | Size) -> float | None:
    if x is None:
        return None
    return size(x).to_unit(SizeUnit.GB)


def bytes_to_tb(x: int | float | Size) -> float | None:
    if x is None:
        return None
    return size(x).to_unit(SizeUnit.TB)


def bytes_to_unit(unit: str, x: int | float | None) -> float | None:
    if x is None:
        return None
    u = SizeUnit.parse(unit)
    if u is None:
        u = SizeUnit.B
    return size(x).to_unit(u)


def bytes_to_str(x: int | float | None) -> str:
    if x is None:
        return "N/A"
    return str(size(x))


class TimeUnit(enum.IntEnum):
    NS = 1
    US = 1000 * NS
    MS = 1000 * US
    S = 1000 * MS
    M = 60 * S
    H = 60 * M
    D = 24 * H

    @classmethod
    def parse(cls, s: str) -> TimeUnit | None:
        s = s.strip().upper()
        for unit in cls:
            if s.endswith(unit.name):
                return unit
        return None

    def __str__(self):
        return self.name.lower()


class Duration(int):

    @classmethod
    def parse(cls, text: str, unit: TimeUnit | None = None) -> Duration:
        text = text.strip()
        if unit is None:
            unit = TimeUnit.parse(text)
            if unit is None:
                unit = TimeUnit.S
            else:
                text = text[: -len(unit.name)].rstrip()
        x = float(text)
        if unit != TimeUnit.NS:
            x *= unit
        return Duration(x)

    @property
    def unit(self) -> TimeUnit:
        if self == 0:
            return TimeUnit.S  # Special case
        it = iter(TimeUnit)
        last = next(it)
        for unit in it:
            if self < unit:
                return last
            last = unit
        return last

    def to_unit(self, unit: TimeUnit) -> float:
        return float(self / unit)

    def ns(self) -> float:
        return self.to_unit(TimeUnit.NS)

    def us(self):
        return self.to_unit(TimeUnit.US)

    def ms(self):
        return self.to_unit(TimeUnit.MS)

    def s(self):
        return self.to_unit(TimeUnit.S)

    def m(self):
        return self.to_unit(TimeUnit.M)

    def h(self):
        return self.to_unit(TimeUnit.H)

    def d(self):
        return self.to_unit(TimeUnit.D)

    def __str__(self):
        unit = self.unit
        if unit == TimeUnit.NS:
            return f"{int(self)}ns"
        if unit <= TimeUnit.S:
            x = self.to_unit(unit)
            if x != int(x):
                return f"{x:.2f}{unit}"
            else:
                return f"{int(x)}{unit}"

        s = int(self.to_unit(TimeUnit.S))
        m, s = divmod(s, 60)
        if unit == TimeUnit.M:
            return f"{m}m {s:02d}s"
        h, m = divmod(m, 60)
        if unit == TimeUnit.H:
            return f"{h}h {m:02d}m {s:02d}s"
        d, h = divmod(h, 24)
        return f"{d}d {h:02d}h {m:02d}m {s:02d}s"


def duration(x: int | float, unit: TimeUnit = TimeUnit.S) -> Duration:
    if isinstance(x, Duration):
        return x
    if x < 0:
        raise TypeError("negative duration")
    return Duration(x * unit)


def seconds_to_ms(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, TimeUnit.S).ms()


def seconds_to_hour_minute_seconds(x: int | float | None) -> tuple[int | None, int | None, int | float | None]:
    if x is None:
        return None, None, None
    m, s = divmod(x, 60)
    h, m = divmod(int(m), 60)
    return h, m, s


def ms_to_seconds(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, TimeUnit.MS).s()


def ms_to_minutes(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, TimeUnit.MS).m()


N = TypeVar("N", float, int)


def factor(n: N) -> Callable[[N], N]:
    return lambda v: v * n


def to_bool(value: str | bool) -> bool:
    if value in ["True", "true", "Yes", "yes", "t", "y", "1", True]:
        return True
    elif value in ["False", "false", "No", "no", "f", "n", "0", False]:
        return False
    raise ValueError(f"Cannot convert [{value}] to bool.")
