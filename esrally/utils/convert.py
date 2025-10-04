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
import enum
import math
from collections.abc import Callable
from typing import TypeVar


class Size(int):

    class Unit(enum.IntEnum):
        B = 1
        KB = 1024 * B
        MB = 1024 * KB
        GB = 1024 * MB
        TB = 1024 * GB

        def __str__(self) -> str:
            return self.name.upper()

    @property
    def unit(self) -> Unit:
        it = iter(Size.Unit)
        last = next(it)
        x = math.fabs(self)
        for unit in it:
            if x < unit:
                return last
            last = unit
        return last

    def to_unit(self, unit: Unit) -> float:
        return float(self / unit)

    def kb(self) -> float:
        return self.to_unit(Size.Unit.KB)

    def mb(self) -> float:
        return self.to_unit(Size.Unit.MB)

    def gb(self) -> float:
        return self.to_unit(Size.Unit.GB)

    def tb(self) -> float:
        return self.to_unit(Size.Unit.TB)

    def __str__(self):
        unit = self.unit
        if unit == Size.Unit.B:
            return f"{int(self)}B"
        x = self.to_unit(unit)
        return f"{x:.1f}{unit}"


def size(x: int | float, unit: Size.Unit = Size.Unit.B) -> Size:
    if isinstance(x, Size):
        return x
    return Size(x * unit)


def bytes_to_human_value(x: int | float | None) -> float | None:
    if x is None:
        return None
    s = size(x)
    return s.to_unit(s.unit)


def bytes_to_human_unit(x: int | float | None) -> str:
    if x is None:
        return "N/A"
    return str(size(x).unit)


def bytes_to_kb(x: int | float | None) -> float | None:
    if x is None:
        return None
    return size(x).kb()


def bytes_to_mb(x: int | float | None) -> float | None:
    if x is None:
        return None
    return size(x).mb()


def bytes_to_gb(x: int | float | None) -> float | None:
    if x is None:
        return None
    return size(x).gb()


def bytes_to_unit(unit: str, x: int | float | None) -> float | None:
    if x is None:
        return None
    u = {
        "B": Size.Unit.B,
        "KB": Size.Unit.KB,
        "MB": Size.Unit.MB,
        "GB": Size.Unit.GB,
        "TB": Size.Unit.TB,
    }.get(unit.strip(), Size.Unit.B)
    return size(x).to_unit(u)


class Duration(int):

    class Unit(enum.IntEnum):
        NS = 1
        US = 1000 * NS
        MS = 1000 * US
        S = 1000 * MS
        M = 60 * S
        H = 60 * M
        D = 24 * H

        def __str__(self) -> str:
            return self.name.lower()

    @property
    def unit(self) -> Unit:
        if self == 0:
            return Duration.Unit.S  # Special case
        it = iter(Duration.Unit)
        last = next(it)
        for unit in it:
            if abs(self) < unit:
                return last
            last = unit
        return last

    def to_unit(self, unit: Unit) -> float:
        return float(self / unit)

    def ns(self) -> float:
        return self.to_unit(Duration.Unit.NS)

    def us(self):
        return self.to_unit(Duration.Unit.US)

    def ms(self):
        return self.to_unit(Duration.Unit.MS)

    def s(self):
        return self.to_unit(Duration.Unit.S)

    def m(self):
        return self.to_unit(Duration.Unit.M)

    def h(self):
        return self.to_unit(Duration.Unit.H)

    def d(self):
        return self.to_unit(Duration.Unit.D)

    def __str__(self):
        unit = self.unit
        if unit == Duration.Unit.NS:
            return f"{int(self)}ns"
        if unit <= Duration.Unit.S:
            x = self.to_unit(unit)
            if x != int(x):
                return f"{x:.2f}{unit}"
            else:
                return f"{int(x)}{unit}"

        s = int(self.to_unit(Duration.Unit.S))
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)
        parts: list[str] = []
        if d:
            parts.append(f"{d}d")
        if h:
            parts.append(f"{h}h")
        if m:
            parts.append(f"{m}m")
        if s:
            parts.append(f"{s}s")
        return " ".join(parts)


def duration(x: int | float, unit: Duration.Unit = Duration.Unit.S) -> Duration:
    if isinstance(x, Duration):
        return x
    return Duration(x * unit)


def seconds_to_ms(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, Duration.Unit.S).ms()


def ms_to_seconds(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, Duration.Unit.MS).s()


def ms_to_minutes(x: int | float | None) -> float | None:
    if x is None:
        return None
    return duration(x, Duration.Unit.MS).m()


N = TypeVar("N", float, int)


def factor(n: N) -> Callable[[N], N]:
    return lambda v: v * n


def to_bool(value: str | bool) -> bool:
    if value in ["True", "true", "Yes", "yes", "t", "y", "1", True]:
        return True
    elif value in ["False", "false", "No", "no", "f", "n", "0", False]:
        return False
    raise ValueError(f"Cannot convert [{value}] to bool.")
