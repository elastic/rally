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

import sys
from abc import abstractmethod
from collections.abc import Hashable, Iterable, Iterator, Sequence, Set
from itertools import chain, islice
from typing import Any, overload

# MAX_LENGTH represents the maximum supported file size
MAX_LENGTH = sys.maxsize


class RangeSet(Sequence["Range"], Set["Range"], Hashable):
    """RangeSet is the abstract base class of all implementations of sets of ranges.

    A range set is an immutable sequence of disjoint ranges sorted by its start value. It implements some mixin methods
    so that the implementation of a range sets is going to be lighter. It implements either the behaviour of a set
    and of a sequence of ranges.
    """

    @property
    @abstractmethod
    def start(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def end(self) -> int:
        raise NotImplementedError

    @property
    @property
    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def split(self, max_size: int = MAX_LENGTH) -> tuple[Range | EmptyRange, RangeSet]:
        """It returns a Range of max size on the left and the rest of the range set on the right.
        :param max_size: if given, returned range is cut after `max_size` bytes, and is excluded part is appended to
        the rangeset remaining part.
        :return: A range of up to max_size bytes on the left and the rest of the range set on the right.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        """It returns a string representation of the set of ranges."""
        return ",".join(str(r) for r in self)

    def __repr__(self) -> str:
        return f"rangeset('{str(self)}')"

    @abstractmethod
    def __or__(self, other: Iterable[Range]) -> RangeSet:  # type: ignore
        """It returns the union of two sets of ranges."""
        raise NotImplementedError

    @abstractmethod
    def __and__(self, other: Iterable[Range]) -> RangeSet:
        """It returns the intersection between two sets of ranges."""
        raise NotImplementedError

    @abstractmethod
    def __sub__(self, other: Iterable[Range]) -> RangeSet:
        """It returns subtract the other range sets from this one."""
        raise NotImplementedError

    @overload
    def __getitem__(self, i: int) -> Range:
        """It returns the renge at the ith position."""

    @overload
    def __getitem__(self, i: slice) -> RangeSet:
        """It returns a set of ranges selected using a slice."""

    def __getitem__(self, i: int | slice) -> RangeSet:
        if isinstance(i, int):
            if i < 0:
                raise IndexError(f"index key can't be negative: {i} < 0")
            try:
                return next(islice(self, i, i + 1))
            except StopIteration:
                raise IndexError(f"index key is too big: {i} >= {len(self)}") from None
        if isinstance(i, slice):
            if i.step not in [None, 1]:
                raise ValueError(f"invalid slice step value: {i.step} is not 1 | None")
            return _rangeset(islice(self, i.start, i.stop, i.step))
        raise TypeError(f"invalid key type: {i} is not int | slice")

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError


class EmptyRange(RangeSet):
    """EmptyRange represents an empty set of ranges."""

    def __contains__(self, item: Any) -> bool:
        return False

    def __len__(self) -> int:
        return 0

    def __iter__(self) -> Iterator[Range]:
        return iter(tuple())

    def __bool__(self) -> bool:
        return False

    def __or__(self, other: Iterable[Range]) -> RangeSet:  # type: ignore
        return _rangeset(other)

    def __and__(self, other: Iterable[Range]) -> EmptyRange:
        return NO_RANGE

    def __sub__(self, other: Iterable[Range]) -> EmptyRange:
        return NO_RANGE

    def split(self, max_size: int = MAX_LENGTH) -> tuple[Range | EmptyRange, RangeSet]:
        return NO_RANGE, NO_RANGE

    @property
    def start(self) -> int:
        raise EmptyRangeError("empty range")

    @property
    def end(self) -> int:
        raise EmptyRangeError("empty range")

    @property
    def size(self) -> int:
        return 0

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, EmptyRange)

    def __hash__(self) -> int:
        return 0


NO_RANGE = EmptyRange()


class Range(RangeSet):

    def __init__(self, start: int = 0, end: int = MAX_LENGTH):
        if start < 0:
            raise ValueError(f"range start can't be negative: {start} < 0")
        if end <= start:
            raise ValueError(f"range end must be greater than start: {end} <= {start}")
        self._start = start
        self._end = end

    @property
    def start(self) -> int:
        return self._start

    @property
    def end(self) -> int:
        return self._end

    @property
    def size(self):
        return self._end - self._start

    def __or__(self, other: Iterable[Range]) -> RangeSet:  # type: ignore
        return _union(self, other)

    def __and__(self, other: Iterable[Range]) -> RangeSet:
        ranges = []
        if not isinstance(other, RangeSet):
            other = _rangeset(other)
        for r in other:
            if r.end <= self._start:
                continue
            if r.start >= self._end:
                break
            ranges.append(Range(max(r._start, self._start), min(r._end, self._end)))
        return _rangeset(ranges)

    def __sub__(self, other: Iterable[Range]) -> RangeSet:
        if not isinstance(other, RangeSet):
            other = _rangeset(other)

        ranges = []
        position = self._start
        for o in other:
            if o._start > position:
                ranges.append(Range(position, min(self._end, o._start)))

            position = max(position, o._end)
            if position >= self._end:
                break
        if position < self._start:
            return self
        if position < self._end:
            ranges.append(Range(position, self._end))
        return _rangeset(ranges)

    def __str__(self) -> str:
        if self._end == self._start + 1:
            return str(self._start)
        return f"{self._start}-{_pretty_end(self._end)}"

    def split(self, max_size: int = MAX_LENGTH) -> tuple[Range | EmptyRange, RangeSet]:
        if max_size == MAX_LENGTH:
            return self, NO_RANGE
        if max_size <= 0:
            return NO_RANGE, self
        max_end = self.start + max_size
        if max_end >= self.end:
            return self, NO_RANGE
        return Range(self.start, max_end), Range(max_end, self.end)

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        return 1

    def __iter__(self) -> Iterator[Range]:
        yield self

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, int):
            return self.start <= item < self.end
        if isinstance(item, RangeSet):
            if item:
                return self.start <= item.start and item.end <= self.end
            # Empty ranges are always contained
            return True
        return False

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Range):
            return self.start == other.start and self.end == other.end
        return False

    def __hash__(self) -> int:
        return hash((self.start, self.end))


class RangeTree(RangeSet):

    def __init__(self, left: RangeSet, right: RangeSet):
        assert isinstance(left, (Range, RangeTree))
        assert isinstance(right, (Range, RangeTree))
        self._left = left
        self._right = right

    @property
    def start(self) -> int:
        return self._left.start

    @property
    def end(self) -> int:
        return self._right.end

    @property
    def size(self) -> int:
        return self._left.size + self._right.size

    def __or__(self, other: Iterable[Range]):  # type: ignore
        return _union(self, other)

    def __and__(self, other: Iterable[Range]) -> RangeSet:
        if not isinstance(other, RangeSet):
            other = _rangeset(other)
        if len(other) < len(self):
            return other & self
        return _rangeset(chain(self._left & other, self._right & other))

    def __sub__(self, other: Iterable[Range]) -> RangeSet:
        if not isinstance(other, RangeSet):
            other = _rangeset(other)
        if len(other) < len(self):
            return other - self
        return _rangeset(chain(self._left - other, self._right - other))

    def split(self, max_size: int = MAX_LENGTH) -> tuple[Range | EmptyRange, RangeSet]:
        left: Range | EmptyRange

        # It separates the top left range from the others.
        left, *others = self
        if max_size == MAX_LENGTH:
            # It re-constructs the right side of the tree
            return left, _rangeset(others)

        # It splits the top left according to max_size value.
        left, mid = left.split(max_size)
        if not mid:
            # It re-constructs the right side of the tree
            return left, _rangeset(others)

        # It re-constructs the right side of the tree
        return left, _rangeset(chain(mid, others))

    def __bool__(self) -> bool:
        return True

    def __len__(self):
        return len(self._left) + len(self._right)

    def __iter__(self) -> Iterator[Range]:
        yield from self._left
        yield from self._right

    def __contains__(self, item: Any) -> bool:
        return item in self._left or item in self._right

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RangeTree):
            return self._left == other._left and self._right == other._right
        return False

    def __hash__(self) -> int:
        return super()._hash()


class RangeError(ValueError):

    def __init__(self, msg: str, ranges: RangeSet = NO_RANGE):
        super().__init__(msg)
        self.range = ranges


class EmptyRangeError(RangeError):
    pass


def _union(*items: Iterable[Range]) -> RangeSet:
    """It returns a sorted set of ranges given some unordered set of ranges.

    :param items: input sequences of ranges.
    :return: a sorted set of disjoint ranges.
    """
    # It sorts ranges by start value.
    it: Iterator[Range] = iter(sorted((r for r in chain(*items) if r), key=lambda r: r.start))
    try:
        left = next(it)
    except StopIteration:
        return NO_RANGE

    disjoint: list[Range] = []
    for right in it:
        if right.start <= left.end and left.start <= right.end:
            # It merges touching ranges to a single one.
            left = Range(min(left.start, right.start), max(right.end, left.end))
            continue
        # It appends disjoint range.
        disjoint.append(left)
        left = right
    disjoint.append(left)
    return _rangeset(disjoint)


def _rangeset(items: Iterable[Range]) -> RangeSet:
    if isinstance(items, RangeSet):
        return items
    if not isinstance(items, Sequence):
        items = list(items)
    if len(items) == 0:
        return NO_RANGE
    if len(items) == 1:
        return items[0]
    lefts = items[: len(items) // 2]
    rights = items[len(lefts) :]
    return RangeTree(_rangeset(lefts), _rangeset(rights))


def _pretty_end(end: int) -> str:
    if end == MAX_LENGTH:
        return "*"
    return f"{end - 1}"


def rangeset(text: str) -> RangeSet:
    ranges = []
    for value in text.replace(" ", "").split(","):
        if not value:
            continue

        if "-" not in value:
            ranges.append(Range(int(value), int(value) + 1))
            continue

        start_text, end_text = value.split("-", 1)
        start = 0
        if start_text:
            start = int(start_text)

        end = MAX_LENGTH
        if end_text and end_text != "*":
            end = int(end_text) + 1
        ranges.append(Range(start, end))

    return _union(*ranges)
