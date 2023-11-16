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

from enum import Enum, auto

# TODO remove ImportError fallback when we drop support for Python 3.10
try:
    from enum import EnumType, member
except ImportError:
    from enum import EnumMeta as EnumType

    def member(enum):
        return enum


class PropEnumType(EnumType):
    __iter__ = None  # Disallows erroneous unpack by star expansion


class PropEnum(str, Enum, metaclass=PropEnumType):
    """StrEnum-like Enum but supports multi-level nesting"""

    def __new__(cls, value):
        if isinstance(value, PropEnumType):
            return value  # Nested Enum should stay original
        elif isinstance(value, str):
            # This prefixing lets A.B.C.D value not just "d" but "a.b.c.d"
            value = f"{cls.__qualname__.lower()}.{value}"
            member = str.__new__(cls, value)
            member._value_ = value
            return member
        else:
            raise TypeError(f"{value!r} is not a string or enum")

    @staticmethod
    def _generate_next_value_(name, *_):
        return name.lower()


class Property(PropEnum):
    def __sections(self):
        """Yields section names from the top level to the bottom"""
        node = globals()
        for name in type(self).__qualname__.split("."):
            node = node[name]
            if issubclass(node, Section):
                yield name
            else:
                break

    @property
    def __section(self):
        return ".".join(map(str.lower, self.__sections()))

    @property
    def __key(self):
        return self.value.replace(f"{self.__section}.", "", 1)

    def __iter__(self):
        """Allows to unpack pair of section and key names by star expansion"""
        return iter(
            (
                self.__section,
                self.__key,
            )
        )

    def __repr__(self):
        return f"<{type(self).__qualname__}.{self.name}: {self.value}>"


class Section(Property):
    pass


class Key(Property):
    pass


def desc(*_):
    """Just a blank function to write a description for each key as a string

    The usage of this function is open-ended for future enhancements. By main-
    taining descriptions as (list of) str in the source code, it may be
    possible to reuse them as e.g. help texts for users in the future.

    Returns `enum.auto()` to let Enum populate an appropriate value to the
    member.
    """
    return auto()


class SYSTEM(Section):
    @member
    class ADMIN(Key):
        TRACK = desc()

    @member
    class LIST(Key):
        FROM_DATE = desc(
            """
            list records only from this date
            """
        )

        MAX_RESULTS = desc(
            """
            the limit of the number of records to return
            """
        )

        TO_DATE = desc(
            """
            list records only to this date
            """
        )

        @member
        class CONFIG(Key):
            OPTION = desc()

        @member
        class RACES(Key):
            BENCHMARK_NAME = desc()
