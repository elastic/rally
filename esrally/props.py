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

from enum import StrEnum, auto

try:
    from enum import EnumType
except ImportError:
    from enum import EnumMeta as EnumType
finally:
    assert EnumType

try:
    from enum import member
except ImportError:

    def member(enum):
        return enum

finally:
    assert member


class PropEnumType(EnumType):
    __iter__ = None


class PropEnum(StrEnum, metaclass=PropEnumType):
    def __new__(cls, value):
        if isinstance(value, PropEnumType):
            return value
        elif isinstance(value, str):
            value = f"{cls.__qualname__.lower()}.{value}"
            member = str.__new__(cls, value)
            member._value_ = value
            return member
        else:
            raise TypeError(f"{value!r} is not a string or enum")


class Property(PropEnum):
    def __sections(self):
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
    return auto()


class SYSTEM(Section):
    @member
    class ADMIN(Key):
        TRACK = desc()

    @member
    class LIST(Key):
        FROM_DATE = desc()
        MAX_RESULTS = desc()
        TO_DATE = desc()

        @member
        class CONFIG(Key):
            OPTION = desc()

        @member
        class RACES(Key):
            BANCHMARK_NAME = desc()
