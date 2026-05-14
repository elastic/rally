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
import base64
from typing import Any

import google_crc32c
from typing_extensions import Self

READ_CHUNK_SIZE = 1024 * 1024


class Checksum(google_crc32c.Checksum):

    def __init__(self, initial_value: bytes | int = 0):
        if isinstance(initial_value, bytes):
            super().__init__(initial_value)
            return
        assert isinstance(initial_value, int)
        super().__init__()
        self._crc = initial_value

    @classmethod
    def from_base64(cls, value: str) -> Self:
        c = cls()
        c._crc = int.from_bytes(base64.b64decode(value), "big")
        return c

    def to_base64(self) -> str:
        return base64.b64encode(self._crc.to_bytes(8, byteorder="big")).decode("ascii")

    @classmethod
    def from_filename(cls, filename: str, chunk_size: int = READ_CHUNK_SIZE) -> Self:
        c = cls()
        with open(filename, "rb") as fd:
            while chunk := fd.read(chunk_size):
                c.update(chunk)
        return c

    @property
    def value(self) -> int:
        return self._crc

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, google_crc32c.Checksum):
            return NotImplemented
        return self._crc == other._crc

    def __hash__(self) -> int:
        return self._crc

    def __repr__(self) -> str:
        return f"Checksum({self._crc})"
