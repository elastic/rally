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
import anycrc
import google_crc32c


class Checksum(google_crc32c.Checksum):

    @property
    def value(self) -> int:
        return self._crc

    def combine(self, value: google_crc32c.Checksum | int, length: int) -> None:
        if isinstance(value, google_crc32c.Checksum):
            value = value._crc
        self._crc = _combine(self._crc, value, length)


# This model is used to combine two CRC32C values
_CRC32C_MODEL = anycrc.Model("CRC32C")


def _combine(checksum1: int, checksum2: int, length: int) -> int:
    return _CRC32C_MODEL.combine(checksum1, checksum2, length)
