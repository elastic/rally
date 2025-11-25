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

from dataclasses import dataclass

from esrally.utils import cases, crc32c


@dataclass
class ChecksumCase:
    chunks: list[bytes]
    want: int


@cases.cases(
    no_cunks=ChecksumCase(chunks=[], want=0),
    one_chunk=ChecksumCase(chunks=[b"Hello world!"], want=2073618257),
    two_thunks=ChecksumCase(chunks=[b"Hello ", b"world!"], want=2073618257),
)
def test_update(case: ChecksumCase):
    c = crc32c.Checksum()
    for chunk in case.chunks:
        c.update(chunk)
    assert c.value == case.want


@cases.cases(
    no_cunks=ChecksumCase(chunks=[], want=0),
    one_chunk=ChecksumCase(chunks=[b"Hello world!"], want=2073618257),
    two_thunks=ChecksumCase(chunks=[b"Hello ", b"world!"], want=2073618257),
)
def test_combine(case: ChecksumCase):
    c = crc32c.Checksum()
    for chunk in case.chunks:
        c.combine(crc32c.Checksum(chunk), len(chunk))
    assert c.value == case.want
