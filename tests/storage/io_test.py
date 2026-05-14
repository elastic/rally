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
import dataclasses
import os

from esrally.storage import RangeError, _io, rangeset
from esrally.utils import cases


@dataclasses.dataclass
class FileWriterCase:
    existing_data: bytes | None = None
    write_data: bytes | None = None
    ranges: str = ""
    close: bool = False
    want_write_size: int | None = None
    want_data: bytes = b""
    want_write_error: type[Exception] | None = None
    want_close_error: type[Exception] | None = None
    want_transferred: int | None = None


EXISTING_FILE_DATA = b"some-existing-data"
WRITE_SOME_DATA = b"write-some-data"


@cases.cases(
    open=FileWriterCase(),
    close=FileWriterCase(close=True),
    open_existing=FileWriterCase(existing_data=EXISTING_FILE_DATA),
    write_data=FileWriterCase(write_data=WRITE_SOME_DATA, want_data=WRITE_SOME_DATA),
    write_closed=FileWriterCase(close=True, write_data=WRITE_SOME_DATA, want_write_error=_io.StreamClosedError, want_write_size=0),
    write_existing=FileWriterCase(existing_data=EXISTING_FILE_DATA, write_data=WRITE_SOME_DATA, want_data=WRITE_SOME_DATA + b"ata"),
    open_rages=FileWriterCase(ranges="3-5"),
    write_with_exact_ranges=FileWriterCase(
        existing_data=EXISTING_FILE_DATA, ranges="2-16", write_data=WRITE_SOME_DATA, want_data=b"so" + WRITE_SOME_DATA + b"a"
    ),
    write_with_smaller_ranges=FileWriterCase(
        existing_data=EXISTING_FILE_DATA,
        ranges="2-14",
        write_data=WRITE_SOME_DATA,
        want_data=(b"so" + WRITE_SOME_DATA[:-2] + b"ata"),
        want_write_error=RangeError,
        want_write_size=13,
    ),
    write_with_larger_ranges=FileWriterCase(
        existing_data=EXISTING_FILE_DATA, ranges="2-20", write_data=WRITE_SOME_DATA, want_data=(b"so" + WRITE_SOME_DATA + b"a")
    ),
)
def test_file_writer(case: FileWriterCase, tmpdir):
    path = tmpdir.join("file.txt")
    if case.existing_data:
        with open(path, "wb") as fd:
            fd.write(case.existing_data)

    # It opens the target file.
    ranges = rangeset(case.ranges)
    with _io.FileWriter.open(path, ranges=ranges) as writer:
        assert writer.writable()
        assert isinstance(writer, _io.FileWriter)
        assert writer.ranges == ranges
        assert writer.path == path

        if case.close:
            writer.close()
            assert writer.fd is None
            assert not writer.writable()

        want_position = ranges and ranges.start or 0
        assert writer.position == want_position
        assert writer.transferred == 0

        # It eventually writes some data.
        if case.write_data is not None:
            want_write_size = case.want_write_size
            if case.want_write_size is None:
                want_write_size = len(case.write_data)
            try:
                assert writer.write(case.write_data) == len(case.write_data)
            except Exception as ex:
                if case.want_write_error is None:
                    raise
                assert isinstance(ex, case.want_write_error)

            want_position += want_write_size
            assert writer.transferred == want_write_size

        assert writer.position == want_position

    assert writer.fd is None
    assert not writer.writable()

    assert os.path.isfile(path)
    want_data = case.want_data or case.existing_data or b""
    assert os.path.getsize(path) == len(want_data)
    if case.want_data is not None:
        with open(path, "rb") as fd:
            assert fd.read() == want_data
