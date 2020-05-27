# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys

import pytest

from esrally.track import params


class StaticSource:
    def __init__(self, contents, mode, encoding="utf-8"):
        self.contents = b'{"geonameid": 2986043, "name": "Pic de Font Blanca", "asciiname": "Pic de Font Blanca"}'
        self.current_index = 0
        self.opened = False

    def open(self):
        self.opened = True
        return self

    def seek(self, offset):
        pass

    def read(self):
        return "\n".join(self.contents)

    def readline(self):
        return self.contents

    def readlines(self, num_lines):
        return [self.contents] * num_lines

    def close(self):
        self._assert_opened()
        self.contents = None
        self.opened = False

    def _assert_opened(self):
        assert self.opened

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __str__(self, *args, **kwargs):
        return "StaticSource"


def create_reader(bulk_size):
    metadata = params.GenerateActionMetaData(index_name="test-idx", type_name=None)

    source = params.Slice(StaticSource, 0, sys.maxsize)
    reader = params.MetadataIndexDataReader(data_file="bogus",
                                            batch_size=bulk_size,
                                            bulk_size=bulk_size,
                                            file_source=source,
                                            action_metadata=metadata,
                                            index_name="test-idx",
                                            type_name=None)
    return reader


@pytest.mark.benchmark(
    group="bulk-params",
    warmup=True,
    warmup_iterations=10000,
    disable_gc=True,
)
def test_index_data_reader_100(benchmark):
    reader = create_reader(bulk_size=100)
    reader.__enter__()
    benchmark(reader.__next__)
    reader.__exit__(None, None, None)


@pytest.mark.benchmark(
    group="bulk-params",
    warmup=True,
    warmup_iterations=10000,
    disable_gc=True,
)
def test_index_data_reader_1000(benchmark):
    reader = create_reader(bulk_size=1000)
    reader.__enter__()
    benchmark(reader.__next__)
    reader.__exit__(None, None, None)


@pytest.mark.benchmark(
    group="bulk-params",
    warmup=True,
    warmup_iterations=10000,
    disable_gc=True,
)
def test_index_data_reader_10000(benchmark):
    reader = create_reader(bulk_size=10000)
    reader.__enter__()
    benchmark(reader.__next__)
    reader.__exit__(None, None, None)


@pytest.mark.benchmark(
    group="bulk-params",
    warmup=True,
    warmup_iterations=10000,
    disable_gc=True,
)
def test_index_data_reader_100000(benchmark):
    reader = create_reader(bulk_size=100000)
    reader.__enter__()
    benchmark(reader.__next__)
    reader.__exit__(None, None, None)
