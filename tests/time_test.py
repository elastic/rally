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

import time
from unittest import mock

import esrally.time


class TestTime:
    def test_split_time_increases(self):
        wait_period_seconds = 0.05

        stop_watch = esrally.time.Clock.stop_watch()
        stop_watch.start()
        prev_split_time = 0
        for _ in range(3):
            time.sleep(wait_period_seconds)
            split_time = stop_watch.split_time()
            assert prev_split_time < split_time
            prev_split_time = split_time
        stop_watch.stop()
        total_time = stop_watch.total_time()
        assert prev_split_time <= total_time

    @mock.patch("esrally.time.StopWatch._now", side_effect=[1, 2])
    def test_total_time_roughly_in_expected_range(self, mock_now):
        stop_watch = esrally.time.Clock.stop_watch()
        stop_watch.start()
        stop_watch.stop()

        interval = stop_watch.total_time()
        assert mock_now.call_count == 2
        assert interval == 2 - 1

    @mock.patch("esrally.time.Clock.now", side_effect=[1, 2])
    def test_millis_conversion_roughly_in_expected_range(self, mock_now):
        start = esrally.time.to_epoch_millis(esrally.time.Clock.now())
        end = esrally.time.to_epoch_millis(esrally.time.Clock.now())

        assert end - start == 2000 - 1000
        assert mock_now.call_count == 2
