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

import time
from unittest import TestCase

import esrally.time


class TimeTests(TestCase):
    def test_split_time_increases(self):
        wait_period_seconds = 0.05

        stop_watch = esrally.time.Clock.stop_watch()
        stop_watch.start()
        prev_split_time = 0
        for _ in range(3):
            time.sleep(wait_period_seconds)
            split_time = stop_watch.split_time()
            self.assertLess(prev_split_time, split_time)
            prev_split_time = split_time
        stop_watch.stop()
        total_time = stop_watch.total_time()
        self.assertLessEqual(prev_split_time, total_time)

    def test_total_time_roughly_in_expected_range(self):
        wait_period_seconds = 0.05
        acceptable_delta_seconds = 0.03

        stop_watch = esrally.time.Clock.stop_watch()
        stop_watch.start()
        time.sleep(wait_period_seconds)
        stop_watch.stop()

        interval = stop_watch.total_time()
        # depending on scheduling accuracy we should end up somewhere in that range
        self.assertGreaterEqual(interval, wait_period_seconds - acceptable_delta_seconds)
        self.assertLessEqual(interval, wait_period_seconds + acceptable_delta_seconds)

    def test_millis_conversion_roughly_in_expected_range(self):
        wait_period_millis = 50
        acceptable_delta_millis = 30

        start = esrally.time.to_epoch_millis(esrally.time.Clock.now())
        time.sleep(wait_period_millis / 1000.0)
        end = esrally.time.to_epoch_millis(esrally.time.Clock.now())

        interval_millis = end - start

        # depending on scheduling accuracy we should end up somewhere in that range
        self.assertGreaterEqual(interval_millis, wait_period_millis - acceptable_delta_millis)
        self.assertLessEqual(interval_millis, wait_period_millis + acceptable_delta_millis)
