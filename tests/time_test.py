from unittest import TestCase

import time

import rally.time


class TimeTests(TestCase):
    def test_split_time_increases(self):
        wait_period_seconds = 0.05

        stop_watch = rally.time.Clock.stop_watch()
        stop_watch.start()
        prev_split_time = 0
        for i in range(3):
            time.sleep(wait_period_seconds)
            split_time = stop_watch.split_time()
            self.assertLess(prev_split_time, split_time)
            prev_split_time = split_time
        stop_watch.stop()
        total_time = stop_watch.total_time()
        self.assertLessEqual(prev_split_time, total_time)

    def test_total_time_roughly_in_expected_range(self):
        wait_period_seconds = 0.05
        acceptable_delta_seconds = 0.01

        stop_watch = rally.time.Clock.stop_watch()
        stop_watch.start()
        time.sleep(wait_period_seconds)
        stop_watch.stop()

        interval = stop_watch.total_time()
        # depending on scheduling accuracy we should end up somewhere in that range
        self.assertGreaterEqual(interval, wait_period_seconds - acceptable_delta_seconds)
        self.assertLessEqual(interval, wait_period_seconds + acceptable_delta_seconds)
