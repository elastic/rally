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
# pylint: disable=protected-access

import random
from unittest import TestCase

from esrally import exceptions
from esrally.driver import scheduler
from esrally.track import track


class SchedulerTestCase(TestCase):
    ITERATIONS = 10000

    def assertThroughputEquals(self, sched, expected_average_throughput, msg="", relative_delta=0.05):
        expected_average_rate = 1 / expected_average_throughput
        sum = 0
        for _ in range(0, SchedulerTestCase.ITERATIONS):
            tn = sched.next(0)
            # schedule must not go backwards in time
            self.assertGreaterEqual(tn, 0, msg)
            sum += tn
        actual_average_rate = sum / SchedulerTestCase.ITERATIONS

        expected_lower_bound = (1.0 - relative_delta) * expected_average_rate
        expected_upper_bound = (1.0 + relative_delta) * expected_average_rate

        self.assertGreaterEqual(actual_average_rate, expected_lower_bound,
                                f"{msg}: expected target rate to be >= [{expected_lower_bound}] but was [{actual_average_rate}].")
        self.assertLessEqual(actual_average_rate, expected_upper_bound,
                             f"{msg}: expected target rate to be <= [{expected_upper_bound}] but was [{actual_average_rate}].")


class DeterministicSchedulerTests(SchedulerTestCase):
    def test_schedule_matches_expected_target_throughput(self):
        target_throughput = random.randint(10, 1000)

        # this scheduler does not make use of the task, thus we won't specify it here
        s = scheduler.DeterministicScheduler(task=None, target_throughput=target_throughput)
        self.assertThroughputEquals(s, target_throughput, f"target throughput=[{target_throughput}] ops/s")


class PoissonSchedulerTests(SchedulerTestCase):
    def test_schedule_matches_expected_target_throughput(self):
        target_throughput = random.randint(10, 1000)
        # this scheduler does not make use of the task, thus we won't specify it here
        s = scheduler.PoissonScheduler(task=None, target_throughput=target_throughput)
        self.assertThroughputEquals(s, target_throughput, f"target throughput=[{target_throughput}] ops/s")


class UnitAwareSchedulerTests(TestCase):
    def test_scheduler_rejects_differing_throughput_units(self):
        task = track.Task(name="bulk-index",
                          operation=track.Operation(
                              name="bulk-index",
                              operation_type=track.OperationType.Bulk.to_hyphenated_string()),
                          clients=4,
                          params={
                              "target-throughput": "5000 MB/s"
                          })

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        with self.assertRaises(exceptions.RallyAssertionError) as ex:
            s.after_request(now=None, weight=1000, unit="docs", request_meta_data=None)
        self.assertEqual("Target throughput for [bulk-index] is specified in [MB/s] but the task throughput "
                         "is measured in [docs/s].", ex.exception.args[0])

    def test_scheduler_adapts_to_changed_weights(self):
        task = track.Task(name="bulk-index",
                          operation=track.Operation(
                              name="bulk-index",
                              operation_type=track.OperationType.Bulk.to_hyphenated_string()),
                          clients=4,
                          params={
                              "target-throughput": "5000 docs/s"
                          })

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0, s.next(0))
        # we'll start with bulks of 1.000 docs, which corresponds to 5 requests per second for all clients
        s.after_request(now=None, weight=1000, unit="docs", request_meta_data=None)
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(1 / 5 * task.clients, s.next(0))

        # bulk size changes to 10.000 docs, which means one request every two seconds for all clients
        s.after_request(now=None, weight=10000, unit="docs", request_meta_data=None)
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(2 * task.clients, s.next(0))

    def test_scheduler_accepts_differing_units_pages_and_ops(self):
        task = track.Task(name="scroll-query",
                          operation=track.Operation(
                              name="scroll-query",
                              operation_type=track.OperationType.Search.to_hyphenated_string()),
                          clients=1,
                          params={
                              # implicitly: ops/s
                              "target-throughput": 10
                          })

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0, s.next(0))
        # no exception despite differing units ...
        s.after_request(now=None, weight=20, unit="pages", request_meta_data=None)
        # ... and it is still throttled in ops/s
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0.1 * task.clients, s.next(0))

    def test_scheduler_does_not_change_throughput_for_empty_requests(self):
        task = track.Task(name="match-all-query",
                          operation=track.Operation(
                              name="query",
                              operation_type=track.OperationType.Search.to_hyphenated_string()),
                          clients=1,
                          params={
                              # implicitly: ops/s
                              "target-throughput": 10
                          })

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled...
        s.before_request(now=0)
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0, s.next(0))
        # ... but it also produced an error (zero ops)
        s.after_request(now=1, weight=0, unit="ops", request_meta_data=None)
        # next request is still unthrottled
        s.before_request(now=1)
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0, s.next(0))
        s.after_request(now=2, weight=1, unit="ops", request_meta_data=None)
        # now we throttle
        s.before_request(now=2)
        # suppress pylint false positive
        # pylint: disable=not-callable
        self.assertEqual(0.1 * task.clients, s.next(0))


class SchedulerCategorizationTests(TestCase):
    class LegacyScheduler:
        # pylint: disable=unused-variable
        def __init__(self, params):
            pass

    class LegacySchedulerWithAdditionalArgs:
        # pylint: disable=unused-variable
        def __init__(self, params, my_default_param=True):
            pass

    def test_detects_legacy_scheduler(self):
        self.assertTrue(scheduler.is_legacy_scheduler(SchedulerCategorizationTests.LegacyScheduler))
        self.assertTrue(scheduler.is_legacy_scheduler(SchedulerCategorizationTests.LegacySchedulerWithAdditionalArgs))

    def test_a_regular_scheduler_is_not_a_legacy_scheduler(self):
        self.assertFalse(scheduler.is_legacy_scheduler(scheduler.DeterministicScheduler))
        self.assertFalse(scheduler.is_legacy_scheduler(scheduler.UnitAwareScheduler))

    def test_is_simple_scheduler(self):
        self.assertTrue(scheduler.is_simple_scheduler(scheduler.PoissonScheduler))

    def test_is_not_simple_scheduler(self):
        self.assertFalse(scheduler.is_simple_scheduler(scheduler.UnitAwareScheduler))


class SchedulerThrottlingTests(TestCase):
    def task(self, schedule=None, target_throughput=None, target_interval=None):
        op = track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string())
        params = {}
        if target_throughput is not None:
            params["target-throughput"] = target_throughput
        if target_interval is not None:
            params["target-interval"] = target_interval
        return track.Task("test", op, schedule=schedule, params=params)

    def test_throttled_by_target_throughput(self):
        self.assertFalse(scheduler.run_unthrottled(self.task(target_throughput=4, schedule="deterministic")))

    def test_throttled_by_target_interval(self):
        self.assertFalse(scheduler.run_unthrottled(self.task(target_interval=2)))

    def test_throttled_by_custom_schedule(self):
        self.assertFalse(scheduler.run_unthrottled(self.task(schedule="my-custom-schedule")))

    def test_unthrottled_by_target_throughput(self):
        self.assertTrue(scheduler.run_unthrottled(self.task(target_throughput=None)))

    def test_unthrottled_by_target_interval(self):
        self.assertTrue(scheduler.run_unthrottled(self.task(target_interval=0, schedule="poisson")))


class LegacyWrappingSchedulerTests(TestCase):
    class SimpleLegacyScheduler:
        # pylint: disable=unused-variable
        def __init__(self, params):
            pass

        def next(self, current):
            return current

    def setUp(self):
        scheduler.register_scheduler("simple", LegacyWrappingSchedulerTests.SimpleLegacyScheduler)

    def tearDown(self):
        scheduler.remove_scheduler("simple")

    def test_legacy_scheduler(self):
        task = track.Task(name="raw-request",
                          operation=track.Operation(
                              name="raw",
                              operation_type=track.OperationType.RawRequest.to_hyphenated_string()),
                          clients=1,
                          schedule="simple")

        s = scheduler.scheduler_for(task)

        self.assertEqual(0, s.next(0))
        self.assertEqual(0, s.next(0))
