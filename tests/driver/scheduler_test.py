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

import random

import pytest

from esrally import exceptions
from esrally.driver import scheduler
from esrally.track import track


def assert_throughput(sched, expected_average_throughput, msg="", relative_delta=0.05):
    ITERATIONS = 10000
    expected_average_rate = 1 / expected_average_throughput
    sum = 0
    for _ in range(0, ITERATIONS):
        tn = sched.next(0)
        # schedule must not go backwards in time
        assert tn >= 0, msg
        sum += tn
    actual_average_rate = sum / ITERATIONS

    expected_lower_bound = (1.0 - relative_delta) * expected_average_rate
    expected_upper_bound = (1.0 + relative_delta) * expected_average_rate
    assert expected_lower_bound <= actual_average_rate <= expected_upper_bound


class TestDeterministicScheduler:
    def test_schedule_matches_expected_target_throughput(self):
        target_throughput = random.randint(10, 1000)

        # this scheduler does not make use of the task, thus we won't specify it here
        s = scheduler.DeterministicScheduler(task=None, target_throughput=target_throughput)
        assert_throughput(s, target_throughput, f"target throughput=[{target_throughput}] ops/s")


class TestPoissonScheduler:
    def test_schedule_matches_expected_target_throughput(self):
        target_throughput = random.randint(10, 1000)
        # this scheduler does not make use of the task, thus we won't specify it here
        s = scheduler.PoissonScheduler(task=None, target_throughput=target_throughput)
        assert_throughput(s, target_throughput, f"target throughput=[{target_throughput}] ops/s")


class TestUnitAwareScheduler:
    def test_scheduler_rejects_differing_throughput_units(self):
        task = track.Task(
            name="bulk-index",
            operation=track.Operation(name="bulk-index", operation_type=track.OperationType.Bulk.to_hyphenated_string()),
            clients=4,
            params={"target-throughput": "5000 MB/s"},
        )

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            s.after_request(now=None, weight=1000, unit="docs", request_meta_data=None)
        assert exc.value.args[0] == (
            "Target throughput for [bulk-index] is specified in [MB/s] but the task throughput is measured in [docs/s]."
        )

    def test_scheduler_adapts_to_changed_weights(self):
        task = track.Task(
            name="bulk-index",
            operation=track.Operation(name="bulk-index", operation_type=track.OperationType.Bulk.to_hyphenated_string()),
            clients=4,
            params={"target-throughput": "5000 docs/s"},
        )

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled
        assert s.next(0) == 0
        # we'll start with bulks of 1.000 docs, which corresponds to 5 requests per second for all clients
        s.after_request(now=None, weight=1000, unit="docs", request_meta_data=None)
        assert s.next(0) == 1 / 5 * task.clients

        # bulk size changes to 10.000 docs, which means one request every two seconds for all clients
        s.after_request(now=None, weight=10000, unit="docs", request_meta_data=None)
        assert s.next(0) == 2 * task.clients

    def test_scheduler_accepts_differing_units_pages_and_ops(self):
        task = track.Task(
            name="scroll-query",
            operation=track.Operation(name="scroll-query", operation_type=track.OperationType.Search.to_hyphenated_string()),
            clients=1,
            params={
                # implicitly: ops/s
                "target-throughput": 10
            },
        )

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled
        assert s.next(0) == 0
        # no exception despite differing units ...
        s.after_request(now=None, weight=20, unit="pages", request_meta_data=None)
        # ... and it is still throttled in ops/s
        assert s.next(0) == 0.1 * task.clients

    def test_scheduler_does_not_change_throughput_for_empty_requests(self):
        task = track.Task(
            name="match-all-query",
            operation=track.Operation(name="query", operation_type=track.OperationType.Search.to_hyphenated_string()),
            clients=1,
            params={
                # implicitly: ops/s
                "target-throughput": 10
            },
        )

        s = scheduler.UnitAwareScheduler(task=task, scheduler_class=scheduler.DeterministicScheduler)
        # first request is unthrottled...
        s.before_request(now=0)
        assert s.next(0) == 0
        # ... but it also produced an error (zero ops)
        s.after_request(now=1, weight=0, unit="ops", request_meta_data=None)
        # next request is still unthrottled
        s.before_request(now=1)
        assert s.next(0) == 0
        s.after_request(now=2, weight=1, unit="ops", request_meta_data=None)
        # now we throttle
        s.before_request(now=2)
        assert s.next(0) == 0.1 * task.clients


class TestSchedulerCategorization:
    class LegacyScheduler:
        # pylint: disable=unused-variable
        def __init__(self, params):
            pass

    class LegacySchedulerWithAdditionalArgs:
        # pylint: disable=unused-variable
        def __init__(self, params, my_default_param=True):
            pass

    def test_detects_legacy_scheduler(self):
        assert scheduler.is_legacy_scheduler(self.LegacyScheduler)
        assert scheduler.is_legacy_scheduler(self.LegacySchedulerWithAdditionalArgs)

    def test_a_regular_scheduler_is_not_a_legacy_scheduler(self):
        assert not scheduler.is_legacy_scheduler(scheduler.DeterministicScheduler)
        assert not scheduler.is_legacy_scheduler(scheduler.UnitAwareScheduler)

    def test_is_simple_scheduler(self):
        assert scheduler.is_simple_scheduler(scheduler.PoissonScheduler)

    def test_is_not_simple_scheduler(self):
        assert not scheduler.is_simple_scheduler(scheduler.UnitAwareScheduler)


class TestSchedulerThrottling:
    def task(self, schedule=None, target_throughput=None, target_interval=None):
        op = track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string())
        params = {}
        if target_throughput is not None:
            params["target-throughput"] = target_throughput
        if target_interval is not None:
            params["target-interval"] = target_interval
        return track.Task("test", op, schedule=schedule, params=params)

    def test_throttled_by_target_throughput(self):
        assert not scheduler.run_unthrottled(self.task(target_throughput=4, schedule="deterministic"))

    def test_throttled_by_target_interval(self):
        assert not scheduler.run_unthrottled(self.task(target_interval=2))

    def test_throttled_by_custom_schedule(self):
        assert not scheduler.run_unthrottled(self.task(schedule="my-custom-schedule"))

    def test_unthrottled_by_target_throughput(self):
        assert scheduler.run_unthrottled(self.task(target_throughput=None))

    def test_unthrottled_by_target_interval(self):
        assert scheduler.run_unthrottled(self.task(target_interval=0, schedule="poisson"))


class TestLegacyWrappingScheduler:
    class SimpleLegacyScheduler:
        # pylint: disable=unused-variable
        def __init__(self, params):
            pass

        def next(self, current):
            return current

    def setup_method(self, method):
        scheduler.register_scheduler("simple", self.SimpleLegacyScheduler)

    def teardown_method(self, method):
        scheduler.remove_scheduler("simple")

    def test_legacy_scheduler(self):
        task = track.Task(
            name="raw-request",
            operation=track.Operation(name="raw", operation_type=track.OperationType.RawRequest.to_hyphenated_string()),
            clients=1,
            schedule="simple",
        )

        s = scheduler.scheduler_for(task)

        assert s.next(0) == 0
        assert s.next(0) == 0
