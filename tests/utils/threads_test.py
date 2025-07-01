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
import threading
import time
from queue import Queue

import pytest

from esrally.utils.threads import (
    ContinuousTimer,
    TimedEvent,
    WaitGroup,
    WaitGroupLimitError,
)


def test_continuous_timer():
    calls = Queue()
    start_time = time.monotonic_ns()

    def func():
        calls.put(time.monotonic_ns() - start_time)

    interval = 0.05
    count = 3

    timer = ContinuousTimer(function=func, interval=interval)
    timer.start()

    times = []
    for _ in range(count):
        times.append(calls.get(timeout=5.0))
    duration = time.monotonic_ns() - start_time

    timer.cancel()

    assert timer.wait(timeout=5.0), "finished not set"

    assert duration >= interval * count, f"finished early: {duration}"
    assert interval <= times[0], f"started early: {times[0]}"
    assert duration >= times[-1], f"finished early: {times[-1]}"


def test_timed_event():
    _test_timed_event(event=TimedEvent())
    _test_timed_event(event=WaitGroup())


def _test_timed_event(event):
    assert event.time is None
    assert not event

    waiting = threading.Event()
    notified = threading.Event()

    for _ in range(5):

        def waiter_func():
            waiting.set()
            event.wait(timeout=5.0)
            notified.set()

        waiting.clear()
        notified.clear()
        waiter = threading.Thread(target=waiter_func)
        waiter.start()
        assert waiting.wait(timeout=5.0)
        assert waiter.is_alive()
        assert not notified.is_set()

        start_time = time.monotonic_ns()
        assert event.set()
        stop_time = time.monotonic_ns()
        assert event.time is not None
        assert start_time <= event.time <= stop_time
        assert event

        assert notified.wait(timeout=5.0)
        waiter.join(timeout=5.0)
        assert not waiter.is_alive()

        assert not event.set()
        assert event.time is not None
        assert start_time <= event.time <= stop_time
        assert event

        event.clear()
        assert event.time is None
        assert not event


def test_wait_group():
    max_count = 4
    wg = WaitGroup(max_count=max_count)
    assert wg.count == 0
    assert wg.max_count == max_count
    assert not wg

    waiting = threading.Event()
    notified = threading.Event()

    def waiter_func():
        waiting.set()
        wg.wait(timeout=5.0)
        notified.set()

    waiter = threading.Thread(target=waiter_func)
    waiter.start()
    assert waiting.wait(timeout=5.0)
    assert waiter.is_alive()

    for i in range(max_count):
        assert wg.count == i
        assert wg.max_count == max_count
        assert not wg
        assert not notified.is_set()
        wg.add(1)

    assert wg.count == max_count
    assert wg.max_count == max_count
    assert not wg
    assert not notified.is_set()

    try:
        wg.add(1)
    except WaitGroupLimitError:
        pass
    else:
        pytest.fail("didn't raise WaitGroupLimitError exception")

    for i in range(max_count - 1):
        assert wg.count == max_count - i
        assert wg.max_count == max_count
        assert not wg
        assert not notified.is_set()
        wg.done()

    assert wg.count == 1
    assert wg.max_count == max_count
    assert not wg
    assert not notified.is_set()
    assert waiter.is_alive()

    wg.done()
    assert wg.count == 0
    assert wg.max_count == max_count
    assert wg
    assert notified.wait(timeout=5.0)
    waiter.join(timeout=5.0)
    assert not waiter.is_alive()
