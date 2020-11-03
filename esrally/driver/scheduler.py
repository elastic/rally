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

import inspect
import logging
import random
import types
from abc import ABC, abstractmethod

import esrally.track
from esrally import exceptions

# Mapping from task to scheduler
__SCHEDULERS = {}

"""
The scheduler module defines an API to determine *when* Rally should issue a request. There are two types of schedulers:

# Simple Schedulers

A simple scheduler has the following signature::

    class MySimpleScheduler:
        def __init__(task, target_throughput):
           ...

        def next(self, current):
            ...

In ``__init__`` the current task and the precalculated target throughput denoted in requests per second is provided.

The implementation of ``next`` gets passed the previous point in time in seconds, starting from zero and needs to
return the next point in time when Rally should issue a request.

# Regular Schedulers

If a simple scheduler is not sufficient, a more complex API can be implemented::

class MyScheduler:
    def __init__(self, task):
        ...

    def before_request(self, now):
        ...

    def after_request(self, now, weight, unit, request_meta_data):
        ...

    def next(self, current):
        ...

* In ``__init__`` the current task is provided. Implementations need to calculate the target throughput themselves based
  on the task's properties.
* ``before_request`` is invoked by Rally before a request is executed. ``now`` provides the current timestamp in seconds.
* Similarly, ``after_request`` is invoked by Rally after a response has been received. ``now`` is the current timestamp,
  ``weight``, ``unit`` and ``request_meta_data`` are passed from the respective runner. For a bulk request, Rally
  passes e.g. ``weight=5000, unit="docs"`` or for a search request ``weight=1, unit="ops"``. Note that when a request
  has finished with an error, the ``weight`` might be zero (depending on the type of error).
* ``next`` needs to behave identical to simple schedulers.

``before_request`` and ``after_request`` can be used to adjust the target throughput based on feedback from the runner.

If the scheduler also needs access to the parameter source, provide a ``parameter_source`` property. Rally injects the
task's parameter source into this property.
"""


def scheduler_for(task: esrally.track.Task):
    """
    Creates a scheduler instance

    :param task: The current task for which a scheduler is needed.
    :return: An initialized scheduler instance.
    """
    logger = logging.getLogger(__name__)
    if not task.throttled:
        return Unthrottled()

    try:
        scheduler_class = __SCHEDULERS[task.schedule]
    except KeyError:
        raise exceptions.RallyError(f"No scheduler available for name [{task.schedule}]")

    # for backwards-compatibility - treat existing schedulers as top-level schedulers
    if is_legacy_scheduler(scheduler_class):
        logger.warning("Scheduler [%s] implements a deprecated API. Please adapt it.", scheduler_class)
        return LegacyWrappingScheduler(task, scheduler_class)
    elif is_simple_scheduler(scheduler_class):
        logger.debug("Treating [%s] for [%s] as a simple scheduler.", scheduler_class, task)
        return UnitAwareScheduler(task, scheduler_class)
    else:
        logger.debug("Treating [%s] for [%s] as non-simple scheduler.", scheduler_class, task)
        return scheduler_class(task)


def is_legacy_scheduler(scheduler_class):
    """
    Determines whether a scheduler is a legacy implementation that gets passed task parameters instead of only the
    target throughput.
    """
    constructor_params = inspect.signature(scheduler_class.__init__).parameters
    return len(constructor_params) == 2 and "params" in constructor_params


def is_simple_scheduler(scheduler_class):
    """
    Determines whether a scheduler is a "simple" scheduler, i.e. it doesn't consider feedback from the runner.
    """
    methods = inspect.getmembers(scheduler_class, inspect.isfunction)
    method_names = [name for name, _ in methods]
    return not all(scheduler_method in method_names for scheduler_method in ["before_request", "after_request", "next"])


def register_scheduler(name, scheduler):
    """
    Registers a new scheduler. Attempting to register a scheduler with a name that is already taken will raise a ``SystemSetupError``.

    :param name: The name under which to register the scheduler.
    :param scheduler: Either a unary function ``float`` -> ``float`` or a class with the same interface as ``Scheduler``.

    """
    logger = logging.getLogger(__name__)
    if name in __SCHEDULERS:
        raise exceptions.SystemSetupError("A scheduler with the name [%s] is already registered." % name)
    # we'd rather use callable() but this will erroneously also classify a class as callable...
    if isinstance(scheduler, types.FunctionType):
        logger.warning("Function-based schedulers are deprecated. Please reimplement [%s] as a class.", str(scheduler))
        logger.debug("Registering function [%s] for [%s].", str(scheduler), str(name))

        # lazy initialize a delegating scheduler
        __SCHEDULERS[name] = lambda _: DelegatingScheduler(scheduler)
    else:
        logger.debug("Registering object [%s] for [%s].", str(scheduler), str(name))
        __SCHEDULERS[name] = scheduler


# Only intended for unit-testing!
def remove_scheduler(name):
    del __SCHEDULERS[name]


class SimpleScheduler(ABC):
    @abstractmethod
    def next(self, current):
        ...


class Scheduler(ABC):
    def before_request(self, now):
        pass

    def after_request(self, now, weight, unit, request_meta_data):
        pass

    @abstractmethod
    def next(self, current):
        ...


# Deprecated
class DelegatingScheduler(SimpleScheduler):
    """
    Delegates to a scheduler function and acts as an adapter to the rest of the system.
    """
    def __init__(self, delegate):
        super().__init__()
        self.delegate = delegate

    def next(self, current):
        return self.delegate(current)


# Deprecated
class LegacyWrappingScheduler(Scheduler):
    """
    Wraps legacy implementations to stay backwards-compatible with older scheduler implementations.
    """
    def __init__(self, task, legacy_scheduler_class):
        super().__init__()
        # the legacy API was based on parameters so only provide these
        self.legacy_scheduler = legacy_scheduler_class(task.params)

    def next(self, current):
        return self.legacy_scheduler(current)


class Unthrottled(Scheduler):
    """
    Rally-internal scheduler to handle unthrottled tasks.
    """
    def next(self, current):
        return 0

    def __str__(self):
        return "unthrottled"


class DeterministicScheduler(SimpleScheduler):
    """
    Schedules the next execution according to a
    `deterministic distribution <https://en.wikipedia.org/wiki/Degenerate_distribution>`_.
    """
    # pylint: disable=unused-variable
    def __init__(self, task, target_throughput):
        super().__init__()
        self.wait_time = 1 / target_throughput

    def next(self, current):
        return current + self.wait_time

    def __str__(self):
        return "deterministic scheduler"


class PoissonScheduler(SimpleScheduler):
    """
    Schedules the next execution according to a
    `Poisson distribution <https://en.wikipedia.org/wiki/Poisson_distribution>`_. A Poisson distribution models random
    independent arrivals of clients which on average match the expected arrival rate which makes it suitable for
    modelling access in open systems.

    See also http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
    """
    # pylint: disable=unused-variable
    def __init__(self, task, target_throughput):
        super().__init__()
        self.rate = target_throughput

    def next(self, current):
        return current + random.expovariate(self.rate)

    def __str__(self):
        return "Poisson scheduler"


class UnitAwareScheduler(Scheduler):
    """
    Scheduler implementation that adjusts target throughput based on feedback from the runner. It delegates actual
    scheduling to the scheduler provided by the user in the track.

    """
    def __init__(self, task, scheduler_class):
        super().__init__()
        self.task = task
        self.first_request = True
        self.current_weight = None
        self.scheduler_class = scheduler_class
        # start unthrottled to avoid conditional logic on the hot code path
        self.scheduler = Unthrottled()

    def after_request(self, now, weight, unit, request_meta_data):
        if self.first_request or self.current_weight != weight:
            expected_unit = self.task.target_throughput.unit
            actual_unit = f"{unit}/s"
            if actual_unit != expected_unit:
                # *temporary* workaround to convert pages/s (scrolls) to ops/s to stay backwards-compatible.
                #
                # This ensures that we throttle based on ops/s but report based on pages/s (as before).
                if actual_unit == "pages/s" and expected_unit == "ops/s":
                    weight = 1
                    if self.first_request:
                        logging.getLogger(__name__).warning("Task [%s] throttles based on ops/s but reports pages/s. "
                                                            "Please specify the target throughput in pages/s instead.",
                                                            self.task)
                else:
                    raise exceptions.RallyAssertionError(f"Target throughput for [{self.task}] is specified in "
                                                         f"[{expected_unit}] but the task throughput is measured "
                                                         f"in [{actual_unit}].")

            self.first_request = False
            self.current_weight = weight
            # throughput in requests/s for this client
            target_throughput = (self.task.target_throughput.value / self.task.clients / self.current_weight)
            self.scheduler = self.scheduler_class(self.task, target_throughput)

    def next(self, current):
        return self.scheduler.next(current)

    def __str__(self):
        return f"Unit-aware scheduler delegating to [{self.scheduler_class}]"


register_scheduler("deterministic", DeterministicScheduler)
register_scheduler("poisson", PoissonScheduler)
