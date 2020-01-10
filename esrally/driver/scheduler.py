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

import logging
import random
import types

from esrally import exceptions

# Mapping from task to scheduler
__SCHEDULERS = {}


def scheduler_for(name, params):
    """
    Creates a scheduler instance

    :param name: The name under which the scheduler is registered.
    :param params: A dict containing the parameters for this scheduler instance.
    :return: An initialized scheduler instance.
    """
    try:
        s = __SCHEDULERS[name]
    except KeyError:
        raise exceptions.RallyError("No scheduler available for name [%s]" % name)
    return s(params)


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
        logger.debug("Registering function [%s] for [%s].", str(scheduler), str(name))
        # lazy initialize a delegating scheduler
        __SCHEDULERS[name] = lambda params: DelegatingScheduler(params, scheduler)
    else:
        logger.debug("Registering object [%s] for [%s].", str(scheduler), str(name))
        __SCHEDULERS[name] = scheduler


class Scheduler:
    def __init__(self, params):
        self.params = params

    def next(self, current):
        raise NotImplementedError("abstract method")


class DelegatingScheduler(Scheduler):
    def __init__(self, params, delegate):
        super().__init__(params)
        self.delegate = delegate

    def next(self, current):
        return self.delegate(current)


def _calculate_wait_time(params):
    clients = params.get("clients", 1)
    target_throughput = params.get("target-throughput")
    target_interval = params.get("target-interval")

    if target_interval is not None and target_throughput is not None:
        raise exceptions.SystemSetupError("Found target-interval [%s] and target-throughput [%s] but only one of them is allowed."
                                          % (str(target_interval), str(target_throughput)))
    if target_interval:
        wait_time = target_interval * clients
    elif target_throughput:
        wait_time = clients / target_throughput
    else:
        wait_time = 0
    return wait_time


class DeterministicScheduler(Scheduler):
    """
    Schedules the next execution according to a `deterministic distribution <https://en.wikipedia.org/wiki/Degenerate_distribution>`_.
    """

    def __init__(self, params):
        super().__init__(params)
        self.wait_time = _calculate_wait_time(params)

    def next(self, current):
        # no need for calculations when we are not rate limiting
        if self.wait_time > 0:
            return current + self.wait_time
        else:
            return 0

    def __str__(self):
        return "deterministic scheduler"


class PoissonScheduler(Scheduler):
    """
    Schedules the next execution according to a `Poisson distribution <https://en.wikipedia.org/wiki/Poisson_distribution>`_. A Poisson
    distribution models random independent arrivals of clients which on average match the expected arrival rate which makes it suitable
    for modelling access in open systems.

    See also http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
    """

    def __init__(self, params):
        super().__init__(params)
        wait_time = _calculate_wait_time(params)
        self.rate = 1 / wait_time if wait_time > 0 else 0

    def next(self, current):
        # no need for calculations when we are not rate limiting
        if self.rate > 0:
            return current + random.expovariate(self.rate)
        else:
            return 0

    def __str__(self):
        return "Poisson scheduler"


register_scheduler("deterministic", DeterministicScheduler)
register_scheduler("poisson", PoissonScheduler)
