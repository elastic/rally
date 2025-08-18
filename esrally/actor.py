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
from __future__ import annotations

import logging
import socket
import traceback
import typing
from typing import Any

import thespian.actors  # type: ignore[import-untyped]
import thespian.system.messages.status  # type: ignore[import-untyped]

from esrally import exceptions, log
from esrally.utils import console, net

LOG = logging.getLogger(__name__)


class BenchmarkFailure:
    """
    Indicates a failure in the benchmark execution due to an exception
    """

    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause


class BenchmarkCancelled:
    """
    Indicates that the benchmark has been cancelled (by the user).
    """


def parametrized(decorator):
    """

    Helper meta-decorator that allows us to provide parameters to a decorator.

    :param decorator: The decorator that should accept parameters.
    """

    def inner(*args, **kwargs):
        def g(f):
            return decorator(f, *args, **kwargs)

        return g

    return inner


@parametrized
def no_retry(f, actor_name):
    """

    Decorator intended for Thespian message handlers with the signature ``receiveMsg_$MSG_NAME(self, msg, sender)``. Thespian will
    assume that a message handler that raises an exception can be retried. It will then retry once and give up afterwards just leaving
    a trace of that in the actor system's internal log file. However, this is usually *not* what we want in Rally. If handling of a
    message fails we instead want to notify a node higher up in the actor hierarchy.

    We achieve that by sending a ``BenchmarkFailure`` message to the original sender. Note that this might as well be the current
    actor (e.g. when handling a ``Wakeup`` message). In that case the actor itself is responsible for forwarding the benchmark failure
    to its parent actor.

    Example usage:

    @no_retry("special forces actor")
    def receiveMsg_DefuseBomb(self, msg, sender):
        # might raise an exception
        pass

    If this message handler raises an exception, the decorator will turn it into a ``BenchmarkFailure`` message with its ``message``
    property set to "Error in special forces actor" which is returned to the original sender.

    :param f: The message handler. Does not need to passed directly, this is handled by the decorator infrastructure.
    :param actor_name: A human readable name of the current actor that should be used in the exception message.
    """

    def guard(self, msg, sender):
        # noinspection PyBroadException
        try:
            return f(self, msg, sender)
        except BaseException:
            # log here as the full trace might get lost.
            logging.getLogger(__name__).exception("Error in %s", actor_name)
            # don't forward the exception as is because the main process might not have this class available on the load path
            # and will fail then while deserializing the cause.
            self.send(sender, BenchmarkFailure(traceback.format_exc()))

    return guard


class RallyActor(thespian.actors.ActorTypeDispatcher):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.children: list[thespian.actors.ActorAddress] = []
        self.received_responses = []
        self.status = None
        log.post_configure_actor_logging()
        self.logger = logging.getLogger(__name__)
        console.set_assume_tty(assume_tty=False)

    # The method name is required by the actor framework
    # noinspection PyPep8Naming
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        for name, value in requirements.items():
            current = capabilities.get(name, None)
            if current != value:
                # A mismatch by is not a problem by itself as long as at least one actor system instance matches the requirements.
                return False
        return True

    def transition_when_all_children_responded(self, sender, msg, expected_status, new_status, transition):
        """

        Waits until all children have sent a specific response message and then transitions this actor to a new status.

        :param sender: The child actor that has responded.
        :param msg: The response message.
        :param expected_status: The status in which this actor should be upon calling this method.
        :param new_status: The new status once all child actors have responded.
        :param transition: A parameter-less function to call immediately after changing the status.
        """
        if self.is_current_status_expected(expected_status):
            self.received_responses.append(msg)
            response_count = len(self.received_responses)
            expected_count = len(self.children)

            self.logger.debug(
                "[%d] of [%d] child actors have responded for transition from [%s] to [%s].",
                response_count,
                expected_count,
                self.status,
                new_status,
            )
            if response_count == expected_count:
                self.logger.debug(
                    "All [%d] child actors have responded. Transitioning now from [%s] to [%s].", expected_count, self.status, new_status
                )
                # all nodes have responded, change status
                self.status = new_status
                self.received_responses = []
                transition()
            elif response_count > expected_count:
                raise exceptions.RallyAssertionError(
                    "Received [%d] responses but only [%d] were expected to transition from [%s] to [%s]. The responses are: %s"
                    % (response_count, expected_count, self.status, new_status, self.received_responses)
                )
        else:
            raise exceptions.RallyAssertionError(
                "Received [%s] from [%s] but we are in status [%s] instead of [%s]." % (type(msg), sender, self.status, expected_status)
            )

    def send_to_children_and_transition(self, sender, msg, expected_status, new_status):
        """

        Sends the provided message to all child actors and immediately transitions to the new status.

        :param sender: The actor from which we forward this message (in case it is message forwarding). Otherwise our own address.
        :param msg: The message to send.
        :param expected_status: The status in which this actor should be upon calling this method.
        :param new_status: The new status.
        """
        if self.is_current_status_expected(expected_status):
            self.logger.debug("Transitioning from [%s] to [%s].", self.status, new_status)
            self.status = new_status
            child: thespian.actors.ActorAddress
            for child in filter(None, self.children):
                self.send(child, msg)
        else:
            raise exceptions.RallyAssertionError(
                "Received [%s] from [%s] but we are in status [%s] instead of [%s]." % (type(msg), sender, self.status, expected_status)
            )

    def is_current_status_expected(self, expected_status):
        # if we don't expect anything, we're always in the right status
        if not expected_status:
            return True
        # do an explicit check for a list here because strings are also iterable and we have very tight control over this code anyway.
        elif isinstance(expected_status, list):
            return self.status in expected_status
        else:
            return self.status == expected_status


SystemBase = typing.Literal["simpleSystemBase", "multiprocQueueBase", "multiprocTCPBase", "multiprocUDPBase"]


__SYSTEM_BASE: SystemBase = "multiprocTCPBase"


def actor_system_already_running(
    ip: str | None = None,
    port: int | None = None,
    system_base: SystemBase | None = None,
) -> bool:
    """It determines whether an actor system is already running by opening a socket connection.

    Notes:
        - It may be possible that another system is running on the same port.
        - This is working only when system base is "multiprocTCPBase"
    """
    if system_base is None:
        system_base = __SYSTEM_BASE
    if system_base != "multiprocTCPBase":
        return False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            ip = ip or "127.0.0.1"
            port = port or 1900
            LOG.info("Looking for an already running actor system (ip='%s', port=%d)...", ip, port)
            sock.connect((ip, port))
            return True
        except Exception as ex:
            LOG.info("Failed to connect to already running actor system (ip='%s', port=%d): %s", ip, port, ex)

    return False


def use_offline_actor_system() -> None:
    global __SYSTEM_BASE
    __SYSTEM_BASE = "multiprocQueueBase"


ProcessStartupMethod = typing.Literal[
    "fork",
    "forkserver",
    "spawn",
]


__PROCESS_STARTUP_METHOD: ProcessStartupMethod | None = None


def use_startup_method(method: ProcessStartupMethod) -> None:
    global __PROCESS_STARTUP_METHOD
    __PROCESS_STARTUP_METHOD = method


def bootstrap_actor_system(
    try_join: bool = False,
    prefer_local_only: bool = False,
    local_ip: str | None = None,
    coordinator_ip: str | None = None,
    coordinator_port: int | None = None,
) -> thespian.actors.ActorSystem:
    system_base = __SYSTEM_BASE
    process_startup_method: ProcessStartupMethod | None = __PROCESS_STARTUP_METHOD
    try:
        if try_join:
            if actor_system_already_running(ip=coordinator_ip, port=coordinator_port, system_base=system_base):
                LOG.debug("Joining already running actor system with system base [%s].", system_base)
                return thespian.actors.ActorSystem(system_base)
        elif prefer_local_only:
            if system_base in ("multiprocTCPBase", "multiprocUDPBase"):
                local_ip = coordinator_ip = "127.0.0.1"
            else:
                local_ip = coordinator_ip = None
        else:
            if str(system_base) not in ("multiprocTCPBase", "multiprocUDPBase"):
                raise exceptions.SystemSetupError("Rally requires a network-capable system base but got [%s]." % system_base)
            if not coordinator_ip:
                raise exceptions.SystemSetupError("coordinator IP is required")
            if not local_ip:
                raise exceptions.SystemSetupError("local IP is required")
            # always resolve the public IP here, even if a DNS name is given, otherwise Thespian will be unhappy

        # if we try to join we can only run on the coordinator...
        capabilities: dict[str, Any] = {}
        if process_startup_method:
            capabilities["Process Startup Method"] = process_startup_method
        if local_ip:
            # just needed to determine whether to run benchmarks locally
            local_ip = net.resolve(local_ip)
            capabilities["ip"] = local_ip
        if coordinator_ip:
            # Make the coordinator node the convention leader
            coordinator_ip = net.resolve(coordinator_ip)
            coordinator_port = coordinator_port or 1900
            capabilities["Convention Address.IPv4"] = f"{coordinator_ip}:{coordinator_port}"
        capabilities["coordinator"] = local_ip == coordinator_ip
        LOG.info("Starting actor system with system base [%s] and capabilities [%s].", system_base, capabilities)
        return thespian.actors.ActorSystem(
            systemBase=system_base,
            capabilities=capabilities,
            logDefs=log.load_configuration(),
        )
    except thespian.actors.ActorSystemException:
        LOG.exception("Could not initialize internal actor system.")
        raise
