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
import logging
import os
import socket
import traceback
import typing
from typing import Any

import thespian.actors  # type: ignore[import-untyped]
import thespian.system.messages.status  # type: ignore[import-untyped]

from esrally import exceptions, log
from esrally.utils import console

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

    def __init__(self):
        super().__init__()
        self.children: list[thespian.actors.ActorAddress] = []
        self.received_responses = []
        self.status = None
        log.post_configure_actor_logging()
        self.logger = logging.getLogger(type(self).__module__)
        console.set_assume_tty(assume_tty=False)
        LOG.info("Actor initialized: %s (pid=%s)", type(self).__name__, os.getpid())

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
) -> bool | None:
    """It determines whether an actor system is already running by opening a socket connection.

    Notes:
        - It may be possible that another system is running on the same port.
        - This is working only when system base is "multiprocTCPBase"
    """
    if system_base is None:
        system_base = __SYSTEM_BASE
    if system_base != "multiprocTCPBase":
        # This system is not supported yet.
        return None

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            ip = ip or "127.0.0.1"
            port = port or 1900
            LOG.info("Looking for an already running actor system (ip='%s', port=%d)...", ip, port)
            sock.connect((ip, port))
            return True
        except OSError as ex:
            LOG.info("Failed to connect to already running actor system (ip='%s', port=%d): %s", ip, port, ex)

    return False


def use_offline_actor_system() -> None:
    global __SYSTEM_BASE
    __SYSTEM_BASE = "multiprocQueueBase"
    LOG.info("Actor system base set to [%s]", __PROCESS_STARTUP_METHOD)


ProcessStartupMethod = typing.Literal[
    "fork",
    "forkserver",
    "spawn",
]


__PROCESS_STARTUP_METHOD: ProcessStartupMethod | None = None


def set_startup_method(method: ProcessStartupMethod) -> None:
    global __PROCESS_STARTUP_METHOD
    __PROCESS_STARTUP_METHOD = method
    LOG.info("Actor process startup method set to [%s]", __PROCESS_STARTUP_METHOD)


def bootstrap_actor_system(
    try_join: bool = False,
    prefer_local_only: bool = False,
    local_ip: str | None = None,
    admin_port: int | None = None,
    coordinator_ip: str | None = None,
    coordinator_port: int | None = None,
) -> thespian.actors.ActorSystem:
    system_base = __SYSTEM_BASE
    capabilities: dict[str, Any] = {}
    log_defs: Any = None
    if try_join and (
        system_base != "multiprocTCPBase" or actor_system_already_running(ip=local_ip, port=admin_port, system_base=system_base)
    ):
        LOG.info("Try joining already running actor system with system base [%s].", system_base)
    else:
        # All actor system are coordinator unless another coordinator is known to exist.
        capabilities["coordinator"] = True

        if system_base in ("multiprocTCPBase", "multiprocUDPBase"):
            if prefer_local_only:
                LOG.info("Bootstrapping locally running actor system with system base [%s].", system_base)
                local_ip = coordinator_ip = "127.0.0.1"

            if local_ip:
                local_ip, admin_port = resolve(local_ip, admin_port)
                capabilities["ip"] = local_ip

            if admin_port:
                capabilities["Admin Port"] = admin_port

            if coordinator_ip:
                coordinator_ip, coordinator_port = resolve(coordinator_ip, coordinator_port)
                if coordinator_port:
                    coordinator_port = int(coordinator_port)
                    if coordinator_port:
                        coordinator_ip += f":{coordinator_port}"
                capabilities["Convention Address.IPv4"] = coordinator_ip

            if coordinator_ip and local_ip and coordinator_ip != local_ip:
                capabilities["coordinator"] = False

        process_startup_method: ProcessStartupMethod | None = __PROCESS_STARTUP_METHOD
        if process_startup_method:
            capabilities["Process Startup Method"] = process_startup_method

        log_defs = log.load_configuration()
        LOG.info("Starting actor system with system base [%s] and capabilities [%s]...", system_base, capabilities)

    try:
        actor_system = thespian.actors.ActorSystem(
            systemBase=system_base,
            capabilities=capabilities,
            logDefs=log_defs,
        )
    except thespian.actors.ActorSystemException:
        LOG.exception("Could not initialize actor system with system base [%s] and capabilities [%s].", system_base, capabilities)
        raise

    LOG.info("Successfully initialized with system base [%s] and capabilities [%s].", system_base, actor_system.capabilities)
    return actor_system


def resolve(host: str, port: int | None = None, family: int = socket.AF_INET, proto: int = socket.IPPROTO_TCP) -> tuple[str, int | None]:
    address_info: tuple[Any, Any, Any, Any, tuple[Any, ...]]
    for address_info in socket.getaddrinfo(host, port=port or None, family=family, proto=proto):
        address = address_info[4]
        if len(address) == 2 and isinstance(address[0], str) and isinstance(address[1], int):
            host, port = address
    return host, port or None
