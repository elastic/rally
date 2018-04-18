import logging
import time
import os

import thespian.actors
import thespian.system.messages.status
from esrally import exceptions
from esrally.utils import console, io, net

logger = logging.getLogger("rally.actor")

root_log_level = logging.INFO
es_log_level = logging.WARNING


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
    pass


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
        try:
            return f(self, msg, sender)
        except BaseException as e:
            msg = "Error in {}".format(actor_name)
            # log here as the full trace might get lost.
            logger.exception(msg)
            self.send(sender, BenchmarkFailure(msg, e))
    return guard


class RallyActor(thespian.actors.ActorTypeDispatcher):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.children = []
        self.received_responses = []
        self.status = None

    @staticmethod
    def configure_logging(actor_logger):
        # see rally#configure_logging() for more details.
        logging.captureWarnings(True)
        # configure each actor's root logger
        actor_logger.parent.setLevel(root_log_level)
        # Also ensure that the elasticsearch logger is properly configured
        logging.getLogger("elasticsearch").setLevel(es_log_level)

    # The method name is required by the actor framework
    # noinspection PyPep8Naming
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        for name, value in requirements.items():
            current = capabilities.get(name, None)
            if current != value:
                # A mismatch by is not a problem by itself as long as at least one actor system instance matches the requirements.
                logger.info("Checking capabilities [%s] against requirements [%s] failed." % (capabilities, requirements))
                return False
        logger.info("Capabilities [%s] match requirements [%s]." % (capabilities, requirements))
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

            logger.info("[%d] of [%d] child actors have responded for transition from [%s] to [%s]." %
                        (response_count, expected_count, self.status, new_status))
            if response_count == expected_count:
                logger.info("All [%d] child actors have responded. Transitioning now from [%s] to [%s]." %
                            (expected_count, self.status, new_status))
                # all nodes have responded, change status
                self.status = new_status
                self.received_responses = []
                transition()
            elif response_count > expected_count:
                raise exceptions.RallyAssertionError(
                    "Received [%d] responses but only [%d] were expected to transition from [%s] to [%s]. The responses are: %s" %
                    (response_count, expected_count, self.status, new_status, self.received_responses))
        else:
            raise exceptions.RallyAssertionError("Received [%s] from [%s] but we are in status [%s] instead of [%s]." %
                                                 (type(msg), sender, self.status, expected_status))

    def send_to_children_and_transition(self, sender, msg, expected_status, new_status):
        """

        Sends the provided message to all child actors and immediately transitions to the new status.

        :param sender: The actor from which we forward this message (in case it is message forwarding). Otherwise our own address.
        :param msg: The message to send.
        :param expected_status: The status in which this actor should be upon calling this method.
        :param new_status: The new status.
        """
        if self.is_current_status_expected(expected_status):
            logger.info("Transitioning from [%s] to [%s]." % (self.status, new_status))
            self.status = new_status
            for m in filter(None, self.children):
                self.send(m, msg)
        else:
            raise exceptions.RallyAssertionError("Received [%s] from [%s] but we are in status [%s] instead of [%s]." %
                                                 (type(msg), sender, self.status, expected_status))

    def is_current_status_expected(self, expected_status):
        # if we don't expect anything, we're always in the right status
        if not expected_status:
            return True
        # do an explicit check for a list here because strings are also iterable and we have very tight control over this code anyway.
        elif isinstance(expected_status, list):
            return self.status in expected_status
        else:
            return self.status == expected_status


# Defined on top-level to allow pickling
def configure_utc_formatter(*args, **kwargs):
    formatter = logging.Formatter(fmt=kwargs["format"], datefmt=kwargs["datefmt"])
    formatter.converter = time.gmtime
    return formatter


def configure_actor_logging():
    log_dir = "%s/.rally/logs" % os.path.expanduser("~")
    io.ensure_dir(log_dir)

    return {
        "version": 1,
        "formatters": {
            "normal": {
                "format": "%(asctime)s,%(msecs)d %(actorAddress)s/PID:%(process)d %(name)s %(levelname)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                # paraphrasing from 70a4bc8: If we use thespian directory functionality, we need to remove the callable formatter.
                # A potential other solution would then be to enforce UTC timezones by setting the TZ env variable before we start
                # the actor system.
                "()": "esrally.actor.configure_utc_formatter"
            },
        },
        "filters": {
            "isActorLog": {
                "()": 'thespian.director.ActorAddressLogFilter'
            },
        },
        "handlers": {
            "rally_log_handler": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": "%s/rally-actor-messages.log" % log_dir,
                "when": "midnight",
                "backupCount": 14,
                "encoding": "UTF-8",
                "formatter": "normal",
                "filters": ["isActorLog"],
                "level": root_log_level
            },
        },
        "root": {
            "handlers": ["rally_log_handler"],
            "level": root_log_level
        },
        "loggers": {
            "elasticsearch": {
                "handlers": ["rally_log_handler"],
                "level": es_log_level,
                # don't let the root logger handle it again
                "propagate": 0
            }
        }
    }


def actor_system_already_running(ip="127.0.0.1"):
    """
    Determines whether an actor system is already running by opening a socket connection.

    Note: It may be possible that another system is running on the same port.
    """
    import socket
    s = socket.socket()
    try:
        s.connect((ip, 1900))
        s.close()
        return True
    except Exception:
        return False


__SYSTEM_BASE = "multiprocTCPBase"


def use_offline_actor_system():
    global __SYSTEM_BASE
    __SYSTEM_BASE = "multiprocQueueBase"


def bootstrap_actor_system(try_join=False, prefer_local_only=False, local_ip=None, coordinator_ip=None):
    system_base = __SYSTEM_BASE
    try:
        if try_join:
            if actor_system_already_running():
                logger.info("Joining already running actor system with system base [%s]." % system_base)
                return thespian.actors.ActorSystem(system_base)
            else:
                logger.info("Creating new actor system with system base [%s] on coordinator node." % system_base)
                # if we try to join we can only run on the coordinator...
                return thespian.actors.ActorSystem(system_base, logDefs=configure_actor_logging(), capabilities={"coordinator": True})
        elif prefer_local_only:
            coordinator = True
            if system_base != "multiprocQueueBase":
                coordinator_ip = "127.0.0.1"
                local_ip = "127.0.0.1"
            else:
                coordinator_ip = None
                local_ip = None
        else:
            if system_base != "multiprocTCPBase" and system_base != "multiprocUDPBase":
                raise exceptions.SystemSetupError("Rally requires a network-capable system base but got [%s]." % system_base)
            if not coordinator_ip:
                raise exceptions.SystemSetupError("coordinator IP is required")
            if not local_ip:
                raise exceptions.SystemSetupError("local IP is required")
            # always resolve the public IP here, even if a DNS name is given. Otherwise Thespian will be unhappy
            local_ip = net.resolve(local_ip)
            coordinator_ip = net.resolve(coordinator_ip)

            coordinator = local_ip == coordinator_ip

        capabilities = {"coordinator": coordinator}
        if local_ip:
            # just needed to determine whether to run benchmarks locally
            capabilities["ip"] = local_ip
        if coordinator_ip:
            # Make the coordinator node the convention leader
            capabilities["Convention Address.IPv4"] = "%s:1900" % coordinator_ip
        logger.info("Starting actor system with system base [%s] and capabilities [%s]." % (system_base, capabilities))
        return thespian.actors.ActorSystem(system_base,
                                           logDefs=configure_actor_logging(),
                                           capabilities=capabilities)
    except thespian.actors.ActorSystemException:
        logger.exception("Could not initialize internal actor system. Terminating.")
        console.error("Could not initialize successfully.\n")
        console.error("Are there are still processes from a previous race?")
        console.error("Please check and terminate related Python processes before running Rally again.\n")
        raise
