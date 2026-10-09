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
"""
Rally's thin layer on top of Ray Core.

Rally actors are plain Python classes deriving from ``RallyActorBase``. They are turned into Ray actor classes lazily
with ``remote_class()`` so that:

* ``ray`` is only imported by the subcommands that need it (``race``, ``prepare-track`` and ``esrallyd``);
* unit tests can instantiate actor classes directly, without a Ray runtime.

See ``docs/architecture/actor_system.md`` for an overview of the actors and how they interact.
"""

import asyncio
import functools
import logging
import os
import signal
import socket
import sys
import time
import traceback
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar

from esrally import exceptions, log
from esrally.utils import console, net

LOG = logging.getLogger(__name__)

# Port of the Ray head (GCS) when Rally runs as a daemon (esrallyd). Rally's daemon has always used port 1900, which also
# avoids colliding with Ray's default port 6379 that is also Redis' default port.
RAY_GCS_PORT = 1900
# Resource that Ray adds to the head node of a cluster. Actors placed on "localhost" use it.
HEAD_NODE_RESOURCE = "node:__internal_head__"
# Every node has a "node:<ip>" resource with capacity 1.0. Each Rally actor consumes this fraction of it, which allows up to
# 1000 Rally actors per node.
NODE_RESOURCE_UNITS = 0.001
# How long to wait for a remote Rally daemon to be part of the cluster before giving up.
DEFAULT_NODE_WAIT_TIMEOUT = 120.0
# How long to wait for an actor to stop gracefully before killing it.
DEFAULT_STOP_TIMEOUT = 60.0
# Default size of the object store of a local, ephemeral Ray instance. Rally exchanges only small objects.
DEFAULT_OBJECT_STORE_MEMORY = 1 << 30
# Environment variables that Rally sets (unless the user did) before importing Ray.
RAY_ENVIRONMENT_DEFAULTS = {
    # Do not send usage statistics to Anyscale.
    "RAY_USAGE_STATS_ENABLED": "0",
    # Do not hide repeated log lines that actors print to stdout / stderr.
    "RAY_DEDUP_LOGS": "0",
    # Do not prefix console output of actors with "(Worker pid=..., ip=...)".
    "RAY_DISABLE_WORKER_LOG_PREFIX": "1",
    # Authenticate all connections within a Ray cluster with a token (stored in ~/.ray/auth_token by default). This is
    # Ray's default for local instances; setting it explicitly also silences Ray's notice about it.
    "RAY_AUTH_MODE": "token",
    # Ray keeps idle worker processes (one per CPU) to start actors quickly. Rally creates its actors when a race starts
    # and idle workers would consume CPU and memory on load drivers during the benchmark: stop them after 30 seconds.
    "RAY_num_workers_soft_limit": "0",
    "RAY_idle_worker_killing_time_threshold_ms": "30000",
    # When Rally runs with `uv run` (directly or in a parent process), Ray would package the current directory and start
    # actors with `uv run` in a new environment. Rally's actors must run in the same environment as Rally itself.
    "RAY_ENABLE_UV_RUN_RUNTIME_ENV": "0",
}


class BenchmarkFailure(exceptions.RallyError):
    """
    Indicates a failure in the benchmark execution.

    ``cause`` is always a string (usually a formatted traceback) because exception classes defined in track plugins may
    not be importable in the process that receives the failure.
    """

    @classmethod
    def from_current_exception(cls, message: str) -> "BenchmarkFailure":
        """
        Creates a failure for the exception that is currently being handled. Must be called in an ``except`` block.
        """
        return cls(message, traceback.format_exc())


class BenchmarkCancelled(exceptions.RallyError):
    """
    Indicates that the benchmark has been cancelled (by the user).
    """

    def __init__(self, message: str = "The benchmark has been cancelled.", cause: str | None = None):
        super().__init__(message, cause)


def unwrap(e: BaseException) -> BaseException:
    """
    Returns the exception raised in an actor method for exceptions raised by Ray when awaiting the result of that method.
    """
    cause = getattr(e, "cause", None)
    if type(e).__name__.startswith("RayTaskError") and isinstance(cause, BaseException):
        return cause
    return e


F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


def _as_failure(e: BaseException, actor_name: str) -> BaseException:
    """
    Converts an exception to ``BenchmarkFailure`` unless it already is one or ``BenchmarkCancelled`` (possibly raised in
    another actor and wrapped by Ray). Must be called in an ``except`` block.
    """
    cause = unwrap(e)
    if isinstance(cause, (BenchmarkFailure, BenchmarkCancelled)):
        return cause
    LOG.exception("Error in %s", actor_name)
    if isinstance(cause, exceptions.RallyError):
        # Rally's own errors carry a message for the user
        return BenchmarkFailure.from_current_exception(f"Error in {actor_name}: {cause.full_message}")
    return BenchmarkFailure.from_current_exception(f"Error in {actor_name}")


def convert_failures(actor_name: str) -> Callable[[F], F]:
    """
    Decorator for actor methods whose result is awaited by the caller.

    Any exception other than ``BenchmarkFailure`` or ``BenchmarkCancelled`` is logged and raised as ``BenchmarkFailure``
    so that the caller can always deserialize it.
    """

    def decorator(f: F) -> F:
        if asyncio.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_guard(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await f(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except BaseException as e:
                    raise _as_failure(e, actor_name) from None

            return async_guard  # type: ignore[return-value]

        @functools.wraps(f)
        def guard(*args: Any, **kwargs: Any) -> Any:
            try:
                return f(*args, **kwargs)
            except BaseException as e:
                raise _as_failure(e, actor_name) from None

        return guard  # type: ignore[return-value]

    return decorator


def report_failures(actor_name: str) -> Callable[[F], F]:
    """
    Decorator for actor methods whose result nobody awaits: fire-and-forget calls and background tasks.

    Any exception is converted to ``BenchmarkFailure`` (``BenchmarkCancelled`` is kept as is) and handed to the actor's
    ``_fail()`` method, which decides how the failure surfaces.
    """

    def decorator(f: F) -> F:
        if asyncio.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_guard(self: "RallyActorBase", *args: Any, **kwargs: Any) -> Any:
                try:
                    return await f(self, *args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except BaseException as e:
                    self._fail(_as_failure(e, actor_name))  # pylint: disable=protected-access
                return None

            return async_guard  # type: ignore[return-value]

        @functools.wraps(f)
        def guard(self: "RallyActorBase", *args: Any, **kwargs: Any) -> Any:
            try:
                return f(self, *args, **kwargs)
            except BaseException as e:
                self._fail(_as_failure(e, actor_name))  # pylint: disable=protected-access
            return None

        return guard  # type: ignore[return-value]

    return decorator


def detach_stdin() -> None:
    """
    Connects the standard input of this process to ``/dev/null``.

    Ray starts actors in their own process group, which is a background process group if Rally runs in a terminal. If an
    actor or one of its child processes (e.g. Gradle) reads from that terminal, the operating system stops the whole
    process group (``SIGTTIN``) and the actor becomes unresponsive. Actors never need input.
    """
    devnull = os.open(os.devnull, os.O_RDONLY)
    try:
        os.dup2(devnull, 0)
    finally:
        os.close(devnull)


class RallyActorBase:
    """
    Base class for all Rally actors. Subclasses are plain Python classes; use ``create_actor()`` to start them as Ray actors.
    """

    def __init__(self, name: str | None = None, cfg: Any = None):
        self.name = name or type(self).__name__
        detach_stdin()
        log.configure_actor_logging(self.name)
        # Ray forwards console output of actors line by line to the process that started the benchmark, which prints it
        # on the user's terminal. Hence, actors print even though their stdout is not a terminal.
        quiet = bool(cfg.opts("system", "quiet.mode", mandatory=False, default_value=False)) if cfg is not None else False
        console.init(quiet=quiet, assume_tty=True)
        self.logger = logging.getLogger(type(self).__module__)
        self._failure: BaseException | None = None
        LOG.info("Actor initialized: %s (pid=%s)", self.name, os.getpid())

    def _fail(self, failure: BaseException) -> None:
        """
        Records a failure that happened outside of an awaited call. Subclasses override this to surface it.
        """
        if self._failure is None:
            self._failure = failure

    @functools.cached_property
    def self_handle(self) -> Any:
        """
        The Ray handle of this actor, which can be passed to other actors so that they can call it.
        """
        import ray  # pylint: disable=import-outside-toplevel

        return ray.get_runtime_context().current_actor

    def this_node_strategy(self) -> Any:
        """
        Scheduling strategy that places an actor on the same node as this actor.
        """
        import ray  # pylint: disable=import-outside-toplevel
        from ray.util.scheduling_strategies import (  # pylint: disable=import-outside-toplevel
            NodeAffinitySchedulingStrategy,
        )

        return NodeAffinitySchedulingStrategy(node_id=ray.get_runtime_context().get_node_id(), soft=False)


_REMOTE_CLASSES: dict[type, Any] = {}


def remote_class(cls: type) -> Any:
    """
    Returns the Ray actor class for a Rally actor class.

    Rally actors do not reserve CPUs: Rally sizes its workers itself and places actors explicitly with node resources.
    Actors are not restarted when they die; Rally treats that as a benchmark failure.
    """
    import ray  # pylint: disable=import-outside-toplevel

    if cls not in _REMOTE_CLASSES:
        _REMOTE_CLASSES[cls] = ray.remote(num_cpus=0, max_restarts=0)(cls)
    return _REMOTE_CLASSES[cls]


def node_resource(host: str) -> str:
    """
    Returns the name of the Ray resource that places an actor on ``host``.

    ``localhost`` (and loopback addresses) denote the coordinator node, i.e. the head of the Ray cluster.
    """
    if host == "localhost" or host.startswith("127."):
        return HEAD_NODE_RESOURCE
    return f"node:{net.resolve(host) or host}"


def create_actor(cls: type, *args: Any, host: str | None = None, strategy: Any = None, name: str | None = None, **kwargs: Any) -> Any:
    """
    Starts a Rally actor and returns its handle.

    :param cls: A subclass of ``RallyActorBase``.
    :param host: Host to place the actor on (see ``node_resource()``). Mutually exclusive with ``strategy``.
    :param strategy: A Ray scheduling strategy.
    :param name: Optional name of the actor (unique per race).
    """
    options: dict[str, Any] = {}
    if name:
        options["name"] = name
    if strategy is not None:
        options["scheduling_strategy"] = strategy
    elif host is not None:
        options["resources"] = {node_resource(host): NODE_RESOURCE_UNITS}
    return remote_class(cls).options(**options).remote(*args, **kwargs)


def _node_is_alive(ip: str) -> bool:
    import ray  # pylint: disable=import-outside-toplevel

    return any(n.get("Alive") and n.get("NodeManagerAddress") == ip for n in ray.nodes())


def _no_daemon_error(ip: str, timeout: float) -> exceptions.LaunchError:
    return exceptions.LaunchError(
        f"No Rally daemon is running on [{ip}]: it did not join the cluster within [{timeout:.0f}] seconds. "
        f"Are Rally daemons on all targeted machines running?"
    )


def require_node(host: str, timeout: float = DEFAULT_NODE_WAIT_TIMEOUT) -> None:
    """
    Waits until a Rally daemon on ``host`` is part of the cluster.

    Without this check, Ray would wait forever for a node to place an actor on.
    """
    if node_resource(host) == HEAD_NODE_RESOURCE:
        return
    ip = net.resolve(host) or host
    deadline = time.monotonic() + timeout
    while not _node_is_alive(ip):
        if time.monotonic() >= deadline:
            raise _no_daemon_error(ip, timeout)
        time.sleep(1)


async def require_node_async(host: str, timeout: float = DEFAULT_NODE_WAIT_TIMEOUT) -> None:
    """
    Same as ``require_node()``, for asyncio code.
    """
    if node_resource(host) == HEAD_NODE_RESOURCE:
        return
    ip = net.resolve(host) or host
    deadline = time.monotonic() + timeout
    while not _node_is_alive(ip):
        if time.monotonic() >= deadline:
            raise _no_daemon_error(ip, timeout)
        await asyncio.sleep(1)


def ray_address_file() -> str:
    """
    Path to the file in which ``ray start`` records the address of the cluster that this machine is part of.
    """
    temp_dir = os.environ.get("RAY_TMPDIR", "/tmp")
    return os.path.join(temp_dir, "ray", "ray_current_cluster")


def daemon_address() -> str | None:
    """
    Returns the address of the Ray head recorded by ``esrallyd start`` on this machine, if any.
    """
    try:
        with open(ray_address_file(), encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def _can_connect(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False


def is_cluster_running() -> bool:
    """
    Determines whether this machine is part of a Rally daemon cluster (started with ``esrallyd start``) that is reachable.
    """
    address = daemon_address()
    if address:
        host, _, port = address.rpartition(":")
        # Local Ray instances started by Rally use a random port. Only connect to Rally daemons.
        if host and port == str(RAY_GCS_PORT) and _can_connect(host, RAY_GCS_PORT):
            return True
    return _can_connect("127.0.0.1", RAY_GCS_PORT)


def is_daemon_running_locally() -> bool:
    """
    Determines whether a Rally daemon runs on this machine, i.e. a Ray node that is part of a cluster on Rally's port.
    """
    import psutil  # pylint: disable=import-outside-toplevel

    suffix = f":{RAY_GCS_PORT}"
    for p in psutil.process_iter():
        try:
            if p.name() == "raylet" and any(arg.startswith("--gcs-address=") and arg.endswith(suffix) for arg in p.cmdline()):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


# Ray fails to start if these variables change after it has been imported.
_RAY_IMPORT_TIME_VARIABLES = frozenset(["RAY_AUTH_MODE"])


def _apply_import_time_settings() -> None:
    """
    Applies settings that Ray only reads when it is imported, in case ``ray`` has been imported before
    ``configure_ray_environment()`` was called.
    """
    from ray._private import ray_constants  # pylint: disable=import-outside-toplevel

    if os.environ.get("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "").lower() in ("0", "false"):
        ray_constants.RAY_ENABLE_UV_RUN_RUNTIME_ENV = False


def configure_ray_environment() -> None:
    """
    Sets Rally's defaults for Ray's environment variables. Should be called before ``ray`` is imported.
    """
    ray_imported = "ray" in sys.modules
    for key, value in RAY_ENVIRONMENT_DEFAULTS.items():
        if ray_imported and key in _RAY_IMPORT_TIME_VARIABLES:
            continue
        os.environ.setdefault(key, value)


def init_ray(*, namespace: str, num_cpus: int | None = None, object_store_memory: int = DEFAULT_OBJECT_STORE_MEMORY) -> bool:
    """
    Connects to the Rally daemon if one is running, otherwise starts a local, ephemeral Ray instance.

    :param namespace: Ray namespace for the actors of this invocation.
    :param num_cpus: CPUs of the local instance. Ray pre-starts idle worker processes for these, which speeds up actor
                     creation. Ignored when connecting to a daemon.
    :param object_store_memory: Size of the object store of the local instance. Ignored when connecting to a daemon.
    :return: ``True`` if Rally connected to a daemon, ``False`` if it started a local instance.
    """
    configure_ray_environment()
    import ray  # pylint: disable=import-outside-toplevel

    _apply_import_time_settings()
    common: dict[str, Any] = {
        "include_dashboard": False,
        # Ray must not touch Rally's logging configuration.
        "configure_logging": False,
        # Forward console output of actors (e.g. download progress) to this process.
        "log_to_driver": True,
        "namespace": namespace,
    }
    if is_cluster_running():
        address = daemon_address() or f"127.0.0.1:{RAY_GCS_PORT}"
        LOG.info("Connecting to Rally daemon at [%s].", address)
        ray.init(address=address, **common)
        return True

    LOG.info("Starting local Ray instance.")
    ray.init(
        address="local",
        _node_ip_address="127.0.0.1",
        num_cpus=num_cpus,
        object_store_memory=object_store_memory,
        **common,
    )
    return False


# Ray forwards console output of actors to this process every 0.1 seconds. Give it time to do so before disconnecting.
LOG_FORWARDING_GRACE_PERIOD = 1.0


async def await_actor_output() -> None:
    """
    Waits until Ray has (most likely) forwarded console output that actors have printed so far. Call it before printing
    to the console in the main process so that messages appear in order.
    """
    await asyncio.sleep(LOG_FORWARDING_GRACE_PERIOD / 2)


def shutdown_ray() -> None:
    """
    Disconnects from Ray. A local instance started by ``init_ray()`` is stopped, including all its actors.
    """
    import ray  # pylint: disable=import-outside-toplevel

    if ray.is_initialized():
        time.sleep(LOG_FORWARDING_GRACE_PERIOD)
        ray.shutdown()


async def await_with_timeout(ref: Any, timeout: float) -> Any:
    """
    Awaits the result of an actor method call with a timeout.
    """
    return await asyncio.wait_for(_as_future(ref), timeout)


def _as_future(ref: Any) -> Awaitable[Any]:
    # ObjectRefs are awaitable but asyncio.wait_for() needs a coroutine or future.
    return asyncio.wrap_future(ref.future())


async def stop_actor(handle: Any, *, timeout: float = DEFAULT_STOP_TIMEOUT, name: str = "") -> None:
    """
    Stops an actor: calls its ``stop()`` method and waits up to ``timeout`` seconds, then kills it in any case.
    """
    try:
        await await_with_timeout(handle.stop.remote(), timeout)
    except asyncio.TimeoutError:
        LOG.warning("Actor [%s] did not stop within [%s] seconds. Killing it.", name, timeout)
    except BaseException as e:  # pylint: disable=broad-exception-caught
        if isinstance(e, asyncio.CancelledError):
            raise
        LOG.warning("Actor [%s] did not stop cleanly: %s", name, e)
    finally:
        kill_actor(handle)


def kill_actor(handle: Any) -> None:
    """
    Kills an actor immediately. Its ``stop()`` method is not called.
    """
    import ray  # pylint: disable=import-outside-toplevel

    try:
        ray.kill(handle, no_restart=True)
    except Exception:  # pylint: disable=broad-exception-caught
        # the actor is already gone
        pass


def run_async(main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """
    Runs a coroutine in a new event loop in the main thread.

    The first Ctrl-C cancels the coroutine, which can clean up (e.g. stop actors) when it catches
    ``asyncio.CancelledError``. A second Ctrl-C cancels the clean-up. In both cases ``KeyboardInterrupt`` is raised.

    :param main: A function returning the coroutine to run.
    """

    async def runner() -> T:
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        interrupted = False

        def on_sigint() -> None:
            nonlocal interrupted
            interrupted = True
            LOG.info("Received SIGINT. Cancelling.")
            task.cancel()

        loop.add_signal_handler(signal.SIGINT, on_sigint)
        try:
            return await main()
        except asyncio.CancelledError:
            if interrupted:
                raise KeyboardInterrupt() from None
            raise
        finally:
            loop.remove_signal_handler(signal.SIGINT)

    return asyncio.run(runner())
