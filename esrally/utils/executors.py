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

import concurrent.futures
import copy
import dataclasses
import logging
import multiprocessing
import os
import typing
from collections.abc import Callable

from thespian import actors
from typing_extensions import Self

from esrally import actor, log, types
from esrally.utils import console, convert

LOG = logging.getLogger(__name__)


@typing.runtime_checkable
class ExecutorProtocol(typing.Protocol):

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """


MAX_WORKERS: int = 63
WORKERS_NAME_PREFIX: str = "esrally-executor-worker"
USE_THREADING: bool = True
LOG_FORWARDER_LEVEL = logging.NOTSET


class ExecutorsConfig(actor.ActorConfig):

    @property
    def log_forwarder_level(self) -> int:
        return int(self.opts("executors", "executors.forwarder.log.level", LOG_FORWARDER_LEVEL or logging.root.level, False))

    @property
    def max_workers(self) -> int:
        return int(self.opts("executors", "executors.max_workers", MAX_WORKERS, False))

    @property
    def use_threading(self) -> bool:
        return convert.to_bool(self.opts("executors", "executors.use_threading", USE_THREADING, False))


@dataclasses.dataclass
class Task:
    func: typing.Callable
    args: tuple[typing.Any, ...] = tuple()
    kwargs: dict[str, typing.Any] | None = None

    def __call__(self):
        try:
            result = self.func(*self.args, **(self.kwargs or {}))
            self.handle_result(result, None)
            return result
        except Exception as ex:
            self.handle_result(None, ex)
            raise

    def handle_result(self, result: typing.Any, error: Exception | None) -> None:
        if error is not None:
            LOG.exception("Unhandled exception: %s", error)


@dataclasses.dataclass
class Executor:
    """This is a wrapper class for concrete asynchronous executors.

    Executor protocol is used by Transfer class to submit tasks execution.
    Notable implementation of this protocol is concurrent.futures.ThreadPoolExecutor[1] class.

    [1] https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    """

    executor: ExecutorProtocol
    max_workers: int = 0

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Executor:
        cfg = ExecutorsConfig.from_config(cfg)
        if cfg.use_threading:
            executor = ThreadPoolExecutor.from_config(cfg)
        else:
            executor = ProcessPoolExecutor.from_config(cfg)
        return cls(executor=executor, max_workers=cfg.max_workers)

    def shutdown(self) -> None:
        if hasattr(self.executor, "shutdown"):
            self.executor.shutdown()

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        return self.executor.submit(self.task(fn, *args, **kwargs))

    def task(self, fn, /, *args, **kwargs) -> Callable[[], typing.Any]:
        """It allows wrapping user function to be executed with the given arguments."""
        return Task(func=fn, args=args, kwargs=kwargs)


class ThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        LOG.info("Creating thread pool executor: max_workers: %d", cfg.max_workers)
        return cls(max_workers=cfg.max_workers, thread_name_prefix=WORKERS_NAME_PREFIX)


class ProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        LOG.info("Creating process pool executor: max_workers: %d", cfg.max_workers)
        return cls(
            max_workers=cfg.max_workers,
            mp_context=multiprocessing.get_context(cfg.process_startup_method),
            initializer=ProcessPoolHelper.from_config(cfg).initialize_subprocess,
        )


@dataclasses.dataclass
class LogRecord:
    record: logging.LogRecord


class ProcessPoolHelperActor(actor.BaseActor):
    def receiveMsg_LogRecord(self, msg: LogRecord, sender: actors.ActorAddress) -> None:
        logging.root.handle(msg.record)


@dataclasses.dataclass
class ProcessPoolHelper:

    cfg: ExecutorsConfig
    actor: actors.ActorAddress
    level: int = LOG_FORWARDER_LEVEL

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        actor_system = actor.system_from_config(cfg)
        helper = cls(
            cfg=cfg,
            actor=actor_system.createActor(ProcessPoolHelperActor, globalName=f"{__name__}:ProcessPoolHelperActor"),
            level=cfg.log_forwarder_level,
        )
        return helper

    def initialize_subprocess(self) -> None:
        """It prepares the new subprocess before taking tasks to execute."""

        # Initialize logging system.
        if self.level:
            logging.root.setLevel(self.level)
        logging.root.addHandler(LogForwarderHandler(self.level, self.actor))
        log.post_configure_logging()
        console.set_assume_tty(assume_tty=False)

        # Initialize actor system.
        actor.system_from_config(cfg=self.cfg)

        LOG.debug("Executor subprocess initialized: pid=%d.", os.getpid())

    def shutdown(self) -> None:
        """It waits for all log records to be handled before shutting down the helper actor."""
        LOG.debug("Send actor exit request.")
        actors.ActorSystem().ask(self.actor, actors.ActorExitRequest())


class LogForwarderHandler(logging.Handler):

    def __init__(self, level: int, actor_addr: actors.ActorAddress) -> None:
        super().__init__(level)
        self.actor_addr = actor_addr

    def emit(self, record: logging.LogRecord) -> None:
        actors.ActorSystem().tell(self.actor_addr, self.prepare(record))

    def prepare(self, record: logging.LogRecord) -> LogRecord:
        """
        Prepare a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message and
        arguments, and removes unpickleable items from the record in-place.
        Specifically, it overwrites the record's `msg` and
        `message` attributes with the merged message (obtained by
        calling the handler's `format` method), and sets the `args`,
        `exc_info` and `exc_text` attributes to None.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also returns the formatted
        # message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info, exc_text and stack_info attributes, as they are no longer
        # needed and, if not None, will typically not be pickleable.
        msg = self.format(record)
        # bpo-35726: make copy of record to avoid affecting other handlers in the chain.
        record = copy.copy(record)
        record.message = msg
        record.msg = msg
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return LogRecord(record)
