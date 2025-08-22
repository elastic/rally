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

import copy
import dataclasses
import logging.handlers
import os
from typing import Any

from thespian import actors  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally.storage._config import AnyConfig, StorageConfig

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class LogForwarder:
    actor: actors.ActorAddress
    system_base: str | None = None
    level: int = logging.NOTSET

    @classmethod
    def from_config(cls, cfg: AnyConfig = None, actor_system: actors.ActorSystem | None = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        if actor_system is None:
            actor_system = actors.ActorSystem(cfg.actor_system_base)
        actor = actor_system.createActor(LogForwarderActor, globalName=cfg.log_actor_name)
        level = cfg.subprocess_log_level or logging.root.level
        forwarder = cls(actor=actor, system_base=cfg.actor_system_base, level=level)
        return forwarder

    def initialize_subprocess(self) -> None:
        logging.root.setLevel(self.level)
        logging.root.handlers = [LogHandler(self)]

    def ask(self, msg: Any, timeout: float | None = None) -> None:
        asys = actors.ActorSystem(self.system_base)
        return asys.ask(self.actor, msg, timeout)

    def tell(self, msg: Any) -> None:
        asys = actors.ActorSystem(self.system_base)
        asys.tell(self.actor, msg)

    def emit(self, record: logging.LogRecord) -> None:
        assert record is not None
        self.tell((record,))

    def shutdown(self) -> None:
        self.ask(actors.ActorExitRequest())


class LogHandler(logging.Handler):

    def __init__(self, forwarder: LogForwarder):
        super().__init__(forwarder.level)
        self.forwarder = forwarder

    def emit(self, record: logging.LogRecord) -> None:
        self.forwarder.emit(self.prepare(record))

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
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
        return record


class LogForwarderActor(actors.Actor):

    def __init__(self):
        super().__init__()
        LOG.debug("Initialized log forwarder actor (pid=%d).", os.getpid())

    def receiveMessage(self, msg: Any, sender: actors.ActorAddress) -> None:
        pid = os.getpid()
        if isinstance(msg, tuple):
            msg = msg[0]
            assert isinstance(msg, logging.LogRecord)
            LOG.info("Received emit request from '%s': %s (pid=%d)", sender, msg, pid)
            logging.root.handle(msg)
            return
        elif isinstance(msg, actors.ActorExitRequest):
            LOG.info("Received exit request from '%s': %s (pid=%d)", sender, msg, pid)
            self.send(sender, None)
            return
        LOG.error("Ignored message from '%s': %s (pid=%d)", sender, msg, pid)
