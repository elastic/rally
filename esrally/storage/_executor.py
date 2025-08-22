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
import logging
from typing import Protocol, runtime_checkable

from typing_extensions import Self

from esrally.storage._config import AnyConfig, StorageConfig
from esrally.storage._log import LogForwarder

LOG = logging.getLogger(__name__)


@runtime_checkable
class Executor(Protocol):
    """This is a protocol class for concrete asynchronous executors.

    Executor protocol is used by Transfer class to submit tasks execution.
    Notable implementation of this protocol is concurrent.futures.ThreadPoolExecutor[1] class.

    [1] https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    """

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        raise NotImplementedError()

    def shutdown(self) -> None:
        pass


class ThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: AnyConfig = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        LOG.debug("Creating thread pool executor: max_workers: %d, thread_name_prefix: '%s'", cfg.max_workers, cfg.thread_name_prefix)
        return cls(max_workers=cfg.max_workers, thread_name_prefix=cfg.thread_name_prefix)


class ProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: AnyConfig = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        LOG.debug("Creating process pool executor: max_workers: %d", cfg.max_workers)
        log_forwarder = LogForwarder.from_config(cfg)
        return cls(max_workers=cfg.max_workers, initializer=log_forwarder.initialize_subprocess)


def executor_from_config(cfg: AnyConfig = None) -> Executor:
    cfg = StorageConfig.from_config(cfg)
    if cfg.use_threads:
        return ThreadPoolExecutor.from_config(cfg)
    else:
        return ProcessPoolExecutor.from_config(cfg)


#
# def test_function():
#     LOG.info("Test function (pid=%d)", os.getpid())
#     LOG.info("Test function (pid=%d)", os.getpid())
#
#
# class actorLogFilter(logging.Filter):
#     def filter(self, logrecord):
#         return "actorAddress" in logrecord.__dict__
#
#
# class notActorLogFilter(logging.Filter):
#     def filter(self, logrecord):
#         return "actorAddress" not in logrecord.__dict__
#
#
# if __name__ == "__main__":
#     cfg = StorageConfig(
#         use_threads=False,
#         actor_system_base="multiprocTCPBase",
#         process_startup_method="fork",
#         subprocess_log_level=logging.DEBUG,
#         max_workers=5,
#     )
#
#     logcfg = {
#         "version": 1,
#         "formatters": {
#             "normal": {"format": "%(levelname)-8s %(message)s"},
#             "actor": {"format": "%(levelname)-8s %(actorAddress)s => %(message)s"},
#         },
#         "filters": {"isActorLog": {"()": actorLogFilter}, "notActorLog": {"()": notActorLogFilter}},
#         "handlers": {
#             "h1": {
#                 "class": "logging.StreamHandler",
#                 "stream": "ext://sys.stderr",
#                 "formatter": "normal",
#                 "filters": ["notActorLog"],
#                 "level": logging.DEBUG,
#             },
#             "h2": {
#                 "class": "logging.StreamHandler",
#                 "stream": "ext://sys.stderr",
#                 "formatter": "actor",
#                 "filters": ["isActorLog"],
#                 "level": logging.DEBUG,
#             },
#         },
#         "loggers": {"": {"handlers": ["h1", "h2"], "level": logging.DEBUG}},
#     }
#
#     actor_system = actors.ActorSystem(cfg.actor_system_base, logDefs=logcfg)
#     log_forwarder = LogForwarder.from_config(cfg, actor_system)
#     executor = executor_from_config(cfg)
#
#     executor.submit(test_function).result()
#
#     executor.shutdown()
#     log_forwarder.shutdown()
#     actor_system.shutdown()
