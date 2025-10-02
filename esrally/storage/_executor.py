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
import concurrent.futures
from typing import Protocol, runtime_checkable

from typing_extensions import Self

from esrally.types import Config

MAX_WORKERS = 32


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


class ThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: Config) -> Self:
        max_workers = int(cfg.opts("storage", "storage.max_workers", MAX_WORKERS, mandatory=False))
        return cls(max_workers=max_workers, thread_name_prefix="esrally.storage.executor")
