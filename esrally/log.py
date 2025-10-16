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
import copy
import json
import logging
import logging.config
import logging.handlers
import os
import time
from collections.abc import Callable
from typing import Any

import ecs_logging

from esrally import paths
from esrally.utils import collections, io

LOG = logging.getLogger(__name__)


# pylint: disable=unused-argument
def configure_utc_formatter(*args: Any, **kwargs: Any) -> logging.Formatter:
    """
    Logging formatter that renders timestamps UTC, or in the local system time zone when the user requests it.
    """
    formatter = logging.Formatter(fmt=kwargs["format"], datefmt=kwargs["datefmt"])
    user_tz = kwargs.get("timezone", None)
    if user_tz == "localtime":
        formatter.converter = time.localtime
    else:
        formatter.converter = time.gmtime

    return formatter


MutatorType = Callable[[logging.LogRecord, dict[str, Any]], None]


class RallyEcsFormatter(ecs_logging.StdlibFormatter):
    def __init__(
        self,
        *args: Any,
        mutators: list[MutatorType] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.mutators = mutators or []

    def format_to_ecs(self, record: logging.LogRecord) -> dict[str, Any]:
        log_dict = super().format_to_ecs(record)
        self.apply_mutators(record, log_dict)
        return log_dict

    def apply_mutators(self, record: logging.LogRecord, log_dict: dict[str, Any]) -> None:
        for mutator in self.mutators:
            mutator(record, log_dict)


def rename_actor_fields(record: logging.LogRecord, log_dict: dict[str, Any]) -> None:
    fields = {}
    if log_dict.get("actorAddress"):
        fields["address"] = log_dict.pop("actorAddress")
    if fields:
        collections.deep_update(log_dict, {"rally": {"thespian": fields}})
    # Thespian does not serialize exception objects kept in exc_info log record attribute when
    # forwarding logs to the logger actor but populates exc_text attribute instead, and sets
    # exc_info to None. This is not recognized as stack trace by ECS Logging package, so we need to
    # work it around setting "error.stack_trace" ECS field explicitly.
    if record.exc_info is None and record.exc_text:
        collections.deep_update(log_dict, {"error": {"stack_trace": record.exc_text}})


# Special case for asyncio fields as they are not part of the standard ECS log dict
def rename_async_fields(record: logging.LogRecord, log_dict: dict[str, Any]) -> None:
    fields = {}
    if hasattr(record, "taskName") and record.taskName is not None:
        fields["task"] = record.taskName
    if fields:
        collections.deep_update(log_dict, {"python": {"asyncio": fields}})


def configure_ecs_formatter(*args: Any, **kwargs: Any) -> ecs_logging.StdlibFormatter:
    """
    ECS Logging formatter
    """
    fmt = kwargs.pop("format", None)
    configurator = logging.config.BaseConfigurator({})
    mutators = kwargs.pop("mutators", [rename_actor_fields, rename_async_fields])
    mutators = [fn if callable(fn) else configurator.resolve(fn) for fn in mutators]

    formatter = RallyEcsFormatter(fmt=fmt, mutators=mutators, *args, **kwargs)
    return formatter


def log_config_path():
    """
    :return: The absolute path to Rally's log configuration file.
    """
    return os.path.join(paths.rally_confdir(), "logging.json")


CONFIG_PATH = log_config_path()
TEMPLATE_PATH = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "logging.json"))


def update_logger_config(
    *,
    config_path: str = CONFIG_PATH,
    template_path: str = TEMPLATE_PATH,
):
    """It appends any missing top level loggers found in resources/logging.json to current log configuration.

    It also ensures "disable_existing_loggers" is set to False by default.
    """

    with open(template_path, encoding="UTF-8") as fd:
        template: dict[str, Any] = json.load(fd)

    with open(config_path, encoding="UTF-8") as fd:
        original: dict[str, Any] = json.load(fd)

    if original == template:
        return

    updated = copy.deepcopy(original)
    updated.setdefault("disable_existing_loggers", template.get("disable_existing_loggers", False))

    template_loggers: dict[str, Any] = template.get("loggers", {})
    config_loggers: dict[str, Any] = updated.setdefault("loggers", template_loggers)
    for name, logger in template_loggers.items():
        config_loggers.setdefault(name, logger)

    if original != updated:
        LOG.info("Update logging configuration file with new values from template: '%s' -> '%s'", template_path, config_path)
        with open(config_path, "w", encoding="UTF-8") as fd:
            json.dump(updated, fd, indent=2)


def install_default_log_config():
    """
    Ensures a log configuration file is present on this machine. The default
    log configuration is based on the template in resources/logging.json.

    It also ensures that the default log path has been created so log files
    can be successfully opened in that directory.
    """
    log_config: str = log_config_path()
    if not io.exists(log_config):
        io.ensure_dir(io.dirname(log_config))
        source_path: str = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "logging.json"))
        with open(log_config, "w", encoding="UTF-8") as target:
            with open(source_path, encoding="UTF-8") as src:
                contents = src.read()
                target.write(contents)
    update_logger_config()
    io.ensure_dir(paths.logs())


# pylint: disable=unused-argument
def configure_file_handler(*, filename: str, encoding: str = "UTF-8", delay: bool = False, **kwargs: Any) -> logging.Handler:
    """
    Configures the WatchedFileHandler supporting expansion of `~` and `${LOG_PATH}` to the user's home and the log path respectively.
    """
    filename = filename.replace("${LOG_PATH}", paths.logs())
    return logging.handlers.WatchedFileHandler(filename=filename, encoding=encoding, delay=delay, **kwargs)


def configure_profile_file_handler(*, filename: str, encoding: str = "UTF-8", delay: bool = False, **kwargs: Any) -> logging.Handler:
    """
    Configures the FileHandler supporting expansion of `~` and `${LOG_PATH}` to the user's home and the log path respectively.
    """
    filename = filename.replace("${LOG_PATH}", paths.logs())
    return logging.FileHandler(filename=filename, encoding=encoding, delay=delay, **kwargs)


def load_configuration() -> dict[str, Any]:
    """
    Loads the logging configuration. This is a low-level method and usually
    `configure_logging()` should be used instead.

    :return: The logging configuration as `dict` instance.
    """
    with open(log_config_path()) as f:
        return json.load(f)


def post_configure_actor_logging() -> None:
    """
    Reconfigures all loggers in actor processes.

    See https://groups.google.com/forum/#!topic/thespianpy/FntU9umtvhc for the rationale.
    """
    # see configure_logging()
    logging.captureWarnings(True)

    # At this point we can assume that a log configuration exists. It has been created already during startup.
    config = load_configuration()
    if root_config := config.get("root"):
        if level := root_config.get("level"):
            logging.root.setLevel(level)

    for name, cfg in config.get("loggers", {}).items():
        if level := cfg.get("level"):
            logging.getLogger(name).setLevel(level)

    if LOG.manager is not logging.root.manager:
        # It just detected that all pre-existing loggers have been forgotten after changing manager.

        # Redirect messages to the same handlers as the new root handler. This way as the very last resort lost logger
        # will still emit messages to the same destination.
        LOG.root.handlers = logging.root.handlers

        # For debugging purpose it we will finally report all recovered logger names here.
        recovered: list[str] = []

        # It replaces attributes values of pre-existing loggers with the attributes of loggers created by the new manager.
        # In this way old logger should behave as the new ones, and dispatch records to the new root logger.
        LOG.__dict__ = logging.getLogger(__name__).__dict__
        lost: dict[str, Any] = LOG.manager.loggerDict
        for name, old_logger in sorted(lost.items()):
            if not isinstance(old_logger, logging.Logger):
                # It filters out for instance place-holders.
                lost.pop(name)
                continue  # Skip place holders and adapters
            new_logger = logging.getLogger(name)
            if type(new_logger) is not type(old_logger):
                continue
            old_logger.__dict__ = new_logger.__dict__
            recovered.append(name)
            lost.pop(name)

        # This ensures this monkey patching will not occur twice.
        assert LOG.manager is logging.root.manager

        if recovered:
            LOG.debug("Recovered loggers from old manager: %s", ", ".join(recovered))
        if lost:
            LOG.warning("Lost loggers from old manager: %s", lost)


def configure_logging() -> None:
    """
    Configures logging for the current process.
    """

    logging.config.dictConfig(load_configuration())

    # Avoid failures such as the following (shortened a bit):
    #
    # ---------------------------------------------------------------------------------------------
    # "esrally/driver/driver.py", line 220, in create_client
    # "thespian-3.8.0-py3.5.egg/thespian/actors.py", line 187, in createActor
    # [...]
    # "thespian-3.8.0-py3.5.egg/thespian/system/multiprocCommon.py", line 348, in _startChildActor
    # "python3.5/multiprocessing/process.py", line 105, in start
    # "python3.5/multiprocessing/context.py", line 267, in _Popen
    # "python3.5/multiprocessing/popen_fork.py", line 18, in __init__
    # sys.stderr.flush()
    #
    # OSError: [Errno 5] Input/output error
    # ---------------------------------------------------------------------------------------------
    #
    # This is caused by urllib3 wanting to send warnings about insecure SSL connections to stderr when we disable them (in client.py) with:
    #
    #   urllib3.disable_warnings()
    #
    # The filtering functionality of the warnings module causes the error above on some systems. If we instead redirect the warning output
    # to our logs instead of stderr (which is the warnings module's default), we can disable warnings safely.
    logging.captureWarnings(True)
