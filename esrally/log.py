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

# Value of the ``actorAddress`` log record attribute in processes that do not host a Rally actor (e.g. the main ``esrally``
# process or ``esrallyd``). This string is part of Rally's log format since its early days, keep it stable.
NOT_AN_ACTOR = "-not-actor-"

_ACTOR_ADDRESS = NOT_AN_ACTOR


class ActorAddressLogFilter(logging.Filter):
    """
    Adds the ``actorAddress`` attribute to log records so that ``%(actorAddress)s`` can be used in format strings.

    In Ray actor processes the address is the name of the actor that owns the process (e.g. ``worker-3``), otherwise it
    is ``-not-actor-``.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "actorAddress"):
            record.actorAddress = _ACTOR_ADDRESS
        return True


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
        collections.deep_update(log_dict, {"rally": {"actor": fields}})
    # Some handlers serialize log records and keep only the formatted traceback in exc_text, with
    # exc_info set to None. This is not recognized as stack trace by ECS Logging package, so we need to
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

# Callables that older Rally versions referenced in logging.json and their replacements. Rally used the Thespian actor
# library until it moved to Ray. Without this migration, logging.config.dictConfig() fails with an ImportError.
_RENAMED_CALLABLES = {
    "thespian.director.ActorAddressLogFilter": "esrally.log.ActorAddressLogFilter",
}
_OBSOLETE_CALLABLE_PREFIXES = ("thespian.",)


def migrate_logger_config(log_config: dict[str, Any]) -> bool:
    """
    Rewrites references to callables that no longer exist in a logging configuration, in place.

    Known callables are renamed. Filters pointing to any other obsolete module are removed, together with all references
    to them from handlers.

    :return: ``True`` if the configuration has been changed.
    """
    changed = False
    removed_filters: set[str] = set()
    for section in ("filters", "formatters", "handlers"):
        entries = log_config.get(section)
        if not isinstance(entries, dict):
            continue
        for name, entry in list(entries.items()):
            if not isinstance(entry, dict):
                continue
            for key in ("()", "class"):
                target = entry.get(key)
                if not isinstance(target, str):
                    continue
                if target in _RENAMED_CALLABLES:
                    entry[key] = _RENAMED_CALLABLES[target]
                    changed = True
                elif section == "filters" and target.startswith(_OBSOLETE_CALLABLE_PREFIXES):
                    del entries[name]
                    removed_filters.add(name)
                    changed = True
                    break

    if removed_filters:
        for handler in (log_config.get("handlers") or {}).values():
            if isinstance(handler, dict) and isinstance(handler.get("filters"), list):
                handler["filters"] = [f for f in handler["filters"] if f not in removed_filters]
    return changed


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
    if migrate_logger_config(updated):
        LOG.info("Migrated obsolete entries in logging configuration file '%s'.", config_path)
    updated.setdefault("disable_existing_loggers", template.get("disable_existing_loggers", False))

    template_loggers: dict[str, Any] = template.get("loggers", {})
    config_loggers: dict[str, Any] = updated.setdefault("loggers", template_loggers)
    for name, logger in template_loggers.items():
        config_loggers.setdefault(name, logger)

    if original != updated:
        LOG.info("Update logging configuration file with new values from template: '%s' -> '%s'", template_path, config_path)
        try:
            with open(config_path, "w", encoding="UTF-8") as fd:
                json.dump(updated, fd, indent=2)
        except OSError as e:
            # load_configuration() applies the same migration in memory, so Rally still starts.
            LOG.warning("Could not update logging configuration file '%s': %s", config_path, e)


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

    Obsolete entries are migrated in memory (see ``migrate_logger_config``) so that Rally can start even if the
    configuration file could not be rewritten.

    :return: The logging configuration as `dict` instance.
    """
    with open(log_config_path()) as f:
        log_config = json.load(f)
    migrate_logger_config(log_config)
    return log_config


_ACTOR_LOGGING_CONFIGURED_PID: int | None = None


def configure_actor_logging(actor_name: str) -> None:
    """
    Configures logging in a process that hosts a Rally actor.

    Ray starts actor processes from its own worker entry point, so they do not inherit the logging configuration of the
    process that created them. This applies Rally's configuration once per process and records the actor name that is
    reported in the ``actorAddress`` log record attribute.
    """
    global _ACTOR_ADDRESS, _ACTOR_LOGGING_CONFIGURED_PID
    _ACTOR_ADDRESS = actor_name
    pid = os.getpid()
    if _ACTOR_LOGGING_CONFIGURED_PID != pid:
        if not io.exists(log_config_path()):
            install_default_log_config()
        configure_logging()
        _ACTOR_LOGGING_CONFIGURED_PID = pid


_ACTOR_ADDRESS_RECORD_FACTORY_INSTALLED = False


def _install_actor_address_record_factory() -> None:
    """
    Adds the ``actorAddress`` attribute to all log records so that format strings can use ``%(actorAddress)s`` even if a
    handler does not use ``ActorAddressLogFilter``.
    """
    global _ACTOR_ADDRESS_RECORD_FACTORY_INSTALLED
    if _ACTOR_ADDRESS_RECORD_FACTORY_INSTALLED:
        return
    default_factory = logging.getLogRecordFactory()

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = default_factory(*args, **kwargs)
        record.actorAddress = _ACTOR_ADDRESS
        return record

    logging.setLogRecordFactory(factory)
    _ACTOR_ADDRESS_RECORD_FACTORY_INSTALLED = True


def configure_logging() -> None:
    """
    Configures logging for the current process.
    """
    _install_actor_address_record_factory()
    logging.config.dictConfig(load_configuration())

    # Avoid failures such as "OSError: [Errno 5] Input/output error" when flushing stderr in processes that are not
    # attached to a terminal.
    #
    # This is caused by urllib3 wanting to send warnings about insecure SSL connections to stderr when we disable them (in client.py) with:
    #
    #   urllib3.disable_warnings()
    #
    # The filtering functionality of the warnings module causes the error above on some systems. If we instead redirect the warning output
    # to our logs instead of stderr (which is the warnings module's default), we can disable warnings safely.
    logging.captureWarnings(True)
