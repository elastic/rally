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

import json
import logging
import logging.config
import os
import time
import typing

import ecs_logging

from esrally import paths
from esrally.utils import collections, io


# pylint: disable=unused-argument
def configure_utc_formatter(*args: typing.Any, **kwargs: typing.Any) -> logging.Formatter:
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


class RallyEcsFormatter(ecs_logging.StdlibFormatter):
    def __init__(
        self,
        mutators: typing.Optional[typing.List[typing.Callable[[typing.Dict[str, typing.Any]], None]]] = None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ):
        super().__init__(*args, **kwargs)
        self.mutators = mutators or []

    def format(self, record: logging.LogRecord) -> str:
        log_dict = super().format_to_ecs(record)
        self.apply_mutators(record, log_dict)
        # ecs_logging._utils is a private module
        # but we need to use it here to get the on spec JSON serialization
        return ecs_logging._utils.json_dumps(log_dict)  # pylint: disable=protected-access

    def apply_mutators(self, record: logging.LogRecord, log_dict: typing.Dict[str, typing.Any]) -> None:
        for mutator in self.mutators:
            mutator(record, log_dict)


def rename_actor_fields(record: logging.LogRecord, log_dict: typing.Dict[str, typing.Any]) -> None:
    fields = {}
    if log_dict.get("actorAddress"):
        fields["address"] = log_dict.pop("actorAddress")
    if fields:
        collections.deep_update(log_dict, {"rally": {"thespian": fields}})


# Special case for asyncio fields as they are not part of the standard ECS log dict
def rename_async_fields(record: logging.LogRecord, log_dict: typing.Dict[str, typing.Any]) -> None:
    fields = {}
    if hasattr(record, "taskName") and record.taskName is not None:
        fields["task"] = record.taskName
    if fields:
        collections.deep_update(log_dict, {"python": {"asyncio": fields}})


def configure_ecs_formatter(*args: typing.Any, **kwargs: typing.Any) -> ecs_logging.StdlibFormatter:
    """
    ECS Logging formatter
    """
    fmt = kwargs.pop("format", None)
    configurator = logging.config.BaseConfigurator({})
    mutators = kwargs.pop("mutators", [rename_actor_fields, rename_async_fields])
    mutators = [fn if callable(fn) else configurator.resolve(fn) for fn in mutators]

    formatter = RallyEcsFormatter(fmt=fmt, mutators=mutators, *args, **kwargs)
    return formatter


def configure_json_formatter(*args: typing.Any, **kwargs: typing.Any) -> RallyJsonFormatter:
    """
    JSON Logging formatter
    """
    formatter = jsonlogger.JsonFormatter(*args, **kwargs)

    return formatter


def log_config_path():
    """
    :return: The absolute path to Rally's log configuration file.
    """
    return os.path.join(paths.rally_confdir(), "logging.json")


def add_missing_loggers_to_config():
    """
    Ensures that any missing top level loggers in resources/logging.json are
    appended to an existing log configuration
    """

    def _missing_loggers(source, target):
        """
        Returns any top-level loggers present in 'source', but not in 'target'
        :return: A dict of all loggers present in 'source', but not in 'target'
        """
        missing_loggers = {}
        for logger in source:
            if logger in source and logger in target:
                continue
            else:
                missing_loggers[logger] = source[logger]
        return missing_loggers

    source_path = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "logging.json"))

    with open(log_config_path(), encoding="UTF-8") as target:
        with open(source_path, encoding="UTF-8") as src:
            template = json.load(src)
            existing_logging_config = json.load(target)
            if missing_loggers := _missing_loggers(source=template["loggers"], target=existing_logging_config["loggers"]):
                existing_logging_config["loggers"].update(missing_loggers)
                updated_config = json.dumps(existing_logging_config, indent=2)

    if missing_loggers:
        with open(log_config_path(), "w", encoding="UTF-8") as target:
            target.write(updated_config)


def install_default_log_config():
    """
    Ensures a log configuration file is present on this machine. The default
    log configuration is based on the template in resources/logging.json.

    It also ensures that the default log path has been created so log files
    can be successfully opened in that directory.
    """
    log_config = log_config_path()
    if not io.exists(log_config):
        io.ensure_dir(io.dirname(log_config))
        source_path = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "logging.json"))
        with open(log_config, "w", encoding="UTF-8") as target:
            with open(source_path, encoding="UTF-8") as src:
                contents = src.read()
                target.write(contents)
    add_missing_loggers_to_config()
    io.ensure_dir(paths.logs())


# pylint: disable=unused-argument
def configure_file_handler(*args, **kwargs) -> logging.Handler:
    """
    Configures the WatchedFileHandler supporting expansion of `~` and `${LOG_PATH}` to the user's home and the log path respectively.
    """
    filename = kwargs.pop("filename").replace("${LOG_PATH}", paths.logs())
    return logging.handlers.WatchedFileHandler(filename=filename, encoding=kwargs["encoding"], delay=kwargs.get("delay", False))


def configure_profile_file_handler(*args, **kwargs) -> logging.Handler:
    """
    Configures the FileHandler supporting expansion of `~` and `${LOG_PATH}` to the user's home and the log path respectively.
    """
    filename = kwargs.pop("filename").replace("${LOG_PATH}", paths.logs())
    return logging.FileHandler(filename=filename, encoding=kwargs["encoding"], delay=kwargs.get("delay", False))


def load_configuration():
    """
    Loads the logging configuration. This is a low-level method and usually
    `configure_logging()` should be used instead.

    :return: The logging configuration as `dict` instance.
    """
    with open(log_config_path()) as f:
        return json.load(f)


def post_configure_actor_logging():
    """
    Reconfigures all loggers in actor processes.

    See https://groups.google.com/forum/#!topic/thespianpy/FntU9umtvhc for the rationale.
    """
    # see configure_logging()
    logging.captureWarnings(True)

    # at this point we can assume that a log configuration exists. It has been created already during startup.
    logger_configuration = load_configuration()
    if "root" in logger_configuration and "level" in logger_configuration["root"]:
        root_logger = logging.getLogger()
        root_logger.setLevel(logger_configuration["root"]["level"])

    if "loggers" in logger_configuration:
        for lgr, cfg in load_configuration()["loggers"].items():
            if "level" in cfg:
                logging.getLogger(lgr).setLevel(cfg["level"])


def configure_logging():
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
