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

from esrally import paths
from esrally.utils import io


# pylint: disable=unused-argument
def configure_utc_formatter(*args, **kwargs):
    """
    Logging formatter that renders timestamps in UTC to ensure consistent
    timestamps across all deployments regardless of machine settings.
    """
    formatter = logging.Formatter(fmt=kwargs["format"], datefmt=kwargs["datefmt"])
    formatter.converter = time.gmtime
    return formatter


def log_config_path():
    """
    :return: The absolute path to Rally's log configuration file.
    """
    return os.path.join(paths.rally_confdir(), "logging.json")


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
                # Ensure we have a trailing path separator as after LOG_PATH there will only be the file name
                log_path = os.path.join(paths.logs(), "")
                # the logging path might contain backslashes that we need to escape
                log_path = io.escape_path(log_path)
                contents = src.read().replace("${LOG_PATH}", log_path)
                target.write(contents)
    io.ensure_dir(paths.logs())


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
