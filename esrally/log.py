# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import logging.config
import json
import time
import os
import hashlib

from esrally.utils import io


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
    return os.path.join(os.path.expanduser("~"), ".rally", "logging.json")


def default_log_path():
    """
    :return: The absolute path to the directory that contains Rally's log file.
    """
    return os.path.join(os.path.expanduser("~"), ".rally", "logs")


def remove_obsolete_default_log_config():
    """
    Log rotation is problematic because Rally uses multiple processes and there is a lurking race condition when
    rolling log files. Hence, we do not rotate logs from within Rally and leverage established tools like logrotate for that.

    Checks whether the user has a problematic out-of-the-box logging configuration delivered with Rally 1.0.0 which
    used log rotation and removes it so it can be replaced by a new one in a later step.
    """
    log_config = log_config_path()
    if io.exists(log_config):
        source_path = io.normalize_path(os.path.join(os.path.dirname(__file__), "resources", "logging_1_0_0.json"))
        with open(source_path, "r", encoding="UTF-8") as src:
            contents = src.read().replace("${LOG_PATH}", default_log_path())
            source_hash = hashlib.sha512(contents.encode()).hexdigest()
        with open(log_config, "r", encoding="UTF-8") as target:
            target_hash = hashlib.sha512(target.read().encode()).hexdigest()
        if source_hash == target_hash:
            os.rename(log_config, "{}.bak".format(log_config))


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
            with open(source_path, "r", encoding="UTF-8") as src:
                contents = src.read().replace("${LOG_PATH}", default_log_path())
                target.write(contents)
    io.ensure_dir(default_log_path())


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
