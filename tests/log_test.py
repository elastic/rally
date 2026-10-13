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

# pylint: disable=protected-access
import copy
import json
import logging
import logging.config
import os
import time
from typing import Any

import pytest

from esrally import log


@pytest.fixture
def template() -> dict[str, Any]:
    with open(log.TEMPLATE_PATH) as fd:
        return json.load(fd)


@pytest.fixture
def config(tmpdir, template: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(template)
    # change existing to differ from source template, showing that we don't overwrite any existing loggers config
    config["loggers"]["rally.profile"]["level"] = "DEBUG"
    # simulate user missing 'elastic_transport' in logging.json
    del config["loggers"]["elastic_transport"]
    return config


@pytest.fixture
def config_path(tmpdir, config: dict[str, Any]) -> str:
    path = os.path.join(tmpdir, "config.json")
    with open(path, "w") as fd:
        json.dump(config, fd)
    return path


def test_update_logger_config(template: dict[str, Any], config: dict[str, Any], config_path: str) -> None:
    log.update_logger_config(config_path=config_path)

    with open(config_path) as fd:
        got = json.load(fd)

    want = copy.deepcopy(config)
    want["loggers"].update((k, v) for k, v in template["loggers"].items() if k not in config["loggers"])

    assert got["loggers"] == want["loggers"]


LOG_FORMAT = "%(asctime)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def test_configure_formatter_utc():
    formatter = log.configure_utc_formatter(format=LOG_FORMAT, datefmt=DATE_FORMAT)
    assert formatter.converter is time.gmtime


def test_configure_formatter_localtime():
    formatter = log.configure_utc_formatter(format=LOG_FORMAT, datefmt=DATE_FORMAT, timezone="localtime")
    assert formatter.converter is time.localtime


THESPIAN_FILTER = "thespian.director.ActorAddressLogFilter"


@pytest.fixture
def thespian_config(template: dict[str, Any]) -> dict[str, Any]:
    """
    A logging configuration as written by Rally versions that used the Thespian actor library.
    """
    config = copy.deepcopy(template)
    config["filters"]["isActorLog"]["()"] = THESPIAN_FILTER
    del config["loggers"]["ray"]
    # a user customization
    config["loggers"]["rally.profile"]["level"] = "DEBUG"
    return config


def _write(tmpdir, config: dict[str, Any]) -> str:
    path = os.path.join(tmpdir, "logging.json")
    with open(path, "w") as fd:
        json.dump(config, fd)
    return path


def test_update_logger_config_migrates_thespian_filter(tmpdir, thespian_config: dict[str, Any]) -> None:
    path = _write(tmpdir, thespian_config)

    log.update_logger_config(config_path=path)

    with open(path) as fd:
        got = json.load(fd)
    assert got["filters"]["isActorLog"] == {"()": "esrally.log.ActorAddressLogFilter"}
    # references to the filter are kept
    assert got["handlers"]["rally_log_handler"]["filters"] == ["isActorLog"]
    # user customizations are kept and new loggers are added
    assert got["loggers"]["rally.profile"]["level"] == "DEBUG"
    assert got["loggers"]["ray"] == {"level": "WARNING"}


def test_update_logger_config_migration_is_idempotent(tmpdir, thespian_config: dict[str, Any]) -> None:
    path = _write(tmpdir, thespian_config)
    log.update_logger_config(config_path=path)
    with open(path) as fd:
        migrated = fd.read()

    log.update_logger_config(config_path=path)

    with open(path) as fd:
        assert fd.read() == migrated


def test_migrate_logger_config_drops_unknown_thespian_filters(thespian_config: dict[str, Any]) -> None:
    thespian_config["filters"]["custom"] = {"()": "thespian.something.Else"}
    thespian_config["handlers"]["rally_log_handler"]["filters"] = ["isActorLog", "custom"]

    assert log.migrate_logger_config(thespian_config)

    assert "custom" not in thespian_config["filters"]
    assert thespian_config["handlers"]["rally_log_handler"]["filters"] == ["isActorLog"]


def test_migrate_logger_config_does_not_change_current_config(template: dict[str, Any]) -> None:
    config = copy.deepcopy(template)
    assert not log.migrate_logger_config(config)
    assert config == template


def test_load_configuration_migrates_in_memory_when_file_is_read_only(tmpdir, monkeypatch, thespian_config: dict[str, Any]) -> None:
    path = _write(tmpdir, thespian_config)
    os.chmod(path, 0o444)
    monkeypatch.setattr(log, "log_config_path", lambda: path)

    # must not fail even though the file cannot be written
    log.update_logger_config(config_path=path)
    got = log.load_configuration()

    assert got["filters"]["isActorLog"] == {"()": "esrally.log.ActorAddressLogFilter"}


def test_migrated_config_is_accepted_by_dict_config(tmpdir, monkeypatch, thespian_config: dict[str, Any]) -> None:
    monkeypatch.setattr(log.paths, "logs", lambda: str(tmpdir))
    config = copy.deepcopy(thespian_config)
    log.migrate_logger_config(config)

    root = logging.getLogger()
    handlers = list(root.handlers)
    try:
        logging.config.dictConfig(config)
        logging.getLogger("esrally.test").info("hello")
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers = handlers

    with open(os.path.join(tmpdir, "rally.log")) as fd:
        assert "-not-actor-/PID" in fd.read()


def test_actor_address_log_filter(monkeypatch) -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", None, None)
    assert log.ActorAddressLogFilter().filter(record)
    assert record.actorAddress == log.NOT_AN_ACTOR

    monkeypatch.setattr(log, "_ACTOR_ADDRESS", "worker-3")
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", None, None)
    log.ActorAddressLogFilter().filter(record)
    assert record.actorAddress == "worker-3"


def test_configure_actor_logging_configures_once_per_process(monkeypatch) -> None:
    configure_logging = []
    monkeypatch.setattr(log, "configure_logging", lambda: configure_logging.append(True))
    monkeypatch.setattr(log, "_ACTOR_LOGGING_CONFIGURED_PID", None)
    monkeypatch.setattr(log, "_ACTOR_ADDRESS", log.NOT_AN_ACTOR)
    monkeypatch.setattr(log.io, "exists", lambda path: True)

    log.configure_actor_logging("worker-0")
    log.configure_actor_logging("worker-0")

    assert configure_logging == [True]
    assert log._ACTOR_ADDRESS == "worker-0"


def test_rename_actor_fields() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", None, None)
    log_dict: dict[str, Any] = {"actorAddress": "driver"}

    log.rename_actor_fields(record, log_dict)

    assert log_dict == {"rally": {"actor": {"address": "driver"}}}


def test_all_log_records_have_an_actor_address(monkeypatch) -> None:
    monkeypatch.setattr(log, "_ACTOR_ADDRESS_RECORD_FACTORY_INSTALLED", False)
    original_factory = logging.getLogRecordFactory()
    try:
        log._install_actor_address_record_factory()
        record = logging.getLogger("esrally.test").makeRecord("esrally.test", logging.INFO, __file__, 1, "message", (), None)
    finally:
        logging.setLogRecordFactory(original_factory)

    assert record.actorAddress == log.NOT_AN_ACTOR
    assert logging.Formatter("%(actorAddress)s %(message)s").format(record) == "-not-actor- message"
