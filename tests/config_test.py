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

import configparser
from collections import abc
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, get_args

import pytest

from esrally import config, exceptions
from esrally.utils.cases import cases


class MockInput:
    def __init__(self, inputs):
        self.inputs = iter(inputs)

    def __call__(self, *args, **kwargs):
        null_output(*args, **kwargs)
        v = next(self.inputs)
        null_output(v)
        return v


def null_output(*args, **kwargs):
    # print(*args, **kwargs)
    pass


class InMemoryConfigStore(config.ConfigFile):

    def __init__(self, config_name: str = None, config: dict | None = None, backup_created: bool = False, present: bool = False):
        super().__init__(config_name)
        # support initialization from a dict
        self.config: configparser.ConfigParser | None = None
        if config is not None:
            self.config = configparser.ConfigParser()
            self.config.read_dict(config)
        self.backup_created = backup_created
        self.present = present

    location = "in-memory"

    config_dir = "in-memory"

    present = False

    def backup(self):
        self.backup_created = True

    def store_default_config(self, template_path: str | None = None):
        self.present = True
        self.config = configparser.ConfigParser()
        self.config.read_dict(
            {
                "distributions": {
                    "release.url": "https://acme.com/releases",
                    "release.cache": "true",
                },
                "system": {"env.name": "existing-unit-test-config"},
                "meta": {"config.version": config.Config.CURRENT_CONFIG_VERSION},
                "benchmarks": {"local.dataset.cache": "/tmp/rally/data"},
            }
        )

    def load(self, interpolation=None):
        # interpolation is not supported in tests, we just mimic the interface
        return self.config


class TestConfig:
    def test_load_non_existing_config(self):
        cfg = config.Config(config_file=InMemoryConfigStore())
        assert not cfg.config_present()
        # standard properties are still available
        assert cfg.opts("provisioning", "node.name.prefix") == "rally-node"

    def test_load_existing_config(self):
        cfg = config.Config(
            config_file=InMemoryConfigStore(
                config={
                    "tests": {"sample.key": "value"},
                    "meta": {"config.version": config.Config.CURRENT_CONFIG_VERSION},
                }
            )
        )
        cfg.load_config()
        # standard properties are still available
        assert cfg.opts("provisioning", "node.name.prefix") == "rally-node"
        assert cfg.opts("tests", "sample.key") == "value"
        # we can also override values
        cfg.add(config.Scope.applicationOverride, "tests", "sample.key", "override")
        assert cfg.opts("tests", "sample.key") == "override"

    def test_load_all_opts_in_section(self):
        cfg = config.Config(
            config_file=InMemoryConfigStore(
                config={
                    "distributions": {
                        "release.url": "https://acme.com/releases",
                        "release.cache": "true",
                        "snapshot.url": "https://acme.com/snapshots",
                        "snapshot.cache": "false",
                    },
                    "system": {"env.name": "local"},
                    "meta": {"config.version": config.Config.CURRENT_CONFIG_VERSION},
                }
            )
        )
        cfg.load_config()
        # override a value so we can see that the scoping logic still works. Default is scope "application"
        cfg.add(config.Scope.applicationOverride, "distributions", "snapshot.cache", "true")

        assert cfg.all_opts("distributions") == {
            "release.url": "https://acme.com/releases",
            "release.cache": "true",
            "snapshot.url": "https://acme.com/snapshots",
            # overridden!
            "snapshot.cache": "true",
        }

    def test_add_all_in_section(self):
        source_cfg = config.Config(
            config_file=InMemoryConfigStore(
                config={
                    "tests": {"sample.key": "value", "sample.key2": "value"},
                    "no_copy": {"other.key": "value"},
                    "meta": {"config.version": config.Config.CURRENT_CONFIG_VERSION},
                }
            )
        )
        source_cfg.load_config()

        target_cfg = config.Config(config_file=InMemoryConfigStore())

        assert target_cfg.opts("tests", "sample.key", mandatory=False) is None

        target_cfg.add_all(source=source_cfg, section="tests")
        assert target_cfg.opts("tests", "sample.key") == "value"
        assert target_cfg.opts("no_copy", "other.key", mandatory=False) is None

        # nonexisting key will not throw an error; intentional use of nonexistent section
        target_cfg.add_all(source=source_cfg, section="this section does not exist")  # type: ignore[arg-type]


class TestAutoLoadConfig:
    def test_can_create_non_existing_config(self):
        base_cfg = config.Config(config_name="unittest", config_file=InMemoryConfigStore())
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")
        base_cfg.add(config.Scope.application, "tracks", "metrics.url", "http://github.com/org/metrics")
        base_cfg.add(config.Scope.application, "teams", "private.url", "http://github.com/org/teams")
        base_cfg.add(config.Scope.application, "distributions", "release.cache", False)
        base_cfg.add(config.Scope.application, "defaults", "preserve_benchmark_candidate", True)

        cfg = config.auto_load_local_config(base_cfg, config_file=InMemoryConfigStore())
        assert cfg.config_file.present
        # did not just copy base config
        assert base_cfg.opts("benchmarks", "local.dataset.cache") != cfg.opts("benchmarks", "local.dataset.cache")
        # copied sections from base config
        self.assert_equals_base_config(base_cfg, cfg, "reporting", "datastore.type")
        self.assert_equals_base_config(base_cfg, cfg, "tracks", "metrics.url")
        self.assert_equals_base_config(base_cfg, cfg, "teams", "private.url")
        self.assert_equals_base_config(base_cfg, cfg, "distributions", "release.cache")
        self.assert_equals_base_config(base_cfg, cfg, "defaults", "preserve_benchmark_candidate")

    def test_can_load_and_amend_existing_config(self):
        base_cfg = config.Config(config_name="unittest", config_file=InMemoryConfigStore())
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "unit-test", "sample.property", "let me copy you")

        cfg = config.auto_load_local_config(
            base_cfg,
            additional_sections=["unit-test"],
            config_file=InMemoryConfigStore(
                present=True,
                config={
                    "distributions": {
                        "release.url": "https://acme.com/releases",
                        "release.cache": "true",
                    },
                    "system": {"env.name": "existing-unit-test-config"},
                    "meta": {"config.version": config.Config.CURRENT_CONFIG_VERSION},
                    "benchmarks": {"local.dataset.cache": "/tmp/rally/data"},
                },
            ),
        )
        assert cfg.config_file.present
        # did not just copy base config
        assert base_cfg.opts("benchmarks", "local.dataset.cache") != cfg.opts("benchmarks", "local.dataset.cache")
        # keeps config properties
        assert cfg.opts("system", "env.name") == "existing-unit-test-config"
        # copies additional properties
        self.assert_equals_base_config(base_cfg, cfg, "unit-test", "sample.property")

    def test_can_migrate_outdated_config(self):
        base_cfg = config.Config(config_name="unittest", config_file=InMemoryConfigStore())
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "unit-test", "sample.property", "let me copy you")

        cfg = config.auto_load_local_config(
            base_cfg,
            additional_sections=["unit-test"],
            config_file=InMemoryConfigStore(
                config={
                    "distributions": {
                        "release.url": "https://acme.com/releases",
                        "release.cache": "true",
                    },
                    "system": {"env.name": "existing-unit-test-config"},
                    # outdated
                    "meta": {
                        # ensure we don't attempt to migrate if that version is unsupported
                        "config.version": max(config.Config.CURRENT_CONFIG_VERSION - 1, config.Config.EARLIEST_SUPPORTED_VERSION)
                    },
                    "benchmarks": {"local.dataset.cache": "/tmp/rally/data"},
                    "runtime": {"java8.home": "/opt/jdk8"},
                },
            ),
        )
        assert cfg.config_file.present
        # did not just copy base config
        assert base_cfg.opts("benchmarks", "local.dataset.cache") != cfg.opts("benchmarks", "local.dataset.cache")
        # migrated existing config
        assert int(cfg.opts("meta", "config.version")) == config.Config.CURRENT_CONFIG_VERSION

    def assert_equals_base_config(self, base_config, local_config, section, key):
        assert base_config.opts(section, key) == local_config.opts(section, key)

    @dataclass
    class BooleanCase:
        value: str | None
        default: bool | None = None
        want: bool | None = None
        want_error: type[Exception] | None = None

    @cases(
        value_none=BooleanCase(value=None, want_error=exceptions.ConfigError),
        value_false=BooleanCase(value="false", want=False),
        value_true=BooleanCase(value="true", want=True),
        default_false=BooleanCase(value=None, default=False, want=False),
        default_true=BooleanCase(value=None, default=True, want=True),
    )
    def test_boolean(self, case):
        cfg = config.Config()
        if case.value is not None:
            cfg.add(scope=None, section="reporting", key="values", value=case.value)
        try:
            got = cfg.boolean(section="reporting", key="values", default=case.default)
        except Exception as ex:
            assert isinstance(ex, case.want_error)
            assert case.want is None
        else:
            assert case.want == got
            assert case.want_error is None


class TestConfigMigration:
    def test_does_not_migrate_outdated_config(self):
        config_file = InMemoryConfigStore(
            "test",
            config={
                "system": {
                    "root.dir": "in-memory",
                },
                "provisioning": {},
                "build": {
                    "maven.bin": "/usr/local/mvn",
                },
                "benchmarks": {
                    "metrics.stats.disk.device": "/dev/hdd1",
                },
                "reporting": {
                    "report.base.dir": "/tests/rally/reporting",
                    "output.html.report.filename": "index.html",
                },
                "runtime": {
                    "java8.home": "/opt/jdk/8",
                },
            },
        )

        with pytest.raises(
            exceptions.ConfigError, match="The config file.*is too old. Please delete it and reconfigure Rally from scratch"
        ):
            config.migrate(config_file, config.Config.EARLIEST_SUPPORTED_VERSION - 1, config.Config.CURRENT_CONFIG_VERSION, out=null_output)

    # catch all test, migrations are checked in more detail in the other tests
    def test_migrate_from_earliest_supported_to_latest(self):
        config_file = InMemoryConfigStore(
            "test",
            config={
                "meta": {
                    "config.version": config.Config.EARLIEST_SUPPORTED_VERSION,
                },
                "system": {
                    "root.dir": "in-memory",
                },
                "provisioning": {},
                "build": {
                    "maven.bin": "/usr/local/mvn",
                },
                "benchmarks": {
                    "metrics.stats.disk.device": "/dev/hdd1",
                },
                "reporting": {
                    "report.base.dir": "/tests/rally/reporting",
                    "output.html.report.filename": "index.html",
                },
                "runtime": {
                    "java8.home": "/opt/jdk/8",
                },
                "distributions": {
                    "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                },
            },
        )

        config.migrate(config_file, config.Config.EARLIEST_SUPPORTED_VERSION, config.Config.CURRENT_CONFIG_VERSION, out=null_output)

        if config.Config.EARLIEST_SUPPORTED_VERSION < config.Config.CURRENT_CONFIG_VERSION:
            assert config_file.backup_created
        assert config_file.config["meta"]["config.version"] == str(config.Config.CURRENT_CONFIG_VERSION)


@pytest.fixture()
def _project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture()
def _project_files(_project_root: Path) -> abc.Iterable[Path]:
    return tuple(f for f in _project_root.glob("esrally/**/*.py") if f.match("*.py"))


class _ProjectSource(NamedTuple):
    file: Path
    source: str


@pytest.fixture()
def _project_sources(_project_files: abc.Iterable[Path]) -> abc.Iterable[_ProjectSource]:
    return tuple(_ProjectSource(file=f, source=f.read_text(encoding="utf-8", errors="replace")) for f in _project_files)


class TestConfigAnnotations:

    def test_section_literals(self, _project_sources: abc.Iterable[_ProjectSource]):
        sections = tuple(get_args(config.Section))
        assert sections == tuple(sorted(set(sections))), "config.Section literals not sorted or unique"

    def test_key_literals(self, _project_sources: abc.Iterable[_ProjectSource]):
        keys = get_args(config.Key)
        assert keys == tuple(sorted(set(keys))), "config.Key literals not sorted or unique"
