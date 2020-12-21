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

import configparser
from unittest import TestCase

from esrally import config, exceptions


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


class InMemoryConfigStore:
    def __init__(self, config_name, config=None, backup_created=False, present=False):
        self.config_name = config_name
        # support initialization from a dict
        if config:
            self.config = configparser.ConfigParser()
            self.config.read_dict(config)
        else:
            self.config = config
        self.backup_created = backup_created
        self.present = present
        self.location = "in-memory"
        self.config_dir = "in-memory"

    def backup(self):
        self.backup_created = True

    def store_default_config(self):
        self.store({
            "distributions": {
                "release.url": "https://acme.com/releases",
                "release.cache": "true",
            },
            "system": {
                "env.name": "existing-unit-test-config"
            },
            "meta": {
                "config.version": config.Config.CURRENT_CONFIG_VERSION
            },
            "benchmarks": {
                "local.dataset.cache": "/tmp/rally/data"
            }
        })

    def store(self, c):
        self.present = True
        self.config = configparser.ConfigParser()
        self.config.read_dict(c)

    def load(self, interpolation=None):
        # interpolation is not supported in tests, we just mimic the interface
        return self.config


class ConfigTests(TestCase):
    def test_load_non_existing_config(self):
        cfg = config.Config(config_file_class=InMemoryConfigStore)
        self.assertFalse(cfg.config_present())
        # standard properties are still available
        self.assertEqual("rally-node", cfg.opts("provisioning", "node.name.prefix"))

    def test_load_existing_config(self):
        cfg = config.Config(config_file_class=InMemoryConfigStore)
        self.assertFalse(cfg.config_present())

        sample_config = {
            "tests": {
                "sample.key": "value"
            },
            "meta": {
                "config.version": config.Config.CURRENT_CONFIG_VERSION
            }
        }
        cfg.config_file.store(sample_config)

        self.assertTrue(cfg.config_present())
        cfg.load_config()
        # standard properties are still available
        self.assertEqual("rally-node", cfg.opts("provisioning", "node.name.prefix"))
        self.assertEqual("value", cfg.opts("tests", "sample.key"))
        # we can also override values
        cfg.add(config.Scope.applicationOverride, "tests", "sample.key", "override")
        self.assertEqual("override", cfg.opts("tests", "sample.key"))

    def test_load_all_opts_in_section(self):
        cfg = config.Config(config_file_class=InMemoryConfigStore)
        self.assertFalse(cfg.config_present())

        sample_config = {
            "distributions": {
                "release.url": "https://acme.com/releases",
                "release.cache": "true",
                "snapshot.url": "https://acme.com/snapshots",
                "snapshot.cache": "false"
            },
            "system": {
                "env.name": "local"
            },
            "meta": {
                "config.version": config.Config.CURRENT_CONFIG_VERSION
            }
        }
        cfg.config_file.store(sample_config)

        self.assertTrue(cfg.config_present())
        cfg.load_config()
        # override a value so we can see that the scoping logic still works. Default is scope "application"
        cfg.add(config.Scope.applicationOverride, "distributions", "snapshot.cache", "true")

        self.assertEqual({
            "release.url": "https://acme.com/releases",
            "release.cache": "true",
            "snapshot.url": "https://acme.com/snapshots",
            # overridden!
            "snapshot.cache": "true"
        }, cfg.all_opts("distributions"))

    def test_add_all_in_section(self):
        source_cfg = config.Config(config_file_class=InMemoryConfigStore)
        sample_config = {
            "tests": {
                "sample.key": "value",
                "sample.key2": "value"
            },
            "no_copy": {
                "other.key": "value"
            },
            "meta": {
                "config.version": config.Config.CURRENT_CONFIG_VERSION
            }
        }
        source_cfg.config_file.store(sample_config)
        source_cfg.load_config()

        target_cfg = config.Config(config_file_class=InMemoryConfigStore)

        self.assertIsNone(target_cfg.opts("tests", "sample.key", mandatory=False))

        target_cfg.add_all(source=source_cfg, section="tests")
        self.assertEqual("value", target_cfg.opts("tests", "sample.key"))
        self.assertIsNone(target_cfg.opts("no_copy", "other.key", mandatory=False))

        # nonexisting key will not throw an error
        target_cfg.add_all(source=source_cfg, section="this section does not exist")


class AutoLoadConfigTests(TestCase):
    def test_can_create_non_existing_config(self):
        base_cfg = config.Config(config_name="unittest", config_file_class=InMemoryConfigStore)
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")
        base_cfg.add(config.Scope.application, "tracks", "metrics.url", "http://github.com/org/metrics")
        base_cfg.add(config.Scope.application, "teams", "private.url", "http://github.com/org/teams")
        base_cfg.add(config.Scope.application, "distributions", "release.cache", False)
        base_cfg.add(config.Scope.application, "defaults", "preserve_benchmark_candidate", True)

        cfg = config.auto_load_local_config(base_cfg, config_file_class=InMemoryConfigStore)
        self.assertTrue(cfg.config_file.present)
        # did not just copy base config
        self.assertNotEqual(base_cfg.opts("benchmarks", "local.dataset.cache"), cfg.opts("benchmarks", "local.dataset.cache"))
        # copied sections from base config
        self.assert_equals_base_config(base_cfg, cfg, "reporting", "datastore.type")
        self.assert_equals_base_config(base_cfg, cfg, "tracks", "metrics.url")
        self.assert_equals_base_config(base_cfg, cfg, "teams", "private.url")
        self.assert_equals_base_config(base_cfg, cfg, "distributions", "release.cache")
        self.assert_equals_base_config(base_cfg, cfg, "defaults", "preserve_benchmark_candidate")

    def test_can_load_and_amend_existing_config(self):
        base_cfg = config.Config(config_name="unittest", config_file_class=InMemoryConfigStore)
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "unit-test", "sample.property", "let me copy you")

        cfg = config.auto_load_local_config(base_cfg, additional_sections=["unit-test"],
                                            config_file_class=InMemoryConfigStore, present=True, config={
            "distributions": {
                "release.url": "https://acme.com/releases",
                "release.cache": "true",
            },
            "system": {
                "env.name": "existing-unit-test-config"
            },
            "meta": {
                "config.version": config.Config.CURRENT_CONFIG_VERSION
            },
            "benchmarks": {
                "local.dataset.cache": "/tmp/rally/data"
            }
        })
        self.assertTrue(cfg.config_file.present)
        # did not just copy base config
        self.assertNotEqual(base_cfg.opts("benchmarks", "local.dataset.cache"), cfg.opts("benchmarks", "local.dataset.cache"))
        # keeps config properties
        self.assertEqual("existing-unit-test-config", cfg.opts("system", "env.name"))
        # copies additional properties
        self.assert_equals_base_config(base_cfg, cfg, "unit-test", "sample.property")

    def test_can_migrate_outdated_config(self):
        base_cfg = config.Config(config_name="unittest", config_file_class=InMemoryConfigStore)
        base_cfg.add(config.Scope.application, "meta", "config.version", config.Config.CURRENT_CONFIG_VERSION)
        base_cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/base-config/data-set-cache")
        base_cfg.add(config.Scope.application, "unit-test", "sample.property", "let me copy you")

        cfg = config.auto_load_local_config(base_cfg, additional_sections=["unit-test"],
                                            config_file_class=InMemoryConfigStore, present=True, config={
                "distributions": {
                    "release.url": "https://acme.com/releases",
                    "release.cache": "true",
                },
                "system": {
                    "env.name": "existing-unit-test-config"
                },
                # outdated
                "meta": {
                    # ensure we don't attempt to migrate if that version is unsupported
                    "config.version": max(config.Config.CURRENT_CONFIG_VERSION - 1, config.Config.EARLIEST_SUPPORTED_VERSION)
                },
                "benchmarks": {
                    "local.dataset.cache": "/tmp/rally/data"
                },
                "runtime": {
                    "java8.home": "/opt/jdk8"
                }
            })
        self.assertTrue(cfg.config_file.present)
        # did not just copy base config
        self.assertNotEqual(base_cfg.opts("benchmarks", "local.dataset.cache"), cfg.opts("benchmarks", "local.dataset.cache"))
        # migrated existing config
        self.assertEqual(config.Config.CURRENT_CONFIG_VERSION, int(cfg.opts("meta", "config.version")))

    def assert_equals_base_config(self, base_config, local_config, section, key):
        self.assertEqual(base_config.opts(section, key), local_config.opts(section, key))


class ConfigMigrationTests(TestCase):
    def test_does_not_migrate_outdated_config(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "system": {
                "root.dir": "in-memory"
            },
            "provisioning": {

            },
            "build": {
                "maven.bin": "/usr/local/mvn"
            },
            "benchmarks": {
                "metrics.stats.disk.device": "/dev/hdd1"
            },
            "reporting": {
                "report.base.dir": "/tests/rally/reporting",
                "output.html.report.filename": "index.html"
            },
            "runtime": {
                "java8.home": "/opt/jdk/8",
            }
        }

        config_file.store(sample_config)
        with self.assertRaisesRegex(exceptions.ConfigError,
                                    "The config file.*is too old. Please delete it and reconfigure Rally from scratch"):
            config.migrate(config_file, config.Config.EARLIEST_SUPPORTED_VERSION - 1, config.Config.CURRENT_CONFIG_VERSION, out=null_output)

    # catch all test, migrations are checked in more detail in the other tests
    def test_migrate_from_earliest_supported_to_latest(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": config.Config.EARLIEST_SUPPORTED_VERSION
            },
            "system": {
                "root.dir": "in-memory"
            },
            "provisioning": {

            },
            "build": {
                "maven.bin": "/usr/local/mvn"
            },
            "benchmarks": {
                "metrics.stats.disk.device": "/dev/hdd1"
            },
            "reporting": {
                "report.base.dir": "/tests/rally/reporting",
                "output.html.report.filename": "index.html"
            },
            "runtime": {
                "java8.home": "/opt/jdk/8",
            },
            "distributions": {
                "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz"
            }
        }

        config_file.store(sample_config)
        config.migrate(config_file, config.Config.EARLIEST_SUPPORTED_VERSION, config.Config.CURRENT_CONFIG_VERSION, out=null_output)

        if config.Config.EARLIEST_SUPPORTED_VERSION < config.Config.CURRENT_CONFIG_VERSION:
            self.assertTrue(config_file.backup_created)
        self.assertEqual(str(config.Config.CURRENT_CONFIG_VERSION), config_file.config["meta"]["config.version"])
