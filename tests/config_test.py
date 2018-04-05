import os
import configparser

from unittest import TestCase
import unittest.mock as mock

from esrally import config
from esrally.utils import io


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
                    "config.version": config.Config.CURRENT_CONFIG_VERSION - 1
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


class ConfigFactoryTests(TestCase):
    @mock.patch("esrally.utils.git.is_working_copy")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    def test_create_simple_config(self, guess_install_location, guess_java_home, is_ea_release, working_copy):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = "/tests/java10/home"
        is_ea_release.return_value = False
        # Rally checks in the parent and sibling directories whether there is an ES working copy. We don't want this detection logic
        # to succeed spuriously (e.g. on developer machines).
        working_copy.return_value = False
        mock_input = MockInput([""])

        f = config.ConfigFactory(i=mock_input, sec_i=mock_input, o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store)
        self.assertIsNotNone(config_store.config)

        for section, _ in config_store.config.items():
            for k, v in config_store.config[section].items():
                print("%s::%s: %s" % (section, k, v))

        self.assertTrue("meta" in config_store.config)
        self.assertEqual("14", config_store.config["meta"]["config.version"])

        self.assertTrue("system" in config_store.config)
        self.assertEqual("local", config_store.config["system"]["env.name"])

        self.assertTrue("node" in config_store.config)
        self.assertEqual(io.normalize_path(os.path.abspath("./in-memory/benchmarks")), config_store.config["node"]["root.dir"])
        self.assertEqual(io.normalize_path(os.path.abspath("./in-memory/benchmarks/src")), config_store.config["node"]["src.root.dir"])

        self.assertTrue("source" in config_store.config)
        self.assertEqual("https://github.com/elastic/elasticsearch.git", config_store.config["source"]["remote.repo.url"])
        self.assertEqual("elasticsearch", config_store.config["source"]["elasticsearch.src.subdir"])

        self.assertTrue("build" in config_store.config)
        self.assertEqual("/tests/usr/bin/gradle", config_store.config["build"]["gradle.bin"])

        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java10/home", config_store.config["runtime"]["java.home"])
        self.assertEqual("/tests/java10/home", config_store.config["runtime"]["java10.home"])

        self.assertTrue("benchmarks" in config_store.config)
        self.assertEqual("${node:root.dir}/data", config_store.config["benchmarks"]["local.dataset.cache"])

        self.assertTrue("reporting" in config_store.config)
        self.assertEqual("in-memory", config_store.config["reporting"]["datastore.type"])
        self.assertEqual("", config_store.config["reporting"]["datastore.host"])
        self.assertEqual("", config_store.config["reporting"]["datastore.port"])
        self.assertEqual("", config_store.config["reporting"]["datastore.secure"])
        self.assertEqual("", config_store.config["reporting"]["datastore.user"])
        self.assertEqual("", config_store.config["reporting"]["datastore.password"])

        self.assertTrue("tracks" in config_store.config)
        self.assertEqual("https://github.com/elastic/rally-tracks", config_store.config["tracks"]["default.url"])

        self.assertTrue("teams" in config_store.config)
        self.assertEqual("https://github.com/elastic/rally-teams", config_store.config["teams"]["default.url"])

        self.assertTrue("defaults" in config_store.config)
        self.assertEqual("False", config_store.config["defaults"]["preserve_benchmark_candidate"])

        self.assertTrue("distributions" in config_store.config)
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.1.url"])
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/"
                         "{{VERSION}}/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.2.url"])
        self.assertEqual("https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.url"])
        self.assertEqual("true", config_store.config["distributions"]["release.cache"])

    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    @mock.patch("esrally.utils.io.normalize_path")
    @mock.patch("os.path.exists")
    def test_create_simple_config_no_java_detected(self, path_exists, normalize_path, guess_install_location, guess_java_home,
                                                   major_jvm_version, jvm_is_early_access_release):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = None
        normalize_path.side_effect = ["/home/user/.rally/benchmarks", "/tests/java10/home", "/tests/java8/home",
                                      "/home/user/.rally/benchmarks/src"]
        major_jvm_version.return_value = 10
        jvm_is_early_access_release.return_value = False
        path_exists.return_value = True

        f = config.ConfigFactory(i=MockInput(["/tests/java10/home", "/Projects/elasticsearch/src", "/tests/java8/home"]), o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store)

        self.assertIsNotNone(config_store.config)
        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java8/home", config_store.config["runtime"]["java.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    def test_create_simple_config_no_java_installed(self, guess_install_location, guess_java_home):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = None

        # the input is the question for the JDK home and the JDK 10 home directory - the user does not define one
        f = config.ConfigFactory(i=MockInput(["", ""]), o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store)

        self.assertIsNotNone(config_store.config)
        self.assertFalse("java.home" in config_store.config["runtime"])
        self.assertFalse("java10.home" in config_store.config["runtime"])

    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    def test_create_advanced_config(self, guess_install_location, guess_java_home, is_ea_release):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.side_effect = ["/tests/java8/home", "/tests/java10/home"]
        is_ea_release.return_value = False

        f = config.ConfigFactory(i=MockInput([
            # benchmark root directory
            "/var/data/rally",
            # src dir
            "/Projects/elasticsearch/src",
            # metrics store type (Elasticsearch)
            "2",
            # data_store_host
            "localhost",
            # data_store_port
            "9200",
            # data_store_secure
            "Yes",
            # data_store_user
            "user",
            # env
            "unittest-env",
            # preserve benchmark candidate
            "y"
        ]), sec_i=MockInput(["pw"]), o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store, advanced_config=True)

        self.assertIsNotNone(config_store.config)
        self.assertTrue("meta" in config_store.config)
        self.assertEqual("14", config_store.config["meta"]["config.version"])
        self.assertTrue("system" in config_store.config)
        self.assertEqual("unittest-env", config_store.config["system"]["env.name"])
        self.assertTrue("node" in config_store.config)
        self.assertEqual("/var/data/rally", config_store.config["node"]["root.dir"])
        self.assertTrue("source" in config_store.config)
        self.assertTrue("build" in config_store.config)
        self.assertEqual("/tests/usr/bin/gradle", config_store.config["build"]["gradle.bin"])
        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java8/home", config_store.config["runtime"]["java.home"])
        self.assertEqual("/tests/java10/home", config_store.config["runtime"]["java10.home"])
        self.assertTrue("benchmarks" in config_store.config)

        self.assertTrue("reporting" in config_store.config)
        self.assertEqual("elasticsearch", config_store.config["reporting"]["datastore.type"])
        self.assertEqual("localhost", config_store.config["reporting"]["datastore.host"])
        self.assertEqual("9200", config_store.config["reporting"]["datastore.port"])
        self.assertEqual("True", config_store.config["reporting"]["datastore.secure"])
        self.assertEqual("user", config_store.config["reporting"]["datastore.user"])
        self.assertEqual("pw", config_store.config["reporting"]["datastore.password"])

        self.assertTrue("tracks" in config_store.config)
        self.assertEqual("https://github.com/elastic/rally-tracks", config_store.config["tracks"]["default.url"])

        self.assertTrue("teams" in config_store.config)
        self.assertEqual("https://github.com/elastic/rally-teams", config_store.config["teams"]["default.url"])

        self.assertTrue("defaults" in config_store.config)
        self.assertEqual("True", config_store.config["defaults"]["preserve_benchmark_candidate"])

        self.assertTrue("distributions" in config_store.config)
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.1.url"])
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/"
                         "{{VERSION}}/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.2.url"])
        self.assertEqual("https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_store.config["distributions"]["release.url"])
        self.assertEqual("true", config_store.config["distributions"]["release.cache"])


class ConfigMigrationTests(TestCase):
    # catch all test, migrations are checked in more detail in the other tests
    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.time.sleep")
    def test_migrate_from_0_to_latest(self, sleep, get_size):
        get_size.return_value = 0
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
        config.migrate(config_file, 0, config.Config.CURRENT_CONFIG_VERSION, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual(str(config.Config.CURRENT_CONFIG_VERSION), config_file.config["meta"]["config.version"])

    def test_migrate_from_2_to_3(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 2
            },
            "system": {
                "root.dir": "in-memory"
            },
            "reporting": {
                "report.base.dir": "/tests/rally/reporting",
                "output.html.report.filename": "index.html"
            },
        }

        config_file.store(sample_config)
        config.migrate(config_file, 2, 3, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("3", config_file.config["meta"]["config.version"])
        # Did not delete the section...
        self.assertTrue("reporting" in config_file.config)
        # ... but the key
        self.assertFalse("report.base.dir" in config_file.config["reporting"])
        self.assertFalse("output.html.report.filename" in config_file.config["reporting"])

    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.time.sleep")
    def test_migrate_from_3_to_4(self, sleep, get_size):
        get_size.return_value = 0
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 3
            },
            "system": {
                "root.dir": "in-memory"
            },
            "reporting": {
                "datastore.host": ""
            },
            "build": {
                "maven.bin": "/usr/local/mvn"
            },
            "benchmarks": {
                "metrics.stats.disk.device": "/dev/hdd1"
            }
        }

        config_file.store(sample_config)
        config.migrate(config_file, 3, 4, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("4", config_file.config["meta"]["config.version"])
        # Did not delete the section...
        self.assertTrue("build" in config_file.config)
        # ... but the key
        self.assertFalse("maven.bin" in config_file.config["build"])
        self.assertTrue("benchmarks" in config_file.config)
        self.assertFalse("metrics.stats.disk.device" in config_file.config["benchmarks"])
        self.assertEqual("in-memory", config_file.config["reporting"]["datastore.type"])

    def test_migrate_from_4_to_5(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 4
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 4, 5, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("5", config_file.config["meta"]["config.version"])
        self.assertTrue("tracks" in config_file.config)
        self.assertEqual("https://github.com/elastic/rally-tracks", config_file.config["tracks"]["default.url"])

    def test_migrate_from_5_to_6(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 5
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 5, 6, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("6", config_file.config["meta"]["config.version"])
        self.assertTrue("defaults" in config_file.config)
        self.assertEqual("False", config_file.config["defaults"]["preserve_benchmark_candidate"])

    def test_migrate_from_6_to_7(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 6
            },
            "system": {
                "log.root.dir": "logs"
            },
            "provisioning": {
                "local.install.dir": "install"
            },
        }
        config_file.store(sample_config)
        config.migrate(config_file, 6, 7, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("7", config_file.config["meta"]["config.version"])
        self.assertTrue("provisioning" not in config_file.config)
        self.assertTrue("log.root.dir" not in config_file.config["system"])

    def test_migrate_from_7_to_8(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 7
            },
            "system": {
                "root.dir": "~/.rally/benchmarks",
                "environment.name": "local"
            },
            "benchmarks": {
                "local.dataset.cache": "${system:root.dir}/data",
                "some.other.cache": "/data"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 7, 8, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("8", config_file.config["meta"]["config.version"])
        self.assertTrue("root.dir" not in config_file.config["system"])
        self.assertEqual("~/.rally/benchmarks", config_file.config["node"]["root.dir"])
        self.assertEqual("local", config_file.config["system"]["environment.name"])
        self.assertEqual("${node:root.dir}/data", config_file.config["benchmarks"]["local.dataset.cache"])
        self.assertEqual("/data", config_file.config["benchmarks"]["some.other.cache"])

    def test_migrate_from_8_to_9(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 8
            },
            "system": {
                "root.dir": "~/.rally/benchmarks",
                "environment.name": "local"
            },
            "benchmarks": {
                "local.dataset.cache": "${system:root.dir}/data",
                "some.other.cache": "/data"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 8, 9, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("9", config_file.config["meta"]["config.version"])
        self.assertTrue("teams" in config_file.config)
        self.assertEqual("https://github.com/elastic/rally-teams", config_file.config["teams"]["default.url"])

    def test_migrate_from_9_to_10(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 9
            },
            "system": {
                "root.dir": "~/.rally/benchmarks",
                "environment.name": "local"
            },
            "benchmarks": {
                "local.dataset.cache": "${system:root.dir}/data",
                "some.other.cache": "/data"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 9, 10, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("10", config_file.config["meta"]["config.version"])
        self.assertTrue("distributions" in config_file.config)
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_file.config["distributions"]["release.1.url"])
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/"
                         "{{VERSION}}/elasticsearch-{{VERSION}}.tar.gz",
                         config_file.config["distributions"]["release.2.url"])
        self.assertEqual("https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
                         config_file.config["distributions"]["release.url"])
        self.assertEqual("true",
                         config_file.config["distributions"]["release.cache"])

    def test_migrate_from_10_to_11(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 10
            },
            "runtime": {
                "java8.home": "/opt/jdk/8",
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 10, 11, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("11", config_file.config["meta"]["config.version"])
        self.assertTrue("runtime" in config_file.config)
        self.assertFalse("java8.home" in config_file.config["runtime"])
        self.assertEqual("/opt/jdk/8", config_file.config["runtime"]["java.home"])

    @mock.patch("esrally.utils.io.exists")
    @mock.patch("os.rename")
    def test_migrate_from_11_to_12_with_default_src_config_repo_checked_out(self, path_rename, path_exists):
        path_exists.return_value = True

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 11
            },
            "node": {
                "root.dir": io.normalize_path("~/.rally/benchmarks")
            },
            "source": {
                "local.src.dir": io.normalize_path("~/.rally/benchmarks/src")
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 11, 12, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("12", config_file.config["meta"]["config.version"])
        self.assertEqual(io.normalize_path("~/.rally/benchmarks/src"), config_file.config["node"]["src.root.dir"])
        self.assertEqual("elasticsearch", config_file.config["source"]["elasticsearch.src.subdir"])

        path_rename.assert_has_calls(
            [
                mock.call(io.normalize_path("~/.rally/benchmarks/src"), io.normalize_path("~/.rally/benchmarks/tmp_src_mig")),
                mock.call(io.normalize_path("~/.rally/benchmarks/tmp_src_mig"),
                          io.normalize_path("~/.rally/benchmarks/src/elasticsearch")),
             ]
        )

    @mock.patch("esrally.utils.io.exists")
    @mock.patch("os.rename")
    def test_migrate_from_11_to_12_with_default_src_config_repo_not_checked_out(self, path_rename, path_exists):
        path_exists.return_value = False

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 11
            },
            "node": {
                "root.dir": io.normalize_path("~/.rally/benchmarks")
            },
            "source": {
                "local.src.dir": io.normalize_path("~/.rally/benchmarks/src")
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 11, 12, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("12", config_file.config["meta"]["config.version"])
        self.assertEqual(io.normalize_path("~/.rally/benchmarks/src"), config_file.config["node"]["src.root.dir"])
        self.assertEqual("elasticsearch", config_file.config["source"]["elasticsearch.src.subdir"])
        # did all the migrations but nothing moved
        path_rename.assert_not_called()

    def test_migrate_from_11_to_12_without_src_config(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 11
            },
            "node": {
                "root.dir": "~/.rally/benchmarks"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 11, 12, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("12", config_file.config["meta"]["config.version"])
        self.assertFalse("src.root.dir" in config_file.config["node"])

    def test_migrate_from_11_to_12_with_partial_src_config(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 11
            },
            "node": {
                "root.dir": "~/.rally/benchmarks"
            },
            "source": {
                # a source config section without any keys should be treated like a missing source config section
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 11, 12, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("12", config_file.config["meta"]["config.version"])
        self.assertFalse("src.root.dir" in config_file.config["node"])
        self.assertFalse("elasticsearch.src.subdir" in config_file.config["source"])

    @mock.patch("esrally.utils.io.exists")
    @mock.patch("os.rename")
    def test_migrate_from_11_to_12_with_custom_src_config(self, path_rename, path_exists):
        path_exists.return_value = False

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 11
            },
            "node": {
                "root.dir": io.normalize_path("~/.rally/benchmarks")
            },
            "source": {
                "local.src.dir": io.normalize_path("~/Projects/elasticsearch/master/es")
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 11, 12, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("12", config_file.config["meta"]["config.version"])
        self.assertEqual(io.normalize_path("~/Projects/elasticsearch/master"), config_file.config["node"]["src.root.dir"])
        self.assertEqual("es", config_file.config["source"]["elasticsearch.src.subdir"])
        # did all the migrations but nothing moved
        path_rename.assert_not_called()

    def test_migrate_from_12_to_13_without_gradle(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 12
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 12, 13, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("13", config_file.config["meta"]["config.version"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    def test_migrate_from_12_to_13_with_gradle_and_jdk8_autodetect_jdk9(self, is_early_access_release, guess_java_home):
        guess_java_home.return_value = "/usr/lib/java9"
        is_early_access_release.return_value = False

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 12
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 12, 13, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("13", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java9", config_file.config["runtime"]["java9.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_12_to_13_with_gradle_and_jdk9(self, major_version, is_early_access_release, guess_java_home):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.return_value = 9

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 12
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java9"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 12, 13, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("13", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java9", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java9", config_file.config["runtime"]["java9.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_12_to_13_with_gradle_and_jdk8_ask_user_and_skip(self, major_version, is_early_access_release, guess_java_home):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.return_value = 8

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 12
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 12, 13, out=null_output, i=MockInput(inputs=[""]))

        self.assertTrue(config_file.backup_created)
        self.assertEqual("13", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertTrue("java9.home" not in config_file.config["runtime"])

    @mock.patch("esrally.utils.io.exists")
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_12_to_13_with_gradle_and_jdk8_ask_user_enter_valid(self, major_version, is_early_access_release, guess_java_home,
                                                                             path_exists):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.side_effect = [8, 9]
        path_exists.return_value = True

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 12
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 12, 13, out=null_output, i=MockInput(inputs=["/usr/lib/java9"]))

        self.assertTrue(config_file.backup_created)
        self.assertEqual("13", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java9", config_file.config["runtime"]["java9.home"])

    def test_migrate_from_13_to_14_without_gradle(self):
        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 13
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 13, 14, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("14", config_file.config["meta"]["config.version"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    def test_migrate_from_13_to_14_with_gradle_and_jdk8_autodetect_jdk10(self, is_early_access_release, guess_java_home):
        guess_java_home.return_value = "/usr/lib/java10"
        is_early_access_release.return_value = False

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 13
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 13, 14, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("14", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java10", config_file.config["runtime"]["java10.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_13_to_14_with_gradle_and_jdk10(self, major_version, is_early_access_release, guess_java_home):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.return_value = 10

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 13
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java10"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 13, 14, out=null_output)

        self.assertTrue(config_file.backup_created)
        self.assertEqual("14", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java10", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java10", config_file.config["runtime"]["java10.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_13_to_14_with_gradle_and_jdk8_ask_user_and_skip(self, major_version, is_early_access_release, guess_java_home):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.return_value = 8

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 13
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 13, 14, out=null_output, i=MockInput(inputs=[""]))

        self.assertTrue(config_file.backup_created)
        self.assertEqual("14", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertTrue("java10.home" not in config_file.config["runtime"])

    @mock.patch("esrally.utils.io.exists")
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.jvm.is_early_access_release")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_migrate_from_13_to_14_with_gradle_and_jdk8_ask_user_enter_valid(self, major_version, is_early_access_release, guess_java_home,
                                                                             path_exists):
        guess_java_home.return_value = None
        is_early_access_release.return_value = False
        major_version.side_effect = [8, 10]
        path_exists.return_value = True

        config_file = InMemoryConfigStore("test")
        sample_config = {
            "meta": {
                "config.version": 13
            },
            "build": {
                "gradle.bin": "/usr/local/bin/gradle"
            },
            "runtime": {
                "java.home": "/usr/lib/java8"
            }
        }
        config_file.store(sample_config)
        config.migrate(config_file, 13, 14, out=null_output, i=MockInput(inputs=["/usr/lib/java10"]))

        self.assertTrue(config_file.backup_created)
        self.assertEqual("14", config_file.config["meta"]["config.version"])
        self.assertEqual("/usr/lib/java8", config_file.config["runtime"]["java.home"])
        self.assertEqual("/usr/lib/java10", config_file.config["runtime"]["java10.home"])
