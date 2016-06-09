import configparser

from unittest import TestCase
import unittest.mock as mock

from esrally import config


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
    def __init__(self, config_name):
        self.config_name = config_name
        self.config = None
        self.backup_created = False
        self.present = False
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


class ConfigFactoryTests(TestCase):
    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    def test_create_simple_config(self, guess_install_location, guess_java_home):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = "/tests/java8/home"
        mock_input = MockInput(["/Projects/elasticsearch/src"])

        f = config.ConfigFactory(i=mock_input, sec_i=mock_input, o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store)
        self.assertIsNotNone(config_store.config)
        self.assertTrue("meta" in config_store.config)
        self.assertEqual("5", config_store.config["meta"]["config.version"])
        self.assertTrue("system" in config_store.config)
        self.assertEqual("local", config_store.config["system"]["env.name"])
        self.assertTrue("source" in config_store.config)
        self.assertTrue("build" in config_store.config)
        self.assertEqual("/tests/usr/bin/gradle", config_store.config["build"]["gradle.bin"])
        self.assertTrue("provisioning" in config_store.config)
        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java8/home", config_store.config["runtime"]["java8.home"])
        self.assertTrue("benchmarks" in config_store.config)
        self.assertTrue("reporting" in config_store.config)
        self.assertEqual("in-memory", config_store.config["reporting"]["datastore.type"])
        self.assertTrue("tracks" in config_store.config)

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    @mock.patch("esrally.utils.io.normalize_path")
    @mock.patch("os.path.exists")
    def test_create_simple_config_no_java8_detected(self, path_exists, normalize_path, guess_install_location,
                                                    guess_java_home):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = None
        normalize_path.return_value = "/tests/java8/home"
        path_exists.return_value = True

        f = config.ConfigFactory(i=MockInput(["/Projects/elasticsearch/src", "/tests/java8/home"]), o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store)

        self.assertIsNotNone(config_store.config)
        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java8/home", config_store.config["runtime"]["java8.home"])

    @mock.patch("esrally.utils.io.guess_java_home")
    @mock.patch("esrally.utils.io.guess_install_location")
    def test_create_advanced_config(self, guess_install_location, guess_java_home):
        guess_install_location.side_effect = ["/tests/usr/bin/git", "/tests/usr/bin/gradle"]
        guess_java_home.return_value = "/tests/java8/home"

        f = config.ConfigFactory(i=MockInput([
            # src dir
            "/Projects/elasticsearch/src",
            # env
            "unittest-env",
            # data_store_host
            "localhost",
            # data_store_port
            "9200",
            # data_store_secure
            "Yes",
            # data_store_user
            "user"
        ]), sec_i=MockInput(["pw"]), o=null_output)

        config_store = InMemoryConfigStore("test")
        f.create_config(config_store, advanced_config=True)

        self.assertIsNotNone(config_store.config)
        self.assertTrue("meta" in config_store.config)
        self.assertEqual("5", config_store.config["meta"]["config.version"])
        self.assertTrue("system" in config_store.config)
        self.assertEqual("unittest-env", config_store.config["system"]["env.name"])
        self.assertTrue("source" in config_store.config)
        self.assertTrue("build" in config_store.config)
        self.assertEqual("/tests/usr/bin/gradle", config_store.config["build"]["gradle.bin"])
        self.assertTrue("provisioning" in config_store.config)
        self.assertTrue("runtime" in config_store.config)
        self.assertEqual("/tests/java8/home", config_store.config["runtime"]["java8.home"])
        self.assertTrue("benchmarks" in config_store.config)
        self.assertTrue("reporting" in config_store.config)
        self.assertEqual("elasticsearch", config_store.config["reporting"]["datastore.type"])
        self.assertEqual("localhost", config_store.config["reporting"]["datastore.host"])
        self.assertEqual("9200", config_store.config["reporting"]["datastore.port"])
        self.assertEqual("True", config_store.config["reporting"]["datastore.secure"])
        self.assertEqual("user", config_store.config["reporting"]["datastore.user"])
        self.assertEqual("pw", config_store.config["reporting"]["datastore.password"])
        self.assertTrue("tracks" in config_store.config)


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
