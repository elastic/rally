from unittest import TestCase
import unittest.mock as mock

from rally import config
from rally.mechanic import builder


class BuilderTests(TestCase):
    @mock.patch("rally.utils.process.run_subprocess")
    @mock.patch("logging.Logger")
    @mock.patch("glob.glob", lambda p: ["elasticsearch.zip"])
    def test_skip_build(self, mock_run_subprocess, mock_logger):
        cfg = config.Config()
        cfg.add(config.Scope.application, "build", "skip", True)
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")

        b = builder.Builder(cfg, mock_logger)
        b.build()

        # should not do anything but still add the binary to the config
        mock_run_subprocess.assert_not_called()
        # but should still setup the binary path
        self.assertEqual(cfg.opts("builder", "candidate.bin.path"), "elasticsearch.zip")

    @mock.patch("glob.glob", lambda p: ["elasticsearch.zip"])
    @mock.patch("rally.utils.io.ensure_dir")
    @mock.patch("os.rename")
    @mock.patch("logging.Logger")
    @mock.patch("rally.utils.process.run_subprocess")
    def test_build(self, mock_run_subprocess, mock_logger, mock_rename, mock_ensure_dir):
        cfg = config.Config()
        cfg.add(config.Scope.application, "build", "skip", False)
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "build", "gradle.bin", "/usr/local/gradle")
        cfg.add(config.Scope.application, "build", "gradle.tasks.clean", "clean")
        cfg.add(config.Scope.application, "build", "gradle.tasks.package", "assemble")
        cfg.add(config.Scope.application, "system", "log.dir", "logs")
        cfg.add(config.Scope.application, "build", "log.dir", "build")

        b = builder.Builder(cfg, mock_logger)
        b.build()

        calls = [
            # Actual call
            mock.call("cd /src; /usr/local/gradle clean > logs/build/build.gradle.tasks.clean.log.tmp 2>&1"),
            # Return value check
            mock.call().__bool__(),
            mock.call("cd /src; /usr/local/gradle assemble > logs/build/build.gradle.tasks.package.log.tmp 2>&1"),
            mock.call().__bool__(),
        ]

        # should not do anything but still add the binary to the config
        mock_run_subprocess.assert_has_calls(calls)
        # but should still setup the binary path
        self.assertEqual(cfg.opts("builder", "candidate.bin.path"), "elasticsearch.zip")
