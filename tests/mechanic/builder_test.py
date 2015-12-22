from unittest import TestCase
import unittest.mock as mock

import rally.config
import rally.mechanic.builder


class BuilderTests(TestCase):
  @mock.patch('rally.utils.process.run_subprocess')
  @mock.patch('logging.Logger')
  @mock.patch('glob.glob', lambda p: ['elasticsearch.zip'])
  def test_skip_build(self, mock_run_subprocess, mock_logger):
    config = rally.config.Config()
    config.add(rally.config.Scope.globalScope, "build", "skip", True)
    config.add(rally.config.Scope.globalScope, "source", "local.src.dir", "/src")

    builder = rally.mechanic.builder.Builder(config, mock_logger)
    builder.build()

    # should not do anything but still add the binary to the config
    mock_run_subprocess.assert_not_called()
    # but should still setup the binary path
    self.assertEqual(config.opts("builder", "candidate.bin.path"), "elasticsearch.zip")

  @mock.patch('glob.glob', lambda p: ['elasticsearch.zip'])
  @mock.patch('rally.utils.io.ensure_dir')
  @mock.patch('os.rename')
  @mock.patch('logging.Logger')
  @mock.patch('rally.utils.process.run_subprocess')
  def test_build(self, mock_run_subprocess, mock_logger, mock_rename, mock_ensure_dir):
    config = rally.config.Config()
    config.add(rally.config.Scope.globalScope, "build", "skip", False)
    config.add(rally.config.Scope.globalScope, "source", "local.src.dir", "/src")
    config.add(rally.config.Scope.globalScope, "build", "gradle.bin", "/usr/local/gradle")
    config.add(rally.config.Scope.globalScope, "build", "gradle.tasks.clean", "clean")
    config.add(rally.config.Scope.globalScope, "build", "gradle.tasks.package", "assemble")
    config.add(rally.config.Scope.globalScope, "system", "log.dir", "logs")
    config.add(rally.config.Scope.globalScope, "build", "log.dir", "build")

    builder = rally.mechanic.builder.Builder(config, mock_logger)
    builder.build()

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
    self.assertEqual(config.opts("builder", "candidate.bin.path"), "elasticsearch.zip")
