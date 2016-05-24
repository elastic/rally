import os
from unittest import TestCase

from esrally.utils import io


def mock_debian(args, fallback=None):
    if args[0] == "update-alternatives":
        return [
            "/usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java",
            "/usr/lib/jvm/java-7-oracle/jre/bin/java",
            "/usr/lib/jvm/java-8-oracle/jre/bin/java"
        ]
    else:
        return None


def mock_red_hat(path):
    if path == "/etc/alternatives/java_sdk_1.8.0":
        return "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64"
    else:
        return None


class IoTests(TestCase):
    def test_guess_java_home_on_mac_os_x(self):
        java_home = io.guess_java_home(major_version=8, runner=lambda args, fallback=None:
        ["/Library/Java/JavaVirtualMachines/jdk1.8.0_74.jdk/Contents/Home"])

        self.assertEqual("/Library/Java/JavaVirtualMachines/jdk1.8.0_74.jdk/Contents/Home", java_home)

    def test_guess_java_home_on_debian(self):
        self.assertEqual("/usr/lib/jvm/java-8-oracle", io.guess_java_home(major_version=8, runner=mock_debian))
        self.assertEqual("/usr/lib/jvm/java-7-openjdk-amd64", io.guess_java_home(major_version=7, runner=mock_debian))

    def test_guess_java_home_on_redhat(self):
        self.assertEqual("/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64",
                         io.guess_java_home(major_version=8, runner=lambda args, fallback=None: None, read_symlink=mock_red_hat))
        # simulate not installed version
        self.assertIsNone(io.guess_java_home(major_version=7, runner=lambda args, fallback=None: None, read_symlink=mock_red_hat))

    def test_is_git_working_copy(self):
        test_dir = os.path.dirname(os.path.dirname(__file__))

        self.assertFalse(io.is_git_working_copy(test_dir, "elastic/rally.git"))
        self.assertTrue(io.is_git_working_copy(os.path.dirname(test_dir), "elastic/rally.git"))
