from unittest import TestCase

from esrally import exceptions
from esrally.utils import versions


class VersionsTests(TestCase):
    def test_is_version_identifier(self):
        self.assertFalse(versions.is_version_identifier(None))
        self.assertFalse(versions.is_version_identifier(""))
        self.assertFalse(versions.is_version_identifier("     \t "))
        self.assertFalse(versions.is_version_identifier("5-ab-c"))
        self.assertTrue(versions.is_version_identifier("5.0.0"))
        self.assertTrue(versions.is_version_identifier("1.7.3"))
        self.assertTrue(versions.is_version_identifier("20.3.7-SNAPSHOT"))

    def test_finds_components_for_valid_version(self):
        self.assertEqual({"major": "5", "minor": "0", "patch": "3"}, versions.components("5.0.3"))
        self.assertEqual({"major": "5", "minor": "0", "patch": "3", "suffix": "SNAPSHOT"}, versions.components("5.0.3-SNAPSHOT"))

    def test_components_ignores_invalid_versions(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            versions.components("5.0.0a")
        self.assertEqual("version string '5.0.0a' does not conform to pattern '^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$'", ctx.exception.args[0])

    def test_versions_parses_correct_version_string(self):
        self.assertEqual(["5.0.3", "5.0", "5"], versions.versions("5.0.3"))
        self.assertEqual(["5.0.0-SNAPSHOT", "5.0.0", "5.0", "5"], versions.versions("5.0.0-SNAPSHOT"))
        self.assertEqual(["10.3.63", "10.3", "10"], versions.versions("10.3.63"))

    def test_versions_rejects_invalid_version_strings(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            versions.versions("5.0.0a-SNAPSHOT")
        self.assertEqual("version string '5.0.0a-SNAPSHOT' does not conform to pattern '^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$'"
                         , ctx.exception.args[0])

