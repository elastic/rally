from unittest import TestCase

from esrally import exceptions
from esrally.utils import versions


class VersionsTests(TestCase):
    def test_is_version_identifier(self):
        self.assertFalse(versions.is_version_identifier(None))
        self.assertFalse(versions.is_version_identifier(""))
        self.assertFalse(versions.is_version_identifier("     \t "))
        self.assertFalse(versions.is_version_identifier("5-ab-c"))
        self.assertFalse(versions.is_version_identifier("5.1"))
        self.assertFalse(versions.is_version_identifier("5"))
        self.assertTrue(versions.is_version_identifier("5.0.0"))
        self.assertTrue(versions.is_version_identifier("1.7.3"))
        self.assertTrue(versions.is_version_identifier("20.3.7-SNAPSHOT"))

        self.assertFalse(versions.is_version_identifier(None, strict=False))
        self.assertFalse(versions.is_version_identifier("", strict=False))
        self.assertTrue(versions.is_version_identifier("5.1", strict=False))
        self.assertTrue(versions.is_version_identifier("5", strict=False))
        self.assertTrue(versions.is_version_identifier("23", strict=False))
        self.assertTrue(versions.is_version_identifier("20.3.7-SNAPSHOT", strict=False))

    def test_finds_components_for_valid_version(self):
        self.assertEqual((5, 0, 3, None), versions.components("5.0.3"))
        self.assertEqual((5, 0, 3, "SNAPSHOT"), versions.components("5.0.3-SNAPSHOT"))

        self.assertEqual((25, None, None, None), versions.components("25", strict=False))
        self.assertEqual((5, 1, None, None), versions.components("5.1", strict=False))

    def test_major_version(self):
        self.assertEqual(5, versions.major_version("5.0.3"))
        self.assertEqual(5, versions.major_version("5.0.3-SNAPSHOT"))
        self.assertEqual(25, versions.major_version("25.0.3"))

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

    def test_find_best_match(self):
        self.assertEqual("master", versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "6.0.0-alpha1"),
                         "Assume master for versions newer than latest alternative available")
        self.assertEqual("5", versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "5.1.0-SNAPSHOT"),
                         "Best match for specific version")
        self.assertEqual("master", versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], None),
                         "Assume master on unknown version")
        self.assertIsNone(versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "0.4"), "Reject versions that are too old")

