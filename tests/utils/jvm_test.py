from unittest import TestCase
from esrally.utils import jvm


class JvmTests(TestCase):
    def test_extract_major_version_7(self):
        self.assertEqual(7, jvm.major_version("1.7", lambda x, y: x))

    def test_extract_major_version_8(self):
        self.assertEqual(8, jvm.major_version("1.8", lambda x, y: x))

    def test_extract_major_version_9(self):
        self.assertEqual(9, jvm.major_version("9", lambda x, y: x))

    def test_extract_major_version_10(self):
        self.assertEqual(10, jvm.major_version("10", lambda x, y: x))

    def test_ea_release(self):
        self.assertTrue(jvm.is_early_access_release("Oracle Corporation,9-ea", self.prop_version_reader))

    def test_ga_release(self):
        self.assertFalse(jvm.is_early_access_release("Oracle Corporation,9", self.prop_version_reader))

    def prop_version_reader(self, java_home, prop):
        props = java_home.split(",")
        return props[1] if prop == "java.version" else props[0]