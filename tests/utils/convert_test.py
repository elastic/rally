from unittest import TestCase

from esrally.utils import convert


class ToBoolTests(TestCase):
    def test_convert_to_true(self):
        values = ["True", "true", "Yes", "yes", "t", "y", "1", True]
        for value in values:
            self.assertTrue(convert.to_bool(value), msg="Expect [%s] of type [%s] to be converted to True." % (str(value), type(value)))

    def test_convert_to_false(self):
        values = ["False", "false", "No", "no", "f", "n", "0", False]
        for value in values:
            self.assertFalse(convert.to_bool(value), msg="Expect [%s] of type [%s] to be converted to False." % (str(value), type(value)))

    def test_cannot_convert_invalid_value(self):
        values = ["Invalid", None, []]
        for value in values:
            with self.assertRaises(ValueError, msg="Expect [%s] of type [%s] to fail to be converted." % (str(value), type(value))) as ctx:
                convert.to_bool(value)
            self.assertEqual("Cannot convert [%s] to bool." % value, ctx.exception.args[0])
