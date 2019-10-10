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
