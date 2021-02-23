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

import io
import json
from unittest import TestCase
from esrally.driver import parsing

class SelectiveJsonParserTests(TestCase):
    def doc_as_text(self, doc):
        return io.StringIO(json.dumps(doc))

    def test_parse_all_expected(self):
        doc = self.doc_as_text({
            "title": "Hello",
            "meta": {
                "length": 100,
                "date": {
                    "year": 2000
                }
            }
        })

        parsed = parsing.parse(doc, [
            # simple property
            "title",
            # a nested property
            "meta.date.year",
            # ignores unknown properties
            "meta.date.month"
        ])

        self.assertEqual("Hello", parsed.get("title"))
        self.assertEqual(2000, parsed.get("meta.date.year"))
        self.assertNotIn("meta.date.month", parsed)

    def test_list_length(self):
        doc = self.doc_as_text({
            "title": "Hello",
            "meta": {
                "length": 100,
                "date": {
                    "year": 2000
                }
            },
            "authors": ["George", "Harry"],
            "readers": [
                {
                    "name": "Tom",
                    "age": 14
                },
                {
                    "name": "Bob",
                    "age": 17
                },
                {
                    "name": "Alice",
                    "age": 22
                }
            ],
            "supporters": []
        })

        parsed = parsing.parse(doc, [
            # simple property
            "title",
            # a nested property
            "meta.date.year",
            # ignores unknown properties
            "meta.date.month"
        ], ["authors", "readers", "supporters"])

        self.assertEqual("Hello", parsed.get("title"))
        self.assertEqual(2000, parsed.get("meta.date.year"))
        self.assertNotIn("meta.date.month", parsed)

        # lists
        self.assertFalse(parsed.get("authors"))
        self.assertFalse(parsed.get("readers"))
        self.assertTrue(parsed.get("supporters"))
