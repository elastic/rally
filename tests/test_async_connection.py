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

import json
from unittest import TestCase

from esrally.async_connection import ResponseMatcher


class ResponseMatcherTests(TestCase):
    def test_matches(self):
        matcher = ResponseMatcher(responses=[
            {
                "path": "*/_bulk",
                "body": {
                    "response-type": "bulk",
                }
            },
            {
                "path": "/_cluster/health*",
                "body": {
                    "response-type": "cluster-health",
                }
            },
            {
                "path": "*",
                "body": {
                    "response-type": "default"
                }
            }
        ])

        self.assert_response_type(matcher, "/_cluster/health", "cluster-health")
        self.assert_response_type(matcher, "/_cluster/health/geonames", "cluster-health")
        self.assert_response_type(matcher, "/geonames/_bulk", "bulk")
        self.assert_response_type(matcher, "/geonames", "default")
        self.assert_response_type(matcher, "/geonames/force_merge", "default")

    def assert_response_type(self, matcher, path, expected_response_type):
        response = json.loads(matcher.response(path))
        self.assertEqual(response["response-type"], expected_response_type)
