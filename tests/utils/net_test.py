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

import random
import unittest.mock as mock
from unittest import TestCase

from esrally.utils import net


class NetTests(TestCase):
    # Mocking boto3 objects directly is too complex so we keep all code in a helper function and mock this instead
    @mock.patch("esrally.utils.net._download_from_s3_bucket")
    def test_download_from_s3_bucket(self, download):
        expected_size = random.choice([None, random.randint(0, 1000)])
        progress_indicator = random.choice([None, "some progress indicator"])

        net.download_s3("s3://mybucket.elasticsearch.org/data/documents.json.bz2", "/tmp/documents.json.bz2",
                        expected_size, progress_indicator)
        download.assert_called_once_with("mybucket.elasticsearch.org", "data/documents.json.bz2",
                                         "/tmp/documents.json.bz2", expected_size, progress_indicator)
