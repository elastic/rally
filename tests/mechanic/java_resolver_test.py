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

import unittest.mock as mock
from unittest import TestCase

from esrally import config, exceptions
from esrally.mechanic import java_resolver


class JavaResolverTests(TestCase):
    @mock.patch("esrally.utils.jvm.resolve_path")
    def test_resolves_java_home_for_default_runtime_jdk(self, resolve_jvm_path):
        resolve_jvm_path.return_value = (12, "/opt/jdk12")

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "runtime.jdk", None)

        major, java_home = java_resolver.java_home("12,11,10,9,8", cfg.opts("mechanic", "runtime.jdk"), True)

        self.assertEqual(major, 12)
        self.assertEqual(java_home, "/opt/jdk12")

    @mock.patch("esrally.utils.jvm.resolve_path")
    def test_resolves_java_home_for_specific_runtime_jdk(self, resolve_jvm_path):
        resolve_jvm_path.return_value = (8, "/opt/jdk8")

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "runtime.jdk", 8)

        major, java_home = java_resolver.java_home("12,11,10,9,8", cfg.opts("mechanic", "runtime.jdk"), True)

        self.assertEqual(major, 8)
        self.assertEqual(java_home, "/opt/jdk8")
        resolve_jvm_path.assert_called_with([8])

    def test_resolves_java_home_for_bundled_jdk(self):

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "runtime.jdk", "bundled")

        major, java_home = java_resolver.java_home("12,11,10,9,8", cfg.opts("mechanic", "runtime.jdk"), True)

        # assumes most recent JDK
        self.assertEqual(major, 12)
        # does not set JAVA_HOME for the bundled JDK
        self.assertEqual(java_home, None)

    def test_disallowed_bundled_jdk(self):

        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "runtime.jdk", "bundled")
        cfg.add(config.Scope.application, "mechanic", "car.names", ["default"])

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            java_resolver.java_home("12,11,10,9,8", cfg.opts("mechanic", "runtime.jdk"))
        self.assertEqual("This Elasticsearch version does not contain a bundled JDK. Please specify a different runtime JDK.", ctx.exception.args[0])
