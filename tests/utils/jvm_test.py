# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest import mock

import pytest

from esrally import exceptions
from esrally.utils import jvm


class TestJvm:
    def test_extract_major_version_7(self):
        assert jvm.major_version("1.7", lambda x, y: x) == 7

    def test_extract_major_version_8(self):
        assert jvm.major_version("1.8", lambda x, y: x) == 8

    def test_extract_major_version_9(self):
        assert jvm.major_version("9", lambda x, y: x) == 9

    def test_extract_major_version_10(self):
        assert jvm.major_version("10", lambda x, y: x) == 10

    def test_ea_release(self):
        assert jvm.is_early_access_release("Oracle Corporation,9-ea", self.prop_version_reader)

    def test_ga_release(self):
        assert not jvm.is_early_access_release("Oracle Corporation,9", self.prop_version_reader)

    def prop_version_reader(self, java_home, prop):
        props = java_home.split(",")
        return props[1] if prop == "java.version" else props[0]

    def path_based_prop_version_reader(self, java_home, prop):
        props = java_home.split("/")
        # assumes a path that contains the major version as last component
        return props[-1] if prop == "java.vm.specification.version" else None

    @mock.patch("os.getenv")
    def test_resolve_path_for_one_version_via_java_home(self, getenv):
        # JAVA8_HOME, JAVA_HOME
        getenv.side_effect = [None, "/opt/jdks/jdk/1.8"]

        major, resolved_path = jvm.resolve_path(majors=8, sysprop_reader=self.path_based_prop_version_reader)
        assert major == 8
        assert resolved_path == "/opt/jdks/jdk/1.8"

    @mock.patch("os.getenv")
    def test_resolve_path_for_one_version_via_java_x_home(self, getenv):
        # JAVA8_HOME, JAVA_HOME
        getenv.side_effect = ["/opt/jdks/jdk/1.8", None]

        major, resolved_path = jvm.resolve_path(majors=8, sysprop_reader=self.path_based_prop_version_reader)
        assert major == 8
        assert resolved_path == "/opt/jdks/jdk/1.8"

    @mock.patch("os.getenv")
    def test_resolve_path_for_one_version_no_matching_version(self, getenv):
        # JAVA8_HOME, JAVA_HOME
        getenv.side_effect = [None, "/opt/jdks/jdk/1.7"]

        with pytest.raises(exceptions.SystemSetupError, match="JAVA_HOME points to JDK 7 but it should point to JDK 8."):
            jvm.resolve_path(majors=8, sysprop_reader=self.path_based_prop_version_reader)

    @mock.patch("os.getenv")
    def test_resolve_path_for_one_version_no_env_vars_defined(self, getenv):
        getenv.return_value = None

        with pytest.raises(exceptions.SystemSetupError, match="Neither JAVA8_HOME nor JAVA_HOME point to a JDK 8 installation."):
            jvm.resolve_path(majors=8, sysprop_reader=self.path_based_prop_version_reader)

    @mock.patch("os.getenv")
    def test_resolve_path_for_multiple_versions(self, getenv):
        getenv.side_effect = [
            # JAVA_HOME
            None,
            # JAVA11_HOME
            None,
            # JAVA_HOME
            None,
            # JAVA10_HOME,
            None,
            # JAVA_HOME
            None,
            # JAVA9_HOME
            "/opt/jdks/jdk/9",
            # JAVA_HOME
            None,
            # JAVA8_HOME
            "/opt/jdks/jdk/1.8",
        ]
        major, resolved_path = jvm.resolve_path(majors=[11, 10, 9, 8], sysprop_reader=self.path_based_prop_version_reader)
        assert major == 9
        assert resolved_path == "/opt/jdks/jdk/9"
