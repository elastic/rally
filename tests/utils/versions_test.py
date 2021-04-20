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
import re

import pytest  # type: ignore

from esrally import exceptions
from esrally.utils import versions


class TestsVersions:
    def test_is_version_identifier(self):
        assert versions.is_version_identifier(None) is False
        assert versions.is_version_identifier("") is False
        assert versions.is_version_identifier("     \t ") is False
        assert versions.is_version_identifier("5-ab-c") is False
        assert versions.is_version_identifier("5.1") is False
        assert versions.is_version_identifier("5") is False

        assert versions.is_version_identifier("5.0.0")
        assert versions.is_version_identifier("1.7.3")
        assert versions.is_version_identifier("20.3.7-SNAPSHOT")

        assert versions.is_version_identifier(None, strict=False) is False
        assert versions.is_version_identifier("", strict=False) is False
        assert versions.is_version_identifier("5.1", strict=False)
        assert versions.is_version_identifier("5", strict=False)
        assert versions.is_version_identifier("23", strict=False)
        assert versions.is_version_identifier("20.3.7-SNAPSHOT", strict=False)

    def test_finds_components_for_valid_version(self):
        assert versions.components("5.0.3") == (5, 0, 3, None)
        assert versions.components("7.12.1-SNAPSHOT") == (7, 12, 1, "SNAPSHOT")

        assert versions.components("25", strict=False) == (25, None, None, None)
        assert versions.components("5.1", strict=False) == (5, 1, None, None)

    def test_major_version(self):
        assert versions.major_version("7.10.2") == 7
        assert versions.major_version("7.12.1-SNAPSHOT") == 7
        assert versions.major_version("25.0.3") == 25

    @pytest.mark.parametrize("seed", range(40))
    def test_latest_bounded_minor(self, seed):
        _alternatives = ["7", "7.10", "7.11.2", "7.2", "5", "6", "master"]
        random.seed(seed)
        alternatives = _alternatives.copy()
        random.shuffle(alternatives)

        assert versions.latest_bounded_minor(alternatives, versions.VersionVariants("7.6.3")) == 2
        assert versions.latest_bounded_minor(alternatives, versions.VersionVariants("7.12.3")) == 10,\
            "Nearest alternative with major.minor, skip alternatives with major.minor.patch"
        assert versions.latest_bounded_minor(alternatives, versions.VersionVariants("7.11.2")) == 10,\
            "Skips all alternatives with major.minor.patch, even if exact match"
        assert versions.latest_bounded_minor(alternatives, versions.VersionVariants("7.1.0")) is None,\
            "No matching alternative with minor version"

    def test_components_ignores_invalid_versions(self):
        with pytest.raises(
                exceptions.InvalidSyntax,
                match=re.escape(
                    r"version string '5.0.0a' does not conform to pattern "
                    r"'^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$'")):
            versions.components("5.0.0a")

    def test_versionvariants_parses_correct_version_string(self):
        assert versions.VersionVariants("5.0.3").all_versions == [
            ("5.0.3", "with_patch"),
            ("5.0", "with_minor"),
            ("5", "with_major")]
        assert versions.VersionVariants("7.12.1-SNAPSHOT").all_versions == [
            ("7.12.1-SNAPSHOT", "with_suffix"),
             ("7.12.1", "with_patch"),
             ("7.12", "with_minor"),
             ("7", "with_major")]
        assert versions.VersionVariants("10.3.63").all_versions == [
            ("10.3.63", "with_patch"),
            ("10.3", "with_minor"),
            ("10", "with_major")]

    def test_versions_rejects_invalid_version_strings(self):
        with pytest.raises(
                exceptions.InvalidSyntax,
                match=re.escape(r"version string '5.0.0a-SNAPSHOT' does not conform to pattern "
                                r"'^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$'")
        ):
            versions.VersionVariants("5.0.0a-SNAPSHOT")

    def test_find_best_match(self):
        assert versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "6.0.0-alpha1") == "master",\
            "Assume master for versions newer than latest alternative available"

        assert versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "5.1.0-SNAPSHOT") == "5",\
            "Best match for specific version"

        assert versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], None) == "master",\
            "Assume master on unknown version"

        assert versions.best_match(["1.7", "2", "5.0.0-alpha1", "5", "master"], "0.4") is None,\
            "Reject versions that are too old"

        assert versions.best_match(["7", "7.10.2", "7.11", "7.2", "5", "6", "master"], "7.10.2") == "7.10.2", \
            "Exact match"

        assert versions.best_match(["7", "7.10", "master"], "7.1.0") == "7", \
            "Best match is major version"

        assert versions.best_match(["7", "7.11", "7.2", "5", "6", "master"], "7.11.0") == "7.11",\
            "Best match for specific minor version"

        assert versions.best_match(["7", "7.11", "7.2", "5", "6", "master"], "7.12.0") == "7.11",\
            "If no exact match, best match is the nearest prior minor"

        assert versions.best_match(["7", "7.11", "7.2", "5", "6", "master"], "7.3.0") == "7.2",\
            "If no exact match, best match is the nearest prior minor"

        assert versions.best_match(["7", "7.11", "7.2", "5", "6", "master"], "7.10.0") == "7.2", \
            "If no exact match, best match is the nearest prior minor"

        assert versions.best_match(["7", "7.1", "7.11.1", "7.11.0", "7.2", "5", "6", "master"], "7.12.0") == "7.2",\
            "Patch or patch-suffix branches are not supported and ignored, best match is nearest prior minor"

        assert versions.best_match(["7", "7.11", "7.2", "5", "6", "master"], "7.1.0") == "7",\
            "If no exact match and no minor match, next best match is major version"

    def test_version_comparison(self):
        assert versions.Version.from_string("7.10.2") < versions.Version.from_string("7.11.0")
        assert versions.Version.from_string("7.10.2") == versions.Version.from_string("7.10.2")
