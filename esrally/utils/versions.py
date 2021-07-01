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

import functools
import re

from esrally import exceptions

VERSIONS = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$")

VERSIONS_OPTIONAL = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:-(.+))?$")


def _versions_pattern(strict):
    return VERSIONS if strict else VERSIONS_OPTIONAL


def is_version_identifier(text, strict=True):
    return text is not None and _versions_pattern(strict).match(text) is not None


def major_version(version):
    """
    Determines the major version of a given version string.

    :param version: A version string in the format major.minor.path-suffix (suffix is optional)
    :return: The major version (as int). In case the version string is invalid, an ``exceptions.InvalidSyntax`` is raised.
    """
    major, _, _, _ = components(version)
    return major


def components(version, strict=True):
    """
    Determines components of a version string.

    :param version: A version string in the format major.minor.path-suffix (suffix is optional)
    :param strict: Determines whether versions need to have at least "major", "minor" and "patch" defined. Default: True
    :return: A tuple with four components determining "major", "minor", "patch" and "suffix" (any part except "major" may be `None`)
    """
    versions_pattern = _versions_pattern(strict)
    matches = versions_pattern.match(version)
    if matches:
        if matches.start(4) > 0:
            return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), matches.group(4)
        elif matches.start(3) > 0:
            return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), None
        elif matches.start(2) > 0:
            return int(matches.group(1)), int(matches.group(2)), None, None
        elif matches.start(1) > 0:
            return int(matches.group(1)), None, None, None
        else:
            return int(version), None, None, None
    raise exceptions.InvalidSyntax("version string '%s' does not conform to pattern '%s'" % (version, versions_pattern.pattern))


@functools.total_ordering
class Version:
    """
    Represents a version with components major, minor, patch and suffix (suffix is optional). Suffixes are not
    considered for version comparisons as its contents are opaque and a semantically correct order cannot be defined.
    """

    def __init__(self, major, minor, patch, suffix=None):
        self.major = major
        self.minor = minor
        self.patch = patch
        self.suffix = suffix

    def __eq__(self, o: object) -> bool:
        return isinstance(o, type(self)) and (self.major, self.minor, self.patch) == (o.major, o.minor, o.patch)

    def __lt__(self, o: object) -> bool:
        return isinstance(o, type(self)) and (self.major, self.minor, self.patch) < (o.major, o.minor, o.patch)

    def __hash__(self) -> int:
        return hash(self.major) ^ hash(self.minor) ^ hash(self.patch) ^ hash(self.suffix)

    def __repr__(self) -> str:
        v = f"{self.major}.{self.minor}.{self.patch}"
        return f"{v}-{self.suffix}" if self.suffix else v

    @classmethod
    def from_string(cls, v):
        return cls(*components(v))


def variants_of(version):
    for v, _ in VersionVariants(version).all_versions:
        yield v


class VersionVariants:
    """
    Build all possible variations of a version.

    e.g. if version "5.0.0-SNAPSHOT" is given, the possible variations are:
        self.with_suffix: "5.0.0-SNAPSHOT",
        self.with_patch: "5.0.0",
        self.with_minor: "5.0",
        self.with_major: "5"
    """

    def __init__(self, version):
        """
        :param version: A version string in the format major.minor.path-suffix (suffix is optional)
        """
        self.major, self.minor, self.patch, self.suffix = components(version)

        self.with_major = f"{int(self.major)}"
        self.with_minor = f"{int(self.major)}.{int(self.minor)}"
        self.with_patch = f"{int(self.major)}.{int(self.minor)}.{int(self.patch)}"
        self.with_suffix = f"{int(self.major)}.{int(self.minor)}.{int(self.patch)}-{self.suffix}" if self.suffix else None

    @property
    def all_versions(self):
        """
        :return: a list of tuples containing version variants and version type
            ordered from most specific to most generic variation.

            Example:
            [("5.0.0-SNAPSHOT", "with_suffix"), ("5.0.0", "with_patch"), ("5.0", "with_minor"), ("5", "with_major")]
        """

        versions = [(self.with_suffix, "with_suffix")] if self.suffix else []
        versions.extend(
            [
                (self.with_patch, "with_patch"),
                (self.with_minor, "with_minor"),
                (self.with_major, "with_major"),
            ]
        )

        return versions


def best_match(available_alternatives, distribution_version):
    """
    Finds the most specific branch for a given distribution version assuming that versions have the pattern:

        major.minor.patch-suffix

    and the provided alternatives reflect this pattern.
    Best matches for distribution_version from available_alternatives may be:
     1. exact matches of major.minor
     2. nearest prior minor within the same major
     3. major version
     4. as a last resort, `master`.

    See test_find_best_match() for examples.

    :param available_alternatives: A list of possible distribution versions (or shortened versions).
    :param distribution_version: An Elasticsearch distribution version.
    :return: The most specific alternative that is available or None.
    """
    if is_version_identifier(distribution_version):
        versions = VersionVariants(distribution_version)
        for version, version_type in versions.all_versions:
            if version in available_alternatives:
                return version
            # match nearest prior minor
            if version_type == "with_minor" and (latest_minor := latest_bounded_minor(available_alternatives, versions)):
                if latest_minor:
                    return f"{versions.major}.{latest_minor}"
        # not found in the available alternatives, it could still be a master version
        major, _, _, _ = components(distribution_version)
        if major > _latest_major(available_alternatives):
            return "master"
    elif not distribution_version:
        return "master"
    return None


def _latest_major(alternatives):
    max_major = -1
    for a in alternatives:
        if is_version_identifier(a, strict=False):
            major, _, _, _ = components(a, strict=False)
            max_major = max(major, max_major)
    return max_major


def latest_bounded_minor(alternatives, target_version):
    """
    Finds the closest minor version that is smaller or eq to target_version from a list of version alternatives.
    Versions including patch or patch-suffix in alternatives are ignored.
    See test_latest_bounded_minor() for examples.

    :param alternatives: list of alternative versions
    :param target_version: a VersionVariants object presenting the distribution version
    :return: the closest minor version (if available) from alternatives otherwise None
    """
    eligible_minors = []
    for a in alternatives:
        if is_version_identifier(a, strict=False):
            major, minor, patch, suffix = components(a, strict=False)
            if patch is not None or suffix is not None:
                # branches containing patch or patch-suffix aren't supported
                continue
            if major == target_version.major and minor and minor <= target_version.minor:
                eligible_minors.append(minor)

    # no matching minor version
    if not eligible_minors:
        return None

    eligible_minors.sort()

    return min(eligible_minors, key=lambda x: abs(x - target_version.minor))
