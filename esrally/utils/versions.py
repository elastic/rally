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


def versions(version):
    """
    Determines possible variations of a version.

    E.g. if version "5.0.0-SNAPSHOT" is given, the possible variations are:

    ["5.0.0-SNAPSHOT", "5.0.0", "5.0", "5"]

    :param version: A version string in the format major.minor.path-suffix (suffix is optional)
    :return: a list of version variations ordered from most specific to most generic variation.
    """
    v = []
    major, minor, patch, suffix = components(version)

    if suffix:
        v.append("%d.%d.%d-%s" % (major, minor, patch, suffix))
    v.append("%d.%d.%d" % (major, minor, patch))
    v.append("%d.%d" % (major, minor))
    v.append("%d" % major)
    return v


def best_match(available_alternatives, distribution_version):
    """

    Finds the most specific branch for a given distribution version assuming that versions have the pattern:

        major.minor.patch-suffix

    and the provided alternatives reflect this pattern.

    :param available_alternatives: A list of possible distribution versions (or shortened versions).
    :param distribution_version: An Elasticsearch distribution version.
    :return: The most specific alternative that is available (most components of the version match) or None.
    """
    if is_version_identifier(distribution_version):
        for version in versions(distribution_version):
            if version in available_alternatives:
                return version
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
