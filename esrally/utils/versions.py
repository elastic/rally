import re

from esrally import exceptions

VERSIONS = re.compile("^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$")


def is_version_identifier(text):
    return text is not None and VERSIONS.match(text) is not None


def components(version):
    """
    Determines components of a version string.

    :param version: A version string in the format major.minor.path-suffix (suffix is optional)
    :return: A dict with the keys "major", "minor", "patch" and optionally "suffix".
    """
    matches = VERSIONS.match(version)
    if matches:
        version_components = {"major": matches.group(1), "minor": matches.group(2), "patch": matches.group(3)}
        if matches.start(4) > 0:
            version_components["suffix"] = matches.group(4)
        return version_components
    else:
        raise exceptions.InvalidSyntax("version string '%s' does not conform to pattern '%s'" % (version, VERSIONS.pattern))


def versions(version):
    """
    Determines possible variations of a version.

    E.g. if version "5.0.0-SNAPSHOT" is given, the possible variations are:

    ["5.0.0-SNAPSHOT", "5.0.0", "5.0", "5"]

    :param version: A version string in the format major.minor.path-suffix (suffix is optional)
    :return: a list of version variations ordered from most specific to most generic variation.
    """
    v = []
    c = components(version)

    if "suffix" in c:
        v.append("%s.%s.%s-%s" % (c["major"], c["minor"], c["patch"], c["suffix"]))
    v.append("%s.%s.%s" % (c["major"], c["minor"], c["patch"]))
    v.append("%s.%s" % (c["major"], c["minor"]))
    v.append("%s" % c["major"])
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
    if distribution_version and len(distribution_version.strip()) > 0:
        for version in versions(distribution_version):
            if version in available_alternatives:
                return version
        return None
    else:
        return "master"
