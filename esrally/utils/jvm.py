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

import os
import re

from esrally import exceptions
from esrally.utils import io, process


def _java(java_home):
    return io.escape_path(os.path.join(java_home, "bin", "java"))


def supports_option(java_home, option):
    """
    Detects support for a specific option (or combination of options) for the JVM version available in java_home.

    :param java_home: The JAVA_HOME to use for probing.
    :param option: The JVM option or combination of JVM options (separated by spaces) to check.
    :return: True iff the provided ``option`` is supported on this JVM.
    """
    return process.exit_status_as_bool(
        lambda: process.run_subprocess_with_logging("{} {} -version".format(_java(java_home), option)))


def system_property(java_home, system_property_name):
    lines = process.run_subprocess_with_output("{} -XshowSettings:properties -version".format(_java(java_home)))
    # matches e.g. "    java.runtime.version = 1.8.0_121-b13" and captures "1.8.0_121-b13"
    sys_prop_pattern = re.compile(r".*%s.*=\s?(.*)" % system_property_name)
    for line in lines:
        m = sys_prop_pattern.match(line)
        if m:
            return m.group(1)

    return None


def version(java_home, sysprop_reader=system_property):
    """
    Determines the version number of JVM available at the provided JAVA_HOME directory.

    :param java_home: The JAVA_HOME directory to check.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: The version number of the JVM available at ``java_home``.
    """
    return sysprop_reader(java_home, "java.version")


def vendor(java_home, sysprop_reader=system_property):
    """
    Determines the version number of JVM available at the provided JAVA_HOME directory.

    :param java_home: The JAVA_HOME directory to check.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: The version number of the JVM available at ``java_home``.
    """
    return sysprop_reader(java_home, "java.vm.specification.vendor")


def major_version(java_home, sysprop_reader=system_property):
    """
    Determines the major version number of JVM available at the provided JAVA_HOME directory.

    :param java_home: The JAVA_HOME directory to check.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: An int, representing the major version number of the JVM available at ``java_home``.
    """
    v = sysprop_reader(java_home, "java.vm.specification.version")
    # are we under the "old" (pre Java 9) or the new (Java 9+) version scheme?
    if v.startswith("1."):
        return int(v[2])
    else:
        return int(v)


def is_early_access_release(java_home, sysprop_reader=system_property):
    """
    Determines whether the JVM available at the provided JAVA_HOME directory is an early access release. It mimicks the corresponding
    bootstrap check in Elasticsearch itself.

    :param java_home: The JAVA_HOME directory to check.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: True iff the JVM available at ``java_home`` is classified as an early access release.
    """
    return vendor(java_home, sysprop_reader) == "Oracle Corporation" and version(java_home, sysprop_reader).endswith("-ea")


def resolve_path(majors, sysprop_reader=system_property):
    """
    Resolves the path to the JDK with the provided major version(s). It checks the versions in the same order specified in ``majors``
    and will return the first match. To achieve this, it first checks the major version x in the environment variable ``JAVAx_HOME``
    and falls back to ``JAVA_HOME``. It also ensures that the environment variable points to the right JDK version.

    If no appropriate version is found, a ``SystemSetupError`` is raised.

    :param majors: Either a list of major versions to check or a single version as an ``int``.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: A tuple of (major version, path to Java home directory).
    """
    if isinstance(majors, int):
        return majors, _resolve_single_path(majors, sysprop_reader=sysprop_reader)
    else:
        for major in majors:
            java_home = _resolve_single_path(major, mandatory=False, sysprop_reader=sysprop_reader)
            if java_home:
                return major, java_home
        raise exceptions.SystemSetupError("Install a JDK with one of the versions {} and point to it with one of {}."
                                          .format(majors, _checked_env_vars(majors)))


def _resolve_single_path(major, mandatory=True, sysprop_reader=system_property):
    """

    Resolves the path to a JDK with the provided major version.

    :param major: The major version to check.
    :param mandatory: Determines if we expect to find a matching JDK.
    :param sysprop_reader: (Optional) only relevant for testing.
    :return: The resolved path to the JDK or ``None`` if ``mandatory`` is ``False`` and no appropriate JDK has been found.
    """
    def do_resolve(env_var, major):
        java_v_home = os.getenv(env_var)
        if java_v_home:
            actual_major = major_version(java_v_home, sysprop_reader)
            if actual_major == major:
                return java_v_home
            elif mandatory:
                raise exceptions.SystemSetupError("{} points to JDK {} but it should point to JDK {}.".format(env_var, actual_major, major))
            else:
                return None
        else:
            return None

    # this has to be consistent with _checked_env_vars()
    specific_env_var = "JAVA{}_HOME".format(major)
    generic_env_var = "JAVA_HOME"
    java_home = do_resolve(specific_env_var, major)
    if java_home:
        return java_home
    else:
        java_home = do_resolve(generic_env_var, major)
        if java_home:
            return java_home
        elif mandatory:
            raise exceptions.SystemSetupError("Neither {} nor {} point to a JDK {} installation.".
                                              format(specific_env_var, generic_env_var, major))
        else:
            return None


def _checked_env_vars(majors):
    """
    Provides a list of environment variables that are checked for the given list of major versions.

    :param majors: A list of major versions.
    :return: A list of checked environment variables.
    """
    checked = ["JAVA{}_HOME".format(major) for major in majors]
    checked.append("JAVA_HOME")
    return checked
