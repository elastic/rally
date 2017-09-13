import re

from esrally.utils import process


def supports_option(java_home, option):
    """
    Detects support for a specific option (or combination of options) for the JVM version available in java_home.
    
    :param java_home: The JAVA_HOME to use for probing.
    :param option: The JVM option or combination of JVM options (separated by spaces) to check.
    :return: True iff the provided ``option`` is supported on this JVM.
    """
    return process.exit_status_as_bool(lambda: process.run_subprocess_with_logging("%s/bin/java %s -version" % (java_home, option)))


def system_property(java_home, system_property_name):
    lines = process.run_subprocess_with_output("%s/bin/java -XshowSettings:properties -version" % java_home)
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