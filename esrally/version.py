import pkg_resources
import re

from esrally import paths
from esrally.utils import git, io

__version__ = pkg_resources.require("esrally")[0].version

__RALLY_VERSION_PATTERN = re.compile("^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:.(.+))?$")


def version():
    """
    :return: The release version string and an optional suffix for the current git revision if Rally is installed in development mode.
    """
    release = __version__
    # noinspection PyBroadException
    try:
        if git.is_working_copy(io.normalize_path("%s/.." % paths.rally_root())):
            revision = git.head_revision(paths.rally_root())
            return "%s (git revision: %s)" % (release, revision.strip())
    except BaseException:
        pass
    # cannot determine head revision so user has probably installed Rally via pip instead of git clone
    return release


def release_version():
    """
    :return: The release version string split into its components: major, minor, patch and optional suffix.
    """

    matches = __RALLY_VERSION_PATTERN.match(__version__)
    if matches.start(4) > 0:
        return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), matches.group(4)
    elif matches.start(3) > 0:
        return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), None
