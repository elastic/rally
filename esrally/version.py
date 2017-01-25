import pkg_resources

from esrally import paths
from esrally.utils import git, io

__version__ = pkg_resources.require("esrally")[0].version


def version():
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
