import os
import pkg_resources

__version__ = pkg_resources.require("esrally")[0].version

# Allow an alternative program name be set in case Rally is invoked a wrapper script
PROGRAM_NAME = os.getenv("RALLY_ALTERNATIVE_BINARY_NAME", "esrally")


if __version__.endswith("dev0"):
    DOC_LINK = "https://esrally.readthedocs.io/en/latest/"
else:
    DOC_LINK = "https://esrally.readthedocs.io/en/%s/" % __version__
