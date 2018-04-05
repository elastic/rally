class RallyError(Exception):
    """
    Base class for all Rally exceptions
    """

    def __init__(self, message, cause=None):
        super().__init__(message, cause)
        self.message = message
        self.cause = cause

    def __repr__(self):
        return self.message


class LaunchError(RallyError):
    """
    Thrown whenever there was a problem launching the benchmark candidate
    """


class SystemSetupError(RallyError):
    """
    Thrown when a user did something wrong, e.g. the metrics store is not started or required software is not installed
    """


class RallyAssertionError(RallyError):
    """
    Thrown when a (precondition) check has been violated.
    """


class DataError(RallyError):
    """
    Thrown when something is wrong with the benchmark data
    """


class SupplyError(RallyError):
    pass


class BuildError(RallyError):
    pass


class InvalidSyntax(RallyError):
    pass


class InvalidName(RallyError):
    pass
