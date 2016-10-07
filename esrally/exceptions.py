class RallyError(Exception):
    """
    Base class for all Rally exceptions
    """
    pass


class LaunchError(RallyError):
    """
    Thrown whenever there was a problem launching the benchmark candidate
    """
    pass


class SystemSetupError(RallyError):
    """
    Thrown when a user did something wrong, e.g. the metrics store is not started or required software is not installed
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class RallyAssertionError(RallyError):
    """
    Thrown when a (precondition) check has been violated.
    """


class DataError(RallyError):
    """
    Thrown when something is wrong with the benchmark data
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class SupplyError(RallyError):
    pass


class BuildError(RallyError):
    pass


class InvalidSyntax(RallyError):
    pass
