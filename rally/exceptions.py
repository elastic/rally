class ImproperlyConfigured(Exception):
    """
    Thrown on configuration errors.
    """
    pass


class LaunchError(Exception):
    """
    Thrown whenever there was a problem launching the benchmark candidate
    """
    pass


class SystemSetupError(Exception):
    """
    Thrown when a user did something wrong, e.g. the metrics store is not started or required software is not installed
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

