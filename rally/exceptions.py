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
