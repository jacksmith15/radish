class RadishError(Exception):
    """Exception to raise from redis interface."""


class RadishKeyError(RadishError, KeyError):
    """Exception to raise when requested key does not exist."""
