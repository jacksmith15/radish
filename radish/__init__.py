from radish.interface import Interface
from radish.exceptions import RadishError, RadishKeyError
from radish.resource import Resource


__version__ = "0.0.0"


__all__ = [
    "Resource",
    "Interface",
    "RadishError",
    "RadishKeyError",
]


# TODO: Allow nested filtering
#   - `.users.filter(orders__id=Contains(10))`
