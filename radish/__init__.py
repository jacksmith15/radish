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


# TODO: Migrate filtering to server side via match
# TODO: Allow nested filtering
# TODO: Allow structured filters:
#   - `.users.filter(name=In("bob", "fred"))`
#   - `.users.filter(name=Match("har.*"))`
#   - `.users.filter(orders__id=Contains(10))`