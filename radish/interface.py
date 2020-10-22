import asyncio
from itertools import combinations
from typing import Any, Callable, cast, ClassVar, Dict, Tuple, Type, TypeVar

from aioredis import create_redis_pool

from radish.resource import _ResourceDescriptor
from radish.exceptions import RadishError


class InterfaceMeta(type):
    """Metaclass for the :class:`~radish.interface.Interface` class.

    Collects resource manager descriptors assigned as class variables.
    """
    _meta: Dict[str, Any]

    def __new__(
        mcs, name: str, bases: Tuple[Type], classdict: Dict[str, Any],
    ):
        resources = {
            attr: value
            for attr, value in classdict.items()
            if isinstance(value, _ResourceDescriptor)
        }

        for left, right in combinations(resources.values(), 2):
            if left.db == right.db and left.prefix == right.prefix:
                raise RadishError(
                    "There are namespace conflicts in the specified resources. Please "
                    f"provide explicit prefix parameters. Resources: {left}, {right}"
                )

        for attr in resources:
            del classdict[attr]
        if "_meta" in classdict:
            raise RadishError(
                "'_meta' is a reserved class property for `Interface` classes"
            )
        cls: InterfaceMeta = cast(InterfaceMeta, type.__new__(mcs, name, bases, classdict))
        cls._meta = {"resources": resources}
        return cls


class Interface(metaclass=InterfaceMeta):
    """A specification of resources to be managed in Redis. Should be subclassed.

    Resources are declared by setting :func:`~radish.resource.Resource` instance as
    class attributes on subclasses of :class:`~radish.interface.Interface`:

    .. code:: python

        class User(BaseModel):
            id: int
            name: str

        class Redis(radish.Interface):
            users = radish.Resource(User, key="id", db=0)

    Connection to Redis can then be made by using the :class:`~radish.interface.Interface`
    subclass as an asynchronous context manager:

    .. code:: python

        async with Redis("redis://redis") as redis:
            user = await redis.users.create(id=1, name="bob")

    You are free to define custom methods and properties on the subclass, and these will
    all be available on the yielded context:

    .. code:: python

        class Redis(radish.Interface):
            users = radish.Resource(User, key="id", db=0)

            async def user_list(self) -> List[User]:
                return [user async for user in self.users]

        async with Redis("redis://redis") as redis:
            user_list = await redis.user_list()

    However the class attribute ``_meta`` is reserved, and will raise
    :exc:`~radish.exceptions.RadishError` if set.
    """
    _meta: ClassVar[Dict[str, Any]]

    def __init__(
        self, *, connection_factory: Callable = create_redis_pool, **redis_settings: Any
    ):
        for attr, resource_meta in type(self)._meta["resources"].items():
            setattr(self, attr, resource_meta(connection_factory, **redis_settings))

    async def __aenter__(self: "InterfaceT") -> "InterfaceT":
        await asyncio.gather(
            *[getattr(self, attr).__aenter__() for attr in type(self)._meta["resources"]]
        )
        return self

    async def __aexit__(self, _exception_type, _exception_value, _traceback):
        await asyncio.gather(
            *[
                getattr(self, attr).__aexit__(_exception_type, _exception_value, _traceback)
                for attr in type(self)._meta["resources"]
            ]
        )


InterfaceT = TypeVar("InterfaceT", bound=Interface)
