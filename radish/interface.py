import asyncio
from typing import Any, Callable, cast, ClassVar, Dict, Tuple, Type, TypeVar

from aioredis import create_redis_pool

from radish.resource import _ResourceDescriptor
from radish.exceptions import RadishError


class InterfaceMeta(type):
    _meta: Dict[str, Any]

    def __new__(
        mcs, name: str, bases: Tuple[Type], classdict: Dict[str, Any],
    ):
        databases = {
            attr: value
            for attr, value in classdict.items()
            if isinstance(value, _ResourceDescriptor)
        }
        for attr in databases:
            del classdict[attr]
        if "_meta" in classdict:
            raise RadishError(
                "'_meta' is a reserved class property for `Interface` classes"
            )
        cls: InterfaceMeta = cast(InterfaceMeta, type.__new__(mcs, name, bases, classdict))
        cls._meta = {"databases": databases}
        return cls


class Interface(metaclass=InterfaceMeta):
    _meta: ClassVar[Dict[str, Any]]

    def __init__(
        self, connection_factory: Callable = create_redis_pool, **redis_settings: Any
    ):
        for attr, database_meta in type(self)._meta["databases"].items():
            setattr(self, attr, database_meta(connection_factory, **redis_settings))

    async def __aenter__(self: "InterfaceT") -> "InterfaceT":
        await asyncio.gather(
            *[getattr(self, attr).__aenter__() for attr in type(self)._meta["databases"]]
        )
        return self

    async def __aexit__(self, _exception_type, _exception_value, _traceback):
        await asyncio.gather(
            *[
                getattr(self, attr).__aexit__(_exception_type, _exception_value, _traceback)
                for attr in type(self)._meta["databases"]
            ]
        )


InterfaceT = TypeVar("InterfaceT", bound=Interface)
