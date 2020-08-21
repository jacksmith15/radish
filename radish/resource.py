import asyncio
from functools import partial
from operator import attrgetter
from typing import Any, Callable, cast, Generic, List, SupportsFloat, Type, Union

from aioredis import create_redis_pool, Redis

from radish.exceptions import RadishKeyError, RadishError
from radish.filter import FilterFactory
from radish.types import Model, SupportsStr


class _ResourceDescriptor(Generic[Model]):
    def __init__(
        self,
        model: Type[Model],
        key: Union[str, Callable[[Model], SupportsStr]],
        db: int = 0,
        prefix: str = None,
    ):
        self.db = db
        self.model = model
        self._key_func: Callable[[Model], SupportsStr] = attrgetter(key) if isinstance(
            key, str
        ) else key
        self.filter_factory = FilterFactory(self.model)
        self.prefix = (prefix or model.__name__).rstrip("-")

    def __call__(
        self, connection_factory: Callable = create_redis_pool, **connection_kwargs: Any,
    ) -> "_Resource[Model]":
        return _Resource(self, connection_factory, **connection_kwargs)

    def _get_instance_key(self, instance: Union[Model, SupportsStr]) -> str:
        if isinstance(instance, self.model):
            return str(self._key_func(instance))
        if isinstance(instance, bytes):
            return instance.decode("utf-8")
        return str(instance)

    def get_key(self, instance: Union[Model, SupportsStr]) -> str:
        return f"{self.prefix}-{self._get_instance_key(instance)}"

    def deserialize(self, data: bytes) -> Model:
        return self.model.parse_raw(data)

    def serialize(self, instance: Model) -> bytes:
        if not type(instance) is self.model:
            if not isinstance(instance, self.model):
                raise RadishError(
                    "Instance type {type(instance)} does not match resource schema. "
                    f"Accepts {self.model}."
                )
            raise RadishError(
                f"Subclasses of {self.model} are not supported, as they will not "
                "be deserialized correctly."
            )

        return instance.json().encode("utf-8")


NOT_PASSED = object()


class _Resource(Generic[Model]):
    def __init__(
        self,
        descriptor: _ResourceDescriptor[Model],
        connection_factory: Callable = create_redis_pool,
        **connection_kwargs: Any,
    ):
        self.descriptor = descriptor
        self._connection_factory = partial(
            connection_factory, **connection_kwargs, db=self.descriptor.db,
        )
        self._connection = None

    async def __aenter__(self):
        if self._connection:
            raise RadishError("Already connected to redis!")
        self._connection = await self._connection_factory()
        return self

    async def __aexit__(self, _exception_type, _exception_value, _traceback):
        self._connection.close()
        await self._connection.wait_closed()
        self._connection = None

    @property
    def connection(self) -> Redis:
        if not self._connection:
            raise RadishError("Connection to redis has not been initialised.")
        return self._connection

    async def save(
        self, *instances: Model, allow_update: bool = True, expire: SupportsFloat = None
    ) -> None:
        if not allow_update:
            existing: List[bool] = await asyncio.gather(
                *[
                    self.connection.exists(self.descriptor.get_key(instance))
                    for instance in instances
                ]
            )
            if any(existing):
                already_exists: List[Model] = [instance for instance, exists in zip(instances, existing) if exists]
                raise RadishError(f"Records for {repr(already_exists)} already exists")
        serialized_instances = [
            (self.descriptor.get_key(instance), self.descriptor.serialize(instance))
            for instance in instances
        ]
        await asyncio.gather(
            *[
                self.connection.set(
                    key, data, pexpire=int(float(expire) * 1000) if expire else None
                )
                for key, data in serialized_instances
            ]
        )

    async def get(self, instance: Union[Model, SupportsStr], default=NOT_PASSED) -> Model:
        key: str = self.descriptor.get_key(instance)
        value = await self.connection.get(key)
        if value is None and default is NOT_PASSED:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")
        return self.descriptor.deserialize(value) if value else default

    async def delete(self, instance: Union[Model, SupportsStr]) -> None:
        key: str = self.descriptor.get_key(instance)
        exists = bool(await self.connection.delete(str(key)))
        if not exists:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")

    async def expire(
        self, instance: Union[Model, SupportsStr], expire: SupportsFloat
    ) -> None:
        key: str = self.descriptor.get_key(instance)
        exists = bool(await self.connection.expire(str(key), float(expire)))
        if not exists:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")

    async def __aiter__(self):
        async for key in self.connection.iscan(match=f"{self.descriptor.prefix}-*"):
            yield await self.get(key[len(self.descriptor.prefix) + 1 :])

    async def filter(self, **filter_kwargs: Any):
        filter_func = self.descriptor.filter_factory(**filter_kwargs)
        async for instance in self:
            if filter_func(instance):
                yield instance


def Resource(
    model: Type[Model],
    key: Union[str, Callable[[Model], SupportsStr]],
    db: int = 0,
    prefix: str = None,
) -> _Resource[Model]:
    """This ensures instance type annotations are correct when descriptors are set on the class."""
    return cast(_Resource, _ResourceDescriptor(model=model, key=key, db=db, prefix=prefix))
