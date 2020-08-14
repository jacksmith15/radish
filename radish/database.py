from abc import abstractmethod
from functools import partial
from operator import attrgetter
from typing import Any, Callable, cast, Generic, SupportsFloat, Type, TypeVar, Union

from aioredis import create_redis_pool, Redis
from pydantic import BaseModel
from typing_extensions import Protocol

from radish.exceptions import RadishKeyError, RadishError


class SupportsStr(Protocol):
    """An ABC with one abstract method __str__."""

    @abstractmethod
    def __str__(self) -> str:
        pass  # pragma: no cover


Model = TypeVar("Model", bound=BaseModel)


class FilterFactory(Generic[Model]):
    def __init__(self, target_model: Type[Model]):
        self.target_model = target_model

    def __call__(self, **filter_kwargs) -> Callable[[Model], bool]:
        bad_kwargs = set(filter_kwargs) - set(self.target_model.__fields__)
        if bad_kwargs:
            raise RadishError(
                f"Invalid filter fields for {self.target_model}: {bad_kwargs}."
            )

        def filter_func(instance: Model):
            for attr, value in filter_kwargs.items():
                if not getattr(instance, attr) == value:
                    return False
            return True

        return filter_func


class _DatabaseDescriptor(Generic[Model]):
    def __init__(
        self,
        database_id: int,
        model: Type[Model],
        key: Union[str, Callable[[Model], SupportsStr]],
    ):
        self.database_id = database_id
        self.model = model
        self._key_func: Callable[[Model], SupportsStr] = attrgetter(key) if isinstance(
            key, str
        ) else key
        self.filter_factory = FilterFactory(self.model)

    def __call__(
        self, connection_factory: Callable = create_redis_pool, **connection_kwargs: Any,
    ) -> "_Database[Model]":
        return _Database(self, connection_factory, **connection_kwargs)

    def get_key(self, instance: Union[Model, SupportsStr]) -> str:
        if isinstance(instance, self.model):
            return str(self._key_func(instance))
        if isinstance(instance, bytes):
            return instance.decode("utf-8")
        return str(instance)

    def deserialize(self, data: bytes) -> Model:
        return self.model.parse_raw(data)

    def serialize(self, instance: Model) -> bytes:
        if not type(instance) is self.model:
            if not isinstance(instance, self.model):
                raise RadishError(
                    "Instance type {type(instance)} does not match database schema. "
                    f"Accepts {self.model}."
                )
            raise RadishError(
                f"Subclasses of {self.model} are not supported, as they will not "
                "be deserialized correctly."
            )

        return instance.json().encode("utf-8")


NOT_PASSED = object()


class _Database(Generic[Model]):
    def __init__(
        self,
        descriptor: _DatabaseDescriptor[Model],
        connection_factory: Callable = create_redis_pool,
        **connection_kwargs: Any,
    ):
        self.descriptor = descriptor
        self._connection_factory = partial(
            connection_factory, **connection_kwargs, db=self.descriptor.database_id,
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

    async def save(self, instance: Model, allow_update: bool = True, expire: SupportsFloat = None) -> None:
        if not allow_update:
            existing = await self.connection.exists(self.descriptor.get_key(instance))
            if existing:
                raise RadishError(f"Record for {repr(instance)} already exists")
        await self.connection.set(
            self.descriptor.get_key(instance),
            self.descriptor.serialize(instance),
            pexpire=int(float(expire)*1000) if expire else None,
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

    async def expire(self, instance: Union[Model, SupportsStr], expire: SupportsFloat) -> None:
        key: str = self.descriptor.get_key(instance)
        exists = bool(await self.connection.expire(str(key), float(expire)))
        if not exists:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")

    async def __aiter__(self):
        async for key in self.connection.iscan():
            yield await self.get(key)

    async def filter(self, **filter_kwargs: Any):
        filter_func = self.descriptor.filter_factory(**filter_kwargs)
        async for instance in self:
            if filter_func(instance):
                yield instance


def Database(
    database_id: int, model: Type[Model], key: Union[str, Callable[[Model], SupportsStr]],
) -> _Database[Model]:
    """This ensures instance type annotations are correct when descriptors are set on the class."""
    return cast(_Database, _DatabaseDescriptor(database_id, model, key))
