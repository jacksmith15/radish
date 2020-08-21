import asyncio
from functools import partial
from operator import attrgetter
from typing import Any, AsyncIterator, Callable, cast, Generic, List, SupportsFloat, Type, Union

from aioredis import create_redis_pool, Redis

from radish.exceptions import RadishKeyError, RadishError
from radish.filter import FilterFactory
from radish.types import Model, SupportsStr


class _ResourceDescriptor(Generic[Model]):
    """Descriptor for resource managers.

    Should not be instantiated directly - used by :class:`radish.interface.Interface`.
    """

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
    ) -> "_ResourceManager[Model]":
        return _ResourceManager(self, connection_factory, **connection_kwargs)

    def _get_instance_key(self, instance: Union[Model, SupportsStr]) -> str:
        if isinstance(instance, self.model):
            return str(self._key_func(instance))
        if isinstance(instance, bytes):
            return instance.decode("utf-8")
        return str(instance)

    def get_key(self, instance: Union[Model, SupportsStr]) -> str:
        """Derive key from arguments to :class:`radish.resource._ResourceManager` methods."""
        return f"{self.prefix}-{self._get_instance_key(instance)}"

    def deserialize(self, data: bytes) -> Model:
        """Deserialize cached data into a model instance."""
        return self.model.parse_raw(data)

    def serialize(self, instance: Model) -> bytes:
        """Serialize a model instance to be stored in Redis.

        :raises: :exc:`~radish.exceptions.RadishError` if the model instance
            is of an incorrect type.
        """
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


_NOT_PASSED = object()


class _ResourceManager(Generic[Model]):
    """A manager for interacting with a particular resource type in Redis.

    Should not be instantiated directly, but instead declared via :func:`~radish.resource.Resource`
    on a :class:`~radish.interface.Interface` subclass.
    """

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

    async def create(self, *args, **kwargs) -> Model:
        """Create a new record in Redis, and return the model instance.

        Usage:

        .. code:: python

            user: User = await redis.users.create(id=1, name="bob")

        Arguments are passed directly through to the resource model to create
        the instance.

        :raises: :exc:`~radish.exceptions.RadishError`: if a record already
            exists with this identifier.
        :return: The newly created and cached model instance.
        """
        instance = self.descriptor.model(*args, **kwargs)
        await self.save(instance, allow_update=False)
        return instance

    async def save(
        self, *instances: Model, allow_update: bool = True, expire: SupportsFloat = None
    ) -> None:
        """Store one or more model instances in the Redis cache.

        .. code:: python

            await redis.users.save(User(id=1, name="bob"), User(id=2, name="frank"))

        :param instances: The set of model instances to store in the cache.
        :param allow_update: Whether to allow updates to existing records.
        :param expire: The number of seconds in which to expire te records.
        """
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

    async def get(self, instance: Union[Model, SupportsStr], default=_NOT_PASSED) -> Model:
        """Retrieve a record from the cache.

        Either accepts lookup by instance ID:

        .. code:: python

            user = await redis.users.get(request.data["id"])

        Or by instance itself:

        .. code:: python

            user = await redis.users.get(user)

        Default values can be provided for the case where the record does not exist:

        .. code:: python

            if not await redis.users.get(user, None):
                await redis.users.save(user)

        :param instance: Either the key to look up a record, or a model instance
            itself.
        :param default: Optional default value to return if the record does not exist.
            Follows the same semantics as ``getattr``.
        :return: The cached model instance, or :paramref:`default` if provided and the
            record is not found.
        :raises: :exc:`~radish.exceptions.RadishKeyError` if no record is found
            and no default is provided.
        """
        key: str = self.descriptor.get_key(instance)
        value = await self.connection.get(key)
        if value is None and default is _NOT_PASSED:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")
        return self.descriptor.deserialize(value) if value else default

    async def delete(self, instance: Union[Model, SupportsStr]) -> None:
        """Delete a record from the cache.

        .. code:: python

            await redis.users.delete(bad_user)

        :param instance: Either the key of the record to delete, or a model instance
            itself.
        :raises: :exc:`~radish.exceptions.RadishKeyError` if no matching record exists.
        """
        key: str = self.descriptor.get_key(instance)
        exists = bool(await self.connection.delete(str(key)))
        if not exists:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")

    async def expire(
        self, instance: Union[Model, SupportsStr], expire: SupportsFloat
    ) -> None:
        """Set a record to expire.

        .. code:: python

            await redis.users.expire(old_user, 10.0)

        :param instance: Either the key of the record to expire, or a model instance
            itself.
        :param expire: The number of seconds in which to expire the record.
        :raises: :exc:`~radish.exceptions.RadishKeyError` if no matching record exists.
        """
        key: str = self.descriptor.get_key(instance)
        exists = bool(await self.connection.expire(str(key), float(expire)))
        if not exists:
            raise RadishKeyError(f"Key {repr(key)} does not exist.")

    async def __aiter__(self) -> AsyncIterator[Model]:
        """Iterate all cached records for this resource type.

        .. code:: python

            async for user in redis.users:
                await send(user.id)

        """
        async for key in self.connection.iscan(match=f"{self.descriptor.prefix}-*"):
            yield await self.get(key[len(self.descriptor.prefix) + 1 :])

    async def filter(self, **filter_kwargs: Any) -> AsyncIterator[Model]:
        """Asynchronously iterate over records matching given criteria.

        Usage:

        .. code:: python

            from radish.filter import like, within, contains

            # Exact field matching:
            results = [result async for result in redis.users.filter(name="bob")]

            # String matching (UNIX glob):
            results = [result async for result in redis.users.filter(name=like("bob *"))]

            # String matching (Regular expression):
            results = [
                result
                async for result in redis.users.filter(
                    name=like("^[Bb]ob.*$"), regex=True
                )
            ]

            # Container field matching:
            results = [
                result
                async for result in redis.users.filter(friends=contains(source_user))
            ]

            # Field within container:
            results = [
                result
                async for result in redis.users.filter(age=within(range(25, 30)))
            ]

            # Combining filters can be achieved using the & and | operators:
            results = [
                result
                async for result in redis.users.filter(
                    age=within(range(0, 10)) | within(range(70, 100))
                )
            ]

        :param filter_kwargs: The keys should match the resource model's fields
            and the values can either be values for exact matching, or be one
            of the filter terms provided in :mod:`radish.filter` (see above).
        :return: An async iterator over the matching records.
        """
        filter_func = self.descriptor.filter_factory(**filter_kwargs)
        async for instance in self:
            if filter_func(instance):
                yield instance


# This is declared as a function to ensure instance type annotations are correct
# when descriptors are set on the class.
def Resource(
    model: Type[Model],
    key: Union[str, Callable[[Model], SupportsStr]],
    db: int = 0,
    prefix: str = None,
) -> _ResourceManager[Model]:
    """Declare a new resource type on an :class:`~radish.interface.Interface` subclass.

    Usage:

    .. code:: python

        class User(BaseModel):
            id: int
            name: str

        class Redis(radish.Interface):
            users = radish.Resource(User, key="id", db=0)

    This creates a record manager on the :class:`~radish.interface.Interface` subclass,
    which allows to save, retrieve and iterate records of this type.

    Record types are differentiated by namespacing the key. For example:

    .. code:: python

        await redis.users.create(id=1, name="bob")

    This would store a serialized user instance at key ``"User-1"`` in the Redis
    database. This is hidden internally if your only interface to Redis is via
    ``radish``, but is important to note if the cached data must be accessed
    elsewhere.

    By default the namespace is taken from the model's class name. If this would
    cause conflicts then the prefix can be set explicitly as follows:

    .. code:: python

        class Redis(radish.Interface):
            users = radish.Resource(User, key="id", db=0, prefix="_radish_user")


    :param model: Subclass of ``pydantic.BaseModel`` which described the
        resource stored on this manager.
    :param key: Either the attribute of the model instance used to generate
        the key in Redis, or a function which will generate a key from an instance.
    :param db: The Redis database in which to store the resource records.
    :param prefix: The prefix used to namespace the resource in Redis.
    :return: The descriptor for the resource manager.
    """
    return cast(_ResourceManager, _ResourceDescriptor(model=model, key=key, db=db, prefix=prefix))
