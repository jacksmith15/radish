.. _usage:

Usage
=====

``radish`` requires that you declare schemas for your cached resources using `Pydantic`_. This ensures a consistent interface, making your code easier to grasp, and allowing static type checkers like `mypy`_ to spot errors without executing your code.

Setting up your interface
-------------------------

You can configure your interface to `Redis`_ by setting a number of :func:`~radish.resource.Resource` attributes on a subclass of :class:`~radish.interface.Interface`.

.. code:: python

    from datetime import datetime
    from typing import List, Tuple

    from pydantic import BaseModel
    import radish


    class Customer(BaseModel):
        id: int
        name: str


    class Order(BaseModel):
        id: int
        item_id: int
        timestamp: datetime
        customer: Customer


    class Radish(radish.Interface):
        customers = radish.Resource(Customer, key="id")
        orders = radish.Resource(Order, key="id")

In the above example, there are two models, ``Customer`` and ``Order``. Each will be stored in the default ``Redis`` database (``0``), and keyed on their ID fields.

.. note::
    Multiple resources can use the same ``Redis`` database, as the keys will be prefixed with the model name.

If you want to store the same model in the same database under different contexts, you will need to manually set the namespace to avoid conflicts:

.. code:: python

    class Person(BaseModel):
        id: int
        email: str

    class Radish(radish.Interface):
        users = radish.Resource(Person, key="id", prefix="user")
        administrators = radish.Resource(Person, key="id", prefix="administrator")


Alternatively, you can store resources in different databases:

.. code:: python

    class Person(BaseModel):
        id: int
        email: str

    class Radish(radish.Interface):
        users = radish.Resource(Person, key="id", db=0)
        administrators = radish.Resource(Person, key="id", db=1)

.. warning::

    Redis support for multiple databases is deprecated and not supported in Redis Cluster. It's recommended that you do not specify the database on your resources, unless you absolutely must.


Connecting to Redis
-------------------

To connect to Redis, use the :class:`~radish.interface.Interface` as an asynchronous context manager:


.. code:: python

    async with Radish(address="redis://redis") as cache:
        ...

The :class:`~radish.interface.Interface` passes its arguments through to ``aioredis.create_redis_pool``. You can override the ``create_redis_pool`` by passing the ``connection_factory`` option:

.. code:: python

    async with Radish(connection_factory=custom_redis_pool, address="redis://redis") as cache:
        ...


Caching records
---------------

To store a record in the cache, you can pass it to the :meth:`~radish.resource._ResourceManager.save` method of its corresponding manager:

.. code:: python

    class Radish(radish.Interface):
        users = radish.Resource(User, key="id", db=0)

    user = User(id=1, name="Bob")

    async with Radish(address="redis://redis") as cache:
        await cache.users.save(user)

You can save multiple records with :meth:`~radish.resource._ResourceManager.save`:

.. code:: python

    bob = User(id=1, name="Bob")
    fred = User(id=2, name="Bob")

    async with Radish(address="redis://redis") as cache:
        await cache.users.save(fred, bob)

By default, :meth:`~radish.resource._ResourceManager.save` will update existing records:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        user: User = await cache.users.get(1)
        user.name = "Fred"
        await cache.users.save(user)

But this behaviour can be disabled:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        await cache.users.save(user, allow_update=False)


You can also set how long you want to keep the cached record for (in seconds):

.. code:: python

    async with Radish(address="redis://redis") as cache:
        await cache.users.save(user, expire=15.0)


The :meth:`~radish.resource._ResourceManager.create` method provides a shorthand for initialising the model instance and caching at the same time:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        user: User = await cache.users.create(id=1, name="frank")

You can set a record to expire or delete it directly, using the :meth:`~radish.resource._ResourceManager.expire` and :meth:`~radish.resource._ResourceManager.delete` methods.

.. code:: python

    async with Radish(address="redis://redis") as cache:
        await cache.users.expire(user1, 15.0)
        await cache.users.delete(user2)


Retrieving from the cache
-------------------------

The :meth:`~radish.resource._ResourceManager.get` method allows you to retrieve a record by ID:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        user = await cache.users.get(1)

If you aren't sure whether the record exists or not, you can set a default:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        user = await cache.users.get(1, None)

You can also pass a model instance directly to :meth:`~radish.resource._ResourceManager.get` to find the current cached version:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        cached_user = await cache.users.get(user)

The resource manager can be treated as an asynchronous iterable over all records:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        all_users = [user async for user in cache.users]

And the filter method allows you to find particular records:

.. code:: python

    async with Radish(address="redis://redis") as cache:
        async for user in cache.users.filter(name="fred"):
            print(user)

See :meth:`~radish.resource._ResourceManager.filter` for more ways to filter on record fields.


.. note::

    Filtering is done on the client-side, and so an iteration over a filter will still retrieve every record from the cache under the hood. This is due to how scan operations work in ``Redis``.


.. _Pydantic: https://pydantic-docs.helpmanual.io/
.. _aioredis: https://aioredis.readthedocs.io/en/stable/
.. _mypy: http://mypy-lang.org/
.. _Redis: https://redis.io/
