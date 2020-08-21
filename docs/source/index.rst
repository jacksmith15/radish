.. Radish documentation master file, created by
   sphinx-quickstart on Fri Aug 21 18:07:58 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Radish's documentation!
==================================

``radish`` is a pythonic `Redis`_ interface with support for `asyncio`_ (PEP 3156) and `type hints`_ (PEP 484).

Example Usage
-------------

``radish`` uses `Pydantic`_ to declare schemas for resources stored in `Redis`_, and handles serialization, validation and namespacing for you:

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
        customers = radish.Resource(Customer, key="id", db=0)
        orders = radish.Resource(Order, key="id", db=1)


    async def get_customer_orders(customer_id: int) -> Tuple[Customer, List[Order]]:
        async with Radish(address="redis://redis") as cache:
            customer = await cache.customers.get(customer_id)
            orders = [
                order async for order in cache.orders.filter(customer=customer)
            ]
            return customer, orders

    async def get_all_customers() -> List[Customer]:
        async with Radish(address="redis://redis") as cache:
            return [customer async for customer in cache.customers]


Requirements
------------

This package is currently tested for Python 3.7.

Contents
========

.. toctree::
   :maxdepth: 2

   api




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _Pydantic: https://pydantic-docs.helpmanual.io/
.. _Redis: https://redis.io/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _type hints: https://docs.python.org/3/library/typing.html
