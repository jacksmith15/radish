.. _api:

API Reference
=============

Radish Interface
----------------

.. module:: radish.interface

.. autoclass:: radish.interface.Interface

.. autoclass:: radish.interface.InterfaceMeta


Radish Resource
---------------

.. module:: radish.resource

.. autofunction:: radish.resource.Resource

When using the :class:`~radish.interface.Interface` as a client, declared resources
behave as resource managers with access to the methods declared on :class:`~radish.resource._ResourceManager`.

.. autoclass:: radish.resource._ResourceManager

    .. automethod:: create

    .. automethod:: save

    .. automethod:: get

    .. automethod:: delete

    .. automethod:: expire

    .. automethod:: __aiter__

    .. automethod:: filter

