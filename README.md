[![Build Status](https://travis-ci.com/jacksmith15/radish.svg?token=JrMQr8Ynsmu5tphpTQ2p&branch=master)](https://travis-ci.com/jacksmith15/radish)
# Radish
`radish` is a pythonic Redis interface with support for asyncio (PEP 3156) and type annotations (PEP 484).

# Usage
`radish` uses `pydantic` to declare database model schemas.

```python
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
```


# Requirements
This package is currently tested for Python 3.7.

# Installation
This project is not currently packaged and so must be installed manually.

Clone the project with the following command:
```
git clone https://github.com/jacksmith15/radish.git
```

Package requirements may be installed via `pip install -r requirements.txt`. Use of a [virtualenv](https://virtualenv.pypa.io/) is recommended.

# Development
1. Clone the repository: `git clone git@github.com:jacksmith15/radish.git && cd radish`
2. Install the requirements: `pip install -r requirements.txt -r requirements-test.txt`
3. Run `pre-commit install`
4. Run the tests: `bash run_test.sh -c -a`

This project uses the following QA tools:
- [PyTest](https://docs.pytest.org/en/latest/) - for running unit tests.
- [Pyflakes](https://github.com/PyCQA/pyflakes) - for catching syntax errors.
- [MyPy](http://mypy-lang.org/) - for static type checking.
- [Travis CI](https://travis-ci.org/) - for continuous integration.
- [Black](https://black.readthedocs.io/en/stable/) - for uniform code formatting.

# License
This project is distributed under the MIT license.
