from abc import abstractmethod
from typing import TypeVar

from pydantic import BaseModel
from typing_extensions import Protocol


Model = TypeVar("Model", bound=BaseModel)


class SupportsStr(Protocol):
    """An ABC with one abstract method __str__."""

    @abstractmethod
    def __str__(self) -> str:
        pass  # pragma: no cover

