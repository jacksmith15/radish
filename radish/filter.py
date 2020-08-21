import fnmatch
import re
from typing import Any, Callable, cast, Generic, Iterable, Type, TypeVar

from radish.exceptions import RadishError
from radish.types import Model


class FilterFactory(Generic[Model]):
    def __init__(self, target_model: Type[Model]):
        self.target_model = target_model

    def __call__(self, **filter_kwargs) -> Callable[[Model], bool]:
        bad_kwargs = set(filter_kwargs) - set(self.target_model.__fields__)
        if bad_kwargs:
            raise RadishError(
                f"Invalid filter fields for {self.target_model}: {bad_kwargs}."
            )

        field_filters = {
            attr: value if callable(value) else equals(value)
            for attr, value in filter_kwargs.items()
        }

        def filter_func(instance: Model):
            return all(
                field_filter(getattr(instance, attr))
                for attr, field_filter in field_filters.items()
            )

        return filter_func


FieldType = TypeVar("FieldType")


FilterFunc = Callable[[FieldType], bool]


class Filter(Generic[FieldType]):
    def __init__(self, function: FilterFunc, repr: str = None):
        self._repr = repr
        self.function: FilterFunc
        if isinstance(function, Filter):
            self.function = function.function
            if not self._repr:
                self._repr = function.__repr__()
        else:
            self.function = cast(FilterFunc, function)

    def __repr__(self):
        return self._repr or repr(self.function)

    def __call__(self, value: FieldType):
        return self.function(value)

    def __and__(self, other: FilterFunc):
        other = Filter(other)
        return Filter(
            lambda value: self(value) and other(value),
            repr=f"({repr(self)} and {repr(other)})",
        )

    def __or__(self, other: FilterFunc):
        other = Filter(other)
        return Filter(
            lambda value: self(value) or other(value),
            repr=f"({repr(self)} or {repr(other)})",
        )


def equals(term: Any) -> Filter:
    return Filter(lambda value: value == term, repr=f"{equals.__name__}({repr(term)})")


def like(pattern: str, regex: bool = False) -> Filter:
    if regex:
        return Filter(
            lambda value: bool(re.match(pattern, str(value))),
            repr=f"{like.__name__}({repr(pattern)}, regex={regex})",
        )
    return Filter(
        lambda value: bool(re.match(fnmatch.translate(pattern), str(value))),
        repr=f"{like.__name__}({repr(pattern)})",
    )


def within(values: Iterable[Any]) -> Filter:
    return Filter(lambda value: value in values, repr=f"{within.__name__}({repr(values)})")


def contains(value: Any) -> Filter:
    return Filter(
        lambda values: value in values, repr=f"{contains.__name__}({repr(value)})"
    )
