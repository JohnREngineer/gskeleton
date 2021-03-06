from numbers import Real
from typing import Union

from pydantic import BaseModel


class User(BaseModel):
    id: int
    name = "Jane Doe"


def multiply_two_numbers(
    a: Union[int, Real], b: Union[int, Real]
) -> Union[int, Real]:
    return a * b
