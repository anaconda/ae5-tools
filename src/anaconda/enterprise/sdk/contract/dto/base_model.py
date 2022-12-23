""" Base Model (Pydantic) Over-Ride """

from pydantic import BaseModel as PydanticBaseModel


def lower_camel_case(string: str) -> str:
    """
    Alias Generator Definition
    Used for external based consumption.
    Parameters
    ----------
    string: str
        The input string to change casing of.
    Returns
    -------
    new_string: str
        A new string which has been camel cased.
    """

    string_list: list[str] = string.split("_")
    prefix: str = string_list[0]
    suffix: str = "".join(word.capitalize() for word in string_list[1:])
    return prefix + suffix


class BaseModel(PydanticBaseModel):
    """BaseModel [Pydantic] Over-Ride"""

    # https://pydantic-docs.helpmanual.io/usage/model_config/#options
    class Config:
        """Pydantic Config Over-Ride"""

        alias_generator = lower_camel_case
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        use_enum_values = True
