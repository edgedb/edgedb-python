from typing import Union, List
from enum import Enum


class FilterOperator(str, Enum):
    EQ = "="
    NE = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    IN = "in"
    NOT_IN = "not in"
    LIKE = "like"
    ILIKE = "ilike"
    ANY = "any"
    ALL = "all"
    CONTAINS = "contains"
    EXISTS = "exists"


class FilterCondition(str, Enum):
    AND = "and"
    OR = "or"


class MetadataFilter:
    """Represents a single metadata filter condition."""

    def __init__(
        self,
        key: str,
        value: Union[int, float, str],
        operator: FilterOperator = FilterOperator.EQ,
    ):
        self.key = key
        self.value = value
        self.operator = operator

    def __repr__(self):
        return (
            f"MetadataFilters(condition={self.condition}, "
            f"filters={self.filters})"
        )


class MetadataFilters:
    """
    Allows grouping multiple MetadataFilter instances using AND/OR conditions.
    """

    def __init__(
        self,
        filters: List[Union["MetadataFilters", MetadataFilter]],
        condition: FilterCondition = FilterCondition.AND,
    ):
        self.filters = filters
        self.condition = condition

    def __repr__(self):
        return (
            f"MetadataFilters(condition={self.condition}, "
            f"filters={self.filters})"
        )
