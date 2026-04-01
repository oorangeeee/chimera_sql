"""转译规则集合。"""

from .aggregate_rules import FixAggregateStarRule
from .from_dual_rules import AddFromDualRule
from .group_by_subquery_rules import UnwrapGroupBySubqueriesRule
from .json_rules import JsonExtractToJsonValueRule, JsonValueToJsonExtractRule
from .recursive_rules import AddRecursiveKeywordRule, RemoveRecursiveKeywordRule
from .set_op_rules import ExceptToMinusRule

__all__ = [
    "FixAggregateStarRule",
    "AddFromDualRule",
    "UnwrapGroupBySubqueriesRule",
    "JsonExtractToJsonValueRule",
    "JsonValueToJsonExtractRule",
    "RemoveRecursiveKeywordRule",
    "AddRecursiveKeywordRule",
    "ExceptToMinusRule",
]
