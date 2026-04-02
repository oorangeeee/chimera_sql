"""转译规则集合。"""

from .aggregate_rules import FixAggregateStarRule
from .cast_rules import CastDateToToCharRule, CastIntToTruncRule
from .from_dual_rules import AddFromDualRule
from .group_by_subquery_rules import UnwrapGroupBySubqueriesRule
from .json_rules import JsonExtractToJsonValueRule, JsonValueToJsonExtractRule
from .recursive_rules import AddRecursiveKeywordRule, RemoveRecursiveKeywordRule
from .set_op_rules import ExceptToMinusRule
from .sqlite_func_rules import DateFuncToToDateLiteralRule, GroupConcatToListaggRule

__all__ = [
    "FixAggregateStarRule",
    "CastDateToToCharRule",
    "CastIntToTruncRule",
    "AddFromDualRule",
    "UnwrapGroupBySubqueriesRule",
    "JsonExtractToJsonValueRule",
    "JsonValueToJsonExtractRule",
    "RemoveRecursiveKeywordRule",
    "AddRecursiveKeywordRule",
    "ExceptToMinusRule",
    "DateFuncToToDateLiteralRule",
    "GroupConcatToListaggRule",
]
