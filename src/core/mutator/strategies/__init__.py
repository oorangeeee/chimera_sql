"""具体变异策略集合。"""

from .aggregate_substitution import AggregateSubstitutionStrategy
from .boundary_injection import BoundaryInjectionStrategy
from .cte_extraction import CTEExtractionStrategy
from .decode_injection import DecodeInjectionStrategy
from .distinct_toggle import DistinctToggleStrategy
from .except_all_toggle import ExceptAllToggleStrategy
from .join_type_switch import JoinTypeSwitchStrategy
from .limit_variation import LimitVariationStrategy
from .logic_tautology import LogicTautologyStrategy
from .median_injection import MedianInjectionStrategy
from .null_injection import NullInjectionStrategy
from .nvl2_injection import NVL2InjectionStrategy
from .operand_swap import OperandSwapStrategy
from .predicate_negation import PredicateNegationStrategy
from .sort_direction_flip import SortDirectionFlipStrategy
from .subquery_wrap import SubqueryWrapStrategy
from .union_type_variation import UnionTypeVariationStrategy

__all__ = [
    "AggregateSubstitutionStrategy",
    "BoundaryInjectionStrategy",
    "CTEExtractionStrategy",
    "DecodeInjectionStrategy",
    "DistinctToggleStrategy",
    "ExceptAllToggleStrategy",
    "JoinTypeSwitchStrategy",
    "LimitVariationStrategy",
    "LogicTautologyStrategy",
    "MedianInjectionStrategy",
    "NullInjectionStrategy",
    "NVL2InjectionStrategy",
    "OperandSwapStrategy",
    "PredicateNegationStrategy",
    "SortDirectionFlipStrategy",
    "SubqueryWrapStrategy",
    "UnionTypeVariationStrategy",
]
