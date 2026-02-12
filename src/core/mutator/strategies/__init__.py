"""具体变异策略集合。"""

from .aggregate_substitution import AggregateSubstitutionStrategy
from .boundary_injection import BoundaryInjectionStrategy
from .distinct_toggle import DistinctToggleStrategy
from .limit_variation import LimitVariationStrategy
from .logic_tautology import LogicTautologyStrategy
from .null_injection import NullInjectionStrategy
from .operand_swap import OperandSwapStrategy
from .predicate_negation import PredicateNegationStrategy
from .sort_direction_flip import SortDirectionFlipStrategy
from .union_type_variation import UnionTypeVariationStrategy

__all__ = [
    "AggregateSubstitutionStrategy",
    "BoundaryInjectionStrategy",
    "DistinctToggleStrategy",
    "LimitVariationStrategy",
    "LogicTautologyStrategy",
    "NullInjectionStrategy",
    "OperandSwapStrategy",
    "PredicateNegationStrategy",
    "SortDirectionFlipStrategy",
    "UnionTypeVariationStrategy",
]
