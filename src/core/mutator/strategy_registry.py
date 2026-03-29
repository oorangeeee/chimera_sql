"""变异策略注册表，管理全部可用变异策略。"""

from typing import List

from .strategy_base import MutationStrategy
from .strategies import (
    AggregateSubstitutionStrategy,
    BoundaryInjectionStrategy,
    CTEExtractionStrategy,
    DecodeInjectionStrategy,
    DistinctToggleStrategy,
    ExceptAllToggleStrategy,
    JoinTypeSwitchStrategy,
    LimitVariationStrategy,
    LogicTautologyStrategy,
    MedianInjectionStrategy,
    NullInjectionStrategy,
    NVL2InjectionStrategy,
    OperandSwapStrategy,
    PredicateNegationStrategy,
    SortDirectionFlipStrategy,
    SubqueryWrapStrategy,
    UnionTypeVariationStrategy,
)


class StrategyRegistry:
    """变异策略注册表。

    管理全部可用的变异策略实例，供引擎在变异时按需查询。
    """

    def __init__(self) -> None:
        self._strategies: List[MutationStrategy] = []

    def register(self, strategy: MutationStrategy) -> None:
        """注册一条变异策略。

        Args:
            strategy: 变异策略实例。
        """
        self._strategies.append(strategy)

    def get_all(self) -> List[MutationStrategy]:
        """获取全部已注册策略（返回副本）。"""
        return list(self._strategies)

    def __repr__(self) -> str:
        names = [s.id for s in self._strategies]
        return f"StrategyRegistry(strategies={names})"


def create_default_registry() -> StrategyRegistry:
    """创建并返回包含全部 17 个策略的默认注册表。"""
    registry = StrategyRegistry()

    # Generic strategies
    registry.register(BoundaryInjectionStrategy())
    registry.register(NullInjectionStrategy())
    registry.register(PredicateNegationStrategy())
    registry.register(LogicTautologyStrategy())
    registry.register(OperandSwapStrategy())
    registry.register(AggregateSubstitutionStrategy())
    registry.register(SortDirectionFlipStrategy())
    registry.register(DistinctToggleStrategy())
    registry.register(LimitVariationStrategy())
    registry.register(UnionTypeVariationStrategy())

    # Structural strategies
    registry.register(SubqueryWrapStrategy())
    registry.register(JoinTypeSwitchStrategy())
    registry.register(CTEExtractionStrategy())

    # Dialect-aware strategies
    registry.register(DecodeInjectionStrategy())
    registry.register(NVL2InjectionStrategy())
    registry.register(MedianInjectionStrategy())
    registry.register(ExceptAllToggleStrategy())

    return registry
