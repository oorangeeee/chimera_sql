"""MEDIAN 函数注入策略（Oracle 方言感知）。

将 AVG 聚合函数替换为 Oracle MEDIAN 函数，
用于检测 MEDIAN 聚合语义差异缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class MedianInjectionStrategy(MutationStrategy):
    """MEDIAN 注入：将 AVG 聚合函数替换为 Oracle MEDIAN 函数。"""

    @property
    def id(self) -> str:
        return "median_injection"

    @property
    def description(self) -> str:
        return "将 AVG 聚合函数替换为 Oracle MEDIAN 函数"

    @property
    def category(self) -> str:
        return "dialect_specific"

    @property
    def requires(self) -> List[str]:
        return ["feature.median"]

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Avg,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """将 AVG(expr) 替换为 MEDIAN(expr)。"""
        if not isinstance(node, exp.Avg):
            return node

        return exp.Anonymous(
            this="MEDIAN",
            expressions=[node.this.copy()],
        )
