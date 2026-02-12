"""DISTINCT 切换策略。

切换 SELECT 语句的 DISTINCT 修饰符（有 → 无，无 → 有），
用于检测去重逻辑正确性。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class DistinctToggleStrategy(MutationStrategy):
    """DISTINCT 切换：添加或移除 SELECT DISTINCT。"""

    @property
    def id(self) -> str:
        return "distinct_toggle"

    @property
    def description(self) -> str:
        return "切换 SELECT DISTINCT（有 → 无，无 → 有）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Select,)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """切换 DISTINCT 修饰符。"""
        if not isinstance(node, exp.Select):
            return node

        if node.args.get("distinct"):
            # 移除 DISTINCT
            node.set("distinct", None)
        else:
            # 添加 DISTINCT
            node.set("distinct", exp.Distinct())
        return node
