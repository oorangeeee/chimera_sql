"""UNION 类型变异策略。

切换 UNION 与 UNION ALL（distinct 标志翻转），
用于检测集合运算去重逻辑缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class UnionTypeVariationStrategy(MutationStrategy):
    """UNION 类型变异：UNION ↔ UNION ALL。"""

    @property
    def id(self) -> str:
        return "union_type_variation"

    @property
    def description(self) -> str:
        return "切换 UNION 与 UNION ALL（distinct 标志翻转）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Union,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """翻转 UNION 的 distinct 标志。"""
        if not isinstance(node, exp.Union):
            return node

        current_distinct = node.args.get("distinct", False)
        node.set("distinct", not current_distinct)
        return node
