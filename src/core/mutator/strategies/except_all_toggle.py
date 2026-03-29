"""EXCEPT ALL 切换策略（方言感知）。

切换 EXCEPT 与 EXCEPT ALL（distinct 标志翻转），
用于检测集合运算去重逻辑缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class ExceptAllToggleStrategy(MutationStrategy):
    """EXCEPT ALL 切换：EXCEPT ↔ EXCEPT ALL。"""

    @property
    def id(self) -> str:
        return "except_all_toggle"

    @property
    def description(self) -> str:
        return "切换 EXCEPT 与 EXCEPT ALL（distinct 标志翻转）"

    @property
    def category(self) -> str:
        return "dialect_specific"

    @property
    def requires(self) -> List[str]:
        return ["feature.except_all"]

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Except,)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """翻转 EXCEPT 的 distinct 标志。"""
        if not isinstance(node, exp.Except):
            return node

        current_distinct = node.args.get("distinct", True)
        node.set("distinct", not current_distinct)
        return node
