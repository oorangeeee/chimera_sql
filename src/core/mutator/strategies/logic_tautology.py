"""逻辑恒真/恒假注入策略。

在 WHERE / HAVING 条件尾部注入 OR 1=1（恒真）或 AND 1=0（恒假），
用于检测条件短路和逻辑正确性。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class LogicTautologyStrategy(MutationStrategy):
    """逻辑恒真/恒假注入：在条件后追加 OR 1=1 或 AND 1=0。"""

    @property
    def id(self) -> str:
        return "logic_tautology"

    @property
    def description(self) -> str:
        return "在 WHERE/HAVING 条件后注入 OR 1=1（恒真）或 AND 1=0（恒假）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Where, exp.Having)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """在条件尾部注入恒真或恒假表达式。"""
        if not isinstance(node, (exp.Where, exp.Having)):
            return node

        original_cond = node.this
        if original_cond is None:
            return node

        # 随机选择恒真（OR 1=1）或恒假（AND 1=0）
        if rng.random() < 0.5:
            # 恒真：OR 1=1
            tautology = exp.EQ(
                this=exp.Literal.number(1),
                expression=exp.Literal.number(1),
            )
            new_cond = exp.Or(this=original_cond.copy(), expression=tautology)
        else:
            # 恒假：AND 1=0
            contradiction = exp.EQ(
                this=exp.Literal.number(1),
                expression=exp.Literal.number(0),
            )
            new_cond = exp.And(this=original_cond.copy(), expression=contradiction)

        node.set("this", new_cond)
        return node
