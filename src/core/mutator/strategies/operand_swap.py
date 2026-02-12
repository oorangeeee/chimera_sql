"""操作数交换策略。

交换二元表达式的左右操作数（this ↔ expression），
用于检测运算顺序敏感的逻辑缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class OperandSwapStrategy(MutationStrategy):
    """操作数交换：交换二元运算的左右操作数。"""

    @property
    def id(self) -> str:
        return "operand_swap"

    @property
    def description(self) -> str:
        return "交换二元运算的左右操作数（this ↔ expression）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.EQ, exp.GT, exp.LT, exp.Add, exp.Mul)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """交换 this 与 expression。"""
        left = node.args.get("this")
        right = node.args.get("expression")
        if left is None or right is None:
            return node

        left_copy = left.copy()
        right_copy = right.copy()
        node.set("this", right_copy)
        node.set("expression", left_copy)
        return node
