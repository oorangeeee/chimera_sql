"""LIMIT/OFFSET 变异策略。

修改 LIMIT 或 OFFSET 的数值（乘 2、设为 0、设为 1 等），
用于检测分页边界条件缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy

# LIMIT/OFFSET 变异选项：(描述, 变换函数)
_VARIATIONS = [
    ("设为0", lambda _: 0),
    ("设为1", lambda _: 1),
    ("乘以2", lambda v: v * 2),
    ("加1", lambda v: v + 1),
    ("减1", lambda v: max(0, v - 1)),
]


class LimitVariationStrategy(MutationStrategy):
    """LIMIT/OFFSET 变异：修改分页参数数值。"""

    @property
    def id(self) -> str:
        return "limit_variation"

    @property
    def description(self) -> str:
        return "修改 LIMIT/OFFSET 数值（乘 2、设为 0、设为 1 等）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Limit, exp.Offset)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """修改 LIMIT/OFFSET 的数值。"""
        if not isinstance(node, (exp.Limit, exp.Offset)):
            return node

        expr = node.expression
        if not isinstance(expr, exp.Literal) or not expr.is_number:
            return node

        try:
            original = int(expr.this)
        except (ValueError, TypeError):
            return node

        _, transform_fn = rng.choice(_VARIATIONS)
        new_value = transform_fn(original)
        node.set("expression", exp.Literal.number(new_value))
        return node
