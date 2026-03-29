"""NVL2 函数注入策略（Oracle 方言感知）。

将 2 参数 COALESCE 表达式转换为 Oracle NVL2 函数，
用于检测 NVL2 与 COALESCE 语义差异缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class NVL2InjectionStrategy(MutationStrategy):
    """NVL2 注入：将 2 参数 COALESCE 转换为 Oracle NVL2 函数。"""

    @property
    def id(self) -> str:
        return "nvl2_injection"

    @property
    def description(self) -> str:
        return "将 2 参数 COALESCE 转换为 Oracle NVL2 函数"

    @property
    def category(self) -> str:
        return "dialect_specific"

    @property
    def requires(self) -> List[str]:
        return ["feature.nvl2"]

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Coalesce,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """将 COALESCE(a, b) 转换为 NVL2(a, a, b)。"""
        if not isinstance(node, exp.Coalesce):
            return node

        # NVL2 仅支持 2 参数形式：COALESCE(a, b) → NVL2(a, a, b)
        expressions = node.args.get("expressions", [])
        if len(expressions) != 1:
            return node

        first_arg = node.this
        second_arg = expressions[0]

        # NVL2(expr, return_if_not_null, return_if_null)
        # COALESCE(a, b) 等价于 NVL2(a, a, b)
        return exp.Anonymous(
            this="NVL2",
            expressions=[first_arg.copy(), first_arg.copy(), second_arg.copy()],
        )
