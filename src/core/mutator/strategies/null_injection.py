"""NULL 注入策略。

将列引用或字面量替换为 NULL，
用于触发 NULL 处理缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class NullInjectionStrategy(MutationStrategy):
    """NULL 注入：将列引用或字面量替换为 NULL。"""

    @property
    def id(self) -> str:
        return "null_injection"

    @property
    def description(self) -> str:
        return "将列引用或字面量替换为 NULL"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Column, exp.Literal)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """替换节点为 NULL。"""
        return exp.Null()
