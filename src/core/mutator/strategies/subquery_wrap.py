"""标量子查询包装策略。

将列引用或字面量包装在标量子查询 (SELECT expr) 中，
用于检测子查询解析和标量子查询上下文处理缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class SubqueryWrapStrategy(MutationStrategy):
    """标量子查询包装：将列引用或字面量包装在 (SELECT expr) 中。"""

    @property
    def id(self) -> str:
        return "subquery_wrap"

    @property
    def description(self) -> str:
        return "将列引用或字面量包装在标量子查询中"

    @property
    def category(self) -> str:
        return "structural"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Column, exp.Literal)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """将节点包装在标量子查询中。"""
        return exp.Subquery(this=exp.Select(expressions=[node.copy()]))
