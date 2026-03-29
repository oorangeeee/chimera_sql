"""标量子查询包装策略。

将列引用或字面量包装在标量子查询 (SELECT expr) 中，
用于检测子查询解析和标量子查询上下文处理缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy

# 需要补 FROM DUAL 的方言
_REQUIRES_FROM_DUAL = {"oracle"}


class SubqueryWrapStrategy(MutationStrategy):
    """标量子查询包装：将列引用或字面量包装在标量子查询中。"""

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

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """将节点包装在标量子查询中。

        Oracle 要求裸标量子查询必须包含 FROM DUAL（ORA-00923），
        此处根据方言自动补全。
        """
        select = exp.Select(expressions=[node.copy()])
        if dialect and dialect.lower() in _REQUIRES_FROM_DUAL:
            select.set(
                "from_",
                exp.From(this=exp.Table(this=exp.Identifier(this="DUAL"))),
            )
        return exp.Subquery(this=select)
