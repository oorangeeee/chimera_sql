"""谓词取反策略。

将比较运算符取反（= ↔ <>, > ↔ <=, < ↔ >=），
用于检测条件逻辑错误。
"""

from random import Random
from typing import Dict, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy

# 比较运算符取反映射
_NEGATION_MAP: Dict[Type[exp.Expression], Type[exp.Expression]] = {
    exp.EQ: exp.NEQ,
    exp.NEQ: exp.EQ,
    exp.GT: exp.LTE,
    exp.GTE: exp.LT,
    exp.LT: exp.GTE,
    exp.LTE: exp.GT,
}


class PredicateNegationStrategy(MutationStrategy):
    """谓词取反：比较运算符取反（= ↔ <>, > ↔ <=, < ↔ >=）。"""

    @property
    def id(self) -> str:
        return "predicate_negation"

    @property
    def description(self) -> str:
        return "比较运算符取反（= ↔ <>, > ↔ <=, < ↔ >=）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """将比较运算符替换为其取反形式。"""
        negated_cls = _NEGATION_MAP.get(type(node))
        if negated_cls is None:
            return node
        return negated_cls(this=node.this.copy(), expression=node.expression.copy())
