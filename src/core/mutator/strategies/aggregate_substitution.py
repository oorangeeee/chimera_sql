"""聚合函数互换策略。

将聚合函数替换为另一种聚合函数（Count→Sum, Sum→Avg 等），
用于检测聚合语义差异缺陷。
"""

from random import Random
from typing import Dict, List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy

# 聚合函数互换映射：每种聚合可替换为的候选列表
_AGGREGATE_ALTERNATIVES: Dict[
    Type[exp.Expression], List[Type[exp.Expression]]
] = {
    exp.Count: [exp.Sum, exp.Min, exp.Max],
    exp.Sum: [exp.Avg, exp.Count, exp.Max],
    exp.Avg: [exp.Sum, exp.Min, exp.Max],
    exp.Min: [exp.Max, exp.Avg, exp.Sum],
    exp.Max: [exp.Min, exp.Avg, exp.Sum],
}


class AggregateSubstitutionStrategy(MutationStrategy):
    """聚合函数互换：将一种聚合函数替换为另一种。"""

    @property
    def id(self) -> str:
        return "aggregate_substitution"

    @property
    def description(self) -> str:
        return "聚合函数互换（Count→Sum, Sum→Avg 等）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """将聚合函数替换为随机选取的另一种聚合函数。"""
        alternatives = _AGGREGATE_ALTERNATIVES.get(type(node))
        if not alternatives:
            return node

        new_cls = rng.choice(alternatives)
        return new_cls(this=node.this.copy())
