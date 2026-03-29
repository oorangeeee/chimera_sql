"""排序方向翻转策略。

将 ORDER BY 的排序方向翻转（ASC ↔ DESC），
用于检测排序方向敏感的逻辑缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class SortDirectionFlipStrategy(MutationStrategy):
    """排序方向翻转：ASC ↔ DESC。"""

    @property
    def id(self) -> str:
        return "sort_direction_flip"

    @property
    def description(self) -> str:
        return "翻转 ORDER BY 排序方向（ASC ↔ DESC）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Ordered,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """翻转排序方向。"""
        if not isinstance(node, exp.Ordered):
            return node
        current_desc = node.args.get("desc", False)
        node.set("desc", not current_desc)
        return node
