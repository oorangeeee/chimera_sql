"""JOIN 类型切换策略。

在 INNER JOIN / LEFT JOIN / RIGHT JOIN / CROSS JOIN 之间随机切换，
用于检测 JOIN 语义差异缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy

# 合法的 (side, kind) 组合
_JOIN_VARIANTS: List[Tuple[str, str]] = [
    ("", "INNER"),   # INNER JOIN
    ("LEFT", ""),    # LEFT JOIN
    ("RIGHT", ""),   # RIGHT JOIN
    ("", "CROSS"),   # CROSS JOIN
]


class JoinTypeSwitchStrategy(MutationStrategy):
    """JOIN 类型切换：在 INNER/LEFT/RIGHT/CROSS JOIN 间随机切换。"""

    @property
    def id(self) -> str:
        return "join_type_switch"

    @property
    def description(self) -> str:
        return "切换 JOIN 类型（INNER/LEFT/RIGHT/CROSS）"

    @property
    def category(self) -> str:
        return "structural"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Join,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """随机切换 JOIN 类型。"""
        if not isinstance(node, exp.Join):
            return node

        current_side = node.args.get("side") or ""
        current_kind = node.args.get("kind") or ""

        alternatives = [
            (s, k) for s, k in _JOIN_VARIANTS
            if s != current_side or k != current_kind
        ]
        if not alternatives:
            return node

        new_side, new_kind = rng.choice(alternatives)
        node.set("side", new_side if new_side else None)
        node.set("kind", new_kind if new_kind else None)
        return node
