"""边界值注入策略。

将数字字面量替换为边界值（0、-1、MAX_INT 等），
用于触发整数溢出、边界条件错误等缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from src.utils.config_loader import ConfigLoader
from ..strategy_base import MutationStrategy

# 默认边界值列表（config.yaml 未配置时使用）
_DEFAULT_BOUNDARY_VALUES = ["-1", "0", "1", "9999999", "2147483647", "-9999999"]


def _load_boundary_values() -> List[str]:
    """从 config.yaml 加载边界值列表，失败则使用默认值。"""
    try:
        config = ConfigLoader()
        values = config.get("mutation.builtin_rules.numeric_boundary_values")
        if isinstance(values, list) and values:
            return [str(v) for v in values]
    except FileNotFoundError:
        pass
    return list(_DEFAULT_BOUNDARY_VALUES)


class BoundaryInjectionStrategy(MutationStrategy):
    """边界值注入：将数字字面量替换为预定义边界值。"""

    def __init__(self) -> None:
        self._boundary_values = _load_boundary_values()

    @property
    def id(self) -> str:
        return "boundary_injection"

    @property
    def description(self) -> str:
        return "将数字字面量替换为边界值（0/-1/MAX_INT 等）"

    @property
    def category(self) -> str:
        return "generic"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Literal,)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """替换数字字面量为随机边界值。"""
        if not isinstance(node, exp.Literal) or not node.is_number:
            return node
        value = rng.choice(self._boundary_values)
        return exp.Literal.number(value)
