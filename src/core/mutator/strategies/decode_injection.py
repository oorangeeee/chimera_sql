"""DECODE 函数注入策略（Oracle 方言感知）。

将简单 CASE 表达式转换为 Oracle DECODE 函数，
用于检测 DECODE 与 CASE 语义差异缺陷。
"""

from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class DecodeInjectionStrategy(MutationStrategy):
    """DECODE 注入：将简单 CASE 表达式转换为 Oracle DECODE 函数。"""

    @property
    def id(self) -> str:
        return "decode_injection"

    @property
    def description(self) -> str:
        return "将简单 CASE 表达式转换为 Oracle DECODE 函数"

    @property
    def category(self) -> str:
        return "dialect_specific"

    @property
    def requires(self) -> List[str]:
        return ["feature.decode"]

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Case,)

    def mutate(self, node: exp.Expression, rng: Random) -> exp.Expression:
        """将简单 CASE 转换为 DECODE 函数。"""
        if not isinstance(node, exp.Case):
            return node

        # 仅处理简单 CASE（Case.this 非空）
        case_this = node.args.get("this")
        if case_this is None:
            return node

        # 构建 DECODE 参数列表: DECODE(expr, when1, then1, when2, then2, ..., else)
        exprs = [case_this.copy()]
        for if_expr in node.args.get("ifs", []):
            exprs.append(if_expr.this.copy())  # WHEN 值
            then_val = if_expr.args.get("true")
            if then_val is not None:
                exprs.append(then_val.copy())  # THEN 值

        default_val = node.args.get("default")
        if default_val is not None:
            exprs.append(default_val.copy())

        return exp.Anonymous(this="DECODE", expressions=exprs)
