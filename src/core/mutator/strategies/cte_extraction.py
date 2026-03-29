"""CTE 提取策略。

将 FROM 子句中的派生表（子查询）提取为 CTE，
用于检测 CTE 解析和引用语义缺陷。
"""

from random import Random
from typing import Tuple, Type

import sqlglot.expressions as exp

from ..strategy_base import MutationStrategy


class CTEExtractionStrategy(MutationStrategy):
    """CTE 提取：将 FROM 子句中的派生表提取为 CTE。"""

    @property
    def id(self) -> str:
        return "cte_extraction"

    @property
    def description(self) -> str:
        return "将 FROM 子句中的派生表（子查询）提取为 CTE"

    @property
    def category(self) -> str:
        return "structural"

    @property
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        return (exp.Subquery,)

    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """将派生表提取为 CTE。"""
        if not isinstance(node, exp.Subquery):
            return node

        # 仅处理 FROM 子句中的派生表（跳过标量子查询等）
        if not isinstance(node.parent, exp.From):
            return node

        # 向上查找包含该 FROM 的 Select 语句
        parent_select = node.parent.parent
        if not isinstance(parent_select, exp.Select):
            return node

        inner_select = node.this
        if not isinstance(inner_select, exp.Select):
            return node

        # 生成 CTE 别名
        cte_name = f"_cte_{rng.randint(0, 99)}"

        # 构建 CTE 定义
        cte_alias = exp.TableAlias(this=exp.Identifier(this=cte_name))
        cte = exp.CTE(this=inner_select.copy(), alias=cte_alias)

        # 合并到已有的 WITH 子句或创建新的
        existing_with = parent_select.args.get("with_")
        if existing_with:
            cte_list = list(existing_with.expressions) + [cte]
            new_with = exp.With(expressions=cte_list)
        else:
            new_with = exp.With(expressions=[cte])

        parent_select.set("with_", new_with)

        # 将子查询替换为表引用
        table_ref = exp.Table(this=exp.Identifier(this=cte_name))
        node.replace(table_ref)

        # 返回原始节点（已从树中脱离），引擎检测到相同引用后跳过 replace()
        return node
