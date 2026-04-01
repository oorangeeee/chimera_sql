"""标量子查询 FROM DUAL 补全规则。

SQLite 允许 (SELECT expr) 这种无 FROM 子句的标量子查询，
Oracle 不允许，必须写成 (SELECT expr FROM DUAL)。
此规则在 SQLite→Oracle 转译时自动补全 FROM DUAL。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class AddFromDualRule(TranspilationRule):
    """为无 FROM 子句的子查询添加 FROM DUAL（SQLite→Oracle）。"""

    @property
    def name(self) -> str:
        return "add_from_dual"

    @property
    def description(self) -> str:
        return "为无 FROM 子句的子查询添加 FROM DUAL（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Subquery):
                return node
            inner = node.this
            if not isinstance(inner, exp.Select):
                return node
            if inner.args.get("from_") is not None:
                return node
            if len(inner.expressions) == 1:
                dual = exp.From(
                    this=exp.Table(this=exp.Identifier(this="DUAL"))
                )
                inner.set("from_", dual)
            return node

        return tree.transform(_transform)
