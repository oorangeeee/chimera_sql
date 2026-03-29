"""标量子查询 FROM DUAL 补全规则。

SQLite 允许 (SELECT expr) 这种无 FROM 子句的标量子查询，
Oracle 不允许，必须写成 (SELECT expr FROM DUAL)。
此规则在 SQLite→Oracle 转译时自动补全 FROM DUAL。

此外，Oracle 不允许在 GROUP BY 中使用标量子查询（ORA-22818），
此规则会展开 GROUP BY 中的标量子查询为原始列引用。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class AddFromDualRule(TranspilationRule):
    """为无 FROM 子句的子查询添加 FROM DUAL（SQLite→Oracle）。

    处理顺序：
    1. 展开 GROUP BY 中的标量子查询为原始列引用（ORA-22818）
    2. 为剩余的无 FROM 标量子查询补 FROM DUAL
    """

    @property
    def name(self) -> str:
        return "add_from_dual"

    @property
    def description(self) -> str:
        return "为无 FROM 子句的子查询添加 FROM DUAL，展开 GROUP BY 中的标量子查询（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        # 步骤1: 展开 GROUP BY 中的标量子查询
        tree = self._unwrap_group_by_subqueries(tree)

        # 步骤2: 为剩余的无 FROM 标量子查询补 FROM DUAL
        tree = self._add_from_dual(tree)

        return tree

    @staticmethod
    def _unwrap_group_by_subqueries(tree: exp.Expression) -> exp.Expression:
        """展开 GROUP BY 中的单值标量子查询为原始列引用。

        Oracle 不允许在 GROUP BY 中使用子查询表达式（ORA-22818）。
        """

        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Group):
                return node
            new_exprs = []
            for expr in node.expressions:
                if isinstance(expr, exp.Subquery) and isinstance(expr.this, exp.Select):
                    inner = expr.this
                    if len(inner.expressions) == 1:
                        new_exprs.append(inner.expressions[0].copy())
                    else:
                        new_exprs.append(expr)
                else:
                    new_exprs.append(expr)
            node.set("expressions", new_exprs)
            return node

        return tree.transform(_transform)

    @staticmethod
    def _add_from_dual(tree: exp.Expression) -> exp.Expression:
        """为无 FROM 子句的子查询添加 FROM DUAL。"""

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
