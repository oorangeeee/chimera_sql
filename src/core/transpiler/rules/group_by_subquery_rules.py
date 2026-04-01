"""GROUP BY 标量子查询展开规则。

Oracle 不允许在 GROUP BY 中使用标量子查询（ORA-22818），
此规则将 GROUP BY 中的单值标量子查询展开为原始列引用。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class UnwrapGroupBySubqueriesRule(TranspilationRule):
    """展开 GROUP BY 中的单值标量子查询为原始列引用（SQLite→Oracle）。

    Oracle 不允许在 GROUP BY 中使用子查询表达式（ORA-22818）。
    当 GROUP BY 中的标量子查询仅包含单个 SELECT 表达式时，
    将其展开为原始列引用。
    """

    @property
    def name(self) -> str:
        return "unwrap_group_by_subqueries"

    @property
    def description(self) -> str:
        return "展开 GROUP BY 中的标量子查询为原始列引用（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
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
