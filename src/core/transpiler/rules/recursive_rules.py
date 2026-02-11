"""WITH RECURSIVE 关键字处理规则。

Oracle 不支持 WITH RECURSIVE 语法（直接使用 WITH），
而 SQLite 要求递归 CTE 必须带 RECURSIVE 关键字。
SQLGlot 原生不处理此差异，需要自定义规则补充。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class RemoveRecursiveKeywordRule(TranspilationRule):
    """移除 WITH RECURSIVE 中的 RECURSIVE 关键字（SQLite→Oracle）。

    Oracle 的递归 CTE 语法不使用 RECURSIVE 关键字，
    直接写 WITH cte AS (...) 即可。
    """

    @property
    def name(self) -> str:
        return "remove_recursive_keyword"

    @property
    def description(self) -> str:
        return "移除 WITH RECURSIVE 中的 RECURSIVE 关键字（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.With) and node.args.get("recursive"):
                node.set("recursive", False)
            return node

        return self._transform(tree, _transform)


class AddRecursiveKeywordRule(TranspilationRule):
    """为递归 CTE 添加 RECURSIVE 关键字（Oracle→SQLite）。

    通过启发式检测判断 CTE 是否为递归 CTE：
    1. CTE 体包含 UNION ALL
    2. CTE 体引用了自身别名（自引用）
    满足以上两个条件时，将 WITH 标记为 RECURSIVE。
    """

    @property
    def name(self) -> str:
        return "add_recursive_keyword"

    @property
    def description(self) -> str:
        return "为递归 CTE 添加 RECURSIVE 关键字（Oracle→SQLite）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.With):
                return node
            if node.args.get("recursive"):
                return node

            for cte in node.expressions:
                alias = cte.alias
                body = cte.this
                # 条件1：CTE 体包含 UNION / UNION ALL
                has_union = any(isinstance(n, exp.Union) for n in body.walk())
                if not has_union:
                    continue
                # 条件2：CTE 体引用了自身别名（自引用）
                tables = {t.name for t in body.find_all(exp.Table)}
                if alias in tables:
                    node.set("recursive", True)
                    break

            return node

        return self._transform(tree, _transform)
