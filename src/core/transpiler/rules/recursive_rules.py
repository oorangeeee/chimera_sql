"""WITH RECURSIVE 关键字处理规则。

Oracle 不支持 WITH RECURSIVE 语法（直接使用 WITH），
而 SQLite 要求递归 CTE 必须带 RECURSIVE 关键字。
SQLGlot 原生不处理此差异，需要自定义规则补充。

此外，SQLGlot 在序列化 SQLite 方言时会丢弃 CTE 的列名列表
（"Named columns are not supported in table alias"），
导致 SQLite→Oracle 转译时递归 CTE 缺少列名列表。
Oracle 要求递归 CTE 必须有列名列表（ORA-32039），
因此移除 RECURSIVE 时需要补回列名列表。
"""

import sqlglot.expressions as exp

from src.utils.logger import get_logger

from ..rule_base import TranspilationRule

logger = get_logger("transpiler.rules.recursive")


class RemoveRecursiveKeywordRule(TranspilationRule):
    """移除 WITH RECURSIVE 中的 RECURSIVE 关键字（SQLite→Oracle）。

    Oracle 的递归 CTE 语法不使用 RECURSIVE 关键字，
    直接写 WITH cte AS (...) 即可。

    同时，由于 SQLGlot 在 SQLite 方言序列化时会丢弃 CTE 列名列表，
    此规则还会检测递归 CTE 并从 SELECT 表达式列表中提取列名补回，
    确保 Oracle 不报 ORA-32039。
    """

    @property
    def name(self) -> str:
        return "remove_recursive_keyword"

    @property
    def description(self) -> str:
        return "移除 WITH RECURSIVE 关键字并补回 CTE 列名列表（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.With):
                return node
            if not node.args.get("recursive"):
                return node

            node.set("recursive", False)

            # 为递归 CTE 补回列名列表
            for cte in node.expressions:
                self._ensure_cte_columns(cte)

            return node

        return self._transform(tree, _transform)

    @staticmethod
    def _ensure_cte_columns(cte: exp.CTE) -> None:
        """确保 CTE 有列名列表，若缺失则从 SELECT 表达式提取。

        仅对递归 CTE（UNION ALL + 自引用）生效。
        """
        alias = cte.alias  # str, e.g. "depth_cte"
        ta = cte.args.get("alias")  # TableAlias or None
        if ta and hasattr(ta, "columns") and ta.columns:
            return  # 已有列名列表

        body = cte.this
        if not isinstance(body, exp.Union):
            return  # 非递归 CTE，不需要列名列表

        # 检测自引用
        tables = {t.name for t in body.find_all(exp.Table)}
        if alias not in tables:
            return

        # 从第一个 SELECT 的 expressions 提取列名
        first_select = body.left if isinstance(body.left, exp.Select) else body
        if not isinstance(first_select, exp.Select):
            return

        columns = []
        for expr in first_select.expressions:
            if isinstance(expr, exp.Alias):
                columns.append(exp.Identifier(this=expr.alias))
            elif isinstance(expr, exp.Column):
                columns.append(exp.Identifier(this=expr.name))
            else:
                # 字面量等：用表达式字符串作为列名
                columns.append(exp.Identifier(this=expr.name or f"col{len(columns)}"))

        if columns:
            new_ta = exp.TableAlias(
                this=exp.Identifier(this=alias),
                columns=columns,
            )
            cte.set("alias", new_ta)

        # Oracle 递归 CTE 要求 UNION ALL（ORA-32040）
        # 变异可能把 UNION ALL 改成 UNION，此处强制恢复
        if isinstance(body, exp.Union) and body.args.get("distinct"):
            logger.warning(
                "递归 CTE '%s' 的 UNION DISTINCT 被强制改为 UNION ALL（Oracle ORA-32040）",
                alias,
            )
            body.set("distinct", False)


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
