"""SQLite 特有函数转译规则。

处理 SQLite 独有的 DATE() 和 GROUP_CONCAT() 到 Oracle 的转译。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class DateFuncToToDateLiteralRule(TranspilationRule):
    """将 SQLite 的 DATE('...') 函数调用转译为 Oracle 的 DATE '...' 字面量。

    SQLite 使用 DATE('2023-01-01') 函数语法，
    Oracle 使用标准 SQL 日期字面量 DATE '2023-01-01'。

    AST 层面：SQLite 的 exp.Date → Oracle 的 exp.DateStrToDate。
    """

    @property
    def name(self) -> str:
        return "date_func_to_date_literal"

    @property
    def description(self) -> str:
        return "将 DATE('...') 转为 Oracle 的 DATE '...' 字面量 (exp.Date → exp.DateStrToDate)"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Date):
                return exp.DateStrToDate(this=node.this)
            return node

        return self._transform(tree, _transform)


class GroupConcatToListaggRule(TranspilationRule):
    """将 SQLite 的 GROUP_CONCAT 转译为 Oracle 的 LISTAGG。

    SQLite: GROUP_CONCAT(tag, ', ')
    Oracle: LISTAGG(tag, ', ') WITHIN GROUP (ORDER BY tag)

    AST 层面：exp.GroupConcat → Anonymous('LISTAGG') + exp.WithinGroup。
    """

    @property
    def name(self) -> str:
        return "group_concat_to_listagg"

    @property
    def description(self) -> str:
        return "将 GROUP_CONCAT(x, sep) 转为 LISTAGG(x, sep) WITHIN GROUP (ORDER BY x)"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.GroupConcat):
                return node

            arg = node.this
            separator = node.args.get("separator") or exp.Literal.string(",")

            listagg = exp.Anonymous(
                this="LISTAGG",
                expressions=[arg, separator],
            )
            return exp.WithinGroup(
                this=listagg,
                expression=exp.Order(expressions=[arg.copy()]),
            )

        return self._transform(tree, _transform)
