"""CAST 类型转换规则。

处理 SQLite 与 Oracle 之间 CAST 函数的行为差异。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule

# SQLite 中 CAST 目标为整型的 DataType 类型集合
_INT_TYPES = frozenset({
    exp.DataType.Type.INT,
    exp.DataType.Type.SMALLINT,
    exp.DataType.Type.BIGINT,
    exp.DataType.Type.TINYINT,
})

# CAST 目标为字符串类型的 DataType 类型集合
_VARCHAR_TYPES = frozenset({
    exp.DataType.Type.VARCHAR,
    exp.DataType.Type.NVARCHAR,
    exp.DataType.Type.TEXT,
    exp.DataType.Type.CHAR,
    exp.DataType.Type.NCHAR,
})


class CastIntToTruncRule(TranspilationRule):
    """将 SQLite 的 CAST(expr AS INTEGER/INT) 转译为 Oracle 的 TRUNC(expr)。

    SQLite CAST AS INTEGER 使用截断（向零取整），
    Oracle CAST AS INT 使用四舍五入。
    TRUNC() 在 Oracle 中也是截断，与 SQLite 语义一致。
    """

    @property
    def name(self) -> str:
        return "cast_int_to_trunc"

    @property
    def description(self) -> str:
        return "将 CAST(x AS INTEGER/INT) 转为 TRUNC(x) 以匹配 SQLite 的截断语义"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Cast):
                return node

            to_type = node.args.get("to")
            if to_type is None:
                return node

            # 获取 DataType 的 this（实际类型枚举）
            dtype = to_type
            if isinstance(to_type, exp.DataType):
                dtype_enum = to_type.this
            else:
                return node

            if dtype_enum not in _INT_TYPES:
                return node

            # CAST(expr AS INT) → TRUNC(expr)
            inner = node.this
            return exp.Anonymous(this="TRUNC", expressions=[inner.copy()])

        return self._transform(tree, _transform)


class CastDateToToCharRule(TranspilationRule):
    """将 SQLite 的 CAST(date_col AS VARCHAR) 转译为 Oracle 的 TO_CHAR(date_col, 'YYYY-MM-DD')。

    SQLite CAST(date AS VARCHAR) 保留 ISO 格式字符串（如 '1995-03-15'），
    Oracle CAST(date AS VARCHAR2) 使用 NLS_DATE_FORMAT（如 '15-MAR-95'），
    两者输出格式不同。使用 TO_CHAR 显式指定格式以确保一致性。
    """

    @property
    def name(self) -> str:
        return "cast_date_to_to_char"

    @property
    def description(self) -> str:
        return "将 CAST(date AS VARCHAR) 转为 TO_CHAR(date, 'YYYY-MM-DD') 以统一日期字符串格式"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Cast):
                return node

            to_type = node.args.get("to")
            if to_type is None:
                return node

            if not isinstance(to_type, exp.DataType):
                return node

            if to_type.this not in _VARCHAR_TYPES:
                return node

            inner = node.this

            # 检测表达式是否为日期列（列名含 date 或 _at 后缀）
            col_name = ""
            if isinstance(inner, exp.Column):
                col_name = inner.name.lower()
            elif isinstance(inner, exp.Identifier):
                col_name = inner.name.lower()

            if not (col_name.endswith("date") or col_name.endswith("_at")):
                return node

            # CAST(birth_date AS VARCHAR(20)) → TO_CHAR(birth_date, 'YYYY-MM-DD')
            return exp.Anonymous(
                this="TO_CHAR",
                expressions=[
                    inner.copy(),
                    exp.Literal.string("YYYY-MM-DD"),
                ],
            )

        return self._transform(tree, _transform)
