"""EXCEPT / MINUS 集合操作转换规则。

Oracle 21c 已原生支持 EXCEPT，因此这些规则默认不注册到规则链中，
仅在需要兼容旧版 Oracle 时手动注册。

SQLGlot 自动将 Oracle 的 MINUS 解析为 exp.Except，
生成 SQLite 方言时自动输出 EXCEPT，因此 MinusToExceptRule 仅作占位。
"""

import re

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class ExceptToMinusRule(TranspilationRule):
    """将 EXCEPT 转换为 MINUS（SQLite→旧版Oracle）。

    默认不注册。Oracle 21c 已支持 EXCEPT，仅在兼容旧版 Oracle 时使用。
    由于 EXCEPT 在 AST 中以 exp.Except 节点表示，直接替换为 MINUS 存在
    类型系统限制，因此在 AST 上标记 meta，由 SQLTranspiler 在生成后
    执行文本级替换。
    """

    @property
    def name(self) -> str:
        return "except_to_minus"

    @property
    def description(self) -> str:
        return "将 EXCEPT 替换为 MINUS（兼容旧版 Oracle，默认不启用）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        has_except = any(isinstance(n, exp.Except) for n in tree.walk())
        if has_except:
            # 标记 meta，由 SQLTranspiler 在生成 SQL 后执行文本替换
            tree.meta["except_to_minus"] = True
        return tree

    @staticmethod
    def post_process(sql: str) -> str:
        """在生成的 SQL 字符串中将 EXCEPT 替换为 MINUS。"""
        return re.sub(r"\bEXCEPT\b", "MINUS", sql)


class MinusToExceptRule(TranspilationRule):
    """将 MINUS 转换为 EXCEPT（Oracle→SQLite）。

    占位规则。SQLGlot 已自动将 Oracle 的 MINUS 解析为 exp.Except，
    SQLite 方言生成器自动输出 EXCEPT，无需额外处理。
    """

    @property
    def name(self) -> str:
        return "minus_to_except"

    @property
    def description(self) -> str:
        return "将 MINUS 转换为 EXCEPT（Oracle→SQLite，SQLGlot 已自动处理）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        # SQLGlot 自动处理，无需变换
        return tree
