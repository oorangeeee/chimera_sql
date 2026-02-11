"""转译规则抽象基类与转译结果数据类。"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List

import sqlglot.expressions as exp

from .dialect import Dialect


class TranspilationRule(ABC):
    """转译规则策略接口。

    每条规则负责处理一个特定的方言差异（如 json_extract → JSON_VALUE）。
    规则通过 apply() 方法接收 AST 树并返回变换后的 AST 树。
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """规则名称（用于日志和调试）。"""

    @property
    @abstractmethod
    def description(self) -> str:
        """规则的中文描述。"""

    @abstractmethod
    def apply(self, tree: exp.Expression) -> exp.Expression:
        """对 AST 树执行变换并返回变换后的树。

        Args:
            tree: SQLGlot AST 表达式树。

        Returns:
            变换后的 AST 表达式树。
        """

    def _transform(self, tree: exp.Expression, fn) -> exp.Expression:
        """便捷方法：包装 tree.transform(fn)，处理 None 返回值。

        Args:
            tree: 待变换的 AST 树。
            fn: 节点变换函数，签名为 (node) -> node | None。

        Returns:
            变换后的 AST 树。
        """
        result = tree.transform(fn)
        return result if result is not None else tree

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


@dataclass
class TranspileResult:
    """单条 SQL 的转译结果。

    Attributes:
        sql: 转译后的 SQL 字符串。
        source_dialect: 源方言。
        target_dialect: 目标方言。
        rules_applied: 已应用的规则名称列表。
        warnings: 转译过程中的警告信息列表。
    """

    sql: str
    source_dialect: Dialect
    target_dialect: Dialect
    rules_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
