"""变异策略抽象基类与变异结果数据类。"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from random import Random
from typing import List, Tuple, Type

import sqlglot.expressions as exp


class MutationStrategy(ABC):
    """变异策略接口。

    每条策略负责一种 AST 级别的变异操作（如边界值注入、谓词取反）。
    策略通过 mutate() 方法接收目标节点并返回变异后的节点。
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """策略唯一标识符。"""

    @property
    @abstractmethod
    def description(self) -> str:
        """策略的中文描述。"""

    @property
    @abstractmethod
    def category(self) -> str:
        """策略分类（generic / dialect_specific）。"""

    @property
    def requires(self) -> List[str]:
        """策略所需的能力标志列表。默认为空（所有方言均可用）。"""
        return []

    @property
    @abstractmethod
    def node_types(self) -> Tuple[Type[exp.Expression], ...]:
        """策略目标 AST 节点类型元组。"""

    @abstractmethod
    def mutate(self, node: exp.Expression, rng: Random, dialect: str | None = None) -> exp.Expression:
        """对目标 AST 节点执行变异并返回变异后的节点。

        Args:
            node: 目标 AST 节点。
            rng: 随机数生成器（确保可复现）。
            dialect: 当前 SQL 的方言名称（策略可用于方言感知的变异）。

        Returns:
            变异后的 AST 节点。
        """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"


@dataclass
class MutationResult:
    """单条 SQL 的变异结果。

    Attributes:
        sql: 变异后的 SQL 字符串。
        seed_file: 源种子文件路径。
        strategies_applied: 已应用的策略 ID 列表。
        warnings: 变异过程中的警告信息列表。
    """

    sql: str
    seed_file: str
    strategies_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
