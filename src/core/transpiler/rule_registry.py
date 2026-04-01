"""转译规则注册表，按 (源方言, 目标方言) 方向管理规则链。"""

from typing import Dict, List, Tuple

from .dialect import Dialect
from .rule_base import TranspilationRule
from .rules import (
    AddFromDualRule,
    AddRecursiveKeywordRule,
    DateFuncToToDateLiteralRule,
    FixAggregateStarRule,
    GroupConcatToListaggRule,
    JsonExtractToJsonValueRule,
    JsonValueToJsonExtractRule,
    RemoveRecursiveKeywordRule,
    UnwrapGroupBySubqueriesRule,
)


class RuleRegistry:
    """规则注册表，按转译方向管理有序规则链。

    每个 (source_dialect, target_dialect) 方向维护一条规则链，
    转译时按注册顺序依次执行。支持动态注册和查询。
    """

    def __init__(self) -> None:
        self._rules: Dict[Tuple[Dialect, Dialect], List[TranspilationRule]] = {}

    def register(
        self, source: Dialect, target: Dialect, rule: TranspilationRule
    ) -> None:
        """注册一条规则到指定的转译方向。

        Args:
            source: 源方言。
            target: 目标方言。
            rule: 转译规则实例。
        """
        key = (source, target)
        if key not in self._rules:
            self._rules[key] = []
        self._rules[key].append(rule)

    def get_rules(self, source: Dialect, target: Dialect) -> List[TranspilationRule]:
        """获取指定转译方向的规则链（返回副本）。

        Args:
            source: 源方言。
            target: 目标方言。

        Returns:
            规则列表（按注册顺序）。若无注册规则则返回空列表。
        """
        return list(self._rules.get((source, target), []))

    def list_rules(self) -> Dict[Tuple[Dialect, Dialect], List[str]]:
        """列出所有已注册的规则（按方向分组，返回规则名称）。

        Returns:
            字典，键为 (source, target) 元组，值为规则名称列表。
        """
        return {
            key: [r.name for r in rules] for key, rules in self._rules.items()
        }

    def __repr__(self) -> str:
        summary = self.list_rules()
        parts = [
            f"  {s.value}→{t.value}: {names}" for (s, t), names in summary.items()
        ]
        return f"RuleRegistry(\n{chr(10).join(parts)}\n)"


def create_default_registry() -> RuleRegistry:
    """创建并返回包含默认规则的注册表。

    默认规则链：
    - SQLite→Oracle: JsonExtractToJsonValue, RemoveRecursiveKeyword
    - Oracle→SQLite: JsonValueToJsonExtract, AddRecursiveKeyword
    """
    registry = RuleRegistry()

    # SQLite → Oracle
    registry.register(Dialect.SQLITE, Dialect.ORACLE, JsonExtractToJsonValueRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, DateFuncToToDateLiteralRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, GroupConcatToListaggRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, RemoveRecursiveKeywordRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, UnwrapGroupBySubqueriesRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, AddFromDualRule())
    registry.register(Dialect.SQLITE, Dialect.ORACLE, FixAggregateStarRule())

    # Oracle → SQLite
    registry.register(Dialect.ORACLE, Dialect.SQLITE, JsonValueToJsonExtractRule())
    registry.register(Dialect.ORACLE, Dialect.SQLITE, AddRecursiveKeywordRule())

    return registry
