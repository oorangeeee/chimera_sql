"""能力画像模块。

从 SQLGlot 方言类自动提取布尔能力标志，
可选合并 config.yaml 中的手工覆盖，生成目标数据库的能力画像。
"""

from typing import Dict, Optional

from sqlglot import Generator, Parser
from sqlglot.dialects.dialect import Dialect

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("mutator.capability")


class CapabilityProfile:
    """数据库方言能力画像。

    能力标志以 "层级.属性名" 格式存储，例如：
    - "dialect.SUPPORTS_SEMI_ANTI_JOIN"
    - "generator.TRY_SUPPORTED"
    - "parser.STRING_ALIASES"
    """

    def __init__(self, dialect_name: str, flags: Dict[str, bool]) -> None:
        self._dialect_name = dialect_name
        self._flags = dict(flags)

    @property
    def dialect_name(self) -> str:
        """方言名称。"""
        return self._dialect_name

    @property
    def flags(self) -> Dict[str, bool]:
        """全部能力标志的只读副本。"""
        return dict(self._flags)

    def has(self, flag_name: str) -> bool:
        """查询是否具有指定能力。

        Args:
            flag_name: 能力标志名称（如 "generator.TRY_SUPPORTED"）。

        Returns:
            标志值；不存在时返回 False。
        """
        return self._flags.get(flag_name, False)

    # ── 工厂方法 ──

    @classmethod
    def from_sqlglot(cls, dialect_name: str) -> "CapabilityProfile":
        """从 SQLGlot 方言类自动提取全部布尔能力标志。

        Args:
            dialect_name: 方言名称（需为 SQLGlot 支持的小写名称，如 "sqlite"）。

        Returns:
            提取后的 CapabilityProfile 实例。

        Raises:
            ValueError: 方言名称不受支持。
        """
        try:
            dialect_obj = Dialect.get_or_raise(dialect_name)
        except ValueError:
            raise ValueError(f"不支持的方言: {dialect_name}")

        flags: Dict[str, bool] = {}
        dialect_cls = type(dialect_obj)

        # 提取 Dialect 类层级的布尔标志
        for attr_name, attr_val in vars(dialect_cls).items():
            if isinstance(attr_val, bool) and not attr_name.startswith("_"):
                flags[f"dialect.{attr_name}"] = attr_val

        # 提取 Generator 子类的布尔标志（与基类差异）
        gen_cls = dialect_obj.generator().__class__
        base_gen_flags = cls._collect_bool_flags(Generator)
        for attr_name, attr_val in cls._collect_bool_flags(gen_cls).items():
            key = f"generator.{attr_name}"
            flags[key] = attr_val

        # 提取 Parser 子类的布尔标志
        parser_cls = dialect_obj.parser().__class__
        for attr_name, attr_val in vars(parser_cls).items():
            if isinstance(attr_val, bool) and not attr_name.startswith("_"):
                flags[f"parser.{attr_name}"] = attr_val

        logger.debug("从 SQLGlot 提取方言 '%s' 的能力标志: %d 项", dialect_name, len(flags))
        return cls(dialect_name, flags)

    @classmethod
    def from_dialect_version(
        cls,
        dialect: str,
        version: Optional[str] = None,
    ) -> tuple:
        """公开入口：SQLGlot 自动提取 + config.yaml profile 合并。

        Args:
            dialect: 目标方言名称。
            version: 数据库版本标识（用于匹配 config.yaml profile）。

        Returns:
            (CapabilityProfile, capability_source) 元组。
            capability_source 为 "sqlglot_only" 或 "sqlglot+profile:{name}"。
        """
        profile = cls.from_sqlglot(dialect)

        # 尝试从 config.yaml 中查找匹配的 profile 并合并
        overrides, matched_profile = cls._load_config_overrides(dialect, version)
        if overrides:
            profile = profile.with_overrides(overrides)
            logger.debug("已合并 config.yaml 覆盖: %d 项", len(overrides))
            capability_source = f"sqlglot+profile:{matched_profile}"
        else:
            logger.warning(
                "mutation.profiles 中未找到 '%s' 版本 '%s' 的精调配置，"
                "能力画像仅基于 SQLGlot 自动提取。如需精调，请在 config.yaml 的 "
                "mutation.profiles 中添加对应条目。",
                dialect, version or "(未指定)",
            )
            capability_source = "sqlglot_only"

        return profile, capability_source

    def with_overrides(self, overrides: Dict[str, bool]) -> "CapabilityProfile":
        """返回合并覆盖后的新 Profile（不修改原对象）。

        Args:
            overrides: 要覆盖的标志字典。

        Returns:
            合并后的新 CapabilityProfile 实例。
        """
        merged = dict(self._flags)
        merged.update(overrides)
        return CapabilityProfile(self._dialect_name, merged)

    # ── 私有方法 ──

    @staticmethod
    def _collect_bool_flags(cls) -> Dict[str, bool]:
        """收集类及其 MRO 链上的全部布尔类属性。"""
        flags: Dict[str, bool] = {}
        for klass in reversed(cls.__mro__):
            for attr_name, attr_val in vars(klass).items():
                if isinstance(attr_val, bool) and not attr_name.startswith("_"):
                    flags[attr_name] = attr_val
        return flags

    @staticmethod
    def _load_config_overrides(
        dialect: str, version: Optional[str]
    ) -> tuple:
        """从 config.yaml mutation.profiles 中加载匹配的覆盖标志。

        Returns:
            (overrides_dict, matched_profile_name) 元组。
            未匹配时返回 ({}, "")。
        """
        try:
            config = ConfigLoader()
        except FileNotFoundError:
            return {}, ""

        profiles = config.get("mutation.profiles", {})
        if not isinstance(profiles, dict):
            return {}, ""

        # 遍历 profiles，查找 dbms 和 version 匹配的条目
        for profile_id, profile_data in profiles.items():
            if not isinstance(profile_data, dict):
                continue
            p_dbms = profile_data.get("dbms", "")
            p_version = profile_data.get("version", "")
            if p_dbms == dialect and (version is None or p_version == version):
                # 将 features 列表转为布尔标志
                overrides: Dict[str, bool] = {}
                for feat in profile_data.get("features", []):
                    overrides[f"feature.{feat}"] = True
                for feat in profile_data.get("disable_features", []):
                    overrides[f"feature.{feat}"] = False
                return overrides, profile_id

        return {}, ""

    def __repr__(self) -> str:
        true_count = sum(1 for v in self._flags.values() if v)
        return (
            f"CapabilityProfile(dialect={self._dialect_name!r}, "
            f"flags={len(self._flags)}, true={true_count})"
        )
