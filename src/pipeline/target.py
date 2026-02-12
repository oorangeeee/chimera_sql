"""目标数据库定义与加载。

从 config.yaml 的 targets 节读取目标数据库配置，
并映射为 TargetDatabase 数据类供流水线使用。
"""

from dataclasses import dataclass
from typing import List, Optional

from src.utils.config_loader import ConfigLoader


@dataclass(frozen=True)
class TargetDatabase:
    """目标数据库定义。

    Attributes:
        name: 配置中的 key，如 "oracle_xe"。
        db_type: ConnectorFactory.create() 参数，如 "oracle" / "sqlite"。
        dialect: Dialect 枚举值，用于转译和变异能力画像，如 "oracle" / "sqlite"。
        version: 数据库版本，用于变异能力画像的版本匹配，如 "21c"。
    """

    name: str
    db_type: str
    dialect: str
    version: str


def load_targets(names: Optional[List[str]] = None) -> List[TargetDatabase]:
    """从 config.yaml targets 节加载目标数据库定义。

    Args:
        names: 要加载的目标名称列表。None 表示加载全部。

    Returns:
        TargetDatabase 列表。

    Raises:
        ValueError: targets 节不存在、为空，或指定名称不存在。
    """
    config = ConfigLoader()
    targets_raw = config.get("targets")

    if not targets_raw or not isinstance(targets_raw, dict):
        raise ValueError(
            "config.yaml 中未找到有效的 targets 配置节。"
            "请参考 config.template.yaml 添加 targets 定义。"
        )

    # 确定要加载的名称列表
    if names is None:
        load_names = list(targets_raw.keys())
    else:
        load_names = names

    if not load_names:
        raise ValueError("目标列表为空，请指定至少一个目标数据库。")

    # 校验所有名称存在
    missing = [n for n in load_names if n not in targets_raw]
    if missing:
        available = list(targets_raw.keys())
        raise ValueError(
            f"以下目标名称在 config.yaml 中不存在: {missing}。"
            f"可用目标: {available}"
        )

    # 构建 TargetDatabase 列表
    result: List[TargetDatabase] = []
    for name in load_names:
        entry = targets_raw[name]
        result.append(
            TargetDatabase(
                name=name,
                db_type=entry.get("db_type", ""),
                dialect=entry.get("dialect", ""),
                version=entry.get("version", ""),
            )
        )

    return result
