"""端到端模糊测试流水线模块。

编排变异引擎、方言转译器、数据库连接器，实现
种子 SQL → AST 变异 → 方言转译 → 多数据库执行 → 生成报告
的完整流水线。

公开 API:
    - CampaignRunner: 流水线编排器（主入口）
    - CampaignResult: 流水线运行结果
    - TargetDatabase: 目标数据库定义
    - load_targets: 从 config 加载目标列表
"""

from .runner import CampaignResult, CampaignRunner
from .target import TargetDatabase, load_targets

__all__ = [
    "CampaignRunner",
    "CampaignResult",
    "TargetDatabase",
    "load_targets",
]
