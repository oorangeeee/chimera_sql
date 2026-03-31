"""端到端模糊测试流水线模块。

编排变异引擎、方言转译器、数据库连接器，实现
种子 SQL → AST 变异 → 方言转译 → 多数据库执行 → 生成报告
的完整流水线。

公开 API:
    - CampaignRunner: 流水线编排器（主入口）
    - CampaignResult: 流水线运行结果
    - DatabaseEntry: 已注册数据库定义
    - load_databases: 从 config 加载已注册数据库列表
    - resolve_database: 根据方言名称查找已注册数据库
"""

from .runner import CampaignResult, CampaignRunner
from .target import DatabaseEntry, load_databases, resolve_database

__all__ = [
    "CampaignRunner",
    "CampaignResult",
    "DatabaseEntry",
    "load_databases",
    "resolve_database",
]
