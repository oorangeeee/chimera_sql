"""AST 变异引擎模块。

基于策略模式的 AST 级变异引擎，通过能力画像门控
确保生成的变异 SQL 对目标数据库方言合法。

公开 API:
    - MutationEngine: 单条 SQL 变异编排器
    - BatchMutationRunner: 批量变异编排器
    - BatchMutationResult: 批量变异运行结果
    - CapabilityProfile: 数据库方言能力画像
    - MutationStrategy: 变异策略抽象基类（扩展用）
    - MutationResult: 单条变异结果数据类
    - StrategyRegistry: 策略注册表
    - create_default_registry: 默认注册表工厂函数
"""

from .batch_runner import BatchMutationResult, BatchMutationRunner
from .capability import CapabilityProfile
from .engine import MutationEngine
from .strategy_base import MutationResult, MutationStrategy
from .strategy_registry import StrategyRegistry, create_default_registry

__all__ = [
    "MutationEngine",
    "BatchMutationRunner",
    "BatchMutationResult",
    "CapabilityProfile",
    "MutationStrategy",
    "MutationResult",
    "StrategyRegistry",
    "create_default_registry",
]
