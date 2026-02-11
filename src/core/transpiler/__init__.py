"""SQL 方言转译模块。

基于 SQLGlot + 自定义规则引擎的两阶段转译管线，
负责将 SQL 从源数据库方言转换为目标数据库方言。

公开 API:
    - SQLTranspiler: 转译器编排器（主入口）
    - Dialect: 数据库方言枚举
    - TranspilationRule: 规则抽象基类（扩展用）
    - TranspileResult: 转译结果数据类
    - RuleRegistry: 规则注册表
    - create_default_registry: 默认注册表工厂函数
    - BatchTranspileRunner: 批量转译编排器
    - BatchTranspileResult: 批量转译运行结果
"""

from .batch_runner import BatchTranspileResult, BatchTranspileRunner
from .dialect import Dialect
from .rule_base import TranspilationRule, TranspileResult
from .rule_registry import RuleRegistry, create_default_registry
from .transpiler import SQLTranspiler

__all__ = [
    "SQLTranspiler",
    "Dialect",
    "TranspilationRule",
    "TranspileResult",
    "RuleRegistry",
    "create_default_registry",
    "BatchTranspileRunner",
    "BatchTranspileResult",
]
