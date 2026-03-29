"""项目级常量定义。

统一管理项目根目录、输出目录等路径常量，避免在各模块中重复计算。
"""

from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
RESULT_ROOT: Path = PROJECT_ROOT / "result"
