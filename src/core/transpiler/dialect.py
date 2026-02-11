"""支持的数据库方言枚举定义。"""

from enum import Enum


class Dialect(Enum):
    """数据库方言枚举，值映射到 SQLGlot 的 dialect 标识符。"""

    SQLITE = "sqlite"
    ORACLE = "oracle"
