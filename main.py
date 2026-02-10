"""ChimeraSQL — 跨数据库模糊测试工具入口。

三阶段初始化流水线：
  1. SchemaInitializer: 在 Oracle/SQLite 中创建统一测试表结构
  2. DataPopulator: 填充覆盖边界条件的测试数据
  3. SeedGenerator: 生成种子 SQL 文件供后续 AST 变异使用
"""

from src.connector.factory import ConnectorFactory
from src.core.data_populator import DataPopulator
from src.core.schema_initializer import SchemaInitializer
from src.core.seed_generator import SeedGenerator
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger


def main():
    logger = get_logger("chimera")
    logger.info("ChimeraSQL starting...")

    config = ConfigLoader()
    logger.info("Configuration loaded")

    # ── 阶段 1 & 2：对每种数据库执行 Schema 初始化 + 数据填充 ──
    for db_type in ("oracle", "sqlite"):
        logger.info("=" * 50)
        connector = ConnectorFactory.create(db_type)
        connector.connect()

        SchemaInitializer(connector, db_type).initialize()
        DataPopulator(connector, db_type).populate_all()

        connector.close()

    # ── 阶段 3：生成种子 SQL 文件 ──
    logger.info("=" * 50)
    SeedGenerator().generate_all()

    logger.info("=" * 50)
    logger.info("初始化流水线完成！")
    # 后续待实现：AST 变异 → 转译 → 差分测试 → 报告


if __name__ == "__main__":
    main()
