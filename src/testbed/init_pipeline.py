"""初始化流水线模块。

编排三阶段初始化：Schema → Data → Seeds。
"""

from pathlib import Path
from typing import Sequence

from src.connector.factory import ConnectorFactory
from src.testbed.data_populator import DataPopulator
from src.testbed.schema_initializer import SchemaInitializer
from src.testbed.seed_generator import SeedGenerator
from src.utils.logger import get_logger

logger = get_logger("init_pipeline")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class InitPipeline:
    """初始化测试基础设施的三阶段流水线。

    阶段：Schema 初始化 → 数据填充 → 种子 SQL 生成。
    """

    def __init__(self, db_types: Sequence[str] = ("oracle", "sqlite")) -> None:
        self._db_types = db_types

    def run(self) -> None:
        """执行完整初始化流水线。"""
        logger.info("ChimeraSQL 初始化流水线启动 ...")

        for db_type in self._db_types:
            logger.info("=" * 50)
            connector = ConnectorFactory.create(db_type)
            connector.connect()

            SchemaInitializer(connector, db_type).initialize()
            DataPopulator(connector, db_type).populate_all()

            connector.close()

        # 模板引擎需要读取 SQLite 数据库来反射 schema
        sqlite_db_path = str(_PROJECT_ROOT / "data" / "test.db")
        logger.info("=" * 50)
        SeedGenerator(db_path=sqlite_db_path).generate_all()

        logger.info("=" * 50)
        logger.info("初始化流水线完成！")
