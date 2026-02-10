"""种子 SQL 全量回归测试脚本。

按 main.py 的三段式流程初始化数据（Schema → Data → Seeds），
随后在所有数据库上执行所有种子 SQL，并将结果记录到文件中。
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# 允许直接运行脚本时找到项目内模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.connector.factory import ConnectorFactory
from src.core.data_populator import DataPopulator
from src.core.schema_initializer import SchemaInitializer
from src.core.seed_generator import SeedGenerator
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("seed_sql_test")


def _project_root() -> Path:
    return PROJECT_ROOT


def _result_dir() -> Path:
    return _project_root() / "tests" / "seed_sql_test" / "result"


def _now_suffix() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_jsonable(value: Any) -> Any:
    """将结果值转换为可 JSON 序列化的形式。"""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    # datetime / date / time 等
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _rows_to_jsonable(rows: List[tuple]) -> List[List[Any]]:
    return [[_to_jsonable(v) for v in row] for row in rows]


def _collect_seed_files(seed_dir: Path) -> List[Path]:
    return sorted(seed_dir.rglob("*.sql"))


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def run() -> Path:
    logger.info("Seed SQL 全量测试启动 ...")

    # ── 阶段 1/2：对每种数据库执行 Schema 初始化 + 数据填充 ──
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

    # ── 读取种子 SQL ──
    config = ConfigLoader()
    seed_dir = config.get("fuzzing.seed_dir", "data/seeds")
    seed_root = (_project_root() / seed_dir).resolve()
    seed_files = _collect_seed_files(seed_root)

    if not seed_files:
        raise FileNotFoundError(f"未找到种子 SQL 文件: {seed_root}")

    logger.info("共发现 %d 个种子 SQL", len(seed_files))

    # ── 执行所有 SQL 并记录结果 ──
    results: List[Dict[str, Any]] = []

    connectors = {}
    for db_type in ("oracle", "sqlite"):
        conn = ConnectorFactory.create(db_type)
        conn.connect()
        connectors[db_type] = conn

    try:
        for seed_path in seed_files:
            sql_name = seed_path.relative_to(seed_root).as_posix()
            sql_text = _read_sql(seed_path)

            entry: Dict[str, Any] = {
                "sql_name": sql_name,
                "sql": sql_text,
                "databases": {},
            }

            for db_type in ("oracle", "sqlite"):
                connector = connectors[db_type]
                try:
                    rows = connector.execute_query(sql_text)
                    entry["databases"][db_type] = {
                        "status": "ok",
                        "row_count": len(rows),
                        "rows": _rows_to_jsonable(rows),
                        "error": None,
                    }
                except Exception as e:
                    entry["databases"][db_type] = {
                        "status": "error",
                        "row_count": 0,
                        "rows": [],
                        "error": str(e),
                    }

            results.append(entry)
    finally:
        for connector in connectors.values():
            connector.close()

    # ── 写入结果文件 ──
    _result_dir().mkdir(parents=True, exist_ok=True)
    output_path = _result_dir() / f"seed_sql_results_{_now_suffix()}.json"
    payload = {
        "generated_at": datetime.now().isoformat(),
        "seed_root": seed_root.as_posix(),
        "seed_count": len(seed_files),
        "results": results,
    }
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("结果已写入: %s", output_path)
    return output_path


if __name__ == "__main__":
    run()
