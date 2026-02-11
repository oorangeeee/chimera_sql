"""种子 SQL 全量回归测试脚本。

按 main.py 的三段式流程初始化数据（Schema → Data → Seeds），
随后通过方言转译器将种子 SQL 转换为各目标数据库方言，
在所有数据库上执行转译后的 SQL，并将结果记录到文件中。
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
from src.testbed import DataPopulator, SchemaInitializer, SeedGenerator
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("seed_sql_test")

# 数据库类型到 Dialect 枚举的映射
_DB_TYPE_TO_DIALECT: Dict[str, Dialect] = {
    "oracle": Dialect.ORACLE,
    "sqlite": Dialect.SQLITE,
}

# 种子 SQL 的源方言（所有种子均以 SQLite 语法编写）
_SEED_DIALECT = Dialect.SQLITE


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


def _rules_to_jsonable(rules: Dict[tuple, List[str]]) -> Dict[str, List[str]]:
    return {f"{s.value}->{t.value}": names for (s, t), names in rules.items()}


def _collect_seed_files(seed_dir: Path) -> List[Path]:
    return sorted(seed_dir.rglob("*.sql"))


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def run() -> Path:
    logger.info("Seed SQL 全量测试启动 ...")

    # ── 初始化转译器 ──
    transpiler = SQLTranspiler()
    logger.info(
        "转译器初始化完成，注册规则: %s",
        _rules_to_jsonable(transpiler.registry.list_rules()),
    )

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
                target_dialect = _DB_TYPE_TO_DIALECT[db_type]

                # 通过转译器将种子 SQL 转换为目标方言
                transpile_result = transpiler.transpile(
                    sql_text, _SEED_DIALECT, target_dialect
                )
                exec_sql = transpile_result.sql

                try:
                    rows = connector.execute_query(exec_sql)
                    entry["databases"][db_type] = {
                        "status": "ok",
                        "row_count": len(rows),
                        "rows": _rows_to_jsonable(rows),
                        "error": None,
                        "executed_sql": exec_sql,
                        "rules_applied": transpile_result.rules_applied,
                        "transpile_warnings": transpile_result.warnings,
                    }
                except Exception as e:
                    logger.warning(
                        "%s 执行失败 [%s]: %s | SQL: %s",
                        sql_name,
                        db_type,
                        e,
                        exec_sql[:200],
                    )
                    entry["databases"][db_type] = {
                        "status": "error",
                        "row_count": 0,
                        "rows": [],
                        "error": str(e),
                        "executed_sql": exec_sql,
                        "rules_applied": transpile_result.rules_applied,
                        "transpile_warnings": transpile_result.warnings,
                    }

            results.append(entry)
    finally:
        for connector in connectors.values():
            connector.close()

    # ── 统计与汇总 ──
    for db_type in ("oracle", "sqlite"):
        ok_count = sum(
            1 for r in results if r["databases"].get(db_type, {}).get("status") == "ok"
        )
        err_count = sum(
            1
            for r in results
            if r["databases"].get(db_type, {}).get("status") == "error"
        )
        logger.info(
            "[%s] 执行结果: %d 成功, %d 失败, 共 %d 条",
            db_type,
            ok_count,
            err_count,
            len(results),
        )

    # ── 写入结果文件 ──
    _result_dir().mkdir(parents=True, exist_ok=True)
    output_path = _result_dir() / f"seed_sql_results_{_now_suffix()}.json"
    payload = {
        "generated_at": datetime.now().isoformat(),
        "seed_root": seed_root.as_posix(),
        "seed_count": len(seed_files),
        "transpiler_rules": _rules_to_jsonable(transpiler.registry.list_rules()),
        "results": results,
    }
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("结果已写入: %s", output_path)
    return output_path


if __name__ == "__main__":
    run()
