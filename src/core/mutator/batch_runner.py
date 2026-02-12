"""批量变异编排模块。

负责递归扫描 SQL 文件、逐条变异、写入结果、生成报告。
业务验证错误抛出 ValueError，由 CLI 层统一捕获。
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from random import Random
from typing import Any, Dict, List, Optional

from src.utils.logger import get_logger

from .capability import CapabilityProfile
from .engine import MutationEngine
from .report import MutationReport, MutationReportSummary
from .strategy_registry import create_default_registry

logger = get_logger("mutator.batch")

# 项目根目录 & 默认输出根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_RESULT_ROOT = _PROJECT_ROOT / "result"


@dataclass
class BatchMutationResult:
    """批量变异运行结果。"""

    output_dir: Path
    report_path: Path
    total_seeds: int
    total_generated: int
    failed_seeds: int
    elapsed_ms: float


class BatchMutationRunner:
    """批量 AST 变异编排器。

    递归扫描输入目录 → 构建能力画像 → 逐条变异 →
    写入结果文件 → 生成报告。
    """

    def __init__(self, result_root: Optional[Path] = None) -> None:
        self._result_root = result_root or _RESULT_ROOT

    def run(
        self,
        input_dir: Path,
        dialect: str,
        version: Optional[str] = None,
        count_per_seed: int = 3,
        random_seed: Optional[int] = None,
    ) -> BatchMutationResult:
        """执行批量变异，返回运行结果。

        Args:
            input_dir: 包含种子 .sql 文件的输入目录。
            dialect: 目标数据库方言（如 "sqlite", "oracle"）。
            version: 可选的数据库版本标识。
            count_per_seed: 每条种子生成的变异数量。
            random_seed: 可选的随机种子（用于可复现结果）。

        Returns:
            批量变异运行结果。

        Raises:
            ValueError: 输入参数校验失败。
        """
        input_dir = input_dir.resolve()
        self._validate(input_dir)

        # 构建能力画像（方言校验在此完成）
        profile = CapabilityProfile.from_dialect_version(dialect, version)

        # 构建策略注册表 & 引擎
        registry = create_default_registry()
        rng = Random(random_seed) if random_seed is not None else Random()
        engine = MutationEngine(profile, registry, rng)

        # 收集 SQL 文件
        sql_files = self._collect_sql_files(input_dir)
        if not sql_files:
            raise ValueError(f"未找到 .sql 文件: {input_dir}")

        logger.info(
            "批量变异: dialect=%s | 输入: %s | 共 %d 个 SQL 文件 | 每条生成 %d 个变异",
            dialect, input_dir, len(sql_files), count_per_seed,
        )

        # 构建输出目录
        output_dir = self._build_output_dir(dialect)
        output_dir.mkdir(parents=True, exist_ok=True)

        start_time = datetime.now()
        details: List[Dict[str, Any]] = []
        total_generated = 0

        for sql_path in sql_files:
            relative = sql_path.relative_to(input_dir)
            sql_text = sql_path.read_text(encoding="utf-8").strip()

            try:
                results = engine.mutate_many(sql_text, str(relative), count_per_seed)

                # 写入变异文件
                for idx, mr in enumerate(results, start=1):
                    out_name = f"{sql_path.stem}_mut{idx:02d}.sql"
                    out_relative = relative.parent / out_name
                    self._write_sql(output_dir, out_relative, mr.sql)

                # 收集所有已应用策略
                all_strategies = set()
                all_warnings = []
                for mr in results:
                    all_strategies.update(mr.strategies_applied)
                    all_warnings.extend(mr.warnings)

                entry: Dict[str, Any] = {
                    "file": str(relative),
                    "status": "ok",
                    "generated": len(results),
                    "strategies_used": sorted(all_strategies),
                    "warnings": all_warnings,
                    "error": "",
                }
                total_generated += len(results)

                if all_warnings:
                    logger.warning(
                        "%s: 生成 %d 个变异（有警告）", relative, len(results),
                    )
                else:
                    logger.debug("%s: 生成 %d 个变异", relative, len(results))

            except Exception as e:
                entry = {
                    "file": str(relative),
                    "status": "error",
                    "generated": 0,
                    "strategies_used": [],
                    "warnings": [],
                    "error": str(e),
                }
                logger.warning("%s: 变异失败: %s", relative, e)

            details.append(entry)

        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000

        # 生成报告
        failed_seeds = sum(1 for d in details if d["status"] == "error")
        summary = MutationReportSummary(
            dialect=dialect,
            version=version or "",
            input_dir=str(input_dir),
            elapsed_ms=elapsed_ms,
            total_seeds=len(details),
            total_generated=total_generated,
            failed_seeds=failed_seeds,
        )
        report_path = MutationReport.generate(output_dir, summary, details)

        return BatchMutationResult(
            output_dir=output_dir,
            report_path=report_path,
            total_seeds=len(details),
            total_generated=total_generated,
            failed_seeds=failed_seeds,
            elapsed_ms=elapsed_ms,
        )

    # ── 私有方法 ──

    @staticmethod
    def _validate(input_dir: Path) -> None:
        """校验输入参数，失败抛 ValueError。"""
        if not input_dir.is_dir():
            raise ValueError(f"输入目录不存在: {input_dir}")

    @staticmethod
    def _collect_sql_files(directory: Path) -> List[Path]:
        """递归收集目录下所有 .sql 文件，按路径排序。"""
        return sorted(directory.rglob("*.sql"))

    def _build_output_dir(self, dialect: str) -> Path:
        """构建输出目录：result/mutate_{时间戳}_{dialect}/。"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_name = f"mutate_{timestamp}_{dialect}"
        return self._result_root / dir_name

    @staticmethod
    def _write_sql(output_dir: Path, relative_path: Path, sql: str) -> Path:
        """将变异后的 SQL 写入输出目录，保持原始目录层级。"""
        output_path = output_dir / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sql + "\n", encoding="utf-8")
        return output_path
