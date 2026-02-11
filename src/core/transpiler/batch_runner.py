"""批量转译编排模块。

负责递归扫描 SQL 文件、逐条转译、写入结果、生成报告。
业务验证错误抛出 ValueError，由 CLI 层统一捕获。
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.transpiler.dialect import Dialect
from src.core.transpiler.report import TranspileReport, TranspileReportSummary
from src.core.transpiler.transpiler import SQLTranspiler
from src.utils.logger import get_logger

logger = get_logger("transpiler.batch")

# 项目根目录 & 默认输出根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_RESULT_ROOT = _PROJECT_ROOT / "result"


@dataclass
class BatchTranspileResult:
    """批量转译运行结果。"""

    output_dir: Path
    report_path: Path
    total: int
    success: int
    failed: int
    elapsed_ms: float


class BatchTranspileRunner:
    """批量方言转译编排器。

    递归扫描输入目录 → 逐条转译 → 写入结果 → 生成报告。
    """

    def __init__(
        self,
        transpiler: Optional[SQLTranspiler] = None,
        result_root: Optional[Path] = None,
    ) -> None:
        self._transpiler = transpiler or SQLTranspiler()
        self._result_root = result_root or _RESULT_ROOT

    # ── 公开方法 ──

    def run(
        self,
        input_dir: Path,
        source: Dialect,
        target: Dialect,
    ) -> BatchTranspileResult:
        """执行批量转译，返回运行结果。

        验证失败抛出 ValueError（目录不存在、同方言、无 SQL 文件）。
        """
        input_dir = input_dir.resolve()
        self._validate(input_dir, source, target)

        sql_files = self._collect_sql_files(input_dir)
        if not sql_files:
            raise ValueError(f"未找到 .sql 文件: {input_dir}")

        logger.info(
            "批量转译: %s → %s | 输入: %s | 共 %d 个 SQL 文件",
            source.value, target.value, input_dir, len(sql_files),
        )

        output_dir = self._build_output_dir(source, target)
        output_dir.mkdir(parents=True, exist_ok=True)

        start_time = datetime.now()
        details: List[Dict[str, Any]] = []

        for sql_path in sql_files:
            relative = sql_path.relative_to(input_dir)
            sql_text = sql_path.read_text(encoding="utf-8").strip()

            try:
                result = self._transpiler.transpile(sql_text, source, target)
                self._write_transpiled_sql(output_dir, relative, result.sql)

                entry: Dict[str, Any] = {
                    "file": str(relative),
                    "status": "ok",
                    "rules_applied": result.rules_applied,
                    "warnings": result.warnings,
                    "error": None,
                }

                if result.warnings:
                    logger.warning("%s: 转译成功（有警告）: %s", relative, result.warnings)
                else:
                    logger.debug("%s: 转译成功", relative)

            except Exception as e:
                # 转译失败：写入原始 SQL 并标记
                self._write_transpiled_sql(
                    output_dir, relative, f"-- TRANSPILE ERROR: {e}\n{sql_text}"
                )
                entry = {
                    "file": str(relative),
                    "status": "error",
                    "rules_applied": [],
                    "warnings": [],
                    "error": str(e),
                }
                logger.warning("%s: 转译失败: %s", relative, e)

            details.append(entry)

        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000

        # 生成报告
        success = sum(1 for d in details if d["status"] == "ok")
        failed = sum(1 for d in details if d["status"] == "error")
        warned = sum(1 for d in details if d["status"] == "ok" and d["warnings"])

        summary = TranspileReportSummary(
            source_dialect=source.value,
            target_dialect=target.value,
            input_dir=str(input_dir),
            elapsed_ms=elapsed_ms,
            total=len(details),
            success=success,
            warned=warned,
            failed=failed,
        )
        report_path = TranspileReport.generate(output_dir, summary, details)

        return BatchTranspileResult(
            output_dir=output_dir,
            report_path=report_path,
            total=len(details),
            success=success,
            failed=failed,
            elapsed_ms=elapsed_ms,
        )

    # ── 私有方法 ──

    @staticmethod
    def _validate(input_dir: Path, source: Dialect, target: Dialect) -> None:
        """校验输入参数，失败抛 ValueError。"""
        if not input_dir.is_dir():
            raise ValueError(f"输入目录不存在: {input_dir}")
        if source == target:
            raise ValueError(f"源方言与目标方言相同 ({source.value})，无需转译")

    @staticmethod
    def _collect_sql_files(directory: Path) -> List[Path]:
        """递归收集目录下所有 .sql 文件，按路径排序。"""
        return sorted(directory.rglob("*.sql"))

    def _build_output_dir(self, source: Dialect, target: Dialect) -> Path:
        """构建输出目录：result/{时间戳}_{source}_{target}/。"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_name = f"{timestamp}_{source.value}_{target.value}"
        return self._result_root / dir_name

    @staticmethod
    def _write_transpiled_sql(
        output_dir: Path,
        relative_path: Path,
        sql: str,
    ) -> Path:
        """将转译后的 SQL 写入输出目录，保持原始目录层级。"""
        output_path = output_dir / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sql + "\n", encoding="utf-8")
        return output_path
