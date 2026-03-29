"""转译正确率验证运行器。

执行种子 SQL → 转译 → 在源/目标数据库执行 → 比较结果 → 生成报告。
提供两级指标：Level 1 执行通过率、Level 2 语义等价率。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.connector.factory import ConnectorFactory
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.json_utils import rows_to_jsonable
from src.utils.logger import get_logger

from .comparator import (
    ComparatorConfig,
    ComparisonDetail,
    ComparisonVerdict,
    ResultComparator,
)

logger = get_logger("verifier")

# 方言到数据库类型的映射
_DIALECT_TO_DB: Dict[Dialect, str] = {
    Dialect.SQLITE: "sqlite",
    Dialect.ORACLE: "oracle",
}


@dataclass
class VerifyMetrics:
    """验证指标汇总。"""

    total: int = 0
    # Level 1: 执行通过率
    source_exec_ok: int = 0
    target_exec_ok: int = 0
    source_exec_fail: int = 0
    target_exec_fail: int = 0
    # Level 2: 语义等价率
    equivalent: int = 0
    partial_match: int = 0
    mismatch: int = 0
    skipped: int = 0

    @property
    def execution_pass_rate(self) -> float:
        """Level 1: 目标库执行成功率。"""
        return self.target_exec_ok / self.total if self.total else 0.0

    @property
    def equivalence_rate(self) -> float:
        """Level 2: 双端结果等价率。"""
        denom = self.equivalent + self.partial_match + self.mismatch
        return self.equivalent / denom if denom else 0.0


@dataclass
class VerifyReport:
    """验证运行报告。"""

    metrics: VerifyMetrics
    details: List[ComparisonDetail]
    source_dialect: Dialect
    target_dialect: Dialect
    report_path: Path
    json_path: Path
    elapsed_ms: float

    def to_dict(self) -> Dict[str, Any]:
        """转为可 JSON 序列化的字典。"""
        return {
            "generated_at": datetime.now().isoformat(),
            "source_dialect": self.source_dialect.value,
            "target_dialect": self.target_dialect.value,
            "elapsed_ms": self.elapsed_ms,
            "metrics": {
                "total": self.metrics.total,
                "execution_pass_rate": round(self.metrics.execution_pass_rate * 100, 2),
                "equivalence_rate": round(self.metrics.equivalence_rate * 100, 2),
                "source_exec_ok": self.metrics.source_exec_ok,
                "target_exec_ok": self.metrics.target_exec_ok,
                "source_exec_fail": self.metrics.source_exec_fail,
                "target_exec_fail": self.metrics.target_exec_fail,
                "equivalent": self.metrics.equivalent,
                "partial_match": self.metrics.partial_match,
                "mismatch": self.metrics.mismatch,
                "skipped": self.metrics.skipped,
            },
            "details": [self._detail_to_dict(d) for d in self.details],
        }

    @staticmethod
    def _detail_to_dict(d: ComparisonDetail) -> Dict[str, Any]:
        return {
            "sql_name": d.sql_name,
            "verdict": d.verdict.value,
            "source_row_count": d.source_row_count,
            "target_row_count": d.target_row_count,
            "source_status": d.source_status,
            "target_status": d.target_status,
            "source_error": d.source_error,
            "target_error": d.target_error,
            "source_sql": d.source_sql,
            "target_sql": d.target_sql,
            "rules_applied": d.rules_applied,
            "diff_type": d.diff_type,
            "mismatch_count": d.mismatch_count,
            "total_cells_compared": d.total_cells_compared,
            "cell_diffs": [
                {
                    "row": cd.row_index,
                    "col": cd.col_index,
                    "source": str(cd.source_value),
                    "target": str(cd.target_value),
                    "diff_type": cd.diff_type,
                }
                for cd in d.cell_diffs
            ],
        }


class VerifyRunner:
    """转译正确率验证运行器。

    Pipeline: 加载种子 → 转译 → 双端执行 → 结果比较 → 生成报告。
    """

    def __init__(
        self,
        transpiler: Optional[SQLTranspiler] = None,
        comparator: Optional[ResultComparator] = None,
        comparator_config: Optional[ComparatorConfig] = None,
    ) -> None:
        self._transpiler = transpiler or SQLTranspiler()
        self._comparator = comparator or ResultComparator(comparator_config)

    def run(
        self,
        seed_dir: Path,
        source_dialect: Dialect,
        target_dialect: Dialect,
        output_dir: Optional[Path] = None,
        init_db: bool = True,
    ) -> VerifyReport:
        """执行完整验证流程。

        Args:
            seed_dir: 种子 SQL 目录。
            source_dialect: 源 SQL 方言。
            target_dialect: 目标 SQL 方言。
            output_dir: 报告输出目录（默认 result/verify_{timestamp}）。
            init_db: 是否在验证前初始化数据库。

        Returns:
            VerifyReport 验证报告。
        """
        t0 = time.perf_counter()
        logger.info("验证启动: %s → %s", source_dialect.value, target_dialect.value)

        source_db = _DIALECT_TO_DB[source_dialect]
        target_db = _DIALECT_TO_DB[target_dialect]

        # 初始化数据库
        if init_db:
            self._init_databases([source_db, target_db])

        # 加载种子 SQL
        seeds = self._collect_seeds(seed_dir)
        if not seeds:
            raise FileNotFoundError(f"未找到种子 SQL 文件: {seed_dir}")
        logger.info("共加载 %d 个种子 SQL", len(seeds))

        # 建立数据库连接
        connectors: Dict[str, Any] = {}
        for db_type in (source_db, target_db):
            conn = ConnectorFactory.create(db_type)
            conn.connect()
            connectors[db_type] = conn

        details: List[ComparisonDetail] = []
        metrics = VerifyMetrics(total=len(seeds))

        try:
            for sql_name, sql_text in seeds:
                detail = self._verify_one(
                    sql_name=sql_name,
                    sql_text=sql_text,
                    source_dialect=source_dialect,
                    target_dialect=target_dialect,
                    source_db=source_db,
                    target_db=target_db,
                    connectors=connectors,
                )
                details.append(detail)

                # 更新指标
                if detail.source_status == "ok":
                    metrics.source_exec_ok += 1
                else:
                    metrics.source_exec_fail += 1

                if detail.target_status == "ok":
                    metrics.target_exec_ok += 1
                else:
                    metrics.target_exec_fail += 1

                if detail.verdict == ComparisonVerdict.EQUIVALENT:
                    metrics.equivalent += 1
                elif detail.verdict == ComparisonVerdict.PARTIAL_MATCH:
                    metrics.partial_match += 1
                elif detail.verdict == ComparisonVerdict.MISMATCH:
                    metrics.mismatch += 1
                elif detail.verdict in (
                    ComparisonVerdict.BOTH_ERROR,
                    ComparisonVerdict.SKIP,
                ):
                    metrics.skipped += 1
        finally:
            for conn in connectors.values():
                conn.close()

        elapsed = (time.perf_counter() - t0) * 1000

        # 生成报告
        report_paths = self._generate_report(
            output_dir=output_dir,
            metrics=metrics,
            details=details,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            elapsed_ms=elapsed,
        )

        return VerifyReport(
            metrics=metrics,
            details=details,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            report_path=report_paths[0],
            json_path=report_paths[1],
            elapsed_ms=round(elapsed, 2),
        )

    def _init_databases(self, db_types: List[str]) -> None:
        """初始化数据库（Schema → Data）。"""
        from src.testbed import DataPopulator, SchemaInitializer

        for db_type in db_types:
            logger.info("[%s] 初始化数据库 ...", db_type)
            connector = ConnectorFactory.create(db_type)
            connector.connect()
            try:
                SchemaInitializer(connector, db_type).initialize()
                DataPopulator(connector, db_type).populate_all()
            finally:
                connector.close()

    @staticmethod
    def _collect_seeds(seed_dir: Path) -> List[Tuple[str, str]]:
        """收集所有种子 SQL 文件。"""
        seeds: List[Tuple[str, str]] = []
        for sql_path in sorted(seed_dir.rglob("*.sql")):
            rel = sql_path.relative_to(seed_dir).as_posix()
            text = sql_path.read_text(encoding="utf-8").strip()
            if text:
                seeds.append((rel, text))
        return seeds

    def _verify_one(
        self,
        sql_name: str,
        sql_text: str,
        source_dialect: Dialect,
        target_dialect: Dialect,
        source_db: str,
        target_db: str,
        connectors: Dict[str, Any],
    ) -> ComparisonDetail:
        """验证单条种子 SQL。"""
        # 转译
        try:
            transpile_result = self._transpiler.transpile(
                sql_text, source_dialect, target_dialect
            )
            target_sql = transpile_result.sql
            rules_applied = transpile_result.rules_applied
        except Exception as e:
            # 转译失败
            logger.warning("%s 转译失败: %s", sql_name, e)
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.TARGET_ERROR,
                source_row_count=0,
                target_row_count=0,
                source_status="ok",
                target_status="error",
                target_error=f"转译失败: {e}",
                source_sql=sql_text,
                target_sql="",
                rules_applied=[],
                diff_type="transpile_error",
            )

        # 源端执行
        source_status, source_rows, source_error = self._execute_on(
            connectors[source_db], sql_text
        )

        # 目标端执行
        target_status, target_rows, target_error = self._execute_on(
            connectors[target_db], target_sql
        )

        # 比较
        return self._comparator.compare(
            sql_name=sql_name,
            source_rows=source_rows,
            target_rows=target_rows,
            source_status=source_status,
            target_status=target_status,
            source_error=source_error,
            target_error=target_error,
            source_sql=sql_text,
            target_sql=target_sql,
            rules_applied=rules_applied,
        )

    @staticmethod
    def _execute_on(connector: Any, sql: str) -> Tuple[str, list, Optional[str]]:
        """在指定数据库上执行 SQL。

        Returns:
            (status, rows, error_message)
        """
        try:
            rows = connector.execute_query(sql)
            return "ok", rows, None
        except Exception as e:
            return "error", [], str(e)

    def _generate_report(
        self,
        output_dir: Optional[Path],
        metrics: VerifyMetrics,
        details: List[ComparisonDetail],
        source_dialect: Dialect,
        target_dialect: Dialect,
        elapsed_ms: float,
    ) -> Tuple[Path, Path]:
        """生成 Markdown + JSON 报告。"""
        project_root = Path(__file__).resolve().parent.parent.parent
        if output_dir is None:
            output_dir = project_root / "result"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_name = f"verify_{timestamp}_{source_dialect.value}_{target_dialect.value}"
        report_dir = output_dir / dir_name
        report_dir.mkdir(parents=True, exist_ok=True)

        # JSON 报告
        report = VerifyReport(
            metrics=metrics,
            details=details,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            report_path=report_dir / "report.md",
            json_path=report_dir / "report.json",
            elapsed_ms=round(elapsed_ms, 2),
        )
        json_path = report_dir / "report.json"
        json_path.write_text(
            json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Markdown 报告
        md_path = report_dir / "report.md"
        md = self._render_markdown(report)
        md_path.write_text(md, encoding="utf-8")

        logger.info("报告已写入: %s", report_dir)
        return md_path, json_path

    @staticmethod
    def _render_markdown(report: VerifyReport) -> str:
        """渲染 Markdown 格式的验证报告。"""
        m = report.metrics
        lines: List[str] = []
        lines.append("# 转译正确率验证报告")
        lines.append("")
        lines.append(f"**方向**: {report.source_dialect.value} → {report.target_dialect.value}")
        lines.append(f"**耗时**: {report.elapsed_ms:.0f} ms")
        lines.append("")

        # 摘要
        lines.append("## 摘要")
        lines.append("")
        lines.append("| 指标 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 种子 SQL 总数 | {m.total} |")
        lines.append(
            f"| Level 1 - 执行通过率 | **{m.execution_pass_rate * 100:.1f}%** ({m.target_exec_ok}/{m.total}) |"
        )
        lines.append(
            f"| Level 2 - 语义等价率 | **{m.equivalence_rate * 100:.1f}%** ({m.equivalent}/{m.equivalent + m.partial_match + m.mismatch}) |"
        )
        lines.append("")

        # 分项
        lines.append("## 分项统计")
        lines.append("")
        lines.append("| 判定 | 数量 |")
        lines.append("|------|------|")
        lines.append(f"| equivalent | {m.equivalent} |")
        lines.append(f"| partial_match | {m.partial_match} |")
        lines.append(f"| mismatch | {m.mismatch} |")
        lines.append(f"| target_error | {m.target_exec_fail} |")
        lines.append(f"| source_error | {m.source_exec_fail} |")
        lines.append(f"| both_error / skip | {m.skipped} |")
        lines.append("")

        # 逐 SQL 明细
        lines.append("## 逐 SQL 明细")
        lines.append("")
        lines.append("| 文件 | 判定 | 源行数 | 目标行数 | 差异类型 |")
        lines.append("|------|------|--------|----------|----------|")
        for d in report.details:
            verdict_icon = {
                ComparisonVerdict.EQUIVALENT: "OK",
                ComparisonVerdict.PARTIAL_MATCH: "~",
                ComparisonVerdict.MISMATCH: "FAIL",
                ComparisonVerdict.TARGET_ERROR: "ERR",
                ComparisonVerdict.SOURCE_ERROR: "SRC_ERR",
                ComparisonVerdict.BOTH_ERROR: "BOTH_ERR",
                ComparisonVerdict.SKIP: "SKIP",
            }.get(d.verdict, "?")
            lines.append(
                f"| `{d.sql_name}` | {verdict_icon} | {d.source_row_count} | {d.target_row_count} | {d.diff_type} |"
            )
        lines.append("")

        # 失败详情
        failures = [
            d for d in report.details
            if d.verdict in (ComparisonVerdict.MISMATCH, ComparisonVerdict.TARGET_ERROR)
        ]
        if failures:
            lines.append(f"## 失败详情 ({len(failures)} 条)")
            lines.append("")
            for d in failures:
                lines.append(f"### `{d.sql_name}` — {d.verdict.value}")
                lines.append("")
                lines.append(f"- **差异类型**: {d.diff_type}")
                if d.rules_applied:
                    lines.append(f"- **应用规则**: {', '.join(d.rules_applied)}")
                if d.target_error:
                    lines.append(f"- **目标端错误**: {d.target_error}")
                if d.source_error:
                    lines.append(f"- **源端错误**: {d.source_error}")
                lines.append(f"- **源端 SQL**:\n```sql\n{d.source_sql}\n```")
                lines.append(f"- **目标端 SQL**:\n```sql\n{d.target_sql}\n```")
                if d.cell_diffs:
                    lines.append(f"- **单元格差异** ({len(d.cell_diffs)} 处):")
                    lines.append("")
                    lines.append("| 行 | 列 | 源值 | 目标值 | 差异类型 |")
                    lines.append("|----|-----|------|--------|----------|")
                    for cd in d.cell_diffs[:20]:  # 最多显示 20 条
                        lines.append(
                            f"| {cd.row_index} | {cd.col_index} | `{cd.source_value}` | `{cd.target_value}` | {cd.diff_type} |"
                        )
                    if len(d.cell_diffs) > 20:
                        lines.append(f"| ... | | 共 {len(d.cell_diffs)} 处 | | |")
                lines.append("")

        return "\n".join(lines)
