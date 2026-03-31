"""端到端流水线编排器。

核心流程：读取种子 SQL → 方言转译 → AST 变异 → 单目标数据库执行 → 分析报告。
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from random import Random
from typing import Any, Dict, List, Optional, Tuple

from src.analyzer import FuzzAnalyzer, AnalysisReport
from src.core.mutator import (
    CapabilityProfile,
    MutationEngine,
    create_default_registry,
)
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.constants import RESULT_ROOT
from src.utils.dialect_detector import DialectDetector
from src.utils.logger import get_logger

from .executor import TargetExecutor, SQLExecutionResult
from .target import DatabaseEntry, resolve_database

logger = get_logger("pipeline.runner")

# Dialect 枚举值映射
_DIALECT_MAP = {d.value: d for d in Dialect}


@dataclass
class TargetRunResult:
    """单目标运行结果。"""

    target_name: str
    total_executed: int = 0
    success: int = 0
    error: int = 0
    skipped: bool = False
    skip_reason: str = ""
    elapsed_ms: float = 0.0


@dataclass
class CampaignResult:
    """完整流水线运行结果。"""

    output_dir: Path
    report_path: Path
    analysis_path: Optional[Path] = None
    targets: List[TargetRunResult] = field(default_factory=list)
    mode: str = "fuzz"
    total_seeds: int = 0
    mutations_per_seed: int = 0
    elapsed_ms: float = 0.0


class CampaignRunner:
    """端到端流水线编排器。

    编排 SQLTranspiler、MutationEngine、TargetExecutor 三大组件，
    实现"转译（可选）→ 变异 → 执行 → 分析报告"的闭环。

    流水线顺序：先转译再变异。这样变异引擎的能力画像与操作对象一致，
    变异产物直接可在目标数据库执行，无需二次转译。
    """

    def __init__(self, result_root: Optional[Path] = None) -> None:
        self._result_root = result_root or RESULT_ROOT

    def run(
        self,
        input_dir: Path,
        source_dialect: str,
        source_version: str,
        target_dialect: str,
        target_version: str,
        mode: str,
        count_per_seed: int = 3,
        random_seed: Optional[int] = None,
    ) -> CampaignResult:
        """执行端到端流水线（单目标）。

        Args:
            input_dir: SQL 输入目录。
            source_dialect: 源 SQL 的方言（如 "sqlite"、"oracle"）。
            source_version: 源数据库版本标识。
            target_dialect: 目标 SQL 的方言（如 "sqlite"、"oracle"）。
            target_version: 目标数据库版本标识。
            mode: 流水线模式，"fuzz"（转译→变异→执行→分析）或 "exec"（转译→执行→分析）。
            count_per_seed: 每条种子生成的变异数量（仅 fuzz 模式）。
            random_seed: 随机种子（可选，仅 fuzz 模式）。

        Returns:
            CampaignResult 流水线运行结果。

        Raises:
            ValueError: 参数校验失败。
        """
        t_start = time.perf_counter()

        # ── 参数校验 ──
        input_dir = input_dir.resolve()
        if not input_dir.is_dir():
            raise ValueError(f"输入目录不存在: {input_dir}")

        source = self._resolve_dialect(source_dialect)
        target_d = self._resolve_dialect(target_dialect)

        sql_files = sorted(input_dir.rglob("*.sql"))
        if not sql_files:
            raise ValueError(f"未找到 .sql 文件: {input_dir}")

        sql_map = DialectDetector.validate_sql_files(
            sql_files, input_dir, source_dialect,
        )

        target_db = self._resolve_database(target_dialect)

        # ── 构建输出目录 ──
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_prefix = "exec" if mode == "exec" else "run"
        output_dir = self._result_root / f"{dir_prefix}_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)

        if mode == "fuzz":
            logger.info(
                "流水线启动 [fuzz]: 源方言=%s | 目标方言=%s | 目标=%s | %d 种子 | 每种子 %d 变异",
                source_dialect, target_dialect, target_db.name, len(sql_files), count_per_seed,
            )
        else:
            logger.info(
                "流水线启动 [exec]: 源方言=%s | 目标方言=%s | 目标=%s | %d SQL 文件",
                source_dialect, target_dialect, target_db.name, len(sql_files),
            )

        # ── 按 mode 分发执行 ──
        if mode == "exec":
            run_result, report_data, all_exec_results = self._run_exec(
                target_db, source, target_d, target_version, sql_files, sql_map, input_dir, output_dir,
            )
        else:
            run_result, report_data, all_exec_results = self._run_fuzz(
                target_db, source, target_d, target_version, sql_files, sql_map, input_dir, output_dir,
                count_per_seed, random_seed,
            )

        total_elapsed = (time.perf_counter() - t_start) * 1000
        effective_mutations = count_per_seed if mode == "fuzz" else 0

        # ── 用户交互：有错误时询问是否保存 ──
        if run_result.error > 0 and not _prompt_save(run_result.error):
            shutil.rmtree(output_dir, ignore_errors=True)
            logger.info("已丢弃本次结果，输出目录已删除。")
            return CampaignResult(
                output_dir=output_dir,
                report_path=Path(""),
                targets=[run_result],
                mode=mode,
                total_seeds=len(sql_files),
                mutations_per_seed=effective_mutations,
                elapsed_ms=round(total_elapsed, 1),
            )

        # ── 生成报告 ──
        campaign_summary = {
            "mode": mode,
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
            "total_seeds": len(sql_files),
            "mutations_per_seed": effective_mutations,
            "random_seed": random_seed,
            "elapsed_ms": round(total_elapsed, 1),
        }
        report_path = CampaignReport.generate(
            output_dir, campaign_summary, [report_data],
        )

        analysis_path = self._run_analysis(
            output_dir, all_exec_results, source_dialect, target_dialect,
            len(sql_files), effective_mutations,
        )

        logger.info("=" * 50)
        logger.info("流水线完成，总耗时 %.0f ms", total_elapsed)

        return CampaignResult(
            output_dir=output_dir,
            report_path=report_path,
            analysis_path=analysis_path,
            targets=[run_result],
            mode=mode,
            total_seeds=len(sql_files),
            mutations_per_seed=effective_mutations,
            elapsed_ms=round(total_elapsed, 1),
        )

    # ── 私有方法 ──

    def _run_fuzz(
        self,
        target_db: DatabaseEntry,
        source: Dialect,
        target_dialect: Dialect,
        target_version: str,
        sql_files: List[Path],
        sql_map: Dict[str, str],
        input_dir: Path,
        output_dir: Path,
        count_per_seed: int,
        random_seed: Optional[int],
    ) -> Tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
        """转译 → 变异 → 执行。"""
        t_start = time.perf_counter()
        logger.info("=" * 50)
        logger.info("处理目标: %s (dialect=%s, db_type=%s)", target_db.name, target_db.sqlglot_dialect, target_db.db_type)

        conn = self._connect_target(target_db, target_version, output_dir)
        if conn is None:
            return _skip_target(target_db, "连接失败（详见上方日志）", t_start)

        executor, target_output, actual_version = conn
        transpiler = SQLTranspiler()

        profile, capability_source = CapabilityProfile.from_dialect_version(
            target_db.sqlglot_dialect, target_version,
        )
        registry = create_default_registry()
        rng = Random(random_seed) if random_seed is not None else Random()
        engine = MutationEngine(profile, registry, rng, source_dialect=target_dialect.value)

        all_exec_results: List[SQLExecutionResult] = []

        try:
            for sql_path in sql_files:
                relative = sql_path.relative_to(input_dir)
                sql_text = sql_map.get(str(relative), sql_path.read_text(encoding="utf-8").strip())

                # 步骤1: 转译为目标方言
                tp = _transpile_one(transpiler, sql_text, source, target_dialect, relative)
                if tp is None:
                    continue
                base_sql, transpile_rules, transpile_warnings = tp

                # 步骤2: 对目标方言 SQL 执行变异
                try:
                    mutations = engine.mutate_many(base_sql, str(relative), count_per_seed)
                except Exception as e:
                    logger.warning("%s: 变异失败（跳过该种子）: %s", relative, e)
                    continue

                # 步骤3: 逐变异写入 + 执行
                for idx, mr in enumerate(mutations, start=1):
                    out_relative = relative.parent / f"{sql_path.stem}_mut{idx:02d}.sql"

                    sql_out_path = target_output / out_relative
                    sql_out_path.parent.mkdir(parents=True, exist_ok=True)
                    sql_out_path.write_text(mr.sql + "\n", encoding="utf-8")

                    metadata = {
                        "file": str(out_relative),
                        "seed_file": str(relative),
                        "mutation_strategies": mr.strategies_applied,
                        "transpile_rules": transpile_rules,
                        "transpile_warnings": transpile_warnings,
                    }
                    all_exec_results.append(executor.execute_one(mr.sql, metadata))
        finally:
            executor.close()

        return _build_target_result(
            target_db, target_output, all_exec_results, t_start,
            target_version=target_version,
            actual_version=actual_version,
            capability_source=capability_source,
        )

    def _run_exec(
        self,
        target_db: DatabaseEntry,
        source: Dialect,
        target_dialect: Dialect,
        target_version: str,
        sql_files: List[Path],
        sql_map: Dict[str, str],
        input_dir: Path,
        output_dir: Path,
    ) -> Tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
        """转译 → 执行（无变异）。"""
        t_start = time.perf_counter()
        logger.info("=" * 50)
        logger.info("处理目标: %s (dialect=%s, db_type=%s)", target_db.name, target_db.sqlglot_dialect, target_db.db_type)

        conn = self._connect_target(target_db, target_version, output_dir)
        if conn is None:
            return _skip_target(target_db, "连接失败（详见上方日志）", t_start)

        executor, target_output, actual_version = conn
        transpiler = SQLTranspiler()

        all_exec_results: List[SQLExecutionResult] = []

        try:
            for sql_path in sql_files:
                relative = sql_path.relative_to(input_dir)
                sql_text = sql_map.get(str(relative), sql_path.read_text(encoding="utf-8").strip())

                tp = _transpile_one(transpiler, sql_text, source, target_dialect, relative)
                if tp is None:
                    continue
                exec_sql, transpile_rules, transpile_warnings = tp

                sql_out_path = target_output / relative
                sql_out_path.parent.mkdir(parents=True, exist_ok=True)
                sql_out_path.write_text(exec_sql + "\n", encoding="utf-8")

                metadata = {
                    "file": str(relative),
                    "seed_file": str(relative),
                    "mutation_strategies": [],
                    "transpile_rules": transpile_rules,
                    "transpile_warnings": transpile_warnings,
                }
                all_exec_results.append(executor.execute_one(exec_sql, metadata))
        finally:
            executor.close()

        return _build_target_result(
            target_db, target_output, all_exec_results, t_start,
            target_version=target_version,
            actual_version=actual_version,
            capability_source="",
        )

    def _run_analysis(
        self,
        output_dir: Path,
        exec_results: List[SQLExecutionResult],
        source_dialect: str,
        target_dialect: str,
        total_seeds: int,
        mutations_per_seed: int,
    ) -> Optional[Path]:
        """运行分析模块并生成分析报告。"""
        analyzer = FuzzAnalyzer()
        analysis = analyzer.analyze(exec_results)

        if analysis.total_executed == 0:
            logger.info("无执行结果，跳过分析报告生成。")
            return None

        return AnalysisReport.generate(
            output_dir, analysis, source_dialect, target_dialect,
            total_seeds, mutations_per_seed,
        )

    @staticmethod
    def _connect_target(
        target_db: DatabaseEntry, target_version: str, output_dir: Path,
    ) -> Optional[Tuple[TargetExecutor, Path, str]]:
        """连接目标数据库，成功返回 (executor, target_output, actual_version)，失败返回 None。"""
        executor = TargetExecutor(target_db)
        try:
            executor.connect()
        except Exception as e:
            logger.warning("跳过目标 %s: 连接失败: %s", target_db.name, e)
            return None

        # 查询实际数据库版本并与用户指定版本对比
        actual_version = executor.get_version()
        if actual_version and actual_version != target_version:
            logger.warning(
                "数据库版本不一致: 用户指定 '%s'，实际 '%s'。",
                target_version, actual_version,
            )
            try:
                choice = input("  是否继续执行？[y/N]: ").strip().lower()
                if choice not in ("y", "yes"):
                    executor.close()
                    return None
            except (EOFError, KeyboardInterrupt):
                sys.stderr.write("\n")
                executor.close()
                return None

        target_output = output_dir / target_db.name
        target_output.mkdir(parents=True, exist_ok=True)
        return executor, target_output, actual_version

    @staticmethod
    def _resolve_dialect(dialect_str: str) -> Dialect:
        """将方言字符串映射为 Dialect 枚举，不支持时抛 ValueError。"""
        dialect = _DIALECT_MAP.get(dialect_str.lower())
        if dialect is None:
            supported = list(_DIALECT_MAP.keys())
            raise ValueError(
                f"不支持的方言: '{dialect_str}'。支持的方言: {supported}"
            )
        return dialect

    @staticmethod
    def _resolve_database(dialect: str) -> DatabaseEntry:
        """根据方言名称查找已注册的数据库。"""
        return resolve_database(dialect)


# ── 模块级辅助函数 ──

def _skip_target(
    target_db: DatabaseEntry, reason: str, t_start: float,
) -> Tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
    """构建跳过目标的结果元组。"""
    elapsed = (time.perf_counter() - t_start) * 1000
    run_result = TargetRunResult(
        target_name=target_db.name,
        skipped=True,
        skip_reason=reason,
        elapsed_ms=round(elapsed, 1),
    )
    report_data = _build_target_report_data(target_db, run_result, [])
    return run_result, report_data, []


def _transpile_one(
    transpiler: SQLTranspiler,
    sql_text: str,
    source: Dialect,
    target_d: Dialect,
    relative: Path,
) -> Optional[Tuple[str, List[str], List[str]]]:
    """转译单条 SQL。成功返回 (sql, rules, warnings)，失败返回 None。"""
    if source == target_d:
        return sql_text, [], []
    try:
        tr = transpiler.transpile(sql_text, source, target_d)
        return tr.sql, tr.rules_applied, tr.warnings
    except Exception as e:
        logger.warning("%s: 转译失败（跳过）: %s", relative, e)
        return None


def _prompt_save(error_count: int) -> bool:
    """提示用户选择是否保存结果。"""
    sys.stderr.write(f"\n  执行过程中出现 {error_count} 个错误。\n\n")
    sys.stderr.flush()
    try:
        choice = input("  是否保存本次结果？[y/N]: ").strip().lower()
        return choice in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        sys.stderr.write("\n")
        return False


def _build_target_result(
    target_db: DatabaseEntry,
    target_output: Path,
    all_exec_results: List[SQLExecutionResult],
    t_start: float,
    target_version: str = "",
    actual_version: str = "",
    capability_source: str = "",
) -> Tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
    """写入 execution.json 并构建返回值。"""
    target_elapsed = (time.perf_counter() - t_start) * 1000
    success_count = sum(1 for r in all_exec_results if r.status == "ok")
    error_count = sum(1 for r in all_exec_results if r.status == "error")

    execution_payload = {
        "target": target_db.name,
        "db_type": target_db.db_type,
        "dialect": target_db.sqlglot_dialect,
        "version": target_version,
        "user_specified_version": target_version,
        "actual_db_version": actual_version,
        "capability_source": capability_source,
        "total": len(all_exec_results),
        "success": success_count,
        "error": error_count,
        "elapsed_ms": round(target_elapsed, 1),
        "results": [r.to_dict() for r in all_exec_results],
    }
    exec_json_path = target_output / "execution.json"
    exec_json_path.write_text(
        json.dumps(execution_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "目标 %s 完成: %d 执行 | %d 成功 | %d 失败 | 耗时 %.0f ms",
        target_db.name, len(all_exec_results), success_count, error_count, target_elapsed,
    )

    run_result = TargetRunResult(
        target_name=target_db.name,
        total_executed=len(all_exec_results),
        success=success_count,
        error=error_count,
        elapsed_ms=round(target_elapsed, 1),
    )
    report_data = _build_target_report_data(
        target_db, run_result, all_exec_results,
        target_version=target_version,
        actual_version=actual_version,
        capability_source=capability_source,
    )
    return run_result, report_data, all_exec_results


def _build_target_report_data(
    target_db: DatabaseEntry,
    run_result: TargetRunResult,
    exec_results: List[SQLExecutionResult],
    target_version: str = "",
    actual_version: str = "",
    capability_source: str = "",
) -> Dict[str, Any]:
    """构建用于报告生成器的目标数据字典。"""
    return {
        "target_name": target_db.name,
        "db_type": target_db.db_type,
        "dialect": target_db.sqlglot_dialect,
        "version": target_version,
        "user_specified_version": target_version,
        "actual_db_version": actual_version,
        "capability_source": capability_source,
        "skipped": run_result.skipped,
        "skip_reason": run_result.skip_reason,
        "total_executed": run_result.total_executed,
        "success": run_result.success,
        "error": run_result.error,
        "elapsed_ms": run_result.elapsed_ms,
        "error_messages": [r.error for r in exec_results if r.error],
    }


class CampaignReport:
    """流水线运行报告生成器。"""

    @staticmethod
    def generate(
        output_dir: Path,
        campaign_summary: Dict[str, Any],
        per_target: List[Dict[str, Any]],
    ) -> Path:
        """生成 report.md + report.json，返回 Markdown 报告路径。"""
        md_path = output_dir / "report.md"
        json_path = output_dir / "report.json"

        md_path.write_text(
            CampaignReport._build_markdown(campaign_summary, per_target),
            encoding="utf-8",
        )
        json_path.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now().isoformat(),
                    "campaign": campaign_summary,
                    "targets": per_target,
                },
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )

        logger.info("报告已生成: %s", md_path)
        return md_path

    @staticmethod
    def _build_markdown(
        summary: Dict[str, Any],
        per_target: List[Dict[str, Any]],
    ) -> str:
        """构建 Markdown 报告内容。"""
        lines: List[str] = []
        is_exec = summary.get("mode", "fuzz") == "exec"

        title = "ChimeraSQL 转译执行报告" if is_exec else "ChimeraSQL 端到端模糊测试报告"
        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        lines.append("## 运行概览")
        lines.append("")
        lines.append("| 项目 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 源方言 | `{summary.get('source_dialect', '')}` |")
        lines.append(f"| 目标方言 | `{summary.get('target_dialect', '')}` |")

        file_label = "SQL 文件数量" if is_exec else "种子数量"
        lines.append(f"| {file_label} | {summary.get('total_seeds', 0)} |")

        if not is_exec:
            lines.append(f"| 每种子变异数 | {summary.get('mutations_per_seed', 0)} |")
            random_seed = summary.get("random_seed")
            lines.append(f"| 随机种子 | {str(random_seed) if random_seed is not None else '随机'} |")

        target_names = [t["target_name"] for t in per_target]
        lines.append(f"| 目标数据库 | {', '.join(target_names)} |")
        lines.append(f"| 能力画像来源 | {', '.join(t.get('capability_source', '') for t in per_target)} |")
        lines.append(f"| 总耗时 | {summary.get('elapsed_ms', 0):.0f} ms |")
        lines.append("")

        # 各目标汇总
        lines.append("## 各目标执行汇总")
        lines.append("")
        lines.append("| 目标 | 方言 | 状态 | 执行数 | 成功 | 失败 | 耗时 (ms) |")
        lines.append("|------|------|------|--------|------|------|-----------|")

        for t in per_target:
            if t.get("skipped"):
                lines.append(
                    f"| {t['target_name']} | {t.get('dialect', '')} "
                    f"| 跳过: {t.get('skip_reason', '')} | - | - | - | - |"
                )
            else:
                lines.append(
                    f"| {t['target_name']} | {t.get('dialect', '')} | 正常 "
                    f"| {t.get('total_executed', 0)} | {t.get('success', 0)} "
                    f"| {t.get('error', 0)} | {t.get('elapsed_ms', 0):.0f} |"
                )
        lines.append("")

        for t in per_target:
            if t.get("actual_db_version") and t.get("actual_db_version") != t.get("user_specified_version"):
                lines.append(
                    f"> **警告:** 实际数据库版本 ({t['actual_db_version']}) "
                    f"与用户指定版本 ({t['user_specified_version']}) 不一致。"
                )
                lines.append("")

        # 错误 Top-N
        for t in per_target:
            if t.get("skipped"):
                continue
            error_messages: List[str] = t.get("error_messages", [])
            if not error_messages:
                continue

            lines.append(f"## 错误 Top-10: {t['target_name']}")
            lines.append("")
            lines.append("| 出现次数 | 错误信息 |")
            lines.append("|----------|----------|")
            for msg, count in Counter(error_messages).most_common(10):
                truncated = msg[:200].replace("|", "\\|")
                if len(msg) > 200:
                    truncated += "..."
                lines.append(f"| {count} | {truncated} |")
            lines.append("")

        return "\n".join(lines)
