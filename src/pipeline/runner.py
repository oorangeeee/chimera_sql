"""端到端模糊测试流水线编排器。

核心流程：读取种子 SQL → AST 变异 → 方言转译 → 单目标数据库执行 → 分析报告。
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from random import Random
from typing import Any, Dict, List, Optional

from src.analyzer import FuzzAnalyzer, AnalysisReport
from src.core.mutator import (
    CapabilityProfile,
    MutationEngine,
    create_default_registry,
)
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.constants import PROJECT_ROOT, RESULT_ROOT
from src.utils.dialect_detector import DialectDetector
from src.utils.logger import get_logger

from .executor import TargetExecutor, SQLExecutionResult
from .target import TargetDatabase, load_targets

logger = get_logger("pipeline.runner")

# Dialect 枚举值映射
_DIALECT_MAP = {d.value: d for d in Dialect}


@dataclass
class TargetRunResult:
    """单目标运行结果。

    Attributes:
        target_name: 目标名称。
        total_executed: 执行 SQL 总数。
        success: 成功数。
        error: 失败数。
        skipped: 是否跳过（连接失败等）。
        skip_reason: 跳过原因。
        elapsed_ms: 该目标总耗时（毫秒）。
    """

    target_name: str
    total_executed: int = 0
    success: int = 0
    error: int = 0
    skipped: bool = False
    skip_reason: str = ""
    elapsed_ms: float = 0.0


@dataclass
class CampaignResult:
    """完整流水线运行结果。

    Attributes:
        output_dir: 输出根目录。
        report_path: 总报告 Markdown 文件路径。
        analysis_path: 分析报告 Markdown 文件路径。
        targets: 各目标运行结果。
        total_seeds: 种子文件数量。
        mutations_per_seed: 每条种子变异数量。
        elapsed_ms: 总耗时（毫秒）。
    """

    output_dir: Path
    report_path: Path
    analysis_path: Optional[Path] = None
    targets: List[TargetRunResult] = field(default_factory=list)
    total_seeds: int = 0
    mutations_per_seed: int = 0
    elapsed_ms: float = 0.0


class CampaignRunner:
    """端到端模糊测试流水线编排器。

    编排 MutationEngine、SQLTranspiler、TargetExecutor 三大组件，
    实现"变异 → 转译（可选）→ 执行 → 分析报告"的闭环。
    """

    def __init__(self, result_root: Optional[Path] = None) -> None:
        """初始化流水线编排器。

        Args:
            result_root: 输出根目录（默认项目根目录下的 result/）。
        """
        self._result_root = result_root or RESULT_ROOT

    def run(
        self,
        input_dir: Path,
        source_dialect: str,
        target_dialect: str,
        count_per_seed: int = 3,
        random_seed: Optional[int] = None,
    ) -> CampaignResult:
        """执行端到端模糊测试流水线（单目标）。

        Args:
            input_dir: 种子 SQL 输入目录。
            source_dialect: 种子 SQL 的方言（如 "sqlite"、"oracle"）。
            target_dialect: 目标 SQL 的方言（如 "sqlite"、"oracle"）。
            count_per_seed: 每条种子生成的变异数量。
            random_seed: 随机种子（可选，用于可复现结果）。

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

        # 收集种子 SQL 文件
        seed_files = sorted(input_dir.rglob("*.sql"))
        if not seed_files:
            raise ValueError(f"未找到 .sql 种子文件: {input_dir}")

        # 校验种子 SQL 与源方言兼容
        seed_sql_map = DialectDetector.validate_sql_files(
            seed_files, input_dir, source_dialect,
        )

        # 根据 target_dialect 自动匹配 config 中的目标
        target = self._match_target(target_d)

        logger.info(
            "流水线启动: 源方言=%s | 目标方言=%s | 目标=%s | %d 种子 | 每种子 %d 变异",
            source_dialect, target_dialect, target.name, len(seed_files), count_per_seed,
        )

        # 构建输出根目录
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self._result_root / f"run_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # ── 单目标执行 ──
        run_result, report_data, all_exec_results = self._execute_target(
            target=target,
            source=source,
            target_dialect=target_d,
            seed_files=seed_files,
            seed_sql_map=seed_sql_map,
            input_dir=input_dir,
            output_dir=output_dir,
            count_per_seed=count_per_seed,
            random_seed=random_seed,
        )

        total_elapsed = (time.perf_counter() - t_start) * 1000

        # ── 用户交互：有错误时询问是否保存 ──
        if run_result.error > 0 and not _prompt_save(run_result.error):
            shutil.rmtree(output_dir, ignore_errors=True)
            logger.info("已丢弃本次结果，输出目录已删除。")
            return CampaignResult(
                output_dir=output_dir,
                report_path=Path(""),
                targets=[run_result],
                total_seeds=len(seed_files),
                mutations_per_seed=count_per_seed,
                elapsed_ms=round(total_elapsed, 1),
            )

        # ── 生成总报告 ──
        campaign_summary = {
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
            "total_seeds": len(seed_files),
            "mutations_per_seed": count_per_seed,
            "random_seed": random_seed,
            "elapsed_ms": round(total_elapsed, 1),
        }
        report_path = CampaignReport.generate(
            output_dir, campaign_summary, [report_data],
        )

        # ── 分析模块 ──
        analysis_path = self._run_analysis(
            output_dir, all_exec_results, source_dialect, target_dialect,
            len(seed_files), count_per_seed,
        )

        logger.info("=" * 50)
        logger.info("流水线完成，总耗时 %.0f ms", total_elapsed)

        return CampaignResult(
            output_dir=output_dir,
            report_path=report_path,
            analysis_path=analysis_path,
            targets=[run_result],
            total_seeds=len(seed_files),
            mutations_per_seed=count_per_seed,
            elapsed_ms=round(total_elapsed, 1),
        )

    # ── 私有方法 ──

    def _execute_target(
        self,
        target: TargetDatabase,
        source: Dialect,
        target_dialect: Dialect,
        seed_files: List[Path],
        seed_sql_map: Dict[str, str],
        input_dir: Path,
        output_dir: Path,
        count_per_seed: int,
        random_seed: Optional[int],
    ) -> tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
        """对单个目标执行完整的变异→转译→执行流程。

        Returns:
            (TargetRunResult, 用于报告的字典, 全部执行结果列表)
        """
        logger.info("=" * 50)
        logger.info("处理目标: %s (dialect=%s, db_type=%s)", target.name, target.dialect, target.db_type)

        t_target_start = time.perf_counter()

        # 构建变异引擎（per-target 的能力画像）
        profile = CapabilityProfile.from_dialect_version(target.dialect, target.version or None)
        registry = create_default_registry()
        rng = Random(random_seed) if random_seed is not None else Random()
        engine = MutationEngine(profile, registry, rng, source_dialect=source.value)

        # 构建转译器
        transpiler = SQLTranspiler()

        # 尝试连接目标数据库
        executor = TargetExecutor(target)
        try:
            executor.connect()
        except Exception as e:
            reason = f"连接失败: {e}"
            logger.warning("跳过目标 %s: %s", target.name, reason)
            elapsed = (time.perf_counter() - t_target_start) * 1000
            run_result = TargetRunResult(
                target_name=target.name,
                skipped=True,
                skip_reason=reason,
                elapsed_ms=round(elapsed, 1),
            )
            report_data = _build_target_report_data(target, run_result, [])
            return run_result, report_data, []

        # 逐种子处理
        all_exec_results: List[SQLExecutionResult] = []
        target_output = output_dir / target.name
        target_output.mkdir(parents=True, exist_ok=True)

        try:
            for seed_path in seed_files:
                relative = seed_path.relative_to(input_dir)
                sql_text = seed_sql_map.get(str(relative), seed_path.read_text(encoding="utf-8").strip())

                # 变异
                try:
                    mutations = engine.mutate_many(sql_text, str(relative), count_per_seed)
                except Exception as e:
                    logger.warning(
                        "%s: 变异失败（跳过该种子）: %s", relative, e,
                    )
                    continue

                # 逐变异转译 + 执行
                _process_mutations(
                    mutations, relative, seed_path, target_output,
                    source, target_dialect, transpiler, executor,
                    all_exec_results,
                )

        finally:
            executor.close()

        # 汇总结果
        return _build_target_result(target, target_output, all_exec_results, t_target_start)

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
    def _match_target(target_dialect: Dialect) -> TargetDatabase:
        """根据目标方言从 config targets 中匹配第一个 dialect 一致的目标。"""
        targets = load_targets()
        for t in targets:
            if t.dialect.lower() == target_dialect.value.lower():
                return t
        available = [t.name for t in targets]
        raise ValueError(
            f"config.yaml targets 中没有方言为 '{target_dialect.value}' 的目标。"
            f"可用目标: {available}"
        )


# ── 模块级辅助函数 ──

def _prompt_save(error_count: int) -> bool:
    """提示用户选择是否保存结果。"""
    import sys as _sys
    _sys.stderr.write(f"\n  执行过程中出现 {error_count} 个错误。\n\n")
    _sys.stderr.flush()
    try:
        choice = input("  是否保存本次结果？[y/N]: ").strip().lower()
        return choice in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        _sys.stderr.write("\n")
        return False


def _process_mutations(
    mutations: List[Any],
    relative: Path,
    seed_path: Path,
    target_output: Path,
    source: Dialect,
    target_dialect: Dialect,
    transpiler: Any,
    executor: Any,
    all_exec_results: List[SQLExecutionResult],
) -> None:
    """对单条种子的所有变异执行转译 + 写入 + 数据库执行。"""
    for idx, mr in enumerate(mutations, start=1):
        out_name = f"{seed_path.stem}_mut{idx:02d}.sql"
        out_relative = relative.parent / out_name

        # 转译（源方言和目标方言相同时跳过）
        if source != target_dialect:
            try:
                tr = transpiler.transpile(mr.sql, source, target_dialect)
                exec_sql = tr.sql
                transpile_rules = tr.rules_applied
                transpile_warnings = tr.warnings
            except Exception as e:
                exec_sql = mr.sql
                transpile_rules = []
                transpile_warnings = [f"转译失败: {e}"]
                logger.debug("%s: 转译失败，使用原始 SQL: %s", out_relative, e)
        else:
            exec_sql = mr.sql
            transpile_rules = []
            transpile_warnings = []

        # 写入 SQL 文件
        sql_out_path = target_output / out_relative
        sql_out_path.parent.mkdir(parents=True, exist_ok=True)
        sql_out_path.write_text(exec_sql + "\n", encoding="utf-8")

        # 执行
        metadata = {
            "file": str(out_relative),
            "seed_file": str(relative),
            "mutation_strategies": mr.strategies_applied,
            "transpile_rules": transpile_rules,
            "transpile_warnings": transpile_warnings,
        }
        result = executor.execute_one(exec_sql, metadata)
        all_exec_results.append(result)


def _build_target_result(
    target: TargetDatabase,
    target_output: Path,
    all_exec_results: List[SQLExecutionResult],
    t_target_start: float,
) -> tuple[TargetRunResult, Dict[str, Any], List[SQLExecutionResult]]:
    """写入 execution.json 并构建返回值。"""
    target_elapsed = (time.perf_counter() - t_target_start) * 1000
    success_count = sum(1 for r in all_exec_results if r.status == "ok")
    error_count = sum(1 for r in all_exec_results if r.status == "error")

    execution_payload = {
        "target": target.name,
        "db_type": target.db_type,
        "dialect": target.dialect,
        "version": target.version,
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
        target.name, len(all_exec_results), success_count, error_count, target_elapsed,
    )

    run_result = TargetRunResult(
        target_name=target.name,
        total_executed=len(all_exec_results),
        success=success_count,
        error=error_count,
        elapsed_ms=round(target_elapsed, 1),
    )
    report_data = _build_target_report_data(target, run_result, all_exec_results)
    return run_result, report_data, all_exec_results


def _build_target_report_data(
    target: TargetDatabase,
    run_result: TargetRunResult,
    exec_results: List[SQLExecutionResult],
) -> Dict[str, Any]:
    """构建用于报告生成器的目标数据字典。"""
    error_messages = [r.error for r in exec_results if r.error]
    return {
        "target_name": target.name,
        "db_type": target.db_type,
        "dialect": target.dialect,
        "version": target.version,
        "skipped": run_result.skipped,
        "skip_reason": run_result.skip_reason,
        "total_executed": run_result.total_executed,
        "success": run_result.success,
        "error": run_result.error,
        "elapsed_ms": run_result.elapsed_ms,
        "error_messages": error_messages,
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

        md_content = CampaignReport._build_markdown(campaign_summary, per_target)
        md_path.write_text(md_content, encoding="utf-8")

        json_payload = {
            "generated_at": datetime.now().isoformat(),
            "campaign": campaign_summary,
            "targets": per_target,
        }
        json_path.write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2),
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

        lines.append("# ChimeraSQL 端到端模糊测试报告")
        lines.append("")
        lines.append(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        lines.append("## 运行概览")
        lines.append("")
        lines.append("| 项目 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 源方言 | `{summary.get('source_dialect', '')}` |")
        lines.append(f"| 目标方言 | `{summary.get('target_dialect', '')}` |")
        lines.append(f"| 种子数量 | {summary.get('total_seeds', 0)} |")
        lines.append(f"| 每种子变异数 | {summary.get('mutations_per_seed', 0)} |")

        random_seed = summary.get("random_seed")
        seed_display = str(random_seed) if random_seed is not None else "随机"
        lines.append(f"| 随机种子 | {seed_display} |")

        target_names = [t["target_name"] for t in per_target]
        lines.append(f"| 目标数据库 | {', '.join(target_names)} |")

        elapsed = summary.get("elapsed_ms", 0)
        lines.append(f"| 总耗时 | {elapsed:.0f} ms |")
        lines.append("")

        # 各目标汇总
        lines.append("## 各目标执行汇总")
        lines.append("")
        lines.append("| 目标 | 方言 | 状态 | 执行数 | 成功 | 失败 | 耗时 (ms) |")
        lines.append("|------|------|------|--------|------|------|-----------|")

        for t in per_target:
            if t.get("skipped"):
                status = f"跳过: {t.get('skip_reason', '')}"
                lines.append(
                    f"| {t['target_name']} | {t.get('dialect', '')} | {status} "
                    f"| - | - | - | - |"
                )
            else:
                lines.append(
                    f"| {t['target_name']} | {t.get('dialect', '')} | 正常 "
                    f"| {t.get('total_executed', 0)} "
                    f"| {t.get('success', 0)} "
                    f"| {t.get('error', 0)} "
                    f"| {t.get('elapsed_ms', 0):.0f} |"
                )
        lines.append("")

        # 错误 Top-N
        for t in per_target:
            if t.get("skipped"):
                continue
            error_messages: List[str] = t.get("error_messages", [])
            if not error_messages:
                continue

            from collections import Counter
            lines.append(f"## 错误 Top-10: {t['target_name']}")
            lines.append("")

            counter = Counter(error_messages)
            top_errors = counter.most_common(10)

            lines.append("| 出现次数 | 错误信息 |")
            lines.append("|----------|----------|")
            for msg, count in top_errors:
                truncated = msg[:200] + "..." if len(msg) > 200 else msg
                truncated = truncated.replace("|", "\\|")
                lines.append(f"| {count} | {truncated} |")
            lines.append("")

        return "\n".join(lines)
