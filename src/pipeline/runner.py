"""端到端模糊测试流水线编排器。

核心流程：读取种子 SQL → AST 变异 → 方言转译 → 多数据库执行 → 生成报告。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from random import Random
from typing import Any, Dict, List, Optional

from src.core.mutator import (
    CapabilityProfile,
    MutationEngine,
    create_default_registry,
)
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.dialect_detector import DialectDetector
from src.utils.logger import get_logger

from .executor import TargetExecutor, SQLExecutionResult
from .report import CampaignReport
from .target import TargetDatabase, load_targets

logger = get_logger("pipeline.runner")

# 项目根目录 & 默认输出根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_RESULT_ROOT = _PROJECT_ROOT / "result"

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
        targets: 各目标运行结果。
        total_seeds: 种子文件数量。
        mutations_per_seed: 每条种子变异数量。
        elapsed_ms: 总耗时（毫秒）。
    """

    output_dir: Path
    report_path: Path
    targets: List[TargetRunResult] = field(default_factory=list)
    total_seeds: int = 0
    mutations_per_seed: int = 0
    elapsed_ms: float = 0.0


class CampaignRunner:
    """端到端模糊测试流水线编排器。

    编排 MutationEngine、SQLTranspiler、TargetExecutor 三大组件，
    实现 per-target 的"变异 → 转译 → 执行"闭环。
    """

    def __init__(self, result_root: Optional[Path] = None) -> None:
        """初始化流水线编排器。

        Args:
            result_root: 输出根目录（默认项目根目录下的 result/）。
        """
        self._result_root = result_root or _RESULT_ROOT

    def run(
        self,
        input_dir: Path,
        source_dialect: str,
        target_names: Optional[List[str]] = None,
        count_per_seed: int = 3,
        random_seed: Optional[int] = None,
    ) -> CampaignResult:
        """执行端到端模糊测试流水线。

        Args:
            input_dir: 种子 SQL 输入目录。
            source_dialect: 种子 SQL 的方言（如 "sqlite"、"oracle"）。
            target_names: 目标数据库名称列表（None 表示 config 中所有 targets）。
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

        # 校验并映射源方言
        source = self._resolve_dialect(source_dialect)

        # 收集种子 SQL 文件
        seed_files = sorted(input_dir.rglob("*.sql"))
        if not seed_files:
            raise ValueError(f"未找到 .sql 种子文件: {input_dir}")

        # 校验种子 SQL 与源方言兼容
        self._validate_source_dialect(seed_files, input_dir, source_dialect)

        # 加载目标列表
        targets = load_targets(target_names)

        logger.info(
            "流水线启动: 源方言=%s | 种子目录=%s | %d 种子 | %d 目标 | 每种子 %d 变异",
            source_dialect, input_dir.name, len(seed_files), len(targets), count_per_seed,
        )

        # 构建输出根目录
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self._result_root / f"run_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # ── 主循环：逐目标处理 ──
        target_run_results: List[TargetRunResult] = []
        per_target_report_data: List[Dict[str, Any]] = []

        for target in targets:
            run_result, report_data = self._run_target(
                target=target,
                source=source,
                seed_files=seed_files,
                input_dir=input_dir,
                output_dir=output_dir,
                count_per_seed=count_per_seed,
                random_seed=random_seed,
            )
            target_run_results.append(run_result)
            per_target_report_data.append(report_data)

        # ── 生成总报告 ──
        total_elapsed = (time.perf_counter() - t_start) * 1000
        campaign_summary = {
            "source_dialect": source_dialect,
            "total_seeds": len(seed_files),
            "mutations_per_seed": count_per_seed,
            "random_seed": random_seed,
            "elapsed_ms": round(total_elapsed, 1),
        }
        report_path = CampaignReport.generate(
            output_dir, campaign_summary, per_target_report_data,
        )

        logger.info("=" * 50)
        logger.info("流水线完成，总耗时 %.0f ms", total_elapsed)

        return CampaignResult(
            output_dir=output_dir,
            report_path=report_path,
            targets=target_run_results,
            total_seeds=len(seed_files),
            mutations_per_seed=count_per_seed,
            elapsed_ms=round(total_elapsed, 1),
        )

    # ── 私有方法 ──

    def _run_target(
        self,
        target: TargetDatabase,
        source: Dialect,
        seed_files: List[Path],
        input_dir: Path,
        output_dir: Path,
        count_per_seed: int,
        random_seed: Optional[int],
    ) -> tuple[TargetRunResult, Dict[str, Any]]:
        """对单个目标执行完整的变异→转译→执行流程。

        Returns:
            (TargetRunResult, 用于报告的字典)
        """
        logger.info("=" * 50)
        logger.info("处理目标: %s (dialect=%s, db_type=%s)", target.name, target.dialect, target.db_type)

        t_target_start = time.perf_counter()

        # 映射目标方言
        target_dialect = self._resolve_dialect(target.dialect)

        # 构建变异引擎（per-target 的能力画像）
        profile = CapabilityProfile.from_dialect_version(target.dialect, target.version or None)
        registry = create_default_registry()
        rng = Random(random_seed) if random_seed is not None else Random()
        engine = MutationEngine(profile, registry, rng)

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
            report_data = self._build_target_report_data(target, run_result, [])
            return run_result, report_data

        # 逐种子处理
        all_exec_results: List[SQLExecutionResult] = []
        target_output = output_dir / target.name
        target_output.mkdir(parents=True, exist_ok=True)

        try:
            for seed_path in seed_files:
                relative = seed_path.relative_to(input_dir)
                sql_text = seed_path.read_text(encoding="utf-8").strip()

                # 变异
                try:
                    mutations = engine.mutate_many(sql_text, str(relative), count_per_seed)
                except Exception as e:
                    logger.warning(
                        "%s: 变异失败（跳过该种子）: %s", relative, e,
                    )
                    continue

                # 逐变异转译 + 执行
                for idx, mr in enumerate(mutations, start=1):
                    out_name = f"{seed_path.stem}_mut{idx:02d}.sql"
                    out_relative = relative.parent / out_name

                    # 转译
                    try:
                        tr = transpiler.transpile(mr.sql, source, target_dialect)
                        exec_sql = tr.sql
                        transpile_rules = tr.rules_applied
                        transpile_warnings = tr.warnings
                    except Exception as e:
                        # 转译失败时使用变异后的原始 SQL
                        exec_sql = mr.sql
                        transpile_rules = []
                        transpile_warnings = [f"转译失败: {e}"]
                        logger.debug("%s: 转译失败，使用原始 SQL: %s", out_relative, e)

                    # 写入变异+转译后的 SQL 文件
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

        finally:
            executor.close()

        # 写入 execution.json
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
        report_data = self._build_target_report_data(target, run_result, all_exec_results)
        return run_result, report_data

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
    def _validate_source_dialect(
        seed_files: List[Path],
        input_dir: Path,
        dialect: str,
    ) -> None:
        """校验所有种子 SQL 与源方言兼容，不兼容时抛 ValueError。"""
        sql_map = {
            str(p.relative_to(input_dir)): p.read_text(encoding="utf-8")
            for p in seed_files
        }
        incompatible = DialectDetector.detect_incompatible(sql_map, dialect)
        if incompatible:
            lines = "\n".join(
                f"  - {item['file']}: {item['reason']}" for item in incompatible
            )
            raise ValueError(
                f"以下种子 SQL 与方言 '{dialect}' 不兼容:\n{lines}\n"
                f"共 {len(incompatible)} 个文件不兼容，请确认种子 SQL 的方言是否正确。"
            )

    @staticmethod
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
