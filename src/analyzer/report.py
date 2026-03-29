"""模糊测试分析报告生成器。

生成 Markdown + JSON 双格式分析报告。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from src.utils.logger import get_logger

from .result import AnalysisResult

logger = get_logger("analyzer.report")


class AnalysisReport:
    """分析报告生成器。"""

    @staticmethod
    def generate(
        output_dir: Path,
        analysis: AnalysisResult,
        source_dialect: str,
        target_dialect: str,
        total_seeds: int,
        mutations_per_seed: int,
    ) -> Path:
        """生成 analysis.md + analysis.json，返回 Markdown 报告路径。

        Args:
            output_dir: 输出目录。
            analysis: AnalysisResult 分析结果。
            source_dialect: 源方言。
            target_dialect: 目标方言。
            total_seeds: 种子数量。
            mutations_per_seed: 每种子变异数量。

        Returns:
            Markdown 报告文件路径。
        """
        md_path = output_dir / "analysis.md"
        json_path = output_dir / "analysis.json"

        md_content = AnalysisReport._build_markdown(
            analysis, source_dialect, target_dialect, total_seeds, mutations_per_seed,
        )
        md_path.write_text(md_content, encoding="utf-8")

        json_payload = {
            "generated_at": datetime.now().isoformat(),
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
            "total_seeds": total_seeds,
            "mutations_per_seed": mutations_per_seed,
            **analysis.to_dict(),
        }
        json_path.write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        logger.info("分析报告已生成: %s", md_path)
        return md_path

    @staticmethod
    def _build_markdown(
        analysis: AnalysisResult,
        source_dialect: str,
        target_dialect: str,
        total_seeds: int,
        mutations_per_seed: int,
    ) -> str:
        """构建 Markdown 报告内容。"""
        lines: List[str] = []

        # 标题
        lines.append("# ChimeraSQL 模糊测试分析报告")
        lines.append("")
        lines.append(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # ── 概览 ──
        lines.append("## 概览")
        lines.append("")
        lines.append("| 项目 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 源方言 | `{source_dialect}` |")
        lines.append(f"| 目标方言 | `{target_dialect}` |")
        lines.append(f"| 种子数量 | {total_seeds} |")
        lines.append(f"| 每种子变异 | {mutations_per_seed} |")
        lines.append(f"| 执行总数 | {analysis.total_executed} |")
        lines.append(f"| 成功 | {analysis.success_count} |")
        lines.append(f"| 失败 | {analysis.error_count} |")
        lines.append(f"| 成功率 | {analysis.success_rate:.1%} |")
        lines.append(f"| 总耗时 | {analysis.total_elapsed_ms:.0f} ms |")
        lines.append(f"| 平均耗时 | {analysis.avg_elapsed_ms:.2f} ms |")
        lines.append("")

        # ── 错误分析 ──
        if analysis.error_categories:
            lines.append("## 错误分析")
            lines.append("")
            lines.append("| 错误类型 | 出现次数 | 示例 |")
            lines.append("|----------|----------|------|")
            for ec in analysis.error_categories:
                # 转义 Markdown 表格中的管道符
                msg = ec.example_message[:120].replace("|", "\\|")
                lines.append(f"| {ec.category} | {ec.count} | `{msg}` |")
            lines.append("")

        # ── 变异策略效果 ──
        if analysis.strategy_stats:
            lines.append("## 变异策略效果")
            lines.append("")
            lines.append("| 策略 | 总数 | 成功 | 失败 | 成功率 |")
            lines.append("|------|------|------|------|--------|")
            for ss in analysis.strategy_stats:
                lines.append(
                    f"| `{ss.strategy_id}` | {ss.total} "
                    f"| {ss.success} | {ss.error} | {ss.success_rate:.1%} |"
                )
            lines.append("")

        # ── 转译效果 ──
        ts = analysis.transpile_stats
        if ts.total_transpiled > 0:
            lines.append("## 转译效果")
            lines.append("")
            lines.append(f"应用转译规则的 SQL 数量: {ts.total_transpiled}")
            lines.append(f"转译后成功: {ts.success_after_transpile}")
            lines.append(f"转译后失败: {ts.error_after_transpile}")
            lines.append(f"转译警告数: {ts.transpile_warnings_count}")
            lines.append("")

            if ts.transpile_applied_rules:
                lines.append("| 转译规则 | 应用次数 |")
                lines.append("|----------|----------|")
                for rule, count in ts.transpile_applied_rules.items():
                    lines.append(f"| `{rule}` | {count} |")
                lines.append("")

        # ── 性能分析 ──
        if analysis.slowest_entries:
            lines.append("## 性能分析（最慢 Top-5）")
            lines.append("")
            lines.append("| 文件 | 耗时 (ms) | 状态 |")
            lines.append("|------|-----------|------|")
            for pe in analysis.slowest_entries:
                lines.append(
                    f"| `{pe.file}` | {pe.elapsed_ms:.2f} | {pe.status} |"
                )
            lines.append("")

        # ── 种子覆盖 ──
        if analysis.seed_coverage:
            lines.append("## 种子覆盖")
            lines.append("")
            lines.append("| 种子文件 | 变异数 | 成功 | 失败 | 成功率 |")
            lines.append("|----------|--------|------|------|--------|")
            for sc in analysis.seed_coverage:
                lines.append(
                    f"| `{sc.seed_file}` | {sc.total} "
                    f"| {sc.success} | {sc.error} | {sc.success_rate:.1%} |"
                )
            lines.append("")

        return "\n".join(lines)
