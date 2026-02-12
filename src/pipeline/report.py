"""流水线运行报告生成器。

生成端到端模糊测试运行的 Markdown + JSON 双格式报告。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.utils.logger import get_logger

logger = get_logger("pipeline.report")


class CampaignReport:
    """端到端模糊测试运行报告生成器。"""

    @staticmethod
    def generate(
        output_dir: Path,
        campaign_summary: Dict[str, Any],
        per_target: List[Dict[str, Any]],
    ) -> Path:
        """生成 report.md + report.json，返回 Markdown 报告路径。

        Args:
            output_dir: 输出根目录。
            campaign_summary: 运行汇总信息，包含键：
                - source_dialect: 源方言
                - total_seeds: 种子数量
                - mutations_per_seed: 每条种子变异数量
                - random_seed: 随机种子（可为 None）
                - elapsed_ms: 总耗时
            per_target: 每目标结果列表，每项包含键：
                - target_name: 目标名称
                - db_type: 数据库类型
                - dialect: 方言
                - version: 版本
                - skipped: 是否跳过
                - skip_reason: 跳过原因
                - total_executed: 执行总数
                - success: 成功数
                - error: 失败数
                - elapsed_ms: 耗时
                - error_messages: 错误信息列表（用于 Top-N）

        Returns:
            Markdown 报告文件路径。
        """
        md_path = output_dir / "report.md"
        json_path = output_dir / "report.json"

        # 生成 Markdown 报告
        md_content = CampaignReport._build_markdown(campaign_summary, per_target)
        md_path.write_text(md_content, encoding="utf-8")

        # 生成 JSON 报告
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

        # 标题
        lines.append("# ChimeraSQL 端到端模糊测试报告")
        lines.append("")
        lines.append(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # 运行概览
        lines.append("## 运行概览")
        lines.append("")
        lines.append(f"| 项目 | 值 |")
        lines.append(f"|------|-----|")
        lines.append(f"| 源方言 | `{summary.get('source_dialect', '')}` |")
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

        # 错误 Top-N（每目标）
        for t in per_target:
            if t.get("skipped"):
                continue
            error_messages: List[str] = t.get("error_messages", [])
            if not error_messages:
                continue

            lines.append(f"## 错误 Top-10: {t['target_name']}")
            lines.append("")

            counter = Counter(error_messages)
            top_errors = counter.most_common(10)

            lines.append("| 出现次数 | 错误信息 |")
            lines.append("|----------|----------|")
            for msg, count in top_errors:
                # 截断过长的错误信息
                truncated = msg[:200] + "..." if len(msg) > 200 else msg
                # 转义 Markdown 表格中的管道符
                truncated = truncated.replace("|", "\\|")
                lines.append(f"| {count} | {truncated} |")
            lines.append("")

        return "\n".join(lines)
