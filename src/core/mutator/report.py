"""变异报告生成模块。

提供 Markdown + JSON 双格式报告生成能力，
供批量变异编排器调用。
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class MutationReportSummary:
    """变异报告汇总统计。"""

    dialect: str
    version: str
    input_dir: str
    elapsed_ms: float
    total_seeds: int
    total_generated: int
    failed_seeds: int


@dataclass
class MutationReportDetail:
    """单个种子文件的变异详情。"""

    file: str
    status: str
    generated: int = 0
    strategies_used: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error: str = ""


class MutationReport:
    """变异报告生成器，输出 Markdown + JSON 双格式报告。"""

    @staticmethod
    def generate(
        output_dir: Path,
        summary: MutationReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成变异报告（report.md + report.json），返回 Markdown 报告路径。"""
        md_path = MutationReport._write_markdown(output_dir, summary, details)
        MutationReport._write_json(output_dir, summary, details)
        return md_path

    @staticmethod
    def _write_markdown(
        output_dir: Path,
        summary: MutationReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成 Markdown 格式报告。"""
        version_str = summary.version or "未指定"
        md_lines = [
            "# 变异报告",
            "",
            f"- **目标方言**: {summary.dialect}",
            f"- **数据库版本**: {version_str}",
            f"- **输入目录**: `{summary.input_dir}`",
            f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **耗时**: {summary.elapsed_ms:.0f} ms",
            "",
            "## 汇总",
            "",
            "| 指标 | 数量 |",
            "|------|------|",
            f"| 种子文件 | {summary.total_seeds} |",
            f"| 生成变异 | {summary.total_generated} |",
            f"| 失败种子 | {summary.failed_seeds} |",
            "",
        ]

        # 失败详情
        failed_items = [d for d in details if d["status"] == "error"]
        if failed_items:
            md_lines.append("## 失败详情")
            md_lines.append("")
            for item in failed_items:
                md_lines.append(f"### `{item['file']}`")
                md_lines.append("")
                md_lines.append(f"**错误**: {item['error']}")
                md_lines.append("")

        # 警告详情
        warned_items = [
            d for d in details if d["status"] == "ok" and d.get("warnings")
        ]
        if warned_items:
            md_lines.append("## 警告详情")
            md_lines.append("")
            for item in warned_items:
                md_lines.append(
                    f"- `{item['file']}`: {', '.join(item['warnings'])}"
                )
            md_lines.append("")

        # 文件清单
        md_lines.append("## 文件清单")
        md_lines.append("")
        md_lines.append("| 种子文件 | 状态 | 变异数 | 应用策略 |")
        md_lines.append("|----------|------|--------|----------|")
        for item in details:
            status_icon = "OK" if item["status"] == "ok" else "FAIL"
            generated = item.get("generated", 0)
            strategies = (
                ", ".join(item.get("strategies_used", []))
                if item.get("strategies_used")
                else "-"
            )
            md_lines.append(
                f"| `{item['file']}` | {status_icon} | {generated} | {strategies} |"
            )
        md_lines.append("")

        md_path = output_dir / "report.md"
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        return md_path

    @staticmethod
    def _write_json(
        output_dir: Path,
        summary: MutationReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成 JSON 格式报告。"""
        json_payload = {
            "generated_at": datetime.now().isoformat(),
            "dialect": summary.dialect,
            "version": summary.version or None,
            "input_dir": summary.input_dir,
            "elapsed_ms": round(summary.elapsed_ms, 1),
            "summary": {
                "total_seeds": summary.total_seeds,
                "total_generated": summary.total_generated,
                "failed_seeds": summary.failed_seeds,
            },
            "details": details,
        }
        json_path = output_dir / "report.json"
        json_path.write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return json_path
