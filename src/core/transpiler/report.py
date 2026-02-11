"""转译报告生成模块。

提供 Markdown + JSON 双格式报告生成能力，
供批量转译编排器调用。
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class TranspileReportSummary:
    """转译报告汇总统计。"""

    source_dialect: str
    target_dialect: str
    input_dir: str
    elapsed_ms: float
    total: int
    success: int
    warned: int
    failed: int


class TranspileReport:
    """转译报告生成器，输出 Markdown + JSON 双格式报告。"""

    @staticmethod
    def generate(
        output_dir: Path,
        summary: TranspileReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成转译报告（report.md + report.json），返回 Markdown 报告路径。"""
        md_path = TranspileReport._write_markdown(output_dir, summary, details)
        TranspileReport._write_json(output_dir, summary, details)
        return md_path

    @staticmethod
    def _write_markdown(
        output_dir: Path,
        summary: TranspileReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成 Markdown 格式报告。"""
        md_lines = [
            f"# 转译报告",
            f"",
            f"- **源方言**: {summary.source_dialect}",
            f"- **目标方言**: {summary.target_dialect}",
            f"- **输入目录**: `{summary.input_dir}`",
            f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **耗时**: {summary.elapsed_ms:.0f} ms",
            f"",
            f"## 汇总",
            f"",
            f"| 指标 | 数量 |",
            f"|------|------|",
            f"| 总计 | {summary.total} |",
            f"| 成功 | {summary.success} |",
            f"| 有警告 | {summary.warned} |",
            f"| 失败 | {summary.failed} |",
            f"",
        ]

        # 失败详情
        failed_items = [d for d in details if d["status"] == "error"]
        if failed_items:
            md_lines.append("## 失败详情")
            md_lines.append("")
            for item in failed_items:
                md_lines.append(f"### `{item['file']}`")
                md_lines.append(f"")
                md_lines.append(f"**错误**: {item['error']}")
                md_lines.append(f"")

        # 警告详情
        warned_items = [d for d in details if d["status"] == "ok" and d["warnings"]]
        if warned_items:
            md_lines.append("## 警告详情")
            md_lines.append("")
            for item in warned_items:
                md_lines.append(f"- `{item['file']}`: {', '.join(item['warnings'])}")
            md_lines.append("")

        # 全量文件清单
        md_lines.append("## 文件清单")
        md_lines.append("")
        md_lines.append("| 文件 | 状态 | 应用规则 |")
        md_lines.append("|------|------|----------|")
        for item in details:
            status_icon = "OK" if item["status"] == "ok" else "FAIL"
            rules = ", ".join(item["rules_applied"]) if item["rules_applied"] else "-"
            md_lines.append(f"| `{item['file']}` | {status_icon} | {rules} |")
        md_lines.append("")

        md_path = output_dir / "report.md"
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        return md_path

    @staticmethod
    def _write_json(
        output_dir: Path,
        summary: TranspileReportSummary,
        details: List[Dict[str, Any]],
    ) -> Path:
        """生成 JSON 格式报告。"""
        json_payload = {
            "generated_at": datetime.now().isoformat(),
            "source_dialect": summary.source_dialect,
            "target_dialect": summary.target_dialect,
            "input_dir": summary.input_dir,
            "elapsed_ms": round(summary.elapsed_ms, 1),
            "summary": {
                "total": summary.total,
                "success": summary.success,
                "warned": summary.warned,
                "failed": summary.failed,
            },
            "details": details,
        }
        json_path = output_dir / "report.json"
        json_path.write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return json_path
