"""转译引擎正确率验证脚本。

两种模式：
  fuzz     — 种子SQL → 变异N条 → 转译 → 双库执行 → 对比（评估变异后SQL的转译质量）
  transpile— 种子SQL → 直接转译 → 双库执行 → 对比（纯净评估转译引擎本身）

用法：
    # fuzz模式（默认）：变异 + 转译 + 执行 + 对比
    python tests/transpile_correctness_test.py
    python tests/transpile_correctness_test.py --seed-dir data/seeds -n 5

    # transpile模式：纯转译验证，不变异
    python tests/transpile_correctness_test.py --mode transpile
    python tests/transpile_correctness_test.py --mode transpile --seed-dir data/seeds

    # 其他参数
    python tests/transpile_correctness_test.py --seed-sql "SELECT * FROM t_users"
    python tests/transpile_correctness_test.py --random-pick 10 --seed 42
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.connector.factory import ConnectorFactory
from src.core.mutator import (
    CapabilityProfile,
    MutationEngine,
    create_default_registry,
)
from src.core.transpiler import Dialect, SQLTranspiler
from src.utils.config_loader import ConfigLoader
from src.utils.json_utils import rows_to_jsonable
from src.utils.logger import get_logger
from src.verifier.comparator import ResultComparator, ComparisonVerdict

logger = get_logger("transpile_correctness_test")

# 静默其他模块的 INFO 日志，只保留本项目脚本的进度输出和全局 WARNING/ERROR
import logging
logging.getLogger().handlers[0].setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

SEED_DIALECT = Dialect.SQLITE
TARGET_DIALECT = Dialect.ORACLE
SQLGLOT_SEED_DIALECT = "sqlite"
SQLGLOT_TARGET_DIALECT = "oracle"

# 每条SQL执行超时（秒）— 防止递归CTE等导致无限执行
EXEC_TIMEOUT = 10


# ── 数据结构 ──


class SQLTestResult:
    """单条 SQL 的测试结果。"""

    __slots__ = (
        "label",
        "seed_sql",
        "mutated_sql",
        "transpiled_sql",
        "strategies_applied",
        "rules_applied",
        "transpile_warnings",
        "source_status",
        "source_rows",
        "source_error",
        "target_status",
        "target_rows",
        "target_error",
        "verdict",
        "source_row_count",
        "target_row_count",
        "diff_type",
    )

    def __init__(self, label: str, seed_sql: str, mutated_sql: str) -> None:
        self.label = label
        self.seed_sql = seed_sql
        self.mutated_sql = mutated_sql
        self.transpiled_sql = ""
        self.strategies_applied: List[str] = []
        self.rules_applied: List[str] = []
        self.transpile_warnings: List[str] = []
        self.source_status = ""
        self.source_rows: List[tuple] = []
        self.source_error: Optional[str] = None
        self.target_status = ""
        self.target_rows: List[tuple] = []
        self.target_error: Optional[str] = None
        self.verdict = ""
        self.source_row_count = 0
        self.target_row_count = 0
        self.diff_type = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "seed_sql": self.seed_sql,
            "mutated_sql": self.mutated_sql,
            "transpiled_sql": self.transpiled_sql,
            "strategies_applied": self.strategies_applied,
            "rules_applied": self.rules_applied,
            "transpile_warnings": self.transpile_warnings,
            "source": {
                "status": self.source_status,
                "row_count": self.source_row_count,
                "rows": rows_to_jsonable(self.source_rows) if self.source_status == "ok" else [],
                "error": self.source_error,
            },
            "target": {
                "status": self.target_status,
                "row_count": self.target_row_count,
                "rows": rows_to_jsonable(self.target_rows) if self.target_status == "ok" else [],
                "error": self.target_error,
            },
            "verdict": self.verdict,
            "diff_type": self.diff_type,
        }


# ── 结果比较（复用项目 ResultComparator） ──

_comparator = ResultComparator()


def judge_verdict(result: SQLTestResult) -> None:
    """根据执行结果判定 verdict。"""
    detail = _comparator.compare(
        sql_name=result.label,
        source_rows=result.source_rows,
        target_rows=result.target_rows,
        source_status=result.source_status,
        target_status=result.target_status,
        source_error=result.source_error,
        target_error=result.target_error,
        source_sql=result.mutated_sql,
        target_sql=result.transpiled_sql,
        rules_applied=result.rules_applied,
    )
    result.verdict = detail.verdict.value
    result.source_row_count = detail.source_row_count
    result.target_row_count = detail.target_row_count
    result.diff_type = detail.diff_type


# ── 核心 ──


def _execute_sqlite_thread(sql: str) -> List[tuple]:
    """在工作线程中创建 SQLite 连接并执行（规避 sqlite3 跨线程限制）。"""
    conn = ConnectorFactory.create("sqlite")
    conn.connect()
    try:
        return conn.execute_query(sql)
    finally:
        conn.close()


def _execute_oracle_thread(connector, sql: str) -> List[tuple]:
    """在工作线程中通过 Oracle 连接执行查询。"""
    return connector.execute_query(sql)


def safe_execute(connector, sql: str, db_type: str, timeout: float = EXEC_TIMEOUT) -> List[tuple]:
    """带超时的 execute_query。

    SQLite: 在子线程中新建连接执行（规避跨线程限制）。
    Oracle: 在子线程中执行（Oracle 无跨线程限制）。
    """
    if db_type == "sqlite":
        future = _executor.submit(_execute_sqlite_thread, sql)
    else:
        future = _executor.submit(_execute_oracle_thread, connector, sql)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        raise TimeoutError(f"SQL 执行超时 ({timeout}s)，已取消")


_executor = ThreadPoolExecutor(max_workers=1)


def collect_seeds(
    seed_dir: Optional[str],
    seed_sql: Optional[str],
    random_pick: Optional[int],
) -> List[Tuple[str, str]]:
    """收集种子 SQL，返回 [(label, sql_text), ...]。"""
    if seed_sql:
        return [("<direct_input>", seed_sql.strip())]

    config = ConfigLoader()
    default_seed_dir = config.get("fuzzing.seed_dir", "data/seeds")
    sd = Path(seed_dir) if seed_dir else (PROJECT_ROOT / default_seed_dir).resolve()
    if not sd.exists():
        raise FileNotFoundError(f"种子目录不存在: {sd}")

    files = sorted(sd.rglob("*.sql"))
    if not files:
        raise FileNotFoundError(f"种子目录中无 .sql 文件: {sd}")

    if random_pick and random_pick < len(files):
        files = random.sample(files, random_pick)

    return [(f.relative_to(sd).as_posix(), f.read_text("utf-8").strip()) for f in files]


def build_mutator(target_dialect: str) -> MutationEngine:
    """构建变异引擎。"""
    profile, _ = CapabilityProfile.from_dialect_version(target_dialect)
    registry = create_default_registry()
    return MutationEngine(
        profile=profile,
        registry=registry,
        source_dialect=SQLGLOT_SEED_DIALECT,
    )


def _execute_and_compare(
    target_conn,
    transpiler: SQLTranspiler,
    items: List[Tuple[str, str, str]],
    # items: [(label, source_sql_to_exec, sql_for_transpile), ...]
) -> List[SQLTestResult]:
    """核心：转译 + 双库执行 + 判定。返回结果列表。"""
    results: List[SQLTestResult] = []

    for label, source_sql, sql_for_transpile in items:
        result = SQLTestResult(label=label, seed_sql=source_sql, mutated_sql=source_sql)

        # 转译：SQLite → Oracle
        try:
            tr = transpiler.transpile(sql_for_transpile, SEED_DIALECT, TARGET_DIALECT)
            result.transpiled_sql = tr.sql
            result.rules_applied = tr.rules_applied
            result.transpile_warnings = tr.warnings
        except Exception as e:
            result.transpile_warnings = [f"转译异常: {e}"]
            result.transpiled_sql = sql_for_transpile
            result.target_status = "error"
            result.target_error = f"转译失败: {e}"

        # 源库 (SQLite) 执行
        try:
            result.source_rows = safe_execute(None, source_sql, "sqlite")
            result.source_status = "ok"
        except Exception as e:
            result.source_status = "error"
            result.source_error = str(e)

        # 目标库 (Oracle) 执行
        if result.target_status != "error":
            try:
                result.target_rows = safe_execute(target_conn, result.transpiled_sql, "oracle")
                result.target_status = "ok"
            except Exception as e:
                result.target_status = "error"
                result.target_error = str(e)

        judge_verdict(result)
        results.append(result)

        icon = _VERDICT_ICON.get(result.verdict, "?")
        logger.info("  [%s] %s", icon, result.label)

    return results


def _build_metrics(results: List[SQLTestResult], **extra) -> Dict[str, Any]:
    """从结果列表计算指标。"""
    total = len(results)
    target_ok = sum(1 for r in results if r.target_status == "ok")
    syntax_rate = target_ok / total * 100 if total else 0

    comparable = [r for r in results if r.source_status == "ok" and r.target_status == "ok"]
    equivalent = sum(1 for r in comparable if r.verdict == "equivalent")
    db_behavior = sum(1 for r in comparable if r.verdict == "db_behavior_diff")
    # 语义正确率 = 等价 + 已知数据库行为差异（转译本身正确）
    semantic_rate = (equivalent + db_behavior) / len(comparable) * 100 if comparable else 0

    return {
        **extra,
        "total_sql": total,
        "target_exec_ok": target_ok,
        "target_exec_error": total - target_ok,
        "syntax_correctness_rate": round(syntax_rate, 2),
        "comparable_count": len(comparable),
        "equivalent_count": equivalent,
        "db_behavior_diff_count": db_behavior,
        "partial_match_count": sum(1 for r in comparable if r.verdict == "partial_match"),
        "mismatch_count": sum(1 for r in comparable if r.verdict == "mismatch"),
        "semantic_equivalence_rate": round(semantic_rate, 2),
        "source_error_count": sum(1 for r in results if r.source_status == "error" and r.target_status != "error"),
        "both_error_count": sum(1 for r in results if r.verdict == "both_error"),
    }


def run_fuzz(
    seeds: List[Tuple[str, str]],
    mutations_per_seed: int,
    rng_seed: Optional[int],
) -> Tuple[List[SQLTestResult], Dict[str, Any]]:
    """fuzz模式：变异 → 转译 → 双库执行 → 对比。"""
    mutator = build_mutator(SQLGLOT_TARGET_DIALECT)
    transpiler = SQLTranspiler()

    target_conn = ConnectorFactory.create("oracle")
    target_conn.connect()
    logger.info("数据库连接成功: Oracle")

    results: List[SQLTestResult] = []
    mutation_fail_seeds: List[str] = []
    t0 = time.perf_counter()

    try:
        for idx, (label, seed_sql) in enumerate(seeds, 1):
            logger.info("[%d/%d] 种子: %s", idx, len(seeds), label)

            mutation_results = mutator.mutate_many(seed_sql, label, mutations_per_seed)
            if not mutation_results:
                logger.warning("  种子 %s 变异失败，跳过", label)
                mutation_fail_seeds.append(label)
                continue

            items = [
                (f"{label}_mut{i:02d}", mr.sql, mr.sql)
                for i, mr in enumerate(mutation_results, 1)
            ]
            batch = _execute_and_compare(target_conn, transpiler, items)

            # 补填 seed_sql 和 strategies_applied
            for r, mr in zip(batch, mutation_results):
                r.seed_sql = seed_sql
                r.strategies_applied = mr.strategies_applied
            results.extend(batch)

    finally:
        target_conn.close()
        _executor.shutdown(wait=False)

    elapsed_ms = (time.perf_counter() - t0) * 1000
    metrics = _build_metrics(
        results,
        seed_count=len(seeds),
        mutations_per_seed=mutations_per_seed,
        expected_mutations=len(seeds) * mutations_per_seed,
        mutation_fail_count=len(mutation_fail_seeds),
        mutation_fail_seeds=mutation_fail_seeds,
        elapsed_ms=round(elapsed_ms, 1),
    )
    return results, metrics


def run_transpile(
    seeds: List[Tuple[str, str]],
) -> Tuple[List[SQLTestResult], Dict[str, Any]]:
    """transpile模式：直接转译种子SQL → 双库执行 → 对比。"""
    transpiler = SQLTranspiler()

    target_conn = ConnectorFactory.create("oracle")
    target_conn.connect()
    logger.info("数据库连接成功: Oracle")

    items = [(label, sql, sql) for label, sql in seeds]
    t0 = time.perf_counter()

    try:
        results = _execute_and_compare(target_conn, transpiler, items)
    finally:
        target_conn.close()
        _executor.shutdown(wait=False)

    elapsed_ms = (time.perf_counter() - t0) * 1000
    metrics = _build_metrics(
        results,
        seed_count=len(seeds),
        mutations_per_seed=0,
        expected_mutations=len(seeds),
        mutation_fail_count=0,
        mutation_fail_seeds=[],
        elapsed_ms=round(elapsed_ms, 1),
    )
    return results, metrics


# ── 报告 ──

_VERDICT_ICON = {
    "equivalent": "[=]", "partial_match": "[~]", "mismatch": "[x]",
    "target_error": "[!]", "source_error": "[S]", "both_error": "[B]",
    "db_behavior_diff": "[D]",
}
_VERDICT_LABEL = {
    "equivalent": "语义等价", "partial_match": "部分匹配(行序不同)",
    "mismatch": "语义不等价", "target_error": "目标库执行失败",
    "source_error": "源库执行失败", "both_error": "双库均失败",
    "db_behavior_diff": "已知数据库行为差异",
}


def print_summary(metrics: Dict[str, Any], mode: str) -> None:
    """打印终端摘要。"""
    m = metrics
    mode_label = "fuzz (变异+转译)" if mode == "fuzz" else "transpile (纯转译)"
    print(f"""
{'='*60}
  转译引擎正确率验证报告  [{mode_label}]
{'='*60}

  种子SQL数:                {m['seed_count']}""")
    if mode == "fuzz":
        print(f"""  每条种子预期变异:          {m['mutations_per_seed']}
  预期变异总数:              {m['expected_mutations']}
  变异失败种子数:            {m['mutation_fail_count']}""")
    print(f"""  实际参与执行的SQL数:       {m['total_sql']}

{'─'*60}

  语法正确率:  {m['syntax_correctness_rate']:>6.2f}%  ({m['target_exec_ok']}/{m['total_sql']} 条在Oracle执行成功)
  语义正确率:  {m['semantic_equivalence_rate']:>6.2f}%  ({m['equivalent_count'] + m.get('db_behavior_diff_count', 0)}/{m['comparable_count']} 条转译结果正确)

{'─'*60}

  判定分布:
    equivalent          {m['equivalent_count']:>4}     target_error      {m['target_exec_error']:>4}
    db_behavior_diff    {m.get('db_behavior_diff_count', 0):>4}     source_error      {m.get('source_error_count', 0):>4}
    partial_match       {m['partial_match_count']:>4}     both_error        {m.get('both_error_count', 0):>4}
    mismatch            {m['mismatch_count']:>4}

  耗时: {m['elapsed_ms']:.1f} ms
{'='*60}
""")


def write_report(
    results: List[SQLTestResult],
    metrics: Dict[str, Any],
    args: argparse.Namespace,
) -> Path:
    """生成 Markdown 详细报告。"""
    output_dir = PROJECT_ROOT / "tests" / "transpile_correctness_result"
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = output_dir / f"report_{ts}.md"
    json_path = output_dir / f"report_{ts}.json"

    m = metrics
    L: List[str] = []

    # ── 标题与参数 ──
    mode_label = "fuzz (变异+转译)" if args.mode == "fuzz" else "transpile (纯转译)"
    L.append(f"# 转译引擎正确率验证报告 [{mode_label}]\n")
    L.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    seed_desc = args.seed_dir or "默认(data/seeds)"
    if args.random_pick:
        seed_desc += f" (随机抽取 {args.random_pick} 条)"
    if args.seed_sql:
        seed_desc = f"直接输入: `{args.seed_sql[:80]}`"
    L.append(f"**种子来源**: {seed_desc}")
    if args.mode == "fuzz":
        L.append(f"**每条种子变异数**: {args.count}")
    L.append(f"**随机种子**: {args.seed if args.seed is not None else '无'}")
    L.append("")

    # ── 统计链 ──
    L.append("## 1. 统计概览\n")
    L.append("| 阶段 | 数量 |")
    L.append("|------|------|")
    L.append(f"| 种子 SQL | {m['seed_count']} |")
    if args.mode == "fuzz":
        L.append(f"| 预期变异总数 (种子 x 每条变异数) | {m['expected_mutations']} |")
        L.append(f"| 变异失败种子 | {m['mutation_fail_count']} |")
        if m["mutation_fail_seeds"]:
            for s in m["mutation_fail_seeds"]:
                L.append(f"| &nbsp;&nbsp;`{s}` | — |")
    L.append(f"| **实际参与执行的 SQL** | **{m['total_sql']}** |")
    L.append("")

    # ── 正确率（放在逐条之后更好，但用户要求先看SQL再算，放最后） ──
    # 先放执行分布
    L.append("## 2. 执行分布\n")
    L.append("| 判定 | 数量 | 说明 |")
    L.append("|------|------|------|")
    for v in ["equivalent", "partial_match", "mismatch", "target_error", "source_error", "both_error"]:
        cnt = sum(1 for r in results if r.verdict == v)
        if cnt > 0:
            L.append(f"| {_VERDICT_LABEL.get(v, v)} | {cnt} | {_VERDICT_ICON.get(v, '?')} |")
    L.append("")

    # ── 失败/不等价的 SQL 详情 ──
    failed = [r for r in results if r.verdict != "equivalent"]
    L.append(f"## 3. 异常 SQL 详情 ({len(failed)} 条)\n")

    if not failed:
        L.append("所有 SQL 执行成功且语义等价。\n")
    else:
        # 按种子SQL分组
        from collections import OrderedDict
        groups: Dict[str, List[SQLTestResult]] = OrderedDict()
        for r in failed:
            groups.setdefault(r.seed_sql, []).append(r)

        for g_idx, (seed_sql, group) in enumerate(groups.items(), 1):
            L.append(f"### 种子 {g_idx}/{len(groups)}\n")
            L.append(f"```sql\n{seed_sql}\n```\n")

            for r in group:
                icon = _VERDICT_ICON.get(r.verdict, "[?]")
                label = _VERDICT_LABEL.get(r.verdict, r.verdict)

                L.append(f"#### {icon} {r.label}\n")
                L.append(f"- **判定**: {label}")

                if args.mode == "fuzz" and r.mutated_sql != r.seed_sql:
                    L.append(f"- **变异SQL** (SQLite):")
                    L.append(f"  ```sql\n  {r.mutated_sql}\n  ```")

                L.append(f"- **转译SQL** (Oracle):")
                L.append(f"  ```sql\n  {r.transpiled_sql}\n  ```")

                L.append(f"- **源库 SQLite**: ", )
                if r.source_status == "ok":
                    L.append(f"`OK` ({r.source_row_count} 行)")
                    if r.source_rows:
                        L.append("  ```")
                        L.append(format_rows(r.source_rows, 10))
                        L.append("  ```")
                else:
                    L.append(f"`ERROR` — {r.source_error}")

                L.append(f"- **目标库 Oracle**: ", )
                if r.target_status == "ok":
                    L.append(f"`OK` ({r.target_row_count} 行)")
                    if r.target_rows:
                        L.append("  ```")
                        L.append(format_rows(r.target_rows, 10))
                        L.append("  ```")
                else:
                    L.append(f"`ERROR` — {r.target_error}")

                if r.diff_type:
                    L.append(f"- **差异类型**: `{r.diff_type}`")

                L.append("")

    # ── 正确率计算（放在最后） ──
    L.append("## 4. 正确率\n")
    L.append(f"> **语法正确率** = 目标库(Oracle)执行成功数 / 参与执行的SQL总数")
    L.append(f"> ")
    L.append(f"> = {m['target_exec_ok']} / {m['total_sql']} = **{m['syntax_correctness_rate']:.2f}%**\n")
    L.append(f"> **语义正确率** = 双库结果等价数 / 双库均执行成功数")
    L.append(f"> ")
    L.append(f"> = {m['equivalent_count']} / {m['comparable_count']} = **{m['semantic_equivalence_rate']:.2f}%**\n")
    L.append(f"\n耗时: {m['elapsed_ms']:.1f} ms\n")

    md_path.write_text("\n".join(L), encoding="utf-8")

    # ── JSON 报告 ──
    json_data = {
        "generated_at": datetime.now().isoformat(),
        "args": vars(args),
        "metrics": metrics,
        "results": [r.to_dict() for r in results],
    }
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")

    return md_path


def format_rows(rows: List[tuple], max_rows: int = 5) -> str:
    """格式化结果行为可读字符串。"""
    if not rows:
        return "(空)"
    shown = rows[:max_rows]
    lines = [str(row) for row in shown]
    if len(rows) > max_rows:
        lines.append(f"... 共 {len(rows)} 行")
    return "\n".join(lines)


# ── CLI ──


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="转译引擎正确率验证",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                                # fuzz模式: 默认种子，每条变异3次
  %(prog)s -n 5                           # fuzz模式: 每条变异5次
  %(prog)s --mode transpile               # transpile模式: 纯转译验证
  %(prog)s --mode transpile --seed-dir data/seeds
  %(prog)s --seed-sql "SELECT * FROM t_users"
  %(prog)s --random-pick 10 --seed 42
        """,
    )
    p.add_argument("--mode", choices=["fuzz", "transpile"], default="fuzz",
                   help="运行模式: fuzz=变异+转译 (默认), transpile=纯转译验证")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--seed-dir", type=str, default=None, help="种子SQL目录 (默认使用 config 中的配置)")
    g.add_argument("--seed-sql", type=str, default=None, help="直接输入一条种子SQL")
    p.add_argument("-n", "--count", type=int, default=3, help="每条种子的变异数量，仅fuzz模式 (默认: 3)")
    p.add_argument("--seed", type=int, default=None, help="随机种子，保证可复现")
    p.add_argument("--random-pick", type=int, default=None, help="从种子目录中随机抽取N条")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    seeds = collect_seeds(args.seed_dir, args.seed_sql, args.random_pick)
    logger.info("收集到 %d 条种子SQL", len(seeds))

    if args.mode == "fuzz":
        results, metrics = run_fuzz(seeds, args.count, args.seed)
    else:
        results, metrics = run_transpile(seeds)

    print_summary(metrics, args.mode)

    report_path = write_report(results, metrics, args)
    print(f"\n详细报告已写入: {report_path}")
