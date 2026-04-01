"""命令行接口模块。

负责 argparse 参数解析、子命令分发、顶层错误处理。
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from src.core.mutator import BatchMutationRunner
from src.core.transpiler import BatchTranspileRunner, Dialect
from src.pipeline import CampaignRunner
from src.testbed import InitPipeline
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("chimera")

# Dialect 枚举名到值的映射（用于 CLI 参数解析）
_DIALECT_CHOICES = {d.value: d for d in Dialect}


def _validate_dialect(raw: str) -> tuple:
    """解析并校验 dialect:version 参数。

    Args:
        raw: 原始参数字符串，如 "oracle:21c"。

    Returns:
        (dialect, version) 元组。

    Raises:
        ValueError: 格式错误、方言不支持、或未在 config.yaml 中注册。
    """
    if ":" not in raw:
        raise ValueError(
            f"参数格式错误: '{raw}'。"
            f"请使用 dialect:version 格式，如 oracle:21c 或 sqlite:3.52.0。"
        )
    dialect, version = raw.split(":", 1)
    dialect = dialect.strip()
    version = version.strip()
    if not dialect or not version:
        raise ValueError(
            f"参数格式错误: '{raw}'。方言和版本均不能为空。"
        )
    if dialect not in _DIALECT_CHOICES:
        raise ValueError(
            f"不支持的方言: '{dialect}'。支持的方言: {list(_DIALECT_CHOICES.keys())}"
        )
    from src.pipeline import resolve_database
    resolve_database(dialect)
    return dialect, version


def _resolve_count(args: argparse.Namespace) -> int:
    """解析变异数量：CLI 参数 > config.yaml > 默认值 3。

    Args:
        args: 包含 count 属性的 argparse Namespace。

    Returns:
        每条种子的变异数量。
    """
    count = args.count
    if count is not None:
        return count
    try:
        config = ConfigLoader()
        return config.get(
            "mutation.policies.balanced_default.max_mutations_per_seed", 3
        )
    except FileNotFoundError:
        return 3


def _build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="chimera_sql",
        description="ChimeraSQL — 跨数据库模糊测试工具",
    )
    subparsers = parser.add_subparsers(dest="command", help="可用子命令")

    # init 子命令
    subparsers.add_parser(
        "init",
        help="初始化测试基础设施（Schema → Data → Seeds）",
    )

    # transpile 子命令
    tp = subparsers.add_parser(
        "transpile",
        help="批量方言转译",
    )
    tp.add_argument(
        "input_dir",
        help="包含 .sql 文件的输入目录（递归扫描）",
    )
    tp.add_argument(
        "-s",
        "--source",
        required=True,
        help="源 SQL 方言（格式: dialect:version，如 sqlite:3.52.0）",
    )
    tp.add_argument(
        "-t",
        "--target",
        required=True,
        help="目标 SQL 方言（格式: dialect:version，如 oracle:21c）",
    )

    # mutate 子命令
    mt = subparsers.add_parser(
        "mutate",
        help="批量 AST 变异",
    )
    mt.add_argument(
        "input_dir",
        help="包含 .sql 种子文件的输入目录（递归扫描）",
    )
    mt.add_argument(
        "-d",
        "--dialect",
        required=True,
        help="目标数据库方言（格式: dialect:version，如 oracle:21c）",
    )
    mt.add_argument(
        "-n",
        "--count",
        type=int,
        default=None,
        help="每条种子生成的变异数量（默认从 config.yaml 读取）",
    )
    mt.add_argument(
        "--seed",
        type=int,
        default=None,
        help="随机种子（用于可复现结果）",
    )

    # verify 子命令
    vf = subparsers.add_parser(
        "verify",
        help="验证转译正确率（双端执行 + 结果比较）",
    )
    vf.add_argument(
        "input_dir",
        help="包含 .sql 种子文件的输入目录（递归扫描）",
    )
    vf.add_argument(
        "-s",
        "--source",
        required=True,
        help="源 SQL 方言（格式: dialect:version，如 sqlite:3.52.0）",
    )
    vf.add_argument(
        "-t",
        "--target",
        required=True,
        help="目标 SQL 方言（格式: dialect:version，如 oracle:21c）",
    )
    vf.add_argument(
        "--skip-init",
        action="store_true",
        help="跳过数据库 Schema/Data 初始化",
    )

    # run 子命令
    rn = subparsers.add_parser(
        "run",
        help="端到端流水线（变异 → 转译 → 执行 → 分析报告）",
    )
    rn.add_argument(
        "input_dir",
        help="包含 .sql 文件的输入目录（递归扫描）",
    )
    rn.add_argument(
        "-s",
        "--source",
        required=True,
        help="源 SQL 方言（格式: dialect:version，如 sqlite:3.52.0）",
    )
    rn.add_argument(
        "-t",
        "--target",
        required=True,
        help="目标 SQL 方言（格式: dialect:version，如 oracle:21c）",
    )
    rn.add_argument(
        "--mode",
        required=True,
        choices=["fuzz", "exec"],
        help="流水线模式: fuzz=变异→转译→执行→分析, exec=转译→执行→分析",
    )
    rn.add_argument(
        "-n",
        "--count",
        type=int,
        default=None,
        help="[fuzz] 每条种子生成的变异数量（默认从 config.yaml 读取）",
    )
    rn.add_argument(
        "--seed",
        type=int,
        default=None,
        help="[fuzz] 随机种子（用于可复现结果）",
    )

    return parser


def _handle_init(_args: argparse.Namespace) -> None:
    """处理 init 子命令。"""
    InitPipeline().run()


def _handle_transpile(args: argparse.Namespace) -> None:
    """处理 transpile 子命令。"""
    input_dir = Path(args.input_dir)
    source_dialect, source_version = _validate_dialect(args.source)
    target_dialect, target_version = _validate_dialect(args.target)

    source = _DIALECT_CHOICES[source_dialect]
    target = _DIALECT_CHOICES[target_dialect]

    result = BatchTranspileRunner().run(input_dir, source, target)

    logger.info("=" * 50)
    logger.info(
        "转译完成: %d 成功, %d 失败, 共 %d 条 | 耗时 %.0f ms",
        result.success,
        result.failed,
        result.total,
        result.elapsed_ms,
    )
    logger.info("输出目录: %s", result.output_dir)
    logger.info("转译报告: %s", result.report_path)


def _handle_mutate(args: argparse.Namespace) -> None:
    """处理 mutate 子命令。"""
    input_dir = Path(args.input_dir)
    dialect, version = _validate_dialect(args.dialect)

    count = _resolve_count(args)

    result = BatchMutationRunner().run(
        input_dir=input_dir,
        dialect=dialect,
        version=version,
        count_per_seed=count,
        random_seed=args.seed,
    )

    logger.info("=" * 50)
    logger.info(
        "变异完成: %d 种子, %d 变异生成, %d 失败 | 耗时 %.0f ms",
        result.total_seeds,
        result.total_generated,
        result.failed_seeds,
        result.elapsed_ms,
    )
    logger.info("输出目录: %s", result.output_dir)
    logger.info("变异报告: %s", result.report_path)


def _handle_verify(args: argparse.Namespace) -> None:
    """处理 verify 子命令（转译正确率验证）。"""
    from src.verifier.runner import VerifyRunner

    input_dir = Path(args.input_dir)
    source_dialect, source_version = _validate_dialect(args.source)
    target_dialect, target_version = _validate_dialect(args.target)

    source = _DIALECT_CHOICES[source_dialect]
    target = _DIALECT_CHOICES[target_dialect]

    runner = VerifyRunner()
    report = runner.run(
        seed_dir=input_dir,
        source_dialect=source,
        target_dialect=target,
        init_db=not args.skip_init,
    )

    logger.info("=" * 50)
    logger.info(
        "Level 1 - 执行通过率: %.1f%% (%d/%d)",
        report.metrics.execution_pass_rate * 100,
        report.metrics.target_exec_ok,
        report.metrics.total,
    )
    logger.info(
        "Level 2 - 语义等价率: %.1f%% (%d/%d)",
        report.metrics.equivalence_rate * 100,
        report.metrics.equivalent,
        report.metrics.equivalent
        + report.metrics.partial_match
        + report.metrics.mismatch,
    )
    logger.info(
        "  equivalent=%d | partial=%d | mismatch=%d | target_error=%d | source_error=%d | skip=%d",
        report.metrics.equivalent,
        report.metrics.partial_match,
        report.metrics.mismatch,
        report.metrics.target_exec_fail,
        report.metrics.source_exec_fail,
        report.metrics.skipped,
    )
    logger.info("报告: %s", report.report_path)
    logger.info("JSON: %s", report.json_path)


def _handle_run(args: argparse.Namespace) -> None:
    """处理 run 子命令（端到端流水线）。"""
    input_dir = Path(args.input_dir)
    source_dialect, source_version = _validate_dialect(args.source)
    target_dialect, target_version = _validate_dialect(args.target)
    mode = args.mode

    count = _resolve_count(args) if mode == "fuzz" else None

    result = CampaignRunner().run(
        input_dir=input_dir,
        source_dialect=source_dialect,
        source_version=source_version,
        target_dialect=target_dialect,
        target_version=target_version,
        mode=mode,
        count_per_seed=count,
        random_seed=args.seed,
    )

    mode_label = "模糊测试" if mode == "fuzz" else "转译执行"
    logger.info("=" * 50)
    for tr in result.targets:
        if tr.skipped:
            logger.info("  [%s] 跳过: %s", tr.target_name, tr.skip_reason)
        else:
            logger.info(
                "  [%s] %d 执行 | %d 成功 | %d 失败 | %.0f ms",
                tr.target_name,
                tr.total_executed,
                tr.success,
                tr.error,
                tr.elapsed_ms,
            )
    logger.info("输出目录: %s", result.output_dir)
    if result.report_path:
        logger.info("运行报告: %s", result.report_path)
    if result.analysis_path:
        logger.info("分析报告: %s", result.analysis_path)


def run(argv: Optional[Sequence[str]] = None) -> None:
    """CLI 公开入口：解析参数 → 分发 → 捕获异常。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "init": _handle_init,
        "transpile": _handle_transpile,
        "mutate": _handle_mutate,
        "verify": _handle_verify,
        "run": _handle_run,
    }

    try:
        dispatch[args.command](args)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(130)
