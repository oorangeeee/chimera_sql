"""命令行接口模块。

负责 argparse 参数解析、子命令分发、顶层错误处理。
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from src.testbed import InitPipeline
from src.core.transpiler import BatchTranspileRunner, Dialect
from src.core.mutator import BatchMutationRunner
from src.pipeline import CampaignRunner
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("chimera")

# Dialect 枚举名到值的映射（用于 CLI 参数解析）
_DIALECT_CHOICES = {d.value: d for d in Dialect}


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
        choices=list(_DIALECT_CHOICES.keys()),
        help="源 SQL 方言",
    )
    tp.add_argument(
        "-t",
        "--target",
        required=True,
        choices=list(_DIALECT_CHOICES.keys()),
        help="目标 SQL 方言",
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
        help="目标数据库方言（如 sqlite, oracle）",
    )
    mt.add_argument(
        "-v",
        "--version",
        default=None,
        help="数据库版本（可选，如 21c）",
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

    # run 子命令
    rn = subparsers.add_parser(
        "run",
        help="端到端模糊测试（变异 → 转译 → 执行 → 报告）",
    )
    rn.add_argument(
        "input_dir",
        help="包含 .sql 种子文件的输入目录（递归扫描）",
    )
    rn.add_argument(
        "-s",
        "--source",
        required=True,
        choices=list(_DIALECT_CHOICES.keys()),
        help="种子 SQL 的方言",
    )
    rn.add_argument(
        "--targets",
        default=None,
        help="逗号分隔的目标名称（默认 config.yaml 中所有 targets）",
    )
    rn.add_argument(
        "-n",
        "--count",
        type=int,
        default=None,
        help="每条种子生成的变异数量（默认从 config.yaml 读取）",
    )
    rn.add_argument(
        "--seed",
        type=int,
        default=None,
        help="随机种子（用于可复现结果）",
    )

    return parser


def _handle_init(_args: argparse.Namespace) -> None:
    """处理 init 子命令。"""
    InitPipeline().run()


def _handle_transpile(args: argparse.Namespace) -> None:
    """处理 transpile 子命令。"""
    input_dir = Path(args.input_dir)
    source = _DIALECT_CHOICES[args.source]
    target = _DIALECT_CHOICES[args.target]

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
    dialect = args.dialect
    version = args.version

    # 确定每条种子的变异数量：CLI 参数 > config.yaml > 默认值 3
    count = args.count
    if count is None:
        try:
            config = ConfigLoader()
            count = config.get("mutation.policies.balanced_default.max_mutations_per_seed", 3)
        except FileNotFoundError:
            count = 3

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


def _handle_run(args: argparse.Namespace) -> None:
    """处理 run 子命令（端到端模糊测试流水线）。"""
    input_dir = Path(args.input_dir)
    source_dialect = args.source

    # 解析 --targets（逗号分隔 → 列表，None 表示全部）
    target_names = None
    if args.targets:
        target_names = [t.strip() for t in args.targets.split(",") if t.strip()]

    # 确定每条种子的变异数量：CLI 参数 > config.yaml > 默认值 3
    count = args.count
    if count is None:
        try:
            config = ConfigLoader()
            count = config.get("mutation.policies.balanced_default.max_mutations_per_seed", 3)
        except FileNotFoundError:
            count = 3

    result = CampaignRunner().run(
        input_dir=input_dir,
        source_dialect=source_dialect,
        target_names=target_names,
        count_per_seed=count,
        random_seed=args.seed,
    )

    logger.info("=" * 50)
    for tr in result.targets:
        if tr.skipped:
            logger.info("  [%s] 跳过: %s", tr.target_name, tr.skip_reason)
        else:
            logger.info(
                "  [%s] %d 执行 | %d 成功 | %d 失败 | %.0f ms",
                tr.target_name, tr.total_executed, tr.success, tr.error, tr.elapsed_ms,
            )
    logger.info("输出目录: %s", result.output_dir)
    logger.info("运行报告: %s", result.report_path)


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
