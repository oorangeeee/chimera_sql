"""命令行接口模块。

负责 argparse 参数解析、子命令分发、顶层错误处理。
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from src.testbed import InitPipeline
from src.core.transpiler import BatchTranspileRunner, Dialect
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
    }

    try:
        dispatch[args.command](args)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(130)
