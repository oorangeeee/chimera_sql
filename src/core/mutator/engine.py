"""变异引擎模块。

负责单条 SQL 的 AST 级变异编排：
解析 → 节点收集 → 门控过滤 → 随机选取 → 变异应用 → 序列化 → 校验。
"""

from random import Random
from typing import List, Optional, Tuple

import sqlglot
import sqlglot.expressions as exp

from src.utils.logger import get_logger

from .capability import CapabilityProfile
from .gate import RuleGate
from .strategy_base import MutationResult, MutationStrategy
from .strategy_registry import StrategyRegistry

logger = get_logger("mutator.engine")

# 默认每条种子变异时最大重试次数
_MAX_RETRY = 3


class MutationEngine:
    """单条 SQL 变异编排器。

    解析 SQL → 收集 AST 节点 → 门控过滤可用 (策略, 节点) 对 →
    随机选取 1~max_apply 个变异操作 → 依次应用 → 序列化并校验。
    """

    def __init__(
        self,
        profile: CapabilityProfile,
        registry: StrategyRegistry,
        rng: Optional[Random] = None,
        max_apply: int = 3,
        source_dialect: Optional[str] = None,
    ) -> None:
        """初始化变异引擎。

        Args:
            profile: 目标数据库能力画像。
            registry: 策略注册表。
            rng: 随机数生成器（用于可复现的变异）。
            max_apply: 单次变异最多应用的策略数量。
            source_dialect: 种子 SQL 的方言名称（用于按方言解析和输出）。
        """
        self._profile = profile
        self._strategies = registry.get_all()
        self._rng = rng or Random()
        self._max_apply = max_apply
        self._source_dialect = source_dialect

    def mutate_one(self, sql: str, seed_file: str) -> MutationResult:
        """对单条 SQL 执行一次变异。

        Args:
            sql: 原始 SQL 字符串。
            seed_file: 种子文件路径（用于报告）。

        Returns:
            变异结果。

        Raises:
            ValueError: SQL 解析失败。
        """
        tree = sqlglot.parse_one(sql, read=self._source_dialect)

        # 收集可用的 (策略, 节点) 对
        candidates = self._collect_candidates(tree)
        if not candidates:
            logger.debug("%s: 无可用变异候选", seed_file)
            return MutationResult(
                sql=sql,
                seed_file=seed_file,
                warnings=["无可用变异候选，返回原始 SQL"],
            )

        # 随机选取 1~max_apply 个候选
        num_apply = self._rng.randint(1, min(self._max_apply, len(candidates)))
        selected = self._rng.sample(candidates, num_apply)

        # 依次应用变异
        strategies_applied: List[str] = []
        warnings: List[str] = []

        for strategy, node in selected:
            try:
                mutated_node = strategy.mutate(node, self._rng)
                if mutated_node is not node:
                    # 在 AST 中原地替换节点（不重建整棵树）
                    node.replace(mutated_node)
                strategies_applied.append(strategy.id)
            except Exception as e:
                warnings.append(f"策略 {strategy.id} 应用失败: {e}")
                logger.warning("策略 %s 应用失败: %s", strategy.id, e)

        # 序列化
        result_sql = tree.sql(dialect=self._source_dialect)

        # 健全性检查：验证变异后的 SQL 可解析
        try:
            sqlglot.parse_one(result_sql, read=self._source_dialect)
        except Exception as e:
            warnings.append(f"变异后 SQL 解析校验失败: {e}")
            logger.debug("变异后 SQL 校验失败: %s -> %s", seed_file, e)

        return MutationResult(
            sql=result_sql,
            seed_file=seed_file,
            strategies_applied=strategies_applied,
            warnings=warnings,
        )

    def mutate_many(
        self,
        sql: str,
        seed_file: str,
        count: int,
    ) -> List[MutationResult]:
        """对单条 SQL 生成多个变异版本。

        Args:
            sql: 原始 SQL 字符串。
            seed_file: 种子文件路径。
            count: 目标变异数量。

        Returns:
            变异结果列表。
        """
        results: List[MutationResult] = []
        attempts = 0
        max_attempts = count * _MAX_RETRY

        while len(results) < count and attempts < max_attempts:
            attempts += 1
            try:
                result = self.mutate_one(sql, seed_file)
                # 跳过与原始 SQL 完全相同的变异
                if result.sql.strip() != sql.strip():
                    results.append(result)
                else:
                    logger.debug(
                        "%s: 变异结果与原始相同，跳过（第 %d 次尝试）",
                        seed_file, attempts,
                    )
            except Exception as e:
                logger.warning("%s: 变异失败（第 %d 次尝试）: %s", seed_file, attempts, e)

        if len(results) < count:
            logger.warning(
                "%s: 仅生成 %d/%d 个有效变异（尝试 %d 次）",
                seed_file, len(results), count, attempts,
            )

        return results

    # ── 私有方法 ──

    def _collect_candidates(
        self, tree: exp.Expression
    ) -> List[Tuple[MutationStrategy, exp.Expression]]:
        """收集所有通过门控检查的 (策略, 节点) 对。"""
        candidates: List[Tuple[MutationStrategy, exp.Expression]] = []

        for node in tree.walk():
            for strategy in self._strategies:
                ok, _ = RuleGate.can_apply(strategy, self._profile, node)
                if ok:
                    candidates.append((strategy, node))

        return candidates
