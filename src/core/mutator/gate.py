"""规则门控模块。

判断某条变异策略对给定能力画像 + AST 节点是否可用。
"""

from typing import Tuple

import sqlglot.expressions as exp

from src.utils.logger import get_logger

from .capability import CapabilityProfile
from .strategy_base import MutationStrategy

logger = get_logger("mutator.gate")


class RuleGate:
    """变异策略门控，检查策略适用性。"""

    @staticmethod
    def can_apply(
        strategy: MutationStrategy,
        profile: CapabilityProfile,
        node: exp.Expression,
    ) -> Tuple[bool, str]:
        """判断策略是否可应用于指定节点。

        检查顺序：
        1. 策略所需能力标志是否在 profile 中为 True。
        2. 节点类型是否在策略目标节点类型元组中。

        Args:
            strategy: 待检查的变异策略。
            profile: 目标数据库能力画像。
            node: 待变异的 AST 节点。

        Returns:
            (True, "ok") 或 (False, "原因描述")。
        """
        # 检查能力标志
        for flag in strategy.requires:
            if not profile.has(flag):
                logger.debug(
                    "门控拒绝: 策略=%s, 节点=%s, 原因=缺少能力 %s",
                    strategy.id, type(node).__name__, flag,
                )
                return False, f"缺少能力: {flag}"

        # 检查节点类型
        if not isinstance(node, strategy.node_types):
            logger.debug(
                "门控拒绝: 策略=%s, 节点=%s, 原因=目标类型不匹配",
                strategy.id, type(node).__name__,
            )
            return False, f"节点类型不匹配: {type(node).__name__}"

        return True, "ok"
