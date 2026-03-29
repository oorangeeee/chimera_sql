"""聚合函数 Star 参数修复规则。

某些聚合函数（如 MAX、MIN、SUM、AVG）在 SQLite 中可以用 * 作为参数，
但 Oracle 不支持这种写法（ORA-00936）。
此规则在 SQLite→Oracle 转译时将此类 * 替换为 1。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class FixAggregateStarRule(TranspilationRule):
    """修复聚合函数中不合法的 * 参数（SQLite→Oracle）。

    COUNT(*) 在所有方言中都合法，但 MAX(*)、MIN(*)、SUM(*)、AVG(*)
    在 Oracle 中会报 ORA-00936。此规则将其替换为 MAX(1)、MIN(1) 等。
    """

    # 需要 * → 1 修复的聚合函数
    _STAR_UNSAFE_FUNCS = frozenset({"MAX", "MIN", "SUM", "AVG", "MEDIAN"})

    @property
    def name(self) -> str:
        return "fix_aggregate_star"

    @property
    def description(self) -> str:
        return "将 MAX(*)/MIN(*)/SUM(*)/AVG(*) 中的 * 替换为 1（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, (exp.Max, exp.Min, exp.Sum, exp.Avg)):
                return node
            func_name = node.key.upper() if node.key else ""
            if func_name not in self._STAR_UNSAFE_FUNCS:
                return node
            # * 可能在 this 或 expressions 中
            if isinstance(node.this, exp.Star):
                node.set("this", exp.Literal.number(1))
            else:
                args = node.args.get("expressions", [])
                if len(args) == 1 and isinstance(args[0], exp.Star):
                    node.set("expressions", [exp.Literal.number(1)])
            return node

        return self._transform(tree, _transform)
