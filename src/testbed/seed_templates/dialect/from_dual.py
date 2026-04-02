"""FROM DUAL 方言差异模板 — 测试 Oracle 无表查询要求 FROM DUAL。

覆盖差异点：
- SQLite: SELECT expr （无需 FROM 子句）
- Oracle: SELECT expr FROM DUAL（必须有 FROM 子句）
- SQLGlot 自动添加/移除 FROM DUAL
- 子查询中的 DUAL 处理
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class FromDualTemplate(SeedTemplate):
    """FROM DUAL 方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "from_dual"

    @property
    def description(self) -> str:
        return "FROM DUAL 方言差异测试（无表查询转译）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._basic_dual())
        seeds.extend(self._dual_functions())
        seeds.extend(self._dual_expressions())
        seeds.extend(self._dual_subquery())
        return seeds

    # ── 基本无表查询 (~8) ───────────────────────────────
    def _basic_dual(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT 1 AS val",
                tags=["dual", "dual_literal_int"],
                desc="SELECT 常量整数 — 最简无表查询",
            ),
            self._seed(
                "SELECT 'hello' AS val",
                tags=["dual", "dual_literal_str"],
                desc="SELECT 常量字符串",
            ),
            self._seed(
                "SELECT 1 AS a, 2 AS b, 3 AS c",
                tags=["dual", "dual_multi_col"],
                desc="SELECT 多个常量列",
            ),
            self._seed(
                "SELECT 42 AS answer, 'test' AS label, 3.14 AS pi",
                tags=["dual", "dual_mixed_types"],
                desc="SELECT 混合类型常量",
            ),
            self._seed(
                "SELECT NULL AS null_val",
                tags=["dual", "dual_null"],
                desc="SELECT NULL 常量",
            ),
            self._seed(
                "SELECT 1 + 2 AS sum_val",
                tags=["dual", "dual_arithmetic"],
                desc="SELECT 算术表达式",
            ),
            self._seed(
                "SELECT CAST(42 AS VARCHAR(10)) AS str_num",
                tags=["dual", "dual_cast"],
                desc="SELECT 含 CAST",
            ),
            self._seed(
                "SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS result",
                tags=["dual", "dual_case"],
                desc="SELECT 含 CASE",
            ),
        ]

    # ── 无表查询 + 函数 (~10) ────────────────────────────
    def _dual_functions(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT ABS(-10) AS abs_val",
                tags=["dual_func", "dual_abs"],
                desc="SELECT ABS() 无表",
            ),
            self._seed(
                "SELECT ROUND(3.14159, 2) AS rounded",
                tags=["dual_func", "dual_round"],
                desc="SELECT ROUND() 无表",
            ),
            self._seed(
                "SELECT UPPER('hello') AS upper_val",
                tags=["dual_func", "dual_upper"],
                desc="SELECT UPPER() 无表",
            ),
            self._seed(
                "SELECT LOWER('HELLO') AS lower_val",
                tags=["dual_func", "dual_lower"],
                desc="SELECT LOWER() 无表",
            ),
            self._seed(
                "SELECT LENGTH('hello world') AS str_len",
                tags=["dual_func", "dual_length"],
                desc="SELECT LENGTH() 无表",
            ),
            self._seed(
                "SELECT COALESCE(NULL, NULL, 42) AS first_non_null",
                tags=["dual_func", "dual_coalesce"],
                desc="SELECT COALESCE() 无表",
            ),
            self._seed(
                "SELECT NULLIF(10, 10) AS null_if_same, NULLIF(10, 20) AS val_if_diff",
                tags=["dual_func", "dual_nullif"],
                desc="SELECT NULLIF() 无表",
            ),
            self._seed(
                "SELECT SUBSTR('abcdef', 1, 3) AS sub_str",
                tags=["dual_func", "dual_substr"],
                desc="SELECT SUBSTR() 无表",
            ),
            self._seed(
                "SELECT REPLACE('hello world', 'world', 'SQL') AS replaced",
                tags=["dual_func", "dual_replace"],
                desc="SELECT REPLACE() 无表",
            ),
            self._seed(
                "SELECT TRIM('  hello  ') AS trimmed",
                tags=["dual_func", "dual_trim"],
                desc="SELECT TRIM() 无表",
            ),
        ]

    # ── 复杂表达式无表查询 (~6) ──────────────────────────
    def _dual_expressions(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT 1 + 2 * 3 AS calc_result",
                tags=["dual_expr", "dual_arith"],
                desc="算术运算优先级",
            ),
            self._seed(
                "SELECT 'Hello' || ' ' || 'World' AS concatenated",
                tags=["dual_expr", "dual_concat"],
                desc="字符串拼接 || 运算",
            ),
            self._seed(
                "SELECT CAST(100 AS REAL) / 3 AS division_result",
                tags=["dual_expr", "dual_division"],
                desc="CAST + 除法",
            ),
            self._seed(
                "SELECT ABS(-5) + ROUND(3.7, 0) AS func_combo",
                tags=["dual_expr", "dual_func_combo"],
                desc="嵌套函数调用",
            ),
            self._seed(
                "SELECT CASE WHEN 1 > 0 THEN 'positive' WHEN 1 < 0 THEN 'negative' ELSE 'zero' END AS sign",
                tags=["dual_expr", "dual_case_expr"],
                desc="CASE WHEN 无表",
            ),
            self._seed(
                "SELECT COALESCE(NULL, ABS(-10)) AS nested_func_null",
                tags=["dual_expr", "dual_nested_null"],
                desc="COALESCE + 函数嵌套",
            ),
        ]

    # ── 子查询中的 DUAL (~6) ─────────────────────────────
    def _dual_subquery(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users WHERE score > "
                "(SELECT 50 AS threshold) ORDER BY id",
                tags=["dual_subquery", "dual_sub_threshold"],
                desc="子查询 SELECT 常量作为阈值",
            ),
            self._seed(
                "SELECT id, name, price FROM t_products "
                "WHERE price > (SELECT 50) ORDER BY id",
                tags=["dual_subquery", "dual_sub_price"],
                desc="子查询 SELECT 常量过滤",
            ),
            self._seed(
                "SELECT u.id, u.username, u.score FROM t_users u "
                "WHERE u.score > (SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) "
                "ORDER BY u.score",
                tags=["dual_subquery", "dual_sub_avg"],
                desc="子查询含 AVG",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary FROM t_employees e "
                "WHERE e.salary > (SELECT 80000) ORDER BY e.salary DESC",
                tags=["dual_subquery", "dual_sub_salary"],
                desc="子查询 SELECT 常量过滤薪资",
            ),
            self._seed(
                "SELECT id, username, score - (SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) AS diff "
                "FROM t_users WHERE score IS NOT NULL ORDER BY diff DESC",
                tags=["dual_subquery", "dual_sub_diff"],
                desc="子查询计算差值",
            ),
            self._seed(
                "SELECT t.id, t.amount FROM t_transactions t "
                "WHERE t.amount > (SELECT AVG(amount) FROM t_transactions WHERE amount IS NOT NULL) "
                "ORDER BY t.amount DESC",
                tags=["dual_subquery", "dual_sub_avg_tx"],
                desc="子查询含 AVG 过滤交易",
            ),
        ]
