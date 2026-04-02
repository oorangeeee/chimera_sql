"""操作符方言差异模板 — 测试 SQLite/Oracle 操作符行为差异。

覆盖差异点：
- BETWEEN 语义（NULL 边界、日期类型）
- IN / NOT IN（含 NULL 子查询）
- LIKE 模式匹配（大小写敏感性）
- IS / IS NOT NULL
- 数学运算（整数除法 vs 浮点除法）
- 比较操作符与 NULL
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class OperatorsTemplate(SeedTemplate):
    """操作符方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "operators"

    @property
    def description(self) -> str:
        return "操作符方言差异测试（BETWEEN/IN/LIKE/IS/数学运算/比较）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._between_queries())
        seeds.extend(self._in_queries())
        seeds.extend(self._like_queries())
        seeds.extend(self._is_null_queries())
        seeds.extend(self._math_queries())
        return seeds

    # ── BETWEEN (~8) ────────────────────────────────────
    def _between_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score FROM t_users "
                "WHERE score BETWEEN 50 AND 90 ORDER BY id",
                tags=["between", "between_score"],
                desc="BETWEEN — score 范围筛选",
            ),
            self._seed(
                "SELECT id, name, price FROM t_products "
                "WHERE price BETWEEN 10 AND 100 ORDER BY id",
                tags=["between", "between_price"],
                desc="BETWEEN — 价格范围",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary BETWEEN 60000 AND 120000 ORDER BY id",
                tags=["between", "between_salary"],
                desc="BETWEEN — 薪资范围",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary NOT BETWEEN 60000 AND 120000 AND salary IS NOT NULL "
                "ORDER BY id",
                tags=["between", "not_between_salary"],
                desc="NOT BETWEEN + NOT NULL — 薪资范围外",
            ),
            self._seed(
                "SELECT id, username, age FROM t_users "
                "WHERE age BETWEEN 20 AND 30 ORDER BY id",
                tags=["between", "between_age"],
                desc="BETWEEN — 年龄范围",
            ),
            self._seed(
                "SELECT id, amount FROM t_transactions "
                "WHERE amount BETWEEN 0 AND 500 ORDER BY id",
                tags=["between", "between_amount"],
                desc="BETWEEN — 交易金额范围",
            ),
            self._seed(
                "SELECT id, name, budget FROM t_departments "
                "WHERE budget BETWEEN 100000 AND 200000 ORDER BY id",
                tags=["between", "between_budget"],
                desc="BETWEEN — 预算范围",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE hire_date BETWEEN DATE('2020-01-01') AND DATE('2023-12-31') "
                "ORDER BY id",
                tags=["between", "between_date"],
                desc="BETWEEN — 日期范围",
            ),
        ]

    # ── IN / NOT IN (~10) ───────────────────────────────
    def _in_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE username IN ('alice', 'bob', 'charlie') ORDER BY id",
                tags=["in", "in_literals"],
                desc="IN — 字面量列表",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE category IN ('electronics', 'clothing') ORDER BY id",
                tags=["in", "in_category"],
                desc="IN — 分类筛选",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE status IN ('active', 'probation') ORDER BY id",
                tags=["in", "in_status"],
                desc="IN — 状态筛选",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE category NOT IN ('electronics', 'clothing') "
                "AND category IS NOT NULL ORDER BY id",
                tags=["in", "not_in_category"],
                desc="NOT IN + NOT NULL — 排除特定分类",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE id IN (SELECT user_id FROM t_orders WHERE status = 'completed') "
                "ORDER BY id",
                tags=["in", "in_subquery"],
                desc="IN 子查询 — 有已完成订单的用户",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE dept_id IN (SELECT id FROM t_departments WHERE budget > 100000) "
                "ORDER BY id",
                tags=["in", "in_subquery_dept"],
                desc="IN 子查询 — 高预算部门员工",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE id NOT IN (SELECT user_id FROM t_orders WHERE user_id IS NOT NULL) "
                "ORDER BY id",
                tags=["in", "not_in_subquery"],
                desc="NOT IN 子查询 — 无订单用户（含 NULL 保护）",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events "
                "WHERE event_type IN ('login', 'logout', 'signup') ORDER BY id",
                tags=["in", "in_event_types"],
                desc="IN — 事件类型筛选",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE dept_id NOT IN (1, 2, 3) AND dept_id IS NOT NULL "
                "ORDER BY id",
                tags=["in", "not_in_dept"],
                desc="NOT IN + NOT NULL — 排除特定部门",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE score IN (SELECT score FROM t_users WHERE score > 90) "
                "ORDER BY id",
                tags=["in", "in_score_subquery"],
                desc="IN 子查询 — 高分相同分数",
            ),
        ]

    # ── LIKE (~8) ───────────────────────────────────────
    def _like_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE username LIKE 'a%' ORDER BY id",
                tags=["like", "like_prefix"],
                desc="LIKE 'a%' — 前缀匹配",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE email LIKE '%@%' ORDER BY id",
                tags=["like", "like_email_at"],
                desc="LIKE '%@%' — 包含 @",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE name LIKE '%Phone%' ORDER BY id",
                tags=["like", "like_phone"],
                desc="LIKE '%Phone%' — 包含 Phone",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE name LIKE '张%' ORDER BY id",
                tags=["like", "like_chinese"],
                desc="LIKE '张%' — 中文前缀",
            ),
            self._seed(
                "SELECT id, tag FROM t_tags "
                "WHERE tag NOT LIKE '%test%' ORDER BY id",
                tags=["like", "not_like_test"],
                desc="NOT LIKE — 排除含 test 的标签",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE username LIKE '_l%' ORDER BY id",
                tags=["like", "like_single_char"],
                desc="LIKE '_l%' — 第二字符为 l",
            ),
            self._seed(
                "SELECT id, name FROM t_departments "
                "WHERE location LIKE '%Building%' ORDER BY id",
                tags=["like", "like_building"],
                desc="LIKE — 包含 Building",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE email LIKE '%.com' OR email LIKE '%.org' ORDER BY id",
                tags=["like", "like_or"],
                desc="LIKE + OR — 多模式匹配",
            ),
        ]

    # ── IS NULL / IS NOT NULL (~6) ──────────────────────
    def _is_null_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, email FROM t_users "
                "WHERE email IS NULL ORDER BY id",
                tags=["is_null", "email_null"],
                desc="IS NULL — 无 email 的用户",
            ),
            self._seed(
                "SELECT id, name, category FROM t_products "
                "WHERE category IS NOT NULL ORDER BY id",
                tags=["is_null", "category_not_null"],
                desc="IS NOT NULL — 有分类的产品",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id FROM t_employees "
                "WHERE salary IS NULL OR dept_id IS NULL ORDER BY id",
                tags=["is_null", "salary_or_dept_null"],
                desc="IS NULL + OR — 无薪资或无部门",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE salary IS NOT NULL AND dept_id IS NOT NULL "
                "AND manager_id IS NOT NULL ORDER BY id",
                tags=["is_null", "all_not_null"],
                desc="多列 IS NOT NULL — 完整数据",
            ),
            self._seed(
                "SELECT id, from_user, to_user FROM t_transactions "
                "WHERE from_user IS NULL OR to_user IS NULL ORDER BY id",
                tags=["is_null", "tx_null_user"],
                desc="IS NULL — 缺少发送者或接收者",
            ),
            self._seed(
                "SELECT id, name, parent_id, budget, location FROM t_departments "
                "WHERE parent_id IS NULL AND budget IS NOT NULL ORDER BY id",
                tags=["is_null", "root_dept_budget"],
                desc="IS NULL + IS NOT NULL — 根部门有预算",
            ),
        ]

    # ── 数学运算 (~8) ──────────────────────────────────
    def _math_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score * 2 AS doubled_score "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["math", "multiply"],
                desc="乘法 — score 翻倍",
            ),
            self._seed(
                "SELECT id, name, price / 2 AS half_price "
                "FROM t_products WHERE price IS NOT NULL ORDER BY id",
                tags=["math", "divide"],
                desc="除法 — 半价（整数 vs 浮点差异）",
            ),
            self._seed(
                "SELECT id, name, price * COALESCE(stock, 0) AS inventory_value "
                "FROM t_products ORDER BY id",
                tags=["math", "multiply_null"],
                desc="乘法 + COALESCE — 库存价值含 NULL",
            ),
            self._seed(
                "SELECT id, username, score - AVG(score) OVER() AS deviation "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["math", "window_deviation"],
                desc="窗口函数 + 减法 — 偏差计算",
            ),
            self._seed(
                "SELECT id, name, CAST(salary AS REAL) / 12 AS monthly_salary "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["math", "monthly_salary"],
                desc="CAST + 除法 — 月薪",
            ),
            self._seed(
                "SELECT id, name, price + 10 AS price_with_tax, "
                "price * 1.1 AS price_inflated "
                "FROM t_products WHERE price IS NOT NULL ORDER BY id",
                tags=["math", "add_multiply"],
                desc="加法 + 乘法 — 含税价",
            ),
            self._seed(
                "SELECT id, username, score + age AS score_plus_age "
                "FROM t_users WHERE score IS NOT NULL AND age IS NOT NULL ORDER BY id",
                tags=["math", "add_columns"],
                desc="列相加 — score + age",
            ),
            self._seed(
                "SELECT id, amount, ABS(amount) AS abs_amount, "
                "amount - LAG(amount) OVER (ORDER BY id) AS diff_from_prev "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["math", "abs_lag"],
                desc="ABS + LAG 窗口 — 金额变化",
            ),
        ]
