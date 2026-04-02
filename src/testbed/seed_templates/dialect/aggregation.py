"""聚合函数方言差异模板 — 测试 SQLite/Oracle 聚合行为差异。

覆盖差异点：
- COUNT(*)/COUNT(col)/COUNT(DISTINCT) 与 NULL 交互
- SUM/AVG/MAX/MIN 对 NULL 的处理
- HAVING vs WHERE 语义差异
- GROUP BY 单列/多列
- 聚合 + 类型转换
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class AggregationTemplate(SeedTemplate):
    """聚合函数方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "aggregation"

    @property
    def description(self) -> str:
        return "聚合函数方言差异测试（COUNT/SUM/AVG/MAX/MIN/HAVING/GROUP BY）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._count_variations())
        seeds.extend(self._sum_avg_null())
        seeds.extend(self._group_by_multi())
        seeds.extend(self._having_queries())
        seeds.extend(self._aggregate_cast())
        return seeds

    # ── COUNT 变体 (~10) ────────────────────────────────
    def _count_variations(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT COUNT(*) AS total, COUNT(email) AS with_email, "
                "COUNT(DISTINCT email) AS distinct_email FROM t_users",
                tags=["count", "count_email_distinct"],
                desc="COUNT(*)/COUNT(col)/COUNT(DISTINCT) — 用户表",
            ),
            self._seed(
                "SELECT COUNT(*) AS total, COUNT(category) AS with_cat, "
                "COUNT(DISTINCT category) AS distinct_cat FROM t_products",
                tags=["count", "count_cat"],
                desc="COUNT 变体 — 产品分类",
            ),
            self._seed(
                "SELECT COUNT(*) AS total, COUNT(salary) AS with_salary, "
                "COUNT(DISTINCT dept_id) AS distinct_dept FROM t_employees",
                tags=["count", "count_salary_dept"],
                desc="COUNT 变体 — 员工薪资和部门",
            ),
            self._seed(
                "SELECT COUNT(*) AS total, "
                "COUNT(CASE WHEN score > 80 THEN 1 END) AS high_scorers, "
                "COUNT(CASE WHEN score IS NULL THEN 1 END) AS no_score "
                "FROM t_users",
                tags=["count", "count_conditional"],
                desc="COUNT + CASE — 条件计数",
            ),
            self._seed(
                "SELECT dept_id, COUNT(*) AS emp_count, "
                "COUNT(salary) AS with_salary, "
                "COUNT(DISTINCT manager_id) AS distinct_managers "
                "FROM t_employees GROUP BY dept_id ORDER BY dept_id",
                tags=["count", "count_group_dept"],
                desc="GROUP BY + COUNT 变体",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT user_id) AS unique_users, "
                "COUNT(DISTINCT status) AS distinct_status, "
                "COUNT(*) AS total_orders FROM t_orders",
                tags=["count", "count_orders_distinct"],
                desc="COUNT DISTINCT — 订单",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT event_type) AS event_types, "
                "COUNT(DISTINCT user_id) AS active_users, "
                "COUNT(*) AS total_events FROM t_events",
                tags=["count", "count_events"],
                desc="COUNT DISTINCT — 事件",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT from_user) AS senders, "
                "COUNT(DISTINCT to_user) AS receivers, "
                "COUNT(DISTINCT tx_type) AS tx_types "
                "FROM t_transactions",
                tags=["count", "count_tx_distinct"],
                desc="COUNT DISTINCT — 交易",
            ),
            self._seed(
                "SELECT status, COUNT(*) AS cnt FROM t_employees "
                "WHERE status IS NOT NULL GROUP BY status ORDER BY status",
                tags=["count", "count_by_status"],
                desc="GROUP BY status 计数",
            ),
            self._seed(
                "SELECT user_id, COUNT(*) AS order_count "
                "FROM t_orders GROUP BY user_id ORDER BY order_count DESC, user_id",
                tags=["count", "count_by_user"],
                desc="GROUP BY user_id — 每用户订单数",
            ),
        ]

    # ── SUM/AVG 与 NULL (~10) ────────────────────────────
    def _sum_avg_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT SUM(score) AS total_score, AVG(score) AS avg_score, "
                "SUM(COALESCE(score, 0)) AS total_filled "
                "FROM t_users",
                tags=["sum_avg", "sum_avg_score"],
                desc="SUM/AVG — score 含 NULL vs COALESCE 填充",
            ),
            self._seed(
                "SELECT SUM(price) AS total_price, AVG(price) AS avg_price, "
                "MAX(price) AS max_price, MIN(price) AS min_price "
                "FROM t_products",
                tags=["sum_avg", "product_stats"],
                desc="聚合统计 — 产品价格",
            ),
            self._seed(
                "SELECT SUM(salary) AS total_salary, AVG(salary) AS avg_salary, "
                "MAX(salary) AS max_sal, MIN(salary) AS min_sal "
                "FROM t_employees",
                tags=["sum_avg", "salary_stats"],
                desc="聚合统计 — 员工薪资",
            ),
            self._seed(
                "SELECT SUM(amount) AS total_amount, AVG(amount) AS avg_amount, "
                "COUNT(amount) AS tx_count, COUNT(*) AS total_rows "
                "FROM t_transactions",
                tags=["sum_avg", "amount_stats"],
                desc="聚合统计 — 交易金额含 NULL",
            ),
            self._seed(
                "SELECT dept_id, SUM(salary) AS dept_salary, AVG(salary) AS dept_avg, "
                "COUNT(*) AS headcount "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["sum_avg", "dept_salary"],
                desc="GROUP BY dept — 部门薪资统计",
            ),
            self._seed(
                "SELECT category, SUM(stock) AS total_stock, AVG(price) AS avg_price, "
                "COUNT(*) AS product_count "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category ORDER BY category",
                tags=["sum_avg", "cat_stats"],
                desc="GROUP BY category — 分类统计",
            ),
            self._seed(
                "SELECT user_id, SUM(COALESCE(amount, 0)) AS total_spent, "
                "AVG(amount) AS avg_amount, COUNT(*) AS tx_count "
                "FROM t_transactions WHERE from_user IS NOT NULL "
                "GROUP BY from_user ORDER BY total_spent DESC",
                tags=["sum_avg", "user_spending"],
                desc="GROUP BY user — 用户消费统计",
            ),
            self._seed(
                "SELECT event_type, COUNT(*) AS cnt, "
                "COUNT(DISTINCT user_id) AS unique_users "
                "FROM t_events GROUP BY event_type ORDER BY cnt DESC",
                tags=["sum_avg", "event_type_stats"],
                desc="GROUP BY event_type — 事件统计",
            ),
            self._seed(
                "SELECT status, COUNT(*) AS cnt, "
                "SUM(amount) AS total, AVG(amount) AS average "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status ORDER BY total DESC",
                tags=["sum_avg", "tx_status_stats"],
                desc="GROUP BY status — 交易状态统计",
            ),
            self._seed(
                "SELECT MAX(score) - MIN(score) AS score_range, "
                "MAX(age) - MIN(age) AS age_range "
                "FROM t_users",
                tags=["sum_avg", "range_calc"],
                desc="MAX - MIN 范围计算",
            ),
        ]

    # ── GROUP BY 多列 (~10) ──────────────────────────────
    def _group_by_multi(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, status, COUNT(*) AS cnt, AVG(salary) AS avg_sal "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id, status ORDER BY dept_id, status",
                tags=["group_multi", "dept_status"],
                desc="GROUP BY 两列 — 部门+状态",
            ),
            self._seed(
                "SELECT category, CASE WHEN price > 100 THEN 'expensive' "
                "ELSE 'affordable' END AS price_tier, COUNT(*) AS cnt "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category, price_tier ORDER BY category, price_tier",
                tags=["group_multi", "cat_price_tier"],
                desc="GROUP BY 含 CASE 表达式",
            ),
            self._seed(
                "SELECT from_user, tx_type, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM t_transactions WHERE from_user IS NOT NULL "
                "GROUP BY from_user, tx_type ORDER BY from_user, tx_type",
                tags=["group_multi", "user_tx_type"],
                desc="GROUP BY 两列 — 用户+交易类型",
            ),
            self._seed(
                "SELECT event_type, "
                "CASE WHEN user_id IS NOT NULL THEN 'known' ELSE 'anonymous' END AS user_status, "
                "COUNT(*) AS cnt "
                "FROM t_events GROUP BY event_type, user_status "
                "ORDER BY event_type, user_status",
                tags=["group_multi", "event_user_status"],
                desc="GROUP BY 含 CASE — 事件+用户状态",
            ),
            self._seed(
                "SELECT d.name AS dept, e.status, COUNT(*) AS cnt, "
                "ROUND(AVG(e.salary), 2) AS avg_sal "
                "FROM t_employees e JOIN t_departments d ON e.dept_id = d.id "
                "GROUP BY d.name, e.status ORDER BY d.name, e.status",
                tags=["group_multi", "join_group"],
                desc="JOIN + GROUP BY 多列",
            ),
            self._seed(
                "SELECT u.username, o.status, COUNT(*) AS order_count, "
                "SUM(o.total_amount) AS total_spent "
                "FROM t_users u JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username, o.status "
                "ORDER BY u.username, o.status",
                tags=["group_multi", "user_order_status"],
                desc="JOIN + GROUP BY — 用户+订单状态",
            ),
            self._seed(
                "SELECT "
                "CASE WHEN score >= 80 THEN 'high' "
                "WHEN score >= 50 THEN 'mid' "
                "WHEN score IS NOT NULL THEN 'low' "
                "ELSE 'null' END AS score_group, "
                "CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS age_group, "
                "COUNT(*) AS cnt "
                "FROM t_users GROUP BY score_group, age_group "
                "ORDER BY score_group, age_group",
                tags=["group_multi", "score_age_groups"],
                desc="GROUP BY 含两个 CASE 表达式",
            ),
            self._seed(
                "SELECT dept_id, manager_id, COUNT(*) AS team_size "
                "FROM t_employees WHERE dept_id IS NOT NULL AND manager_id IS NOT NULL "
                "GROUP BY dept_id, manager_id ORDER BY dept_id, manager_id",
                tags=["group_multi", "dept_manager"],
                desc="GROUP BY 两列 — 部门+经理",
            ),
            self._seed(
                "SELECT d.name, COUNT(e.id) AS emp_count, "
                "SUM(COALESCE(e.salary, 0)) AS total_salary "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["group_multi", "dept_salary_left_join"],
                desc="LEFT JOIN + GROUP BY — 含无员工部门",
            ),
            self._seed(
                "SELECT status, "
                "CASE WHEN amount > 500 THEN 'large' ELSE 'small' END AS size, "
                "COUNT(*) AS cnt, AVG(amount) AS avg_amt "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status, size ORDER BY status, size",
                tags=["group_multi", "tx_status_size"],
                desc="GROUP BY status + CASE 大小分类",
            ),
        ]

    # ── HAVING (~10) ────────────────────────────────────
    def _having_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, COUNT(*) AS cnt FROM t_employees "
                "WHERE dept_id IS NOT NULL GROUP BY dept_id "
                "HAVING COUNT(*) > 2 ORDER BY dept_id",
                tags=["having", "having_dept_size"],
                desc="HAVING — 大于 2 人的部门",
            ),
            self._seed(
                "SELECT dept_id, AVG(salary) AS avg_sal FROM t_employees "
                "WHERE dept_id IS NOT NULL AND salary IS NOT NULL "
                "GROUP BY dept_id HAVING AVG(salary) > 80000 "
                "ORDER BY avg_sal DESC",
                tags=["having", "having_avg_salary"],
                desc="HAVING — 平均薪资 > 80000",
            ),
            self._seed(
                "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category HAVING AVG(price) > 50 "
                "ORDER BY avg_price DESC",
                tags=["having", "having_cat_price"],
                desc="HAVING — 平均价格 > 50 的分类",
            ),
            self._seed(
                "SELECT user_id, COUNT(*) AS order_count "
                "FROM t_orders GROUP BY user_id "
                "HAVING COUNT(*) >= 2 ORDER BY order_count DESC",
                tags=["having", "having_repeat_buyers"],
                desc="HAVING — 重复购买用户",
            ),
            self._seed(
                "SELECT from_user, SUM(amount) AS total "
                "FROM t_transactions WHERE from_user IS NOT NULL "
                "GROUP BY from_user HAVING SUM(amount) > 200 "
                "ORDER BY total DESC",
                tags=["having", "having_big_spenders"],
                desc="HAVING — 总消费 > 200",
            ),
            self._seed(
                "SELECT event_type, COUNT(*) AS cnt "
                "FROM t_events GROUP BY event_type "
                "HAVING COUNT(*) >= 3 ORDER BY cnt DESC",
                tags=["having", "having_frequent_events"],
                desc="HAVING — 频繁事件类型",
            ),
            self._seed(
                "SELECT dept_id, COUNT(*) AS cnt, SUM(salary) AS total_sal "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id "
                "HAVING COUNT(*) > 1 AND SUM(salary) > 100000 "
                "ORDER BY dept_id",
                tags=["having", "having_multi_cond"],
                desc="HAVING 多条件 — 部门人数和薪资",
            ),
            self._seed(
                "SELECT status, COUNT(*) AS cnt "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status HAVING COUNT(*) > 5 ORDER BY cnt DESC",
                tags=["having", "having_tx_status"],
                desc="HAVING — 交易状态计数",
            ),
            self._seed(
                "SELECT user_id, COUNT(*) AS cnt, MAX(total_amount) AS max_order "
                "FROM t_orders GROUP BY user_id "
                "HAVING COUNT(*) > 1 AND MAX(total_amount) > 100 "
                "ORDER BY cnt DESC",
                tags=["having", "having_orders_max"],
                desc="HAVING — 多条件含 MAX",
            ),
            self._seed(
                "SELECT d.name, COUNT(e.id) AS emp_cnt "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name "
                "HAVING COUNT(e.id) > 0 ORDER BY emp_cnt DESC",
                tags=["having", "having_left_join"],
                desc="LEFT JOIN + HAVING — 排除空部门",
            ),
        ]

    # ── 聚合 + CAST (~10) ────────────────────────────────
    def _aggregate_cast(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, "
                "CAST(AVG(salary) AS INTEGER) AS avg_sal_int, "
                "ROUND(AVG(salary), 2) AS avg_sal_round "
                "FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["agg_cast", "avg_cast_int"],
                desc="AVG + CAST + ROUND — 类型转换差异",
            ),
            self._seed(
                "SELECT category, "
                "CAST(SUM(stock) AS REAL) AS total_stock_real, "
                "CAST(AVG(price) AS INTEGER) AS avg_price_int "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category ORDER BY category",
                tags=["agg_cast", "sum_cast_real"],
                desc="SUM/AVG + CAST — 数值类型转换",
            ),
            self._seed(
                "SELECT CAST(COUNT(*) AS VARCHAR(10)) AS count_str, "
                "CAST(AVG(score) AS VARCHAR(20)) AS avg_str "
                "FROM t_users",
                tags=["agg_cast", "count_cast_varchar"],
                desc="聚合结果 CAST 为字符串",
            ),
            self._seed(
                "SELECT CAST(SUM(amount) AS INTEGER) AS total_int, "
                "CAST(AVG(amount) AS INTEGER) AS avg_int, "
                "SUM(amount) AS total_orig, AVG(amount) AS avg_orig "
                "FROM t_transactions",
                tags=["agg_cast", "amount_cast_int"],
                desc="聚合 CAST INTEGER — 截断 vs 四舍五入差异",
            ),
            self._seed(
                "SELECT dept_id, "
                "CAST(MAX(salary) AS VARCHAR(20)) AS max_str, "
                "CAST(MIN(salary) AS VARCHAR(20)) AS min_str "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["agg_cast", "max_min_to_str"],
                desc="MAX/MIN + CAST VARCHAR",
            ),
            self._seed(
                "SELECT status, "
                "CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM t_transactions) AS pct "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status ORDER BY pct DESC",
                tags=["agg_cast", "pct_calc"],
                desc="CAST + 子查询计算百分比",
            ),
            self._seed(
                "SELECT d.name, "
                "CAST(SUM(e.salary) AS INTEGER) AS total_int, "
                "SUM(e.salary) AS total_dec "
                "FROM t_departments d JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["agg_cast", "dept_total_cast"],
                desc="JOIN + SUM + CAST — 部门薪资总额",
            ),
            self._seed(
                "SELECT ROUND(SUM(price), 0) AS total_rounded, "
                "CAST(SUM(price) AS INTEGER) AS total_cast, "
                "SUM(price) AS total_exact FROM t_products",
                tags=["agg_cast", "sum_round_vs_cast"],
                desc="ROUND vs CAST — 截断 vs 四舍五入",
            ),
            self._seed(
                "SELECT CAST(AVG(score) AS INTEGER) AS avg_int, "
                "ROUND(AVG(score)) AS avg_round, "
                "ROUND(AVG(score), 1) AS avg_1dec "
                "FROM t_users WHERE score IS NOT NULL",
                tags=["agg_cast", "avg_round_cast_compare"],
                desc="AVG + CAST vs ROUND — 精度对比",
            ),
            self._seed(
                "SELECT CAST(COUNT(DISTINCT category) AS VARCHAR(10)) || ' categories' AS cat_info, "
                "CAST(COUNT(*) AS VARCHAR(10)) || ' products' AS prod_info "
                "FROM t_products",
                tags=["agg_cast", "count_concat"],
                desc="COUNT + CAST + 字符串拼接",
            ),
        ]
