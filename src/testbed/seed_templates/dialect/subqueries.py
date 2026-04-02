"""子查询方言差异模板 — 覆盖标量子查询、IN/EXISTS、派生表、关联子查询、CAST 子查询、HAVING 子查询。

生成 ~70 条种子 SQL，重点测试 Oracle/SQLite 在子查询处理上的差异：
- 标量子查询：SELECT col, (SELECT ...) FROM table
- IN / NOT IN 子查询：WHERE col IN (SELECT ...)
- EXISTS / NOT EXISTS 子查询：WHERE EXISTS (SELECT 1 FROM ... WHERE correlated)
- 派生表（derived table）：FROM (SELECT ... GROUP BY ...) AS sub JOIN table
- 关联子查询：WHERE col > (SELECT AVG(col) FROM table WHERE group = outer.group)
- 子查询 + CAST：(SELECT CAST(col AS INTEGER) FROM ...)
- HAVING 子查询：HAVING COUNT(*) > (SELECT AVG(cnt) FROM (...))

所有 SQL 使用 SQLite 方言语法，后续由 transpiler 转译为 Oracle。
每条 SQL 均以 ORDER BY 结尾，保证跨数据库结果集可比较。
"""

from __future__ import annotations

from typing import List

from src.testbed.seed_templates.base import SchemaMetadata, SeedSQL, SeedTemplate
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SubqueriesTemplate(SeedTemplate):
    """子查询方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "subqueries"

    @property
    def description(self) -> str:
        return "子查询方言差异种子（标量子查询、IN/EXISTS、派生表、关联子查询、CAST子查询、HAVING子查询）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._scalar_subqueries())
        seeds.extend(self._in_subqueries())
        seeds.extend(self._exists_subqueries())
        seeds.extend(self._derived_tables())
        seeds.extend(self._correlated_subqueries())
        seeds.extend(self._subquery_cast())
        seeds.extend(self._having_subqueries())
        logger.info("SubqueriesTemplate 生成 %d 条种子", len(seeds))
        return seeds

    # ────────────────────────────────────────────────────────
    # 1. Scalar subqueries (~15)
    # ────────────────────────────────────────────────────────
    def _scalar_subqueries(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "(SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) AS avg_score "
                    "FROM t_users "
                    "WHERE score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "avg"],
                desc="标量子查询 — 全局 AVG(score)",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "(SELECT MAX(score) FROM t_users) AS max_score "
                    "FROM t_users "
                    "ORDER BY id"
                ),
                tags=["scalar", "max"],
                desc="标量子查询 — 全局 MAX(score)",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "(SELECT MIN(score) FROM t_users WHERE score IS NOT NULL) AS min_score "
                    "FROM t_users "
                    "WHERE score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "min"],
                desc="标量子查询 — 全局 MIN(score)",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, "
                    "(SELECT COUNT(*) FROM t_orders WHERE user_id = u.id) AS order_count "
                    "FROM t_users u "
                    "ORDER BY id"
                ),
                tags=["scalar", "count", "correlated"],
                desc="关联标量子查询 — 每个用户的订单数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price, "
                    "(SELECT AVG(price) FROM t_products WHERE category = p.category AND price IS NOT NULL) AS cat_avg "
                    "FROM t_products p "
                    "WHERE price IS NOT NULL AND category IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "avg", "correlated"],
                desc="关联标量子查询 — 同类商品均价",
            ),
            self._seed(
                sql=(
                    "SELECT id, user_id, total_price, "
                    "(SELECT SUM(total_price) FROM t_orders WHERE user_id = o.user_id) AS user_total "
                    "FROM t_orders o "
                    "ORDER BY id"
                ),
                tags=["scalar", "sum", "correlated"],
                desc="关联标量子查询 — 每个用户的订单总额",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "(SELECT MAX(score) FROM t_users WHERE active = u.active AND score IS NOT NULL) AS active_max "
                    "FROM t_users u "
                    "WHERE score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "max", "correlated"],
                desc="关联标量子查询 — 同 active 状态的 MAX score",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, "
                    "(SELECT COUNT(*) FROM t_tags WHERE entity_type = 'user' AND entity_id = u.id) AS tag_count "
                    "FROM t_users u "
                    "ORDER BY id"
                ),
                tags=["scalar", "count", "correlated"],
                desc="关联标量子查询 — 每个用户的标签数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price, "
                    "(SELECT MIN(price) FROM t_products WHERE category = p.category) AS cat_min_price "
                    "FROM t_products p "
                    "WHERE category IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "min", "correlated"],
                desc="关联标量子查询 — 同类最低价",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "(SELECT AVG(score) FROM t_users WHERE age = u.age AND score IS NOT NULL) AS age_avg_score "
                    "FROM t_users u "
                    "WHERE age IS NOT NULL AND score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "avg", "correlated"],
                desc="关联标量子查询 — 同龄人平均分",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, salary, "
                    "(SELECT AVG(salary) FROM t_employees WHERE dept_id = e.dept_id AND salary IS NOT NULL) AS dept_avg "
                    "FROM t_employees e "
                    "WHERE salary IS NOT NULL AND dept_id IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "avg", "correlated"],
                desc="关联标量子查询 — 同部门平均薪资",
            ),
            self._seed(
                sql=(
                    "SELECT id, user_id, metric_value, "
                    "(SELECT MAX(metric_value) FROM t_metrics WHERE user_id = m.user_id AND metric_value IS NOT NULL) AS user_max "
                    "FROM t_metrics m "
                    "WHERE metric_value IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["scalar", "max", "correlated"],
                desc="关联标量子查询 — 用户最大指标值",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, "
                    "(SELECT SUM(amount) FROM t_transactions WHERE from_user = u.id) AS total_sent "
                    "FROM t_users u "
                    "ORDER BY id"
                ),
                tags=["scalar", "sum", "correlated"],
                desc="关联标量子查询 — 用户总转出金额",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price, "
                    "(SELECT COUNT(*) FROM t_orders WHERE product_id = p.id) AS times_ordered "
                    "FROM t_products p "
                    "ORDER BY id"
                ),
                tags=["scalar", "count", "correlated"],
                desc="关联标量子查询 — 商品被订购次数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, budget, "
                    "(SELECT SUM(budget) FROM t_departments WHERE parent_id = d.id) AS children_budget "
                    "FROM t_departments d "
                    "ORDER BY id"
                ),
                tags=["scalar", "sum", "correlated"],
                desc="关联标量子查询 — 子部门预算总和",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 2. IN subqueries (~10)
    # ────────────────────────────────────────────────────────
    def _in_subqueries(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users "
                    "WHERE id IN (SELECT DISTINCT user_id FROM t_orders) "
                    "ORDER BY id"
                ),
                tags=["in", "distinct"],
                desc="IN 子查询 — 有订单的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users "
                    "WHERE id NOT IN (SELECT DISTINCT user_id FROM t_orders WHERE user_id IS NOT NULL) "
                    "ORDER BY id"
                ),
                tags=["not_in", "null_safe"],
                desc="NOT IN 子查询 — 无订单的用户（IS NOT NULL 防空值）",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price FROM t_products "
                    "WHERE id IN (SELECT product_id FROM t_orders WHERE status = 'delivered') "
                    "ORDER BY id"
                ),
                tags=["in", "status_filter"],
                desc="IN 子查询 — 已交付订单中的商品",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users "
                    "WHERE id IN (SELECT DISTINCT user_id FROM t_orders WHERE total_price > 500) "
                    "ORDER BY id"
                ),
                tags=["in", "price_filter"],
                desc="IN 子查询 — 有大额订单的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name FROM t_products "
                    "WHERE category IN (SELECT DISTINCT category FROM t_products WHERE price > 100 AND category IS NOT NULL) "
                    "AND category IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["in", "self_referential"],
                desc="IN 子查询 — 同类有高价商品的类别",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users "
                    "WHERE id IN (SELECT from_user FROM t_transactions WHERE tx_type = 'transfer') "
                    "ORDER BY id"
                ),
                tags=["in", "transaction"],
                desc="IN 子查询 — 有转账记录的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name FROM t_products "
                    "WHERE id NOT IN (SELECT product_id FROM t_orders WHERE product_id IS NOT NULL) "
                    "ORDER BY id"
                ),
                tags=["not_in", "unsold"],
                desc="NOT IN 子查询 — 未被订购的商品",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users "
                    "WHERE id IN (SELECT entity_id FROM t_tags WHERE entity_type = 'user' AND tag = 'premium') "
                    "ORDER BY id"
                ),
                tags=["in", "tags"],
                desc="IN 子查询 — 带 premium 标签的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, salary FROM t_employees "
                    "WHERE dept_id IN (SELECT id FROM t_departments WHERE parent_id IS NULL) "
                    "ORDER BY id"
                ),
                tags=["in", "top_level_dept"],
                desc="IN 子查询 — 顶级部门下的员工",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price FROM t_products "
                    "WHERE id NOT IN (SELECT product_id FROM t_orders WHERE status = 'cancelled' AND product_id IS NOT NULL) "
                    "ORDER BY id"
                ),
                tags=["not_in", "not_cancelled"],
                desc="NOT IN 子查询 — 无取消订单的商品",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 3. EXISTS / NOT EXISTS subqueries (~10)
    # ────────────────────────────────────────────────────────
    def _exists_subqueries(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users u "
                    "WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id AND o.status = 'delivered') "
                    "ORDER BY id"
                ),
                tags=["exists", "status"],
                desc="EXISTS 子查询 — 有已交付订单的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users u "
                    "WHERE NOT EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id) "
                    "ORDER BY id"
                ),
                tags=["not_exists"],
                desc="NOT EXISTS 子查询 — 无订单的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name FROM t_products p "
                    "WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.product_id = p.id AND o.quantity >= 5) "
                    "ORDER BY id"
                ),
                tags=["exists", "quantity"],
                desc="EXISTS 子查询 — 被大量订购的商品",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, salary FROM t_employees e "
                    "WHERE EXISTS (SELECT 1 FROM t_employees m WHERE m.id = e.manager_id AND m.salary > e.salary) "
                    "ORDER BY id"
                ),
                tags=["exists", "self_join", "salary"],
                desc="EXISTS 子查询 — 薪资低于经理的员工",
            ),
            self._seed(
                sql=(
                    "SELECT id, name FROM t_departments d "
                    "WHERE EXISTS (SELECT 1 FROM t_departments c WHERE c.parent_id = d.id) "
                    "ORDER BY id"
                ),
                tags=["exists", "hierarchy"],
                desc="EXISTS 子查询 — 有子部门的部门",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users u "
                    "WHERE EXISTS (SELECT 1 FROM t_metrics m WHERE m.user_id = u.id AND m.metric_value > 80) "
                    "ORDER BY id"
                ),
                tags=["exists", "metrics"],
                desc="EXISTS 子查询 — 有高指标值的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name FROM t_departments d "
                    "WHERE NOT EXISTS (SELECT 1 FROM t_employees e WHERE e.dept_id = d.id) "
                    "ORDER BY id"
                ),
                tags=["not_exists", "empty_dept"],
                desc="NOT EXISTS 子查询 — 无员工的部门",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users u "
                    "WHERE EXISTS (SELECT 1 FROM t_tags t WHERE t.entity_type = 'user' AND t.entity_id = u.id) "
                    "ORDER BY id"
                ),
                tags=["exists", "tags"],
                desc="EXISTS 子查询 — 有标签的用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price FROM t_products p "
                    "WHERE NOT EXISTS (SELECT 1 FROM t_orders o WHERE o.product_id = p.id) "
                    "ORDER BY id"
                ),
                tags=["not_exists", "unsold"],
                desc="NOT EXISTS 子查询 — 无订单的商品",
            ),
            self._seed(
                sql=(
                    "SELECT id, username FROM t_users u "
                    "WHERE EXISTS (SELECT 1 FROM t_transactions t WHERE t.from_user = u.id AND t.status = 'completed') "
                    "AND NOT EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id AND o.status = 'cancelled') "
                    "ORDER BY id"
                ),
                tags=["exists", "not_exists", "compound"],
                desc="EXISTS + NOT EXISTS 复合子查询",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 4. Derived tables (~10)
    # ────────────────────────────────────────────────────────
    def _derived_tables(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT sub.user_id, sub.order_count, u.username "
                    "FROM (SELECT user_id, COUNT(*) AS order_count FROM t_orders GROUP BY user_id) AS sub "
                    "INNER JOIN t_users u ON sub.user_id = u.id "
                    "ORDER BY sub.user_id"
                ),
                tags=["derived", "count", "join"],
                desc="派生表 — 用户订单数统计",
            ),
            self._seed(
                sql=(
                    "SELECT sub.category, sub.avg_price, sub.cnt "
                    "FROM (SELECT category, AVG(price) AS avg_price, COUNT(*) AS cnt "
                    "  FROM t_products WHERE category IS NOT NULL GROUP BY category) AS sub "
                    "WHERE sub.cnt > 1 "
                    "ORDER BY sub.category"
                ),
                tags=["derived", "avg", "filter"],
                desc="派生表 — 类别均价（过滤 cnt > 1）",
            ),
            self._seed(
                sql=(
                    "SELECT u.username, sub.total_spent "
                    "FROM (SELECT user_id, SUM(total_price) AS total_spent FROM t_orders GROUP BY user_id) AS sub "
                    "INNER JOIN t_users u ON sub.user_id = u.id "
                    "ORDER BY sub.total_spent DESC, u.id"
                ),
                tags=["derived", "sum", "join"],
                desc="派生表 — 用户消费总额",
            ),
            self._seed(
                sql=(
                    "SELECT p.name, sub.total_qty "
                    "FROM (SELECT product_id, SUM(quantity) AS total_qty FROM t_orders GROUP BY product_id) AS sub "
                    "INNER JOIN t_products p ON sub.product_id = p.id "
                    "ORDER BY sub.total_qty DESC, p.id"
                ),
                tags=["derived", "sum", "join"],
                desc="派生表 — 商品总销量",
            ),
            self._seed(
                sql=(
                    "SELECT u.username, sub.metric_count, sub.avg_value "
                    "FROM (SELECT user_id, COUNT(*) AS metric_count, AVG(metric_value) AS avg_value "
                    "  FROM t_metrics WHERE metric_value IS NOT NULL GROUP BY user_id) AS sub "
                    "INNER JOIN t_users u ON sub.user_id = u.id "
                    "ORDER BY sub.avg_value DESC, u.id"
                ),
                tags=["derived", "avg", "count", "join"],
                desc="派生表 — 用户指标统计",
            ),
            self._seed(
                sql=(
                    "SELECT d.name AS dept_name, sub.emp_count, sub.avg_salary "
                    "FROM (SELECT dept_id, COUNT(*) AS emp_count, AVG(salary) AS avg_salary "
                    "  FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id) AS sub "
                    "INNER JOIN t_departments d ON sub.dept_id = d.id "
                    "ORDER BY d.name"
                ),
                tags=["derived", "avg", "count", "join"],
                desc="派生表 — 部门薪资统计",
            ),
            self._seed(
                sql=(
                    "SELECT sub.status, sub.cnt, sub.total "
                    "FROM (SELECT status, COUNT(*) AS cnt, SUM(total_price) AS total "
                    "  FROM t_orders WHERE status IS NOT NULL GROUP BY status) AS sub "
                    "ORDER BY sub.total DESC"
                ),
                tags=["derived", "sum", "count"],
                desc="派生表 — 订单状态统计（无外部 JOIN）",
            ),
            self._seed(
                sql=(
                    "SELECT u.username, sub.tx_count "
                    "FROM (SELECT from_user, COUNT(*) AS tx_count FROM t_transactions GROUP BY from_user) AS sub "
                    "INNER JOIN t_users u ON sub.from_user = u.id "
                    "ORDER BY sub.tx_count DESC, u.id"
                ),
                tags=["derived", "count", "join"],
                desc="派生表 — 用户交易次数",
            ),
            self._seed(
                sql=(
                    "SELECT sub.entity_type, sub.tag_count "
                    "FROM (SELECT entity_type, COUNT(DISTINCT tag) AS tag_count "
                    "  FROM t_tags GROUP BY entity_type) AS sub "
                    "ORDER BY sub.entity_type"
                ),
                tags=["derived", "count_distinct"],
                desc="派生表 — 各实体类型的标签数",
            ),
            self._seed(
                sql=(
                    "SELECT p.name, p.price, sub.cat_avg "
                    "FROM t_products p "
                    "INNER JOIN (SELECT category, AVG(price) AS cat_avg "
                    "  FROM t_products WHERE category IS NOT NULL AND price IS NOT NULL GROUP BY category) AS sub "
                    "ON p.category = sub.category "
                    "WHERE p.category IS NOT NULL "
                    "ORDER BY p.category, p.id"
                ),
                tags=["derived", "avg", "self_join"],
                desc="派生表 — 商品价格与类别均价对比",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 5. Correlated subqueries (~10)
    # ────────────────────────────────────────────────────────
    def _correlated_subqueries(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT id, username, score FROM t_users u "
                    "WHERE score > (SELECT AVG(score) FROM t_users WHERE active = u.active AND score IS NOT NULL) "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "active"],
                desc="关联子查询 — 分数高于同 active 状态平均",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price FROM t_products p "
                    "WHERE price > (SELECT AVG(price) FROM t_products WHERE category = p.category AND price IS NOT NULL) "
                    "AND category IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "category"],
                desc="关联子查询 — 价格高于同类均价",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, salary FROM t_employees e "
                    "WHERE salary > (SELECT AVG(salary) FROM t_employees WHERE dept_id = e.dept_id AND salary IS NOT NULL) "
                    "AND dept_id IS NOT NULL AND salary IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "dept"],
                desc="关联子查询 — 薪资高于部门均值",
            ),
            self._seed(
                sql=(
                    "SELECT id, user_id, total_price FROM t_orders o "
                    "WHERE total_price > (SELECT AVG(total_price) FROM t_orders WHERE user_id = o.user_id) "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "user_orders"],
                desc="关联子查询 — 订单金额高于用户均值",
            ),
            self._seed(
                sql=(
                    "SELECT id, user_id, metric_value FROM t_metrics m "
                    "WHERE metric_value > (SELECT AVG(metric_value) FROM t_metrics WHERE user_id = m.user_id AND metric_value IS NOT NULL) "
                    "AND metric_value IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "metrics"],
                desc="关联子查询 — 指标值高于用户均值",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, budget FROM t_departments d "
                    "WHERE budget > (SELECT AVG(budget) FROM t_departments WHERE parent_id = d.parent_id AND budget IS NOT NULL) "
                    "AND budget IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "dept_budget"],
                desc="关联子查询 — 预算高于同级部门均值",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score FROM t_users u "
                    "WHERE score >= (SELECT MAX(score) FROM t_users WHERE manager_id = u.manager_id AND score IS NOT NULL) "
                    "AND manager_id IS NOT NULL AND score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "max", "manager"],
                desc="关联子查询 — 同经理下最高分用户",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price FROM t_products p "
                    "WHERE price = (SELECT MIN(price) FROM t_products WHERE category = p.category) "
                    "AND category IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "min", "category"],
                desc="关联子查询 — 同类最低价商品",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, age FROM t_users u "
                    "WHERE age > (SELECT AVG(age) FROM t_users WHERE manager_id = u.manager_id AND age IS NOT NULL) "
                    "AND manager_id IS NOT NULL AND age IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "age", "manager"],
                desc="关联子查询 — 年龄高于同经理下均值",
            ),
            self._seed(
                sql=(
                    "SELECT id, amount, status FROM t_transactions t "
                    "WHERE amount > (SELECT AVG(amount) FROM t_transactions WHERE tx_type = t.tx_type AND amount IS NOT NULL) "
                    "AND amount IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["correlated", "avg", "transaction_type"],
                desc="关联子查询 — 金额高于同类型均值",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 6. Subquery + CAST (~8)
    # ────────────────────────────────────────────────────────
    def _subquery_cast(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT id, username, "
                    "(SELECT CAST(AVG(score) AS INTEGER) FROM t_users WHERE score IS NOT NULL) AS avg_score_int "
                    "FROM t_users "
                    "WHERE score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "integer"],
                desc="CAST 子查询 — 平均分转整数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, price, "
                    "(SELECT CAST(AVG(price) AS DECIMAL(10,2)) FROM t_products WHERE category = p.category AND price IS NOT NULL) AS cat_avg_dec "
                    "FROM t_products p "
                    "WHERE category IS NOT NULL AND price IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "decimal", "correlated"],
                desc="CAST 子查询 — 类别均价转 DECIMAL",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, score, "
                    "CAST((SELECT MAX(score) FROM t_users WHERE active = u.active) AS INTEGER) AS active_max_int "
                    "FROM t_users u "
                    "WHERE score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "max", "correlated"],
                desc="CAST 子查询 — active 组最大分转整数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, "
                    "CAST((SELECT COUNT(*) FROM t_orders WHERE product_id = p.id) AS INTEGER) AS order_count "
                    "FROM t_products p "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "count", "correlated"],
                desc="CAST 子查询 — 商品订单数转整数",
            ),
            self._seed(
                sql=(
                    "SELECT id, username, "
                    "CAST((SELECT AVG(score) FROM t_users WHERE manager_id = u.manager_id AND score IS NOT NULL) AS INTEGER) AS mgr_avg_int "
                    "FROM t_users u "
                    "WHERE manager_id IS NOT NULL AND score IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "avg", "correlated", "nested"],
                desc="CAST 子查询 — 同经理下平均分转整数",
            ),
            self._seed(
                sql=(
                    "SELECT id, name, salary, "
                    "CAST((SELECT AVG(salary) FROM t_employees WHERE dept_id = e.dept_id AND salary IS NOT NULL) AS DECIMAL(10,2)) AS dept_avg_dec "
                    "FROM t_employees e "
                    "WHERE salary IS NOT NULL AND dept_id IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "avg", "correlated", "decimal"],
                desc="CAST 子查询 — 部门均薪转 DECIMAL",
            ),
            self._seed(
                sql=(
                    "SELECT sub.dept_id, sub.avg_salary_int "
                    "FROM (SELECT dept_id, CAST(AVG(salary) AS INTEGER) AS avg_salary_int "
                    "  FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id) AS sub "
                    "ORDER BY sub.dept_id"
                ),
                tags=["cast", "derived", "avg", "integer"],
                desc="CAST 派生表 — 部门均薪转整数",
            ),
            self._seed(
                sql=(
                    "SELECT id, user_id, metric_value, "
                    "CAST((SELECT SUM(metric_value) FROM t_metrics WHERE user_id = m.user_id AND metric_value IS NOT NULL) AS INTEGER) AS user_total_int "
                    "FROM t_metrics m "
                    "WHERE metric_value IS NOT NULL "
                    "ORDER BY id"
                ),
                tags=["cast", "scalar", "sum", "correlated"],
                desc="CAST 子查询 — 用户指标总和转整数",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 7. Subquery in HAVING (~7)
    # ────────────────────────────────────────────────────────
    def _having_subqueries(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "SELECT category, COUNT(*) AS cnt "
                    "FROM t_products "
                    "WHERE category IS NOT NULL "
                    "GROUP BY category "
                    "HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM t_products WHERE category IS NOT NULL GROUP BY category)) "
                    "ORDER BY category"
                ),
                tags=["having", "avg", "nested"],
                desc="HAVING 子查询 — 类别商品数高于平均值",
            ),
            self._seed(
                sql=(
                    "SELECT user_id, COUNT(*) AS order_count "
                    "FROM t_orders "
                    "GROUP BY user_id "
                    "HAVING SUM(total_price) > (SELECT AVG(total_price) FROM t_orders) "
                    "ORDER BY user_id"
                ),
                tags=["having", "sum", "avg"],
                desc="HAVING 子查询 — 用户消费总额高于全局均值",
            ),
            self._seed(
                sql=(
                    "SELECT status, COUNT(*) AS cnt, SUM(total_price) AS total "
                    "FROM t_orders "
                    "WHERE status IS NOT NULL "
                    "GROUP BY status "
                    "HAVING COUNT(*) >= (SELECT COUNT(*) / COUNT(DISTINCT status) FROM t_orders WHERE status IS NOT NULL) "
                    "ORDER BY status"
                ),
                tags=["having", "count", "avg_status"],
                desc="HAVING 子查询 — 状态订单数高于平均",
            ),
            self._seed(
                sql=(
                    "SELECT dept_id, COUNT(*) AS emp_count "
                    "FROM t_employees "
                    "WHERE dept_id IS NOT NULL "
                    "GROUP BY dept_id "
                    "HAVING AVG(salary) > (SELECT AVG(salary) FROM t_employees WHERE salary IS NOT NULL) "
                    "ORDER BY dept_id"
                ),
                tags=["having", "avg", "salary"],
                desc="HAVING 子查询 — 部门均薪高于全局均值",
            ),
            self._seed(
                sql=(
                    "SELECT user_id, COUNT(*) AS metric_count "
                    "FROM t_metrics "
                    "GROUP BY user_id "
                    "HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM t_metrics GROUP BY user_id)) "
                    "ORDER BY user_id"
                ),
                tags=["having", "avg", "nested", "metrics"],
                desc="HAVING 子查询 — 用户指标数高于平均值",
            ),
            self._seed(
                sql=(
                    "SELECT entity_type, COUNT(DISTINCT tag) AS unique_tags "
                    "FROM t_tags "
                    "GROUP BY entity_type "
                    "HAVING COUNT(DISTINCT tag) > (SELECT AVG(tag_cnt) FROM (SELECT COUNT(DISTINCT tag) AS tag_cnt FROM t_tags GROUP BY entity_type)) "
                    "ORDER BY entity_type"
                ),
                tags=["having", "count_distinct", "avg", "nested"],
                desc="HAVING 子查询 — 实体类型标签数高于均值",
            ),
            self._seed(
                sql=(
                    "SELECT parent_id, COUNT(*) AS child_count "
                    "FROM t_departments "
                    "WHERE parent_id IS NOT NULL "
                    "GROUP BY parent_id "
                    "HAVING SUM(budget) > (SELECT AVG(budget) FROM t_departments WHERE budget IS NOT NULL) "
                    "ORDER BY parent_id"
                ),
                tags=["having", "sum", "avg", "budget"],
                desc="HAVING 子查询 — 子部门预算总和高于全局均值",
            ),
        ]
