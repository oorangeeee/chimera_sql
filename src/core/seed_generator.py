"""种子 SQL 生成器 — 按类别生成覆盖各类 SQL 特性的种子文件。

生成约 70 个 .sql 文件，分为 11 个类别子目录。
所有种子使用通用 SQL 方言，后续由 transpiler 模块转译。

设计原则：
1. 每条种子都带确定性 ORDER BY，保证跨数据库结果集可比较
2. 避免数据库特有语法（不用 NVL/ROWNUM/FETCH FIRST）
3. 只引用 5 张测试表
4. 故意查询含 NULL 的列（NULL 处理是 Oracle/SQLite 差异重灾区）
5. 不含 RIGHT JOIN（SQLite 3.39.0 前不支持）
6. 递归 CTE 使用 WITH RECURSIVE（SQLite 要求），transpiler 转译时为 Oracle 去掉 RECURSIVE
7. JSON 函数使用 json_extract()（SQLite 原生），transpiler 转译为 Oracle 的 JSON_VALUE()
"""

from pathlib import Path
from typing import Dict, List, Tuple

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ────────────────────────────────────────────────────────
# 种子 SQL 定义（类别名 → [(文件名, SQL语句), ...]）
# ────────────────────────────────────────────────────────

_SEEDS: Dict[str, List[Tuple[str, str]]] = {
    # ── 01 基础查询 ──────────────────────────────────
    "01_basic_select": [
        (
            "select_all_users.sql",
            "SELECT id, username, email, age, score, active FROM t_users ORDER BY id",
        ),
        (
            "where_equality.sql",
            "SELECT id, username, score FROM t_users WHERE active = 1 ORDER BY id",
        ),
        (
            "where_in.sql",
            "SELECT id, name, price FROM t_products WHERE category IN ('electronics', 'books') ORDER BY id",
        ),
        (
            "where_between.sql",
            "SELECT id, username, age FROM t_users WHERE age BETWEEN 18 AND 35 ORDER BY id",
        ),
        (
            "where_like.sql",
            "SELECT id, name, category FROM t_products WHERE name LIKE 'Widget%' ORDER BY id",
        ),
        (
            "distinct_category.sql",
            "SELECT DISTINCT category FROM t_products WHERE category IS NOT NULL ORDER BY category",
        ),
        (
            "order_by_desc.sql",
            "SELECT id, username, score FROM t_users WHERE score IS NOT NULL ORDER BY score DESC, id",
        ),
        (
            "limit_offset.sql",
            "SELECT id, username, age FROM t_users ORDER BY id LIMIT 5 OFFSET 3",
        ),
        (
            "multiple_conditions.sql",
            "SELECT id, username, age, score FROM t_users WHERE active = 1 AND age > 20 AND score IS NOT NULL ORDER BY id",
        ),
        (
            "or_condition.sql",
            "SELECT id, name, price FROM t_products WHERE price < 10 OR price > 1000 ORDER BY id",
        ),
    ],
    # ── 02 聚合查询 ──────────────────────────────────
    "02_aggregation": [
        (
            "count_all.sql",
            "SELECT COUNT(*) AS cnt FROM t_users",
        ),
        (
            "count_non_null.sql",
            "SELECT COUNT(email) AS cnt_email FROM t_users",
        ),
        (
            "sum_avg.sql",
            "SELECT SUM(total_price) AS total, AVG(total_price) AS avg_price FROM t_orders",
        ),
        (
            "min_max.sql",
            "SELECT MIN(price) AS min_price, MAX(price) AS max_price FROM t_products",
        ),
        (
            "group_by_category.sql",
            "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category ORDER BY category",
        ),
        (
            "group_by_status.sql",
            "SELECT status, COUNT(*) AS cnt, SUM(total_price) AS total FROM t_orders WHERE status IS NOT NULL GROUP BY status ORDER BY status",
        ),
        (
            "having_filter.sql",
            "SELECT category, COUNT(*) AS cnt FROM t_products WHERE category IS NOT NULL GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
        ),
    ],
    # ── 03 连接查询 ──────────────────────────────────
    "03_join": [
        (
            "inner_join.sql",
            "SELECT o.id, u.username, p.name, o.quantity, o.total_price "
            "FROM t_orders o "
            "INNER JOIN t_users u ON o.user_id = u.id "
            "INNER JOIN t_products p ON o.product_id = p.id "
            "ORDER BY o.id",
        ),
        (
            "left_join_users_orders.sql",
            "SELECT u.id, u.username, o.id AS order_id, o.total_price "
            "FROM t_users u "
            "LEFT JOIN t_orders o ON u.id = o.user_id "
            "ORDER BY u.id, o.id",
        ),
        (
            "left_join_null_check.sql",
            "SELECT u.id, u.username "
            "FROM t_users u "
            "LEFT JOIN t_orders o ON u.id = o.user_id "
            "WHERE o.id IS NULL "
            "ORDER BY u.id",
        ),
        (
            "self_join_users.sql",
            "SELECT a.id AS id1, b.id AS id2, a.username AS user1, b.username AS user2 "
            "FROM t_users a "
            "INNER JOIN t_users b ON a.age = b.age AND a.id < b.id "
            "ORDER BY a.id, b.id",
        ),
        (
            "join_with_aggregation.sql",
            "SELECT u.id, u.username, COUNT(o.id) AS order_count, SUM(o.total_price) AS total_spent "
            "FROM t_users u "
            "LEFT JOIN t_orders o ON u.id = o.user_id "
            "GROUP BY u.id, u.username "
            "ORDER BY u.id",
        ),
        (
            "multi_table_join.sql",
            "SELECT u.username, p.name, o.quantity, o.status "
            "FROM t_orders o "
            "INNER JOIN t_users u ON o.user_id = u.id "
            "INNER JOIN t_products p ON o.product_id = p.id "
            "WHERE o.status = 'delivered' "
            "ORDER BY o.id",
        ),
    ],
    # ── 04 子查询 ────────────────────────────────────
    "04_subquery": [
        (
            "scalar_subquery.sql",
            "SELECT id, username, score, "
            "(SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) AS avg_score "
            "FROM t_users "
            "WHERE score IS NOT NULL "
            "ORDER BY id",
        ),
        (
            "in_subquery.sql",
            "SELECT id, username FROM t_users "
            "WHERE id IN (SELECT DISTINCT user_id FROM t_orders) "
            "ORDER BY id",
        ),
        (
            "not_in_subquery.sql",
            "SELECT id, username FROM t_users "
            "WHERE id NOT IN (SELECT DISTINCT user_id FROM t_orders WHERE user_id IS NOT NULL) "
            "ORDER BY id",
        ),
        (
            "exists_subquery.sql",
            "SELECT id, username FROM t_users u "
            "WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id AND o.status = 'delivered') "
            "ORDER BY id",
        ),
        (
            "derived_table.sql",
            "SELECT sub.user_id, sub.order_count, u.username "
            "FROM (SELECT user_id, COUNT(*) AS order_count FROM t_orders GROUP BY user_id) sub "
            "INNER JOIN t_users u ON sub.user_id = u.id "
            "ORDER BY sub.user_id",
        ),
        (
            "correlated_subquery.sql",
            "SELECT id, username, score FROM t_users u "
            "WHERE score > (SELECT AVG(score) FROM t_users WHERE active = u.active AND score IS NOT NULL) "
            "ORDER BY id",
        ),
    ],
    # ── 05 集合操作 ──────────────────────────────────
    "05_set_operations": [
        (
            "union.sql",
            "SELECT tag FROM t_tags WHERE entity_type = 'user' "
            "UNION "
            "SELECT tag FROM t_tags WHERE entity_type = 'product' "
            "ORDER BY tag",
        ),
        (
            "union_all.sql",
            "SELECT tag FROM t_tags WHERE entity_type = 'user' "
            "UNION ALL "
            "SELECT tag FROM t_tags WHERE entity_type = 'product' "
            "ORDER BY tag",
        ),
        (
            "intersect.sql",
            "SELECT tag FROM t_tags WHERE entity_type = 'user' "
            "INTERSECT "
            "SELECT tag FROM t_tags WHERE entity_type = 'product' "
            "ORDER BY tag",
        ),
        (
            "except.sql",
            "SELECT tag FROM t_tags WHERE entity_type = 'user' "
            "EXCEPT "
            "SELECT tag FROM t_tags WHERE entity_type = 'product' "
            "ORDER BY tag",
        ),
    ],
    # ── 06 窗口函数 ──────────────────────────────────
    "06_window_functions": [
        (
            "row_number.sql",
            "SELECT id, user_id, metric_name, metric_value, "
            "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id) AS rn "
            "FROM t_metrics "
            "ORDER BY user_id, id",
        ),
        (
            "rank.sql",
            "SELECT id, username, score, "
            "RANK() OVER (ORDER BY score DESC) AS rnk "
            "FROM t_users "
            "WHERE score IS NOT NULL "
            "ORDER BY rnk, id",
        ),
        (
            "dense_rank.sql",
            "SELECT id, username, score, "
            "DENSE_RANK() OVER (ORDER BY score DESC) AS drnk "
            "FROM t_users "
            "WHERE score IS NOT NULL "
            "ORDER BY drnk, id",
        ),
        (
            "sum_over.sql",
            "SELECT id, user_id, total_price, "
            "SUM(total_price) OVER (PARTITION BY user_id ORDER BY id) AS running_total "
            "FROM t_orders "
            "ORDER BY user_id, id",
        ),
        (
            "avg_over.sql",
            "SELECT id, user_id, metric_value, "
            "AVG(metric_value) OVER (PARTITION BY metric_name) AS avg_by_metric "
            "FROM t_metrics "
            "WHERE metric_value IS NOT NULL "
            "ORDER BY id",
        ),
        (
            "count_over.sql",
            "SELECT id, user_id, status, "
            "COUNT(*) OVER (PARTITION BY status) AS status_count "
            "FROM t_orders "
            "ORDER BY status, id",
        ),
    ],
    # ── 07 NULL 处理 ─────────────────────────────────
    "07_null_handling": [
        (
            "is_null.sql",
            "SELECT id, username, email FROM t_users WHERE email IS NULL ORDER BY id",
        ),
        (
            "is_not_null.sql",
            "SELECT id, username, score FROM t_users WHERE score IS NOT NULL ORDER BY id",
        ),
        (
            "coalesce.sql",
            "SELECT id, username, COALESCE(email, 'N/A') AS email_display "
            "FROM t_users ORDER BY id",
        ),
        (
            "null_in_aggregation.sql",
            "SELECT COUNT(*) AS total, COUNT(score) AS non_null_score, "
            "COUNT(*) - COUNT(score) AS null_score_count "
            "FROM t_users",
        ),
        (
            "null_in_sort.sql",
            "SELECT id, username, age FROM t_users ORDER BY age, id",
        ),
        (
            "coalesce_in_comparison.sql",
            "SELECT id, name, COALESCE(stock, 0) AS effective_stock "
            "FROM t_products "
            "WHERE COALESCE(stock, 0) > 0 "
            "ORDER BY id",
        ),
    ],
    # ── 08 表达式 ────────────────────────────────────
    "08_expressions": [
        (
            "arithmetic.sql",
            "SELECT id, quantity, total_price, "
            "total_price / quantity AS unit_price "
            "FROM t_orders "
            "WHERE quantity > 0 "
            "ORDER BY id",
        ),
        (
            "case_expression.sql",
            "SELECT id, username, age, "
            "CASE WHEN age < 18 THEN 'minor' "
            "WHEN age BETWEEN 18 AND 30 THEN 'young' "
            "WHEN age > 30 THEN 'senior' "
            "ELSE 'unknown' END AS age_group "
            "FROM t_users "
            "ORDER BY id",
        ),
        (
            "case_with_null.sql",
            "SELECT id, username, "
            "CASE WHEN score IS NULL THEN 'no_score' "
            "WHEN score >= 90 THEN 'excellent' "
            "WHEN score >= 60 THEN 'pass' "
            "ELSE 'fail' END AS grade "
            "FROM t_users "
            "ORDER BY id",
        ),
        (
            "cast_integer.sql",
            "SELECT id, CAST(price AS INTEGER) AS price_int "
            "FROM t_products "
            "ORDER BY id",
        ),
        (
            "nested_expression.sql",
            "SELECT id, username, "
            "(score * 2 + COALESCE(age, 0)) AS composite "
            "FROM t_users "
            "WHERE score IS NOT NULL "
            "ORDER BY id",
        ),
        (
            "arithmetic_with_null.sql",
            "SELECT id, username, age, score, "
            "age + score AS age_plus_score "
            "FROM t_users "
            "ORDER BY id",
        ),
        (
            "complex_case.sql",
            "SELECT o.id, u.username, p.name, "
            "CASE WHEN o.total_price > 1000 THEN 'high' "
            "WHEN o.total_price > 100 THEN 'medium' "
            "ELSE 'low' END AS order_tier "
            "FROM t_orders o "
            "INNER JOIN t_users u ON o.user_id = u.id "
            "INNER JOIN t_products p ON o.product_id = p.id "
            "ORDER BY o.id",
        ),
    ],
    # ── 09 递归/自关联 ─────────────────────────────
    "09_recursive_self_join": [
        (
            "direct_reports.sql",
            "SELECT m.id AS manager_id, m.username AS manager, "
            "e.id AS employee_id, e.username AS employee "
            "FROM t_users e "
            "INNER JOIN t_users m ON e.manager_id = m.id "
            "ORDER BY m.id, e.id",
        ),
        (
            "recursive_hierarchy.sql",
            "WITH RECURSIVE hierarchy(id, username, manager_id, lvl) AS ("
            "SELECT id, username, manager_id, 0 AS lvl "
            "FROM t_users WHERE manager_id IS NULL "
            "UNION ALL "
            "SELECT e.id, e.username, e.manager_id, h.lvl + 1 "
            "FROM t_users e INNER JOIN hierarchy h ON e.manager_id = h.id"
            ") SELECT id, username, manager_id, lvl FROM hierarchy ORDER BY id",
        ),
        (
            "recursive_depth.sql",
            "WITH RECURSIVE depth_cte(id, username, lvl) AS ("
            "SELECT id, username, 0 AS lvl "
            "FROM t_users WHERE manager_id IS NULL "
            "UNION ALL "
            "SELECT e.id, e.username, d.lvl + 1 "
            "FROM t_users e INNER JOIN depth_cte d ON e.manager_id = d.id"
            ") SELECT id, username, lvl FROM depth_cte ORDER BY lvl, id",
        ),
        (
            "recursive_subordinate_count.sql",
            "WITH RECURSIVE sub_tree(id, root_id) AS ("
            "SELECT id, id AS root_id "
            "FROM t_users WHERE manager_id IS NULL "
            "UNION ALL "
            "SELECT e.id, s.root_id "
            "FROM t_users e INNER JOIN sub_tree s ON e.manager_id = s.id"
            ") SELECT root_id, COUNT(*) - 1 AS subordinate_count "
            "FROM sub_tree GROUP BY root_id ORDER BY root_id",
        ),
        (
            "root_nodes.sql",
            "SELECT id, username FROM t_users "
            "WHERE manager_id IS NULL ORDER BY id",
        ),
        (
            "same_manager_pairs.sql",
            "SELECT a.id AS id1, b.id AS id2, "
            "a.username AS user1, b.username AS user2, "
            "a.manager_id "
            "FROM t_users a "
            "INNER JOIN t_users b ON a.manager_id = b.manager_id AND a.id < b.id "
            "WHERE a.manager_id IS NOT NULL "
            "ORDER BY a.manager_id, a.id, b.id",
        ),
    ],
    # ── 10 字符串排序/Unicode ──────────────────────
    "10_string_collation": [
        (
            "upper_lower.sql",
            "SELECT id, username, UPPER(username) AS upper_name, "
            "LOWER(username) AS lower_name "
            "FROM t_users ORDER BY id",
        ),
        (
            "length_unicode.sql",
            "SELECT id, username, LENGTH(username) AS name_len "
            "FROM t_users ORDER BY id",
        ),
        (
            "like_unicode.sql",
            "SELECT id, username FROM t_users "
            "WHERE username LIKE '%e%' ORDER BY id",
        ),
        (
            "string_comparison.sql",
            "SELECT id, username FROM t_users "
            "WHERE username > 'b' ORDER BY id",
        ),
        (
            "trim_strings.sql",
            "SELECT id, TRIM(username) AS trimmed_name, "
            "LENGTH(TRIM(username)) AS trimmed_len "
            "FROM t_users ORDER BY id",
        ),
        (
            "substr_unicode.sql",
            "SELECT id, username, SUBSTR(username, 1, 3) AS prefix "
            "FROM t_users ORDER BY id",
        ),
    ],
    # ── 11 JSON 处理 ─────────────────────────────
    # 使用 json_extract() 语法（SQLite 原生），transpiler 转译为 Oracle 的 JSON_VALUE()
    "11_json_handling": [
        (
            "json_extract_scalar.sql",
            "SELECT id, username, "
            "json_extract(profile, '$.theme') AS theme, "
            "json_extract(profile, '$.lang') AS lang "
            "FROM t_users WHERE profile IS NOT NULL ORDER BY id",
        ),
        (
            "json_null_handling.sql",
            "SELECT id, username, profile, "
            "json_extract(profile, '$.theme') AS theme "
            "FROM t_users ORDER BY id",
        ),
        (
            "json_in_where.sql",
            "SELECT id, username FROM t_users "
            "WHERE json_extract(profile, '$.theme') = 'dark' ORDER BY id",
        ),
        (
            "json_with_aggregation.sql",
            "SELECT json_extract(profile, '$.theme') AS theme, "
            "COUNT(*) AS cnt "
            "FROM t_users "
            "WHERE profile IS NOT NULL "
            "AND json_extract(profile, '$.theme') IS NOT NULL "
            "GROUP BY json_extract(profile, '$.theme') "
            "ORDER BY theme",
        ),
        (
            "json_nested_array.sql",
            "SELECT id, name, "
            "json_extract(metadata, '$.warranty') AS warranty "
            "FROM t_products "
            "WHERE metadata IS NOT NULL "
            "AND json_extract(metadata, '$.warranty') IS NOT NULL "
            "ORDER BY id",
        ),
        (
            "json_coalesce.sql",
            "SELECT id, username, "
            "COALESCE(json_extract(profile, '$.lang'), 'unknown') AS lang "
            "FROM t_users ORDER BY id",
        ),
    ],
}


class SeedGenerator:
    """种子 SQL 文件生成器。

    将预定义的种子 SQL 按类别写入 data/seeds/ 目录，
    每个 .sql 文件包含单条 SQL 语句。
    """

    def __init__(self) -> None:
        config = ConfigLoader()
        seed_dir = config.get("fuzzing.seed_dir", "data/seeds")
        # 以项目根目录为基准解析相对路径
        project_root = Path(__file__).resolve().parent.parent.parent
        self._seed_dir = project_root / seed_dir

    def generate_all(self) -> None:
        """生成所有类别的种子 SQL 文件。"""
        logger.info("开始生成种子 SQL 文件 ...")

        total_files = 0
        for category, seeds in _SEEDS.items():
            category_dir = self._seed_dir / category
            category_dir.mkdir(parents=True, exist_ok=True)

            for filename, sql in seeds:
                filepath = category_dir / filename
                filepath.write_text(sql + "\n", encoding="utf-8")
                total_files += 1

            logger.info("类别 %s: 生成 %d 个种子文件", category, len(seeds))

        logger.info(
            "种子 SQL 生成完成（共 %d 个类别, %d 个文件）",
            len(_SEEDS),
            total_files,
        )
