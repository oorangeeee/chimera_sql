"""日期函数方言差异模板 — 生成 ~70 条种子 SQL，覆盖 SQLite/Oracle 日期函数差异。

关键差异点：
- DATE() / STRFTIME()  vs  TO_DATE / TO_CHAR
- 日期格式字符串（%Y-%m-%d  vs  YYYY-MM-DD）
- 日期算术（date(col, '+N day')  vs  col + N）
- EXTRACT 函数
"""

from __future__ import annotations

from typing import List

from src.testbed.seed_templates.base import SchemaMetadata, SeedSQL, SeedTemplate


class DateFuncsTemplate(SeedTemplate):
    """日期函数方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "date_funcs"

    @property
    def description(self) -> str:
        return "日期函数方言差异测试（DATE/STRFTIME vs TO_DATE/TO_CHAR、日期算术、EXTRACT）"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._date_function())
        seeds.extend(self._strftime_format())
        seeds.extend(self._date_comparisons())
        seeds.extend(self._date_arithmetic())
        seeds.extend(self._date_group_by())
        seeds.extend(self._date_coalesce())
        seeds.extend(self._date_cast())
        seeds.extend(self._date_ordering_null())
        return seeds

    # ────────────────────────────────────────────────────
    # 1. DATE() 函数 (~12)
    # ────────────────────────────────────────────────────
    def _date_function(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date = DATE('2000-01-01') "
                "ORDER BY id",
                tags=["date_func", "literal"],
                desc="DATE() literal equality",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date > DATE('1990-01-01') "
                "ORDER BY id",
                tags=["date_func", "comparison"],
                desc="DATE() greater-than comparison",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date < DATE('2000-12-31') "
                "ORDER BY id",
                tags=["date_func", "comparison"],
                desc="DATE() less-than comparison",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date >= DATE('1985-06-15') "
                "ORDER BY id",
                tags=["date_func", "comparison"],
                desc="DATE() greater-equal comparison",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date <= DATE('2005-03-20') "
                "ORDER BY id",
                tags=["date_func", "comparison"],
                desc="DATE() less-equal comparison",
            ),
            self._seed(
                "SELECT id, username, birth_date, DATE('2024-01-01') AS ref_date "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["date_func", "select_literal"],
                desc="DATE() as SELECT column",
            ),
            self._seed(
                "SELECT id, name, release_date "
                "FROM t_products "
                "WHERE release_date = DATE('2024-06-01') "
                "ORDER BY id",
                tags=["date_func", "product"],
                desc="DATE() literal on products release_date",
            ),
            self._seed(
                "SELECT id, name, release_date "
                "FROM t_products "
                "WHERE release_date > DATE('2023-01-01') AND release_date < DATE('2024-12-31') "
                "ORDER BY release_date, id",
                tags=["date_func", "range"],
                desc="DATE() range filter on products",
            ),
            self._seed(
                "SELECT id, metric_name, metric_value, measurement_date "
                "FROM t_metrics "
                "WHERE measurement_date > DATE('2024-01-01') "
                "ORDER BY measurement_date, id",
                tags=["date_func", "metrics"],
                desc="DATE() filter on measurement_date",
            ),
            self._seed(
                "SELECT id, name, hire_date "
                "FROM t_employees "
                "WHERE hire_date > DATE('2020-01-01') "
                "ORDER BY hire_date, id",
                tags=["date_func", "employee"],
                desc="DATE() filter on hire_date",
            ),
            self._seed(
                "SELECT id, event_type, event_date "
                "FROM t_events "
                "WHERE event_date = DATE('2024-07-15') "
                "ORDER BY id",
                tags=["date_func", "event"],
                desc="DATE() literal equality on events",
            ),
            self._seed(
                "SELECT id, event_type, event_date "
                "FROM t_events "
                "WHERE event_date >= DATE('2024-01-01') "
                "ORDER BY event_date, id",
                tags=["date_func", "event"],
                desc="DATE() greater-equal on event_date",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 2. STRFTIME 格式 (~10)
    # ────────────────────────────────────────────────────
    def _strftime_format(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, STRFTIME('%Y', birth_date) AS birth_year "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "year"],
                desc="STRFTIME extract year",
            ),
            self._seed(
                "SELECT id, username, STRFTIME('%m', birth_date) AS birth_month "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "month"],
                desc="STRFTIME extract month",
            ),
            self._seed(
                "SELECT id, username, STRFTIME('%d', birth_date) AS birth_day "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "day"],
                desc="STRFTIME extract day",
            ),
            self._seed(
                "SELECT id, username, STRFTIME('%Y-%m', birth_date) AS birth_ym "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "year_month"],
                desc="STRFTIME year-month format",
            ),
            self._seed(
                "SELECT id, username, STRFTIME('%m-%d', birth_date) AS birth_md "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "month_day"],
                desc="STRFTIME month-day format",
            ),
            self._seed(
                "SELECT id, name, STRFTIME('%Y', release_date) AS release_year "
                "FROM t_products "
                "WHERE release_date IS NOT NULL "
                "ORDER BY id",
                tags=["strftime", "product_year"],
                desc="STRFTIME year from release_date",
            ),
            self._seed(
                "SELECT id, metric_name, STRFTIME('%Y-%m', measurement_date) AS meas_month "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "ORDER BY measurement_date, id",
                tags=["strftime", "metric_month"],
                desc="STRFTIME year-month from measurement_date",
            ),
            self._seed(
                "SELECT id, name, STRFTIME('%Y-%m-%d', hire_date) AS hire_fmt "
                "FROM t_employees "
                "WHERE hire_date IS NOT NULL "
                "ORDER BY hire_date, id",
                tags=["strftime", "employee_format"],
                desc="STRFTIME full date format on hire_date",
            ),
            self._seed(
                "SELECT id, event_type, STRFTIME('%Y', event_date) AS event_year, "
                "STRFTIME('%m', event_date) AS event_month "
                "FROM t_events "
                "WHERE event_date IS NOT NULL "
                "ORDER BY event_date, id",
                tags=["strftime", "event_multi"],
                desc="STRFTIME multiple format parts on event_date",
            ),
            self._seed(
                "SELECT DISTINCT STRFTIME('%Y', measurement_date) AS year "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "ORDER BY year",
                tags=["strftime", "distinct_year"],
                desc="STRFTIME distinct year values",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 3. 日期比较 (~8)
    # ────────────────────────────────────────────────────
    def _date_comparisons(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date > DATE('1990-01-01') "
                "AND birth_date < DATE('2000-12-31') "
                "ORDER BY birth_date, id",
                tags=["comparison", "range"],
                desc="Date range comparison with AND",
            ),
            self._seed(
                "SELECT id, name, release_date "
                "FROM t_products "
                "WHERE release_date BETWEEN DATE('2023-01-01') AND DATE('2024-12-31') "
                "ORDER BY release_date, id",
                tags=["comparison", "between"],
                desc="BETWEEN with DATE() literals",
            ),
            self._seed(
                "SELECT id, metric_name, measurement_date "
                "FROM t_metrics "
                "WHERE measurement_date BETWEEN DATE('2024-01-01') AND DATE('2024-06-30') "
                "ORDER BY measurement_date, id",
                tags=["comparison", "between"],
                desc="BETWEEN on measurement_date",
            ),
            self._seed(
                "SELECT id, name, hire_date "
                "FROM t_employees "
                "WHERE hire_date BETWEEN DATE('2020-01-01') AND DATE('2024-12-31') "
                "ORDER BY hire_date, id",
                tags=["comparison", "between"],
                desc="BETWEEN on hire_date",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "AND birth_date != DATE('2000-01-01') "
                "ORDER BY birth_date, id",
                tags=["comparison", "not_equal"],
                desc="Date not-equal comparison",
            ),
            self._seed(
                "SELECT id, event_type, event_date "
                "FROM t_events "
                "WHERE event_date BETWEEN DATE('2024-06-01') AND DATE('2024-08-31') "
                "ORDER BY event_date, id",
                tags=["comparison", "between"],
                desc="BETWEEN on event_date summer range",
            ),
            self._seed(
                "SELECT a.id, a.event_date, b.id AS id2, b.event_date AS date2 "
                "FROM t_events a "
                "INNER JOIN t_events b ON a.event_date < b.event_date AND a.id < b.id "
                "ORDER BY a.event_date, a.id, b.id",
                tags=["comparison", "cross_compare"],
                desc="Cross-compare dates between rows",
            ),
            self._seed(
                "SELECT id, name, release_date "
                "FROM t_products "
                "WHERE release_date >= DATE('2024-01-01') "
                "OR release_date IS NULL "
                "ORDER BY release_date, id",
                tags=["comparison", "or_null"],
                desc="Date comparison with OR NULL",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 4. 日期算术 (~10)
    # ────────────────────────────────────────────────────
    def _date_arithmetic(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, birth_date, "
                "DATE(birth_date, '+1 day') AS next_day "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY birth_date, id",
                tags=["arithmetic", "add_day"],
                desc="DATE add 1 day",
            ),
            self._seed(
                "SELECT id, username, birth_date, "
                "DATE(birth_date, '+7 days') AS week_later "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY birth_date, id",
                tags=["arithmetic", "add_week"],
                desc="DATE add 7 days",
            ),
            self._seed(
                "SELECT id, username, birth_date, "
                "DATE(birth_date, '-7 days') AS week_before "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY birth_date, id",
                tags=["arithmetic", "sub_week"],
                desc="DATE subtract 7 days",
            ),
            self._seed(
                "SELECT id, username, birth_date, "
                "DATE(birth_date, '+1 month') AS next_month "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY birth_date, id",
                tags=["arithmetic", "add_month"],
                desc="DATE add 1 month",
            ),
            self._seed(
                "SELECT id, name, release_date, "
                "DATE(release_date, '+1 year') AS next_year "
                "FROM t_products "
                "WHERE release_date IS NOT NULL "
                "ORDER BY release_date, id",
                tags=["arithmetic", "add_year"],
                desc="DATE add 1 year",
            ),
            self._seed(
                "SELECT id, name, hire_date, "
                "DATE(hire_date, '-30 days') AS month_before_hire "
                "FROM t_employees "
                "WHERE hire_date IS NOT NULL "
                "ORDER BY hire_date, id",
                tags=["arithmetic", "sub_30"],
                desc="DATE subtract 30 days from hire_date",
            ),
            self._seed(
                "SELECT id, metric_name, measurement_date, "
                "DATE(measurement_date, '+14 days') AS fortnight_later "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "ORDER BY measurement_date, id",
                tags=["arithmetic", "add_14"],
                desc="DATE add 14 days to measurement_date",
            ),
            self._seed(
                "SELECT id, event_type, event_date, "
                "DATE(event_date, '-1 day') AS prev_day "
                "FROM t_events "
                "WHERE event_date IS NOT NULL "
                "ORDER BY event_date, id",
                tags=["arithmetic", "sub_day"],
                desc="DATE subtract 1 day from event_date",
            ),
            self._seed(
                "SELECT id, username, birth_date, "
                "DATE(birth_date, '+3 months') AS quarter_later "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY birth_date, id",
                tags=["arithmetic", "add_quarter"],
                desc="DATE add 3 months (quarter)",
            ),
            self._seed(
                "SELECT id, name, release_date, "
                "DATE(release_date, '-6 months') AS half_year_before "
                "FROM t_products "
                "WHERE release_date IS NOT NULL "
                "ORDER BY release_date, id",
                tags=["arithmetic", "sub_half_year"],
                desc="DATE subtract 6 months",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 5. 日期 GROUP BY (~8)
    # ────────────────────────────────────────────────────
    def _date_group_by(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT measurement_date, COUNT(*) AS cnt, AVG(metric_value) AS avg_val "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "GROUP BY measurement_date "
                "ORDER BY measurement_date",
                tags=["group_by", "date_key"],
                desc="GROUP BY measurement_date",
            ),
            self._seed(
                "SELECT STRFTIME('%Y', measurement_date) AS year, "
                "COUNT(*) AS cnt, AVG(metric_value) AS avg_val "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "GROUP BY STRFTIME('%Y', measurement_date) "
                "ORDER BY year",
                tags=["group_by", "strftime_year"],
                desc="GROUP BY STRFTIME year",
            ),
            self._seed(
                "SELECT STRFTIME('%Y-%m', measurement_date) AS month, "
                "COUNT(*) AS cnt "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "GROUP BY STRFTIME('%Y-%m', measurement_date) "
                "ORDER BY month",
                tags=["group_by", "strftime_month"],
                desc="GROUP BY STRFTIME year-month",
            ),
            self._seed(
                "SELECT STRFTIME('%Y', hire_date) AS hire_year, "
                "COUNT(*) AS cnt, AVG(salary) AS avg_salary "
                "FROM t_employees "
                "WHERE hire_date IS NOT NULL "
                "GROUP BY STRFTIME('%Y', hire_date) "
                "ORDER BY hire_year",
                tags=["group_by", "employee_year"],
                desc="GROUP BY STRFTIME year on hire_date",
            ),
            self._seed(
                "SELECT event_date, COUNT(*) AS cnt "
                "FROM t_events "
                "WHERE event_date IS NOT NULL "
                "GROUP BY event_date "
                "HAVING COUNT(*) > 1 "
                "ORDER BY event_date",
                tags=["group_by", "having"],
                desc="GROUP BY event_date with HAVING",
            ),
            self._seed(
                "SELECT STRFTIME('%Y', release_date) AS release_year, "
                "category, COUNT(*) AS cnt, AVG(price) AS avg_price "
                "FROM t_products "
                "WHERE release_date IS NOT NULL AND category IS NOT NULL "
                "GROUP BY STRFTIME('%Y', release_date), category "
                "ORDER BY release_year, category",
                tags=["group_by", "composite"],
                desc="GROUP BY STRFTIME year + category composite",
            ),
            self._seed(
                "SELECT STRFTIME('%m', birth_date) AS birth_month, "
                "COUNT(*) AS cnt "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "GROUP BY STRFTIME('%m', birth_date) "
                "ORDER BY birth_month",
                tags=["group_by", "month_only"],
                desc="GROUP BY STRFTIME month only",
            ),
            self._seed(
                "SELECT STRFTIME('%Y-%m', event_date) AS event_month, "
                "event_type, COUNT(*) AS cnt "
                "FROM t_events "
                "WHERE event_date IS NOT NULL "
                "GROUP BY STRFTIME('%Y-%m', event_date), event_type "
                "ORDER BY event_month, event_type",
                tags=["group_by", "event_composite"],
                desc="GROUP BY STRFTIME year-month + event_type",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 6. Date + COALESCE (~6)
    # ────────────────────────────────────────────────────
    def _date_coalesce(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, COALESCE(release_date, DATE('2000-01-01')) AS effective_date "
                "FROM t_products "
                "ORDER BY effective_date, id",
                tags=["coalesce", "default_date"],
                desc="COALESCE release_date with DATE default",
            ),
            self._seed(
                "SELECT id, username, COALESCE(birth_date, DATE('1990-01-01')) AS safe_birth "
                "FROM t_users "
                "ORDER BY safe_birth, id",
                tags=["coalesce", "user_birth"],
                desc="COALESCE birth_date with DATE default",
            ),
            self._seed(
                "SELECT id, name, COALESCE(hire_date, DATE('2024-01-01')) AS safe_hire "
                "FROM t_employees "
                "ORDER BY safe_hire, id",
                tags=["coalesce", "employee_hire"],
                desc="COALESCE hire_date with DATE default",
            ),
            self._seed(
                "SELECT id, metric_name, "
                "COALESCE(measurement_date, DATE('2024-01-01')) AS meas_date "
                "FROM t_metrics "
                "ORDER BY meas_date, id",
                tags=["coalesce", "metric_date"],
                desc="COALESCE measurement_date with DATE default",
            ),
            self._seed(
                "SELECT id, event_type, "
                "COALESCE(event_date, DATE('2024-01-01')) AS safe_event_date "
                "FROM t_events "
                "ORDER BY safe_event_date, id",
                tags=["coalesce", "event_date"],
                desc="COALESCE event_date with DATE default",
            ),
            self._seed(
                "SELECT id, status, "
                "COALESCE(order_date, DATE('2024-01-01')) AS safe_order_date "
                "FROM t_orders "
                "ORDER BY safe_order_date, id",
                tags=["coalesce", "order_date"],
                desc="COALESCE order_date with DATE default",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 7. Date + CAST (~8)
    # ────────────────────────────────────────────────────
    def _date_cast(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, "
                "CAST(birth_date AS VARCHAR(30)) AS birth_str "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "date_to_varchar"],
                desc="CAST date column to VARCHAR",
            ),
            self._seed(
                "SELECT id, name, "
                "CAST(release_date AS VARCHAR(20)) AS release_str "
                "FROM t_products "
                "WHERE release_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "date_to_varchar"],
                desc="CAST release_date to VARCHAR",
            ),
            self._seed(
                "SELECT id, name, "
                "CAST(hire_date AS VARCHAR(30)) AS hire_str "
                "FROM t_employees "
                "WHERE hire_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "date_to_varchar"],
                desc="CAST hire_date to VARCHAR",
            ),
            self._seed(
                "SELECT id, metric_name, "
                "CAST(measurement_date AS VARCHAR(20)) AS meas_str "
                "FROM t_metrics "
                "WHERE measurement_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "date_to_varchar"],
                desc="CAST measurement_date to VARCHAR",
            ),
            self._seed(
                "SELECT id, event_type, "
                "CAST(event_date AS VARCHAR(30)) AS event_str "
                "FROM t_events "
                "WHERE event_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "date_to_varchar"],
                desc="CAST event_date to VARCHAR",
            ),
            self._seed(
                "SELECT id, username, "
                "CAST('2024-06-15' AS DATE) AS literal_date "
                "FROM t_users "
                "ORDER BY id",
                tags=["cast", "string_to_date"],
                desc="CAST string literal to DATE",
            ),
            self._seed(
                "SELECT id, name, "
                "CAST(COALESCE(release_date, '2000-01-01') AS VARCHAR(20)) AS date_str "
                "FROM t_products "
                "ORDER BY id",
                tags=["cast", "coalesce_date"],
                desc="CAST COALESCE(date) to VARCHAR",
            ),
            self._seed(
                "SELECT id, username, "
                "CAST(STRFTIME('%Y-%m-%d', birth_date) AS VARCHAR(20)) AS formatted_date "
                "FROM t_users "
                "WHERE birth_date IS NOT NULL "
                "ORDER BY id",
                tags=["cast", "strftime_to_varchar"],
                desc="CAST STRFTIME result to VARCHAR",
            ),
        ]

    # ────────────────────────────────────────────────────
    # 8. Date ordering with NULL (~8)
    # ────────────────────────────────────────────────────
    def _date_ordering_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "ORDER BY birth_date, id",
                tags=["ordering", "null_first"],
                desc="ORDER BY date with NULLs interspersed",
            ),
            self._seed(
                "SELECT id, name, release_date "
                "FROM t_products "
                "ORDER BY release_date DESC, id",
                tags=["ordering", "desc_null"],
                desc="ORDER BY date DESC with NULLs",
            ),
            self._seed(
                "SELECT id, name, hire_date "
                "FROM t_employees "
                "ORDER BY hire_date, id",
                tags=["ordering", "hire_null"],
                desc="ORDER BY hire_date with NULLs",
            ),
            self._seed(
                "SELECT id, name, hire_date "
                "FROM t_employees "
                "ORDER BY hire_date DESC, id",
                tags=["ordering", "hire_desc"],
                desc="ORDER BY hire_date DESC",
            ),
            self._seed(
                "SELECT id, metric_name, measurement_date, metric_value "
                "FROM t_metrics "
                "ORDER BY measurement_date DESC, id",
                tags=["ordering", "metric_desc"],
                desc="ORDER BY measurement_date DESC with NULLs",
            ),
            self._seed(
                "SELECT id, event_type, event_date "
                "FROM t_events "
                "ORDER BY event_date, id",
                tags=["ordering", "event_null"],
                desc="ORDER BY event_date with NULLs",
            ),
            self._seed(
                "SELECT id, username, birth_date "
                "FROM t_users "
                "WHERE birth_date IS NULL "
                "ORDER BY id",
                tags=["ordering", "only_null"],
                desc="SELECT rows where date IS NULL",
            ),
            self._seed(
                "SELECT id, username, birth_date, "
                "CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END AS is_null_flag "
                "FROM t_users "
                "ORDER BY is_null_flag, birth_date, id",
                tags=["ordering", "null_flag"],
                desc="ORDER BY NULL flag then date",
            ),
        ]
