"""混合类型运算方言差异模板 — 测试 SQLite/Oracle 类型系统差异。

覆盖差异点：
- INT + REAL 运算（隐式类型提升）
- 字符串 + 数字（隐式转换）
- 日期 + 整数（日期运算差异）
- 跨类型比较
- 隐式类型转换行为
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class MixedTypesTemplate(SeedTemplate):
    """混合类型运算方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "mixed_types"

    @property
    def description(self) -> str:
        return "混合类型运算方言差异测试（INT+REAL/字符串+数字/日期运算/隐式转换）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._int_real())
        seeds.extend(self._string_number())
        seeds.extend(self._date_arithmetic())
        seeds.extend(self._implicit_cast())
        return seeds

    # ── INT + REAL 运算 (~8) ─────────────────────────────
    def _int_real(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, age + score AS age_plus_score, "
                "age * 1.5 AS age_scaled "
                "FROM t_users WHERE age IS NOT NULL AND score IS NOT NULL ORDER BY id",
                tags=["int_real", "age_score_arith"],
                desc="INT + INT / INT * REAL — 类型提升",
            ),
            self._seed(
                "SELECT id, name, price * stock AS total_value, "
                "price / 3 AS third_price "
                "FROM t_products WHERE price IS NOT NULL AND stock IS NOT NULL ORDER BY id",
                tags=["int_real", "price_stock_calc"],
                desc="DECIMAL * INTEGER / DECIMAL 除法",
            ),
            self._seed(
                "SELECT id, name, salary + 0.5 AS salary_plus_half, "
                "salary / 12.0 AS monthly "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["int_real", "salary_calc"],
                desc="DECIMAL + REAL / DECIMAL 除法",
            ),
            self._seed(
                "SELECT id, amount + 0.01 AS amount_plus, "
                "amount * 100 AS cents "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["int_real", "amount_calc"],
                desc="DECIMAL + REAL / DECIMAL * INT",
            ),
            self._seed(
                "SELECT id, CAST(age AS REAL) + score AS real_sum, "
                "age + CAST(score AS REAL) AS real_sum2 "
                "FROM t_users WHERE age IS NOT NULL AND score IS NOT NULL ORDER BY id",
                tags=["int_real", "explicit_real_cast"],
                desc="CAST(REAL) + INT — 显式类型转换",
            ),
            self._seed(
                "SELECT id, ROUND(price, 0) + stock AS rounded_plus_stock "
                "FROM t_products WHERE price IS NOT NULL AND stock IS NOT NULL ORDER BY id",
                tags=["int_real", "round_plus_int"],
                desc="ROUND(DECIMAL) + INT — 四舍五入后加法",
            ),
            self._seed(
                "SELECT id, budget / COUNT(*) OVER () AS avg_per_dept "
                "FROM t_departments WHERE budget IS NOT NULL ORDER BY id",
                tags=["int_real", "budget_window_div"],
                desc="DECIMAL / 窗口函数 COUNT — 除法精度",
            ),
            self._seed(
                "SELECT id, salary - AVG(salary) OVER () AS salary_deviation "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["int_real", "salary_window_deviation"],
                desc="DECIMAL - 窗口函数 AVG — 偏差计算",
            ),
        ]

    # ── 字符串 + 数字 (~7) ───────────────────────────────
    def _string_number(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, CAST(score AS VARCHAR(10)) || '/100' AS score_label "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["str_num", "cast_concat_score"],
                desc="CAST + 拼接 — 数字转字符串后拼接",
            ),
            self._seed(
                "SELECT id, name, '$' || CAST(price AS VARCHAR(20)) AS price_display "
                "FROM t_products WHERE price IS NOT NULL ORDER BY id",
                tags=["str_num", "price_display"],
                desc="字符串 + CAST — 价格显示格式",
            ),
            self._seed(
                "SELECT id, 'EMP-' || CAST(id AS VARCHAR(10)) AS emp_code, name "
                "FROM t_employees ORDER BY id",
                tags=["str_num", "emp_code"],
                desc="字符串 + CAST(id) — 生成编号",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN CAST(age AS VARCHAR(10)) = '25' THEN 'exact_25' ELSE 'other' END AS age_check "
                "FROM t_users WHERE age IS NOT NULL ORDER BY id",
                tags=["str_num", "cast_compare_str"],
                desc="CAST + 字符串比较 — 数字转字符串后比较",
            ),
            self._seed(
                "SELECT id, name, "
                "'Salary: ' || CAST(ROUND(salary, 0) AS VARCHAR(20)) AS salary_info "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["str_num", "salary_info"],
                desc="ROUND + CAST + 拼接 — 薪资信息",
            ),
            self._seed(
                "SELECT id, event_type, "
                "'Event #' || CAST(id AS VARCHAR(10)) || ': ' || event_type AS event_desc "
                "FROM t_events ORDER BY id",
                tags=["str_num", "event_desc"],
                desc="多段拼接 — 事件描述",
            ),
            self._seed(
                "SELECT id, amount, "
                "CAST(amount AS VARCHAR(20)) || ' (' || tx_type || ')' AS tx_display "
                "FROM t_transactions WHERE amount IS NOT NULL AND tx_type IS NOT NULL ORDER BY id",
                tags=["str_num", "tx_display"],
                desc="CAST + 多段拼接 — 交易显示",
            ),
        ]

    # ── 日期运算 (~8) ──────────────────────────────────
    def _date_arithmetic(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, hire_date, "
                "hire_date + 30 AS plus_30_days "
                "FROM t_employees WHERE hire_date IS NOT NULL ORDER BY id",
                tags=["date_arith", "hire_plus_30"],
                desc="日期 + 整数 — 30天后",
            ),
            self._seed(
                "SELECT id, name, hire_date, "
                "DATE(hire_date, '+1 year') AS one_year_later "
                "FROM t_employees WHERE hire_date IS NOT NULL ORDER BY id",
                tags=["date_arith", "hire_plus_year"],
                desc="DATE(col, '+1 year') — 一年后",
            ),
            self._seed(
                "SELECT id, username, "
                "DATE('now') - DATE(created_at) AS days_since_creation "
                "FROM t_users WHERE created_at IS NOT NULL ORDER BY id",
                tags=["date_arith", "days_since"],
                desc="日期相减 — 天数差",
            ),
            self._seed(
                "SELECT id, event_date, "
                "DATE(event_date, '-7 days') AS week_before "
                "FROM t_events WHERE event_date IS NOT NULL ORDER BY id",
                tags=["date_arith", "event_week_before"],
                desc="DATE(col, '-7 days') — 一周前",
            ),
            self._seed(
                "SELECT id, name, hire_date, "
                "CASE WHEN hire_date >= DATE('2023-01-01') THEN 'new' "
                "WHEN hire_date >= DATE('2020-01-01') THEN 'mid' "
                "ELSE 'old' END AS tenure_group "
                "FROM t_employees WHERE hire_date IS NOT NULL ORDER BY id",
                tags=["date_arith", "hire_tenure_group"],
                desc="日期 + CASE — 入职时间分组",
            ),
            self._seed(
                "SELECT id, event_date, "
                "DATE(event_date, 'start of month') AS month_start "
                "FROM t_events WHERE event_date IS NOT NULL ORDER BY id",
                tags=["date_arith", "month_start"],
                desc="DATE(col, 'start of month') — 月初",
            ),
            self._seed(
                "SELECT id, created_at, "
                "DATE(created_at, '+6 months') AS six_months_later "
                "FROM t_orders WHERE created_at IS NOT NULL ORDER BY id",
                tags=["date_arith", "order_plus_6m"],
                desc="DATE + '+6 months' — 六个月后",
            ),
            self._seed(
                "SELECT id, name, hire_date, "
                "CAST(ROUND((JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25) AS INTEGER) AS years_of_service "
                "FROM t_employees WHERE hire_date IS NOT NULL ORDER BY years_of_service DESC",
                tags=["date_arith", "years_of_service"],
                desc="JULIANDAY 差值 — 服务年限",
            ),
        ]

    # ── 隐式类型转换 (~7) ────────────────────────────────
    def _implicit_cast(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users WHERE score > '80' ORDER BY id",
                tags=["implicit_cast", "compare_str_num"],
                desc="score > '80' — 字符串与数字比较（隐式转换）",
            ),
            self._seed(
                "SELECT id, name FROM t_products WHERE price > '50' ORDER BY id",
                tags=["implicit_cast", "price_str_compare"],
                desc="price > '50' — 隐式类型转换",
            ),
            self._seed(
                "SELECT id, name FROM t_employees WHERE salary > '80000' ORDER BY id",
                tags=["implicit_cast", "salary_str_compare"],
                desc="salary > '80000' — 隐式类型转换",
            ),
            self._seed(
                "SELECT id, username, score + 0.5 AS score_adjusted "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["implicit_cast", "int_plus_real"],
                desc="INT + 0.5 — 隐式 REAL 提升",
            ),
            self._seed(
                "SELECT id, name, price + 0 AS price_int_safe "
                "FROM t_products WHERE price IS NOT NULL ORDER BY id",
                tags=["implicit_cast", "plus_zero"],
                desc="DECIMAL + 0 — 类型保持测试",
            ),
            self._seed(
                "SELECT id, amount, amount + 0.0 AS amount_real "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["implicit_cast", "amount_plus_zero_real"],
                desc="DECIMAL + 0.0 — 类型提升测试",
            ),
            self._seed(
                "SELECT id, username, "
                "COALESCE(score, 0) + COALESCE(age, 0) * 1.0 AS weighted "
                "FROM t_users ORDER BY id",
                tags=["implicit_cast", "mixed_coalesce"],
                desc="COALESCE(INT) + COALESCE(INT) * REAL — 混合运算",
            ),
        ]
