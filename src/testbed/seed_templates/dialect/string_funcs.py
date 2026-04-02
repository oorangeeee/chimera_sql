"""字符串函数方言差异模板 — GROUP_CONCAT/LISTAGG、SUBSTR 负索引、INSTR、
REPLACE 嵌套、TRIM 方向、LIKE 大小写、CONCAT/||、UPPER/LOWER。"""

from __future__ import annotations

from typing import List

from ..base import SeedSQL, SeedTemplate


class StringFuncsTemplate(SeedTemplate):
    """生成测试 SQLite/Oracle 字符串函数差异的种子 SQL。"""

    category_prefix = "dialect"
    domain = "string_funcs"
    description = "字符串函数方言差异"

    # ------------------------------------------------------------------
    # 公开入口
    # ------------------------------------------------------------------
    def generate(self, schema) -> List[SeedSQL]:  # noqa: D401
        """生成 ~80 条字符串函数方言差异种子 SQL。"""
        seeds: List[SeedSQL] = []
        seeds.extend(self._group_concat())
        seeds.extend(self._substr_negative())
        seeds.extend(self._instr())
        seeds.extend(self._replace())
        seeds.extend(self._trim())
        seeds.extend(self._like_case())
        seeds.extend(self._concat_pipe())
        seeds.extend(self._upper_lower())
        return seeds

    # ==================================================================
    # 1. GROUP_CONCAT (~15)
    # ==================================================================
    def _group_concat(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT entity_type, GROUP_CONCAT(tag, ', ') AS tags "
                "FROM t_tags GROUP BY entity_type ORDER BY entity_type",
                tags=["group_concat", "aggregate", "separator"],
                desc="GROUP_CONCAT 基本用法，逗号分隔符",
            ),
            self._seed(
                "SELECT entity_type, GROUP_CONCAT(tag) AS tags "
                "FROM t_tags GROUP BY entity_type ORDER BY entity_type",
                tags=["group_concat", "aggregate", "no_separator"],
                desc="GROUP_CONCAT 不指定分隔符（默认逗号）",
            ),
            self._seed(
                "SELECT entity_type, GROUP_CONCAT(DISTINCT tag, '|') AS tags "
                "FROM t_tags GROUP BY entity_type ORDER BY entity_type",
                tags=["group_concat", "distinct", "separator"],
                desc="GROUP_CONCAT DISTINCT 去重，竖线分隔",
            ),
            self._seed(
                "SELECT u.id, u.username, "
                "GROUP_CONCAT(t.tag, ', ') AS user_tags "
                "FROM t_users u LEFT JOIN t_tags t "
                "ON t.entity_type = 'user' AND t.entity_id = u.id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["group_concat", "join", "left_join"],
                desc="GROUP_CONCAT 通过 LEFT JOIN 关联用户标签",
            ),
            self._seed(
                "SELECT entity_type, GROUP_CONCAT(tag, '-') AS tags "
                "FROM t_tags WHERE entity_type = 'user' "
                "GROUP BY entity_type ORDER BY entity_type",
                tags=["group_concat", "where_filter", "separator"],
                desc="GROUP_CONCAT 带 WHERE 过滤",
            ),
            self._seed(
                "SELECT p.category, GROUP_CONCAT(p.name, '; ') AS products "
                "FROM t_products p WHERE p.category IS NOT NULL "
                "GROUP BY p.category ORDER BY p.category",
                tags=["group_concat", "product_name", "category"],
                desc="GROUP_CONCAT 产品名按分类聚合",
            ),
            self._seed(
                "SELECT o.status, GROUP_CONCAT(CAST(o.id AS TEXT), ',') AS order_ids "
                "FROM t_orders o GROUP BY o.status ORDER BY o.status",
                tags=["group_concat", "cast", "order_ids"],
                desc="GROUP_CONCAT 整数列需 CAST 为文本",
            ),
            self._seed(
                "SELECT e.dept_id, GROUP_CONCAT(e.name, ', ') AS employees "
                "FROM t_employees e WHERE e.dept_id IS NOT NULL "
                "GROUP BY e.dept_id ORDER BY e.dept_id",
                tags=["group_concat", "employee_name", "department"],
                desc="GROUP_CONCAT 员工名按部门聚合",
            ),
            self._seed(
                "SELECT entity_type, GROUP_CONCAT(tag, ', ') AS tags "
                "FROM t_tags "
                "GROUP BY entity_type "
                "HAVING COUNT(*) > 2 "
                "ORDER BY entity_type",
                tags=["group_concat", "having", "aggregate"],
                desc="GROUP_CONCAT 带 HAVING 过滤",
            ),
            self._seed(
                "SELECT d.name AS dept_name, "
                "GROUP_CONCAT(e.name, ', ') AS members "
                "FROM t_departments d "
                "LEFT JOIN t_employees e ON e.dept_id = d.id "
                "GROUP BY d.id, d.name ORDER BY d.id",
                tags=["group_concat", "department", "left_join"],
                desc="GROUP_CONCAT 员工名按部门名聚合（含空部门）",
            ),
            self._seed(
                "SELECT event_type, GROUP_CONCAT(CAST(id AS TEXT), ',') AS event_ids "
                "FROM t_events WHERE event_type != '' "
                "GROUP BY event_type ORDER BY event_type",
                tags=["group_concat", "event_type", "cast"],
                desc="GROUP_CONCAT 事件 ID 按事件类型聚合",
            ),
            self._seed(
                "SELECT entity_type, "
                "GROUP_CONCAT(tag, ' -> ') AS chain "
                "FROM t_tags WHERE entity_type = 'product' "
                "GROUP BY entity_type ORDER BY entity_type",
                tags=["group_concat", "custom_separator", "arrow"],
                desc="GROUP_CONCAT 自定义箭头分隔符",
            ),
            self._seed(
                "SELECT e.status, "
                "GROUP_CONCAT(e.name || '(' || COALESCE(CAST(e.salary AS TEXT), 'N/A') || ')', ', ') AS info "
                "FROM t_employees e "
                "GROUP BY e.status ORDER BY e.status",
                tags=["group_concat", "concat_inside", "coalesce"],
                desc="GROUP_CONCAT 内部拼接复杂字符串",
            ),
            self._seed(
                "SELECT u.id, u.username, "
                "(SELECT GROUP_CONCAT(t2.tag, ',') "
                " FROM t_tags t2 WHERE t2.entity_type = 'user' AND t2.entity_id = u.id) AS tags "
                "FROM t_users u ORDER BY u.id",
                tags=["group_concat", "subquery", "correlated"],
                desc="GROUP_CONCAT 在相关子查询中",
            ),
            self._seed(
                "SELECT p.id, p.name, "
                "(SELECT GROUP_CONCAT(t3.tag, ',') "
                " FROM t_tags t3 WHERE t3.entity_type = 'product' AND t3.entity_id = p.id) AS product_tags "
                "FROM t_products p ORDER BY p.id",
                tags=["group_concat", "subquery", "product_tags"],
                desc="GROUP_CONCAT 相关子查询获取产品标签",
            ),
        ]

    # ==================================================================
    # 2. SUBSTR 负索引 (~10)
    # ==================================================================
    def _substr_negative(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, SUBSTR(username, -3) AS last3 "
                "FROM t_users ORDER BY id",
                tags=["substr", "negative_index"],
                desc="SUBSTR 负索引取最后 3 个字符",
            ),
            self._seed(
                "SELECT id, username, SUBSTR(username, -2, 2) AS last2 "
                "FROM t_users ORDER BY id",
                tags=["substr", "negative_index", "length"],
                desc="SUBSTR 负索引 + 长度参数",
            ),
            self._seed(
                "SELECT id, username, SUBSTR(username, -1) AS last_char "
                "FROM t_users ORDER BY id",
                tags=["substr", "negative_index", "single_char"],
                desc="SUBSTR 负索引取最后 1 个字符",
            ),
            self._seed(
                "SELECT id, name, SUBSTR(name, 1, 3) AS first3 "
                "FROM t_products ORDER BY id",
                tags=["substr", "positive_index", "prefix"],
                desc="SUBSTR 正索引取前 3 个字符",
            ),
            self._seed(
                "SELECT id, name, SUBSTR(name, -1) AS last_char "
                "FROM t_products ORDER BY id",
                tags=["substr", "negative_index", "product"],
                desc="SUBSTR 负索引取产品名最后一个字符",
            ),
            self._seed(
                "SELECT id, email, SUBSTR(email, INSTR(email, '@') + 1) AS domain "
                "FROM t_users WHERE email IS NOT NULL AND email != '' "
                "ORDER BY id",
                tags=["substr", "instr", "combined"],
                desc="SUBSTR + INSTR 提取邮箱域名部分",
            ),
            self._seed(
                "SELECT id, name, SUBSTR(name, -5) AS suffix "
                "FROM t_products WHERE LENGTH(name) >= 5 ORDER BY id",
                tags=["substr", "negative_index", "length_guard"],
                desc="SUBSTR 负索引取产品名后 5 个字符（带长度守卫）",
            ),
            self._seed(
                "SELECT id, tag, SUBSTR(tag, 1, 3) AS tag_prefix "
                "FROM t_tags ORDER BY id",
                tags=["substr", "positive_index", "tag"],
                desc="SUBSTR 取标签前缀",
            ),
            self._seed(
                "SELECT id, status, SUBSTR(status, -3) AS status_suffix "
                "FROM t_employees WHERE status IS NOT NULL ORDER BY id",
                tags=["substr", "negative_index", "status"],
                desc="SUBSTR 负索引取员工状态后缀",
            ),
            self._seed(
                "SELECT id, event_type, SUBSTR(event_type, 1, 4) AS prefix4, "
                "SUBSTR(event_type, -4) AS suffix4 "
                "FROM t_events WHERE event_type != '' ORDER BY id",
                tags=["substr", "positive_negative", "combined"],
                desc="SUBSTR 同时取事件类型前缀和后缀",
            ),
        ]

    # ==================================================================
    # 3. INSTR (~10)
    # ==================================================================
    def _instr(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, INSTR(username, 'a') AS pos "
                "FROM t_users ORDER BY id",
                tags=["instr", "basic"],
                desc="INSTR 查找字符 a 在用户名中的位置",
            ),
            self._seed(
                "SELECT id, name, INSTR(name, ' ') AS space_pos "
                "FROM t_products WHERE name LIKE '% %' ORDER BY id",
                tags=["instr", "space", "where"],
                desc="INSTR 查找产品名中空格位置",
            ),
            self._seed(
                "SELECT id, email, INSTR(email, '@') AS at_pos "
                "FROM t_users WHERE email IS NOT NULL ORDER BY id",
                tags=["instr", "at_sign", "email"],
                desc="INSTR 查找邮箱中 @ 符号位置",
            ),
            self._seed(
                "SELECT id, username, INSTR(username, 'e') AS e_pos "
                "FROM t_users ORDER BY id",
                tags=["instr", "char_search"],
                desc="INSTR 查找字符 e 在用户名中的位置",
            ),
            self._seed(
                "SELECT id, name, INSTR(LOWER(name), 'oo') AS oo_pos "
                "FROM t_products ORDER BY id",
                tags=["instr", "case_insensitive", "multi_char"],
                desc="INSTR 在 LOWER 后查找多字符子串",
            ),
            self._seed(
                "SELECT id, bio, INSTR(COALESCE(bio, ''), 'engineer') AS eng_pos "
                "FROM t_employees WHERE bio IS NOT NULL ORDER BY id",
                tags=["instr", "coalesce", "word_search"],
                desc="INSTR 在 COALESCE 处理后的 bio 中查找单词",
            ),
            self._seed(
                "SELECT id, tag, INSTR(tag, 'vip') AS is_vip "
                "FROM t_tags ORDER BY id",
                tags=["instr", "tag", "substring"],
                desc="INSTR 查找标签中 vip 子串位置",
            ),
            self._seed(
                "SELECT id, location, INSTR(location, ' ') AS space_pos "
                "FROM t_departments WHERE location IS NOT NULL "
                "AND location != '' ORDER BY id",
                tags=["instr", "location", "space"],
                desc="INSTR 查找部门地址中空格位置",
            ),
            self._seed(
                "SELECT id, event_type, "
                "INSTR(event_type, '_') AS underscore_pos "
                "FROM t_events WHERE event_type LIKE '%_%' "
                "ORDER BY id",
                tags=["instr", "underscore", "event"],
                desc="INSTR 查找事件类型中下划线位置",
            ),
            self._seed(
                "SELECT id, username, "
                "INSTR(username, 'a') AS first_a, "
                "INSTR(username, 'a', INSTR(username, 'a') + 1) AS second_a "
                "FROM t_users ORDER BY id",
                tags=["instr", "occurrence", "advanced"],
                desc="INSTR 查找第 2 次出现的位置（三参数用法）",
            ),
        ]

    # ==================================================================
    # 4. REPLACE (~8)
    # ==================================================================
    def _replace(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, REPLACE(username, 'a', 'X') AS replaced "
                "FROM t_users ORDER BY id",
                tags=["replace", "basic", "char_replace"],
                desc="REPLACE 替换用户名中的 a 为 X",
            ),
            self._seed(
                "SELECT id, name, REPLACE(name, 'Widget', 'Item') AS renamed "
                "FROM t_products ORDER BY id",
                tags=["replace", "word_replace", "product"],
                desc="REPLACE 替换产品名中的 Widget 为 Item",
            ),
            self._seed(
                "SELECT id, name, "
                "REPLACE(REPLACE(name, 'Widget', 'W'), 'Gadget', 'G') AS abbreviated "
                "FROM t_products ORDER BY id",
                tags=["replace", "nested", "abbreviation"],
                desc="嵌套 REPLACE 缩写产品名",
            ),
            self._seed(
                "SELECT id, username, "
                "REPLACE(REPLACE(username, 'a', 'A'), 'e', 'E') AS upper_vowels "
                "FROM t_users ORDER BY id",
                tags=["replace", "nested", "vowels"],
                desc="嵌套 REPLACE 大写元音字母",
            ),
            self._seed(
                "SELECT id, email, REPLACE(email, '@', ' [at] ') AS safe_email "
                "FROM t_users WHERE email IS NOT NULL AND email != '' "
                "ORDER BY id",
                tags=["replace", "email", "obfuscation"],
                desc="REPLACE 隐藏邮箱 @ 符号",
            ),
            self._seed(
                "SELECT id, location, "
                "REPLACE(location, 'Building', 'Bldg') AS short_loc "
                "FROM t_departments WHERE location IS NOT NULL "
                "ORDER BY id",
                tags=["replace", "location", "abbreviation"],
                desc="REPLACE 缩写部门地址 Building",
            ),
            self._seed(
                "SELECT id, bio, "
                "REPLACE(COALESCE(bio, ''), ' ', '_') AS bio_underscore "
                "FROM t_employees ORDER BY id",
                tags=["replace", "coalesce", "space_to_underscore"],
                desc="REPLACE 将 bio 中空格替换为下划线",
            ),
            self._seed(
                "SELECT id, status, "
                "REPLACE(REPLACE(REPLACE(status, 'active', 'ACT'), "
                "'inactive', 'INA'), 'on_leave', 'LV') AS short_status "
                "FROM t_employees WHERE status IS NOT NULL ORDER BY id",
                tags=["replace", "triple_nested", "status"],
                desc="三重嵌套 REPLACE 缩写员工状态",
            ),
        ]

    # ==================================================================
    # 5. TRIM (~8)
    # ==================================================================
    def _trim(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, TRIM(username) AS trimmed "
                "FROM t_users ORDER BY id",
                tags=["trim", "basic"],
                desc="TRIM 去除用户名两端空白",
            ),
            self._seed(
                "SELECT id, LTRIM(username) AS ltrimmed "
                "FROM t_users ORDER BY id",
                tags=["trim", "leading", "ltrim"],
                desc="LTRIM 去除用户名左侧空白",
            ),
            self._seed(
                "SELECT id, RTRIM(username) AS rtrimmed "
                "FROM t_users ORDER BY id",
                tags=["trim", "trailing", "rtrim"],
                desc="RTRIM 去除用户名右侧空白",
            ),
            self._seed(
                "SELECT id, TRIM(initials) AS trimmed "
                "FROM t_users WHERE initials IS NOT NULL ORDER BY id",
                tags=["trim", "initials", "char_type"],
                desc="TRIM 处理 CHAR 类型缩写字段（Oracle 空格填充）",
            ),
            self._seed(
                "SELECT id, TRIM(LEADING 'a' FROM username) AS trimmed_lead "
                "FROM t_users ORDER BY id",
                tags=["trim", "leading", "specific_char"],
                desc="TRIM LEADING 指定字符（SQLite 语法差异）",
            ),
            self._seed(
                "SELECT id, TRIM(TRAILING FROM COALESCE(initials, '')) AS trimmed_trail "
                "FROM t_users ORDER BY id",
                tags=["trim", "trailing", "coalesce"],
                desc="TRIM TRAILING 去除 COALESCE 后的尾部空白",
            ),
            self._seed(
                "SELECT id, LTRIM(RTRIM(name)) AS fully_trimmed "
                "FROM t_products ORDER BY id",
                tags=["trim", "ltrim_rtrim", "combined"],
                desc="LTRIM + RTRIM 组合等同于 TRIM",
            ),
            self._seed(
                "SELECT id, TRIM(BOTH FROM COALESCE(status, 'unknown')) AS clean_status "
                "FROM t_employees ORDER BY id",
                tags=["trim", "both", "coalesce"],
                desc="TRIM BOTH 处理 NULL 值的 COALESCE 结果",
            ),
        ]

    # ==================================================================
    # 6. LIKE 大小写 (~8)
    # ==================================================================
    def _like_case(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE username LIKE 'A%' ORDER BY id",
                tags=["like", "case_sensitive", "uppercase"],
                desc="LIKE 大写 A 开头（SQLite 大小写不敏感，Oracle 敏感）",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE username LIKE '%A%' ORDER BY id",
                tags=["like", "case_sensitive", "contains"],
                desc="LIKE 包含大写 A（大小写行为差异）",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE name LIKE 'w%' ORDER BY id",
                tags=["like", "case_sensitive", "lowercase"],
                desc="LIKE 小写 w 开头（SQLite 不敏感可能匹配 Widget）",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE UPPER(name) LIKE 'W%' ORDER BY id",
                tags=["like", "upper", "case_insensitive"],
                desc="UPPER + LIKE 实现大小写不敏感匹配",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE LOWER(username) LIKE '%e%' ORDER BY id",
                tags=["like", "lower", "case_insensitive"],
                desc="LOWER + LIKE 实现大小写不敏感包含匹配",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE name LIKE 'Gadget%' ORDER BY id",
                tags=["like", "case_sensitive", "prefix"],
                desc="LIKE 精确大小写前缀匹配 Gadget",
            ),
            self._seed(
                "SELECT id, status FROM t_employees "
                "WHERE LOWER(status) LIKE 'active' ORDER BY id",
                tags=["like", "exact_lower", "status"],
                desc="LOWER + LIKE 精确匹配状态值 active",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events "
                "WHERE event_type LIKE '_o%' ORDER BY id",
                tags=["like", "wildcard", "underscore"],
                desc="LIKE 下划线通配符（第二字符为 o）",
            ),
        ]

    # ==================================================================
    # 7. CONCAT / || 操作符 (~10)
    # ==================================================================
    def _concat_pipe(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username || ' (' || COALESCE(email, 'N/A') || ')' AS contact "
                "FROM t_users ORDER BY id",
                tags=["concat", "pipe", "coalesce"],
                desc="|| 拼接用户名与 COALESCE 处理的邮箱",
            ),
            self._seed(
                "SELECT id, name || ': ' || CAST(price AS VARCHAR(10)) AS label "
                "FROM t_products ORDER BY id",
                tags=["concat", "pipe", "cast"],
                desc="|| 拼接产品名与 CAST 价格",
            ),
            self._seed(
                "SELECT id, COALESCE(email, 'N/A') || ' - ' || username AS email_user "
                "FROM t_users ORDER BY id",
                tags=["concat", "pipe", "null_handling"],
                desc="|| 拼接含 NULL 的 COALESCE 结果",
            ),
            self._seed(
                "SELECT id, name || COALESCE(category, 'none') AS name_cat "
                "FROM t_products ORDER BY id",
                tags=["concat", "pipe", "null_category"],
                desc="|| 拼接产品名与可为 NULL 的分类",
            ),
            self._seed(
                "SELECT id, "
                "'[' || UPPER(username) || ']' AS bracketed "
                "FROM t_users ORDER BY id",
                tags=["concat", "pipe", "upper"],
                desc="|| 拼接方括号包裹的大写用户名",
            ),
            self._seed(
                "SELECT id, "
                "first_name || ' ' || last_name AS full_name FROM ("
                "SELECT id, SUBSTR(username, 1, 1) AS first_name, "
                "COALESCE(SUBSTR(username, 2), '') AS last_name "
                "FROM t_users) ORDER BY id",
                tags=["concat", "pipe", "subquery", "substr"],
                desc="|| 在子查询中拼接拆分的用户名",
            ),
            self._seed(
                "SELECT id, name || ' (' || COALESCE(category, 'uncategorized') || ')' "
                "|| ' - $' || CAST(price AS VARCHAR(10)) AS product_info "
                "FROM t_products ORDER BY id",
                tags=["concat", "pipe", "complex"],
                desc="|| 多段拼接复杂产品信息字符串",
            ),
            self._seed(
                "SELECT id, e.name || ' [' || d.name || ']' AS emp_dept "
                "FROM t_employees e "
                "LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["concat", "pipe", "join"],
                desc="|| 拼接员工名与部门名（含 NULL 部门）",
            ),
            self._seed(
                "SELECT id, "
                "COALESCE(initials, SUBSTR(username, 1, 1)) || '.' AS short_name "
                "FROM t_users ORDER BY id",
                tags=["concat", "pipe", "coalesce", "substr"],
                desc="|| 拼接缩写或首字母加点号",
            ),
            self._seed(
                "SELECT id, event_type || '#' || CAST(id AS VARCHAR(5)) AS event_code "
                "FROM t_events WHERE event_type IS NOT NULL "
                "AND event_type != '' ORDER BY id",
                tags=["concat", "pipe", "cast", "event"],
                desc="|| 拼接事件类型与 ID 生成事件代码",
            ),
        ]

    # ==================================================================
    # 8. UPPER / LOWER (~6)
    # ==================================================================
    def _upper_lower(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, UPPER(username) AS up, LOWER(username) AS low "
                "FROM t_users ORDER BY id",
                tags=["upper", "lower", "basic"],
                desc="UPPER 和 LOWER 同时转换用户名",
            ),
            self._seed(
                "SELECT id, UPPER(name) AS up, LOWER(name) AS low "
                "FROM t_products ORDER BY id",
                tags=["upper", "lower", "product"],
                desc="UPPER 和 LOWER 同时转换产品名",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE LOWER(username) = 'alice' ORDER BY id",
                tags=["lower", "case_insensitive", "comparison"],
                desc="LOWER 实现大小写不敏感精确匹配",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE UPPER(category) = 'ELECTRONICS' ORDER BY id",
                tags=["upper", "case_insensitive", "category"],
                desc="UPPER 实现分类的大小写不敏感匹配",
            ),
            self._seed(
                "SELECT id, UPPER(SUBSTR(username, 1, 1)) || LOWER(SUBSTR(username, 2)) AS capitalized "
                "FROM t_users ORDER BY id",
                tags=["upper", "lower", "substr", "combined"],
                desc="UPPER 首字母 + LOWER 其余实现首字母大写",
            ),
            self._seed(
                "SELECT id, UPPER(event_type) AS up_event "
                "FROM t_events WHERE event_type IS NOT NULL "
                "AND event_type != '' ORDER BY id",
                tags=["upper", "event_type"],
                desc="UPPER 转换事件类型为大写",
            ),
        ]
