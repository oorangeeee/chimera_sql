"""空字符串 vs NULL 语义差异种子模板。

Oracle 将 VARCHAR2 列的空字符串 '' 视为 NULL，
而 SQLite 严格区分 '' 和 NULL。
本模板生成 ~80 条种子 SQL 覆盖这一关键方言差异。
"""

from __future__ import annotations

from typing import List

from ..base import SeedTemplate, SchemaMetadata, SeedSQL


# 目标表及其可空字符串列（排除 id / 数值 / 日期列）
_NULL_STRING_TABLES: dict[str, list[str]] = {
    "t_users": ["email", "initials"],
    "t_employees": ["bio", "status"],
    "t_departments": ["location"],
    "t_events": [],  # event_type is NOT NULL, but useful for comparison queries
}

# 所有参与查询的表
_ALL_TABLES = list(_NULL_STRING_TABLES.keys())


class NullSemanticsTemplate(SeedTemplate):
    """生成测试空字符串 vs NULL 语义差异的种子 SQL。"""

    category_prefix = "dialect"
    domain = "null_semantics"
    description = "空字符串vs NULL语义差异"

    # ── 公开接口 ────────────────────────────────────

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._is_null_with_empty_string())
        seeds.extend(self._count_with_empty_null())
        seeds.extend(self._coalesce_with_empty_string())
        seeds.extend(self._string_concat_with_empty())
        seeds.extend(self._like_with_empty_null())
        seeds.extend(self._group_by_with_empty_string())
        seeds.extend(self._distinct_with_empty_null())
        seeds.extend(self._subquery_with_empty_null())
        seeds.extend(self._comparison_with_empty_string())
        return seeds

    # ── 1. IS NULL with empty string columns (~15) ──

    def _is_null_with_empty_string(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "is_null_empty"

        # t_users.email — core test
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email IS NULL ORDER BY id",
            tags=[tag],
            desc="Oracle: '' 等同 NULL; SQLite: '' 与 NULL 不同",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email = '' ORDER BY id",
            tags=[tag],
            desc="Oracle: 永远为空结果 ('' = NULL → unknown); SQLite: 返回空字符串行",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email IS NOT NULL ORDER BY id",
            tags=[tag],
            desc="Oracle: 排除 '' 和 NULL; SQLite: 仅排除 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email IS NOT NULL AND email != '' ORDER BY id",
            tags=[tag],
            desc="两方言一致: 排除 NULL 和空字符串行",
        ))

        # t_users.initials — CHAR(3) 列
        seeds.append(self._seed(
            "SELECT id, initials FROM t_users WHERE initials IS NULL ORDER BY id",
            tags=[tag],
            desc="CHAR 列 IS NULL 测试",
        ))
        seeds.append(self._seed(
            "SELECT id, initials FROM t_users WHERE initials = '' ORDER BY id",
            tags=[tag],
            desc="CHAR 列 = '' 测试: Oracle 不可能匹配, SQLite 可能",
        ))

        # t_employees.bio
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio IS NULL ORDER BY id",
            tags=[tag],
            desc="bio 列 IS NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio = '' ORDER BY id",
            tags=[tag],
            desc="bio 列 = '' Oracle 空结果 vs SQLite 可能非空",
        ))
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio IS NOT NULL ORDER BY id",
            tags=[tag],
            desc="bio 列 IS NOT NULL",
        ))

        # t_employees.status
        seeds.append(self._seed(
            "SELECT id, status FROM t_employees WHERE status IS NULL ORDER BY id",
            tags=[tag],
            desc="status 列 IS NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, status FROM t_employees WHERE status = '' ORDER BY id",
            tags=[tag],
            desc="status 列 = '' Oracle 空结果 vs SQLite 可能非空",
        ))

        # t_departments.location
        seeds.append(self._seed(
            "SELECT id, name, location FROM t_departments WHERE location IS NULL ORDER BY id",
            tags=[tag],
            desc="location 列 IS NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, name, location FROM t_departments WHERE location = '' ORDER BY id",
            tags=[tag],
            desc="location 列 = ''",
        ))

        # 组合条件
        seeds.append(self._seed(
            "SELECT id, email, initials FROM t_users "
            "WHERE email IS NULL AND initials IS NULL ORDER BY id",
            tags=[tag],
            desc="多列同时 IS NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, email, initials FROM t_users "
            "WHERE email IS NULL OR initials IS NULL ORDER BY id",
            tags=[tag],
            desc="任一列为 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users "
            "WHERE COALESCE(email, '') = '' ORDER BY id",
            tags=[tag],
            desc="COALESCE 后比较空字符串: Oracle 中 COALESCE(NULL,'')='' → NULL",
        ))

        return seeds

    # ── 2. COUNT with empty/NULL (~10) ──

    def _count_with_empty_null(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "count_empty_null"

        # COUNT(col) vs COUNT(*) 对比
        seeds.append(self._seed(
            "SELECT COUNT(email) AS cnt_email, COUNT(*) AS cnt_all FROM t_users",
            tags=[tag],
            desc="COUNT(email) 跳过 NULL; Oracle 中 '' 也被跳过",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(bio) AS cnt_bio, COUNT(*) AS cnt_all FROM t_employees",
            tags=[tag],
            desc="COUNT(bio) vs COUNT(*)",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(status) AS cnt_status, COUNT(*) AS cnt_all FROM t_employees",
            tags=[tag],
            desc="COUNT(status) vs COUNT(*)",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(location) AS cnt_location, COUNT(*) AS cnt_all FROM t_departments",
            tags=[tag],
            desc="COUNT(location) vs COUNT(*)",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(initials) AS cnt_initials, COUNT(*) AS cnt_all FROM t_users",
            tags=[tag],
            desc="COUNT(initials) vs COUNT(*)",
        ))

        # 多列 COUNT 对比
        seeds.append(self._seed(
            "SELECT COUNT(email) AS cnt_email, "
            "COUNT(initials) AS cnt_initials, "
            "COUNT(*) AS cnt_all FROM t_users",
            tags=[tag],
            desc="多列 COUNT 对比 NULL 行数差异",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(bio) AS cnt_bio, "
            "COUNT(status) AS cnt_status, "
            "COUNT(*) AS cnt_all FROM t_employees",
            tags=[tag],
            desc="多列 COUNT 对比",
        ))

        # 条件 COUNT
        seeds.append(self._seed(
            "SELECT COUNT(CASE WHEN email IS NULL THEN 1 END) AS null_email_cnt, "
            "COUNT(CASE WHEN email = '' THEN 1 END) AS empty_email_cnt, "
            "COUNT(*) AS total FROM t_users",
            tags=[tag],
            desc="条件 COUNT 区分 NULL 与 '' — Oracle 中 empty_email_cnt 应为 0",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(CASE WHEN bio IS NULL THEN 1 END) AS null_bio_cnt, "
            "COUNT(CASE WHEN bio = '' THEN 1 END) AS empty_bio_cnt, "
            "COUNT(*) AS total FROM t_employees",
            tags=[tag],
            desc="bio 列条件 COUNT 区分 NULL 与 ''",
        ))
        seeds.append(self._seed(
            "SELECT COUNT(*) AS total, "
            "COUNT(email) AS has_email, "
            "COUNT(*) - COUNT(email) AS null_email_diff FROM t_users",
            tags=[tag],
            desc="用差值计算 NULL 行数",
        ))

        return seeds

    # ── 3. COALESCE with empty string (~10) ──

    def _coalesce_with_empty_string(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "coalesce_empty"

        seeds.append(self._seed(
            "SELECT id, COALESCE(email, 'N/A') AS email_display "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="COALESCE 默认值 — Oracle: '' 也替换为 N/A",
        ))
        seeds.append(self._seed(
            "SELECT id, COALESCE(bio, 'No bio') AS bio_display "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="COALESCE(bio, 'No bio')",
        ))
        seeds.append(self._seed(
            "SELECT id, COALESCE(status, 'UNKNOWN') AS status_display "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="COALESCE(status, 'UNKNOWN')",
        ))
        seeds.append(self._seed(
            "SELECT id, COALESCE(location, 'N/A') AS location_display "
            "FROM t_departments ORDER BY id",
            tags=[tag],
            desc="COALESCE(location, 'N/A')",
        ))
        seeds.append(self._seed(
            "SELECT id, COALESCE(initials, 'N/A') AS initials_display "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="COALESCE(initials, 'N/A')",
        ))

        # CASE WHEN 区分 '' 和 NULL
        seeds.append(self._seed(
            "SELECT id, "
            "CASE WHEN email = '' THEN 'empty' "
            "WHEN email IS NULL THEN 'null' "
            "ELSE email END AS email_type "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="CASE 区分 '' 与 NULL — Oracle: 永远不会走 'empty' 分支",
        ))
        seeds.append(self._seed(
            "SELECT id, "
            "CASE WHEN bio = '' THEN 'empty' "
            "WHEN bio IS NULL THEN 'null' "
            "ELSE 'has_value' END AS bio_type "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="bio 列 CASE 区分 '' 与 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, "
            "CASE WHEN COALESCE(email, '') = '' THEN 'blank' "
            "ELSE 'filled' END AS email_state "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="COALESCE 再比较 '' — Oracle 中 COALESCE(NULL,'') 返回 NULL",
        ))

        # 多层 COALESCE
        seeds.append(self._seed(
            "SELECT id, COALESCE(email, initials, 'none') AS first_non_null "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="多层 COALESCE 取第一个非 NULL 值",
        ))
        seeds.append(self._seed(
            "SELECT e.id, COALESCE(e.status, e.bio, 'unknown') AS first_val "
            "FROM t_employees e ORDER BY e.id",
            tags=[tag],
            desc="跨列 COALESCE",
        ))

        return seeds

    # ── 4. String concatenation with '' (~10) ──

    def _string_concat_with_empty(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "concat_empty"

        # || 在 Oracle 中任一操作数为 NULL 则整体为 NULL
        seeds.append(self._seed(
            "SELECT id, username || email AS combined "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="|| 拼接: Oracle 中 email 若为 ''/NULL 则整体为 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, username || COALESCE(email, '') AS combined "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="COALESCE(email,'') || username — Oracle 中仍可能为 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, name || COALESCE(bio, '') AS combined "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="name || COALESCE(bio,'') — 拼接空字符串列",
        ))
        seeds.append(self._seed(
            "SELECT id, COALESCE(email, 'no-email') || '@test' AS suffixed "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="COALESCE 后拼接常量 — 确保非 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, username || ' <' || COALESCE(email, 'N/A') || '>' AS formatted "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="多段拼接: 格式化 username <email>",
        ))
        seeds.append(self._seed(
            "SELECT id, name || ' - ' || COALESCE(status, 'N/A') AS name_status "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="多段拼接 name - status",
        ))
        seeds.append(self._seed(
            "SELECT id, name || ' @ ' || COALESCE(location, 'N/A') AS name_loc "
            "FROM t_departments ORDER BY id",
            tags=[tag],
            desc="部门名 + 地点拼接",
        ))
        seeds.append(self._seed(
            "SELECT id, "
            "CASE WHEN email IS NULL THEN username "
            "ELSE username || ' (' || email || ')' END AS display "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="CASE 保护拼接: 避免 NULL 传播",
        ))
        seeds.append(self._seed(
            "SELECT id, "
            "COALESCE(initials, '') || ':' || COALESCE(email, '') AS compact "
            "FROM t_users ORDER BY id",
            tags=[tag],
            desc="双列 COALESCE 拼接 — Oracle 中 COALESCE(NULL,'') 仍为 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, "
            "COALESCE(status || ' - ' || bio, status, bio, 'N/A') AS info "
            "FROM t_employees ORDER BY id",
            tags=[tag],
            desc="COALESCE 包裹拼接表达式作为回退",
        ))

        return seeds

    # ── 5. LIKE with empty/NULL (~8) ──

    def _like_with_empty_null(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "like_empty_null"

        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email LIKE '%' ORDER BY id",
            tags=[tag],
            desc="LIKE '%': Oracle 中不匹配 NULL/空字符串行; SQLite 匹配所有非 NULL",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email NOT LIKE '%@%' ORDER BY id",
            tags=[tag],
            desc="NOT LIKE '%@%': 筛选不含 @ 的行 (含 ''/NULL)",
        ))
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio LIKE '%' ORDER BY id",
            tags=[tag],
            desc="bio LIKE '%'",
        ))
        seeds.append(self._seed(
            "SELECT id, status FROM t_employees WHERE status LIKE '%' ORDER BY id",
            tags=[tag],
            desc="status LIKE '%'",
        ))
        seeds.append(self._seed(
            "SELECT id, location FROM t_departments WHERE location LIKE '%' ORDER BY id",
            tags=[tag],
            desc="location LIKE '%'",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email LIKE '' ORDER BY id",
            tags=[tag],
            desc="LIKE '': Oracle 中 '' = NULL → 匹配 NULL(应为空); SQLite 中匹配空字符串行",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users "
            "WHERE email IS NOT NULL AND email NOT LIKE '%' ORDER BY id",
            tags=[tag],
            desc="IS NOT NULL AND NOT LIKE '%' — 测试空字符串是否匹配 LIKE",
        ))
        seeds.append(self._seed(
            "SELECT id, initials FROM t_users WHERE initials LIKE '%' ORDER BY id",
            tags=[tag],
            desc="initials LIKE '%' — CHAR 列",
        ))

        return seeds

    # ── 6. GROUP BY with empty string (~8) ──

    def _group_by_with_empty_string(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "groupby_empty"

        seeds.append(self._seed(
            "SELECT email, COUNT(*) AS cnt FROM t_users GROUP BY email ORDER BY email",
            tags=[tag],
            desc="GROUP BY email: Oracle 中 '' 和 NULL 合并为一组; SQLite 分为两组",
        ))
        seeds.append(self._seed(
            "SELECT bio, COUNT(*) AS cnt FROM t_employees GROUP BY bio ORDER BY bio",
            tags=[tag],
            desc="GROUP BY bio",
        ))
        seeds.append(self._seed(
            "SELECT status, COUNT(*) AS cnt FROM t_employees GROUP BY status ORDER BY status",
            tags=[tag],
            desc="GROUP BY status",
        ))
        seeds.append(self._seed(
            "SELECT location, COUNT(*) AS cnt FROM t_departments GROUP BY location ORDER BY location",
            tags=[tag],
            desc="GROUP BY location",
        ))
        seeds.append(self._seed(
            "SELECT status, bio, COUNT(*) AS cnt "
            "FROM t_employees GROUP BY status, bio ORDER BY status, bio",
            tags=[tag],
            desc="多列 GROUP BY",
        ))
        seeds.append(self._seed(
            "SELECT email, initials, COUNT(*) AS cnt "
            "FROM t_users GROUP BY email, initials ORDER BY email, initials",
            tags=[tag],
            desc="t_users 多列 GROUP BY",
        ))
        seeds.append(self._seed(
            "SELECT COALESCE(email, 'NULL') AS email_group, COUNT(*) AS cnt "
            "FROM t_users GROUP BY COALESCE(email, 'NULL') ORDER BY email_group",
            tags=[tag],
            desc="COALESCE 后 GROUP BY — 合并 NULL 和 '' 到同一组",
        ))
        seeds.append(self._seed(
            "SELECT CASE WHEN email IS NULL THEN 'NULL' "
            "WHEN email = '' THEN 'EMPTY' "
            "ELSE 'HAS_VALUE' END AS email_state, "
            "COUNT(*) AS cnt "
            "FROM t_users "
            "GROUP BY CASE WHEN email IS NULL THEN 'NULL' "
            "WHEN email = '' THEN 'EMPTY' "
            "ELSE 'HAS_VALUE' END "
            "ORDER BY email_state",
            tags=[tag],
            desc="CASE 分组: Oracle 不区分 EMPTY 和 NULL",
        ))

        return seeds

    # ── 7. DISTINCT with empty/NULL (~5) ──

    def _distinct_with_empty_null(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "distinct_empty_null"

        seeds.append(self._seed(
            "SELECT DISTINCT email FROM t_users ORDER BY email",
            tags=[tag],
            desc="DISTINCT email: Oracle 中 '' 和 NULL 视为相同; SQLite 视为不同",
        ))
        seeds.append(self._seed(
            "SELECT DISTINCT bio FROM t_employees ORDER BY bio",
            tags=[tag],
            desc="DISTINCT bio",
        ))
        seeds.append(self._seed(
            "SELECT DISTINCT status FROM t_employees ORDER BY status",
            tags=[tag],
            desc="DISTINCT status",
        ))
        seeds.append(self._seed(
            "SELECT DISTINCT location FROM t_departments ORDER BY location",
            tags=[tag],
            desc="DISTINCT location",
        ))
        seeds.append(self._seed(
            "SELECT DISTINCT initials FROM t_users ORDER BY initials",
            tags=[tag],
            desc="DISTINCT initials",
        ))

        return seeds

    # ── 8. Subquery with empty/NULL (~6) ──

    def _subquery_with_empty_null(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "subquery_empty_null"

        seeds.append(self._seed(
            "SELECT id, username FROM t_users "
            "WHERE email NOT IN "
            "(SELECT email FROM t_users WHERE email IS NOT NULL) "
            "ORDER BY id",
            tags=[tag],
            desc="NOT IN 非 NULL email — Oracle: 包含 '' 行; SQLite: 不包含 '' 行",
        ))
        seeds.append(self._seed(
            "SELECT id, name FROM t_employees "
            "WHERE bio NOT IN "
            "(SELECT bio FROM t_employees WHERE bio IS NOT NULL) "
            "ORDER BY id",
            tags=[tag],
            desc="NOT IN 非 NULL bio",
        ))
        seeds.append(self._seed(
            "SELECT id, username FROM t_users "
            "WHERE email IN "
            "(SELECT email FROM t_users WHERE email = '') "
            "ORDER BY id",
            tags=[tag],
            desc="IN 子查询筛选 email = '' — Oracle: 空结果; SQLite: 可能返回行",
        ))
        seeds.append(self._seed(
            "SELECT id, name FROM t_employees "
            "WHERE status IN "
            "(SELECT status FROM t_employees WHERE status = '') "
            "ORDER BY id",
            tags=[tag],
            desc="IN 子查询筛选 status = ''",
        ))
        # 关联子查询
        seeds.append(self._seed(
            "SELECT u.id, u.username "
            "FROM t_users u "
            "WHERE EXISTS "
            "(SELECT 1 FROM t_users u2 WHERE u2.email = u.email AND u2.id != u.id) "
            "ORDER BY u.id",
            tags=[tag],
            desc="EXISTS 关联子查询: 相同 email 的不同用户",
        ))
        seeds.append(self._seed(
            "SELECT u.id, u.username, "
            "(SELECT COUNT(*) FROM t_employees e WHERE e.status = u.email) AS match_cnt "
            "FROM t_users u ORDER BY u.id",
            tags=[tag],
            desc="标量子查询: 比较 status 和 email 列的值",
        ))

        return seeds

    # ── 9. Comparison with empty string (~8) ──

    def _comparison_with_empty_string(self) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        tag = "comparison_empty"

        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email > '' ORDER BY id",
            tags=[tag],
            desc="email > '' : Oracle 中 '' = NULL → 永远 unknown; SQLite 返回非空行",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email < 'z' ORDER BY id",
            tags=[tag],
            desc="email < 'z'",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users WHERE email >= '' ORDER BY id",
            tags=[tag],
            desc="email >= '' : Oracle 空结果; SQLite 返回含空字符串行",
        ))
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio > '' ORDER BY id",
            tags=[tag],
            desc="bio > ''",
        ))
        seeds.append(self._seed(
            "SELECT id, bio FROM t_employees WHERE bio < 'z' ORDER BY id",
            tags=[tag],
            desc="bio < 'z'",
        ))
        seeds.append(self._seed(
            "SELECT id, status FROM t_employees WHERE status > '' ORDER BY id",
            tags=[tag],
            desc="status > ''",
        ))
        seeds.append(self._seed(
            "SELECT id, location FROM t_departments WHERE location > '' ORDER BY id",
            tags=[tag],
            desc="location > ''",
        ))
        seeds.append(self._seed(
            "SELECT id, email FROM t_users "
            "WHERE email != '' AND email IS NOT NULL ORDER BY id",
            tags=[tag],
            desc="email != '' AND IS NOT NULL — Oracle 中 != '' 对 NULL 为 unknown, "
                 "SQLite 中排除空字符串",
        ))

        return seeds
