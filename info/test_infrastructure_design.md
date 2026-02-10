# 测试基础设施设计详解

> 本文档详细记录 ChimeraSQL 测试数据库初始化流水线的设计思路、表结构、数据规划和种子 SQL 设计，供毕业论文撰写参考。

---

## 1. 总体设计思路

### 1.1 为什么需要测试基础设施

差分测试（Differential Testing）的前提是：**两个被测数据库必须在相同的数据集上执行相同语义的 SQL，才能对比结果的一致性**。这要求：

1. **统一的表结构** — Oracle 和 SQLite 必须拥有语义等价的表
2. **相同的测试数据** — 两端数据完全一致，消除数据差异带来的噪声
3. **高质量的种子 SQL** — 覆盖 SQL 语言各维度特性，为后续 AST 变异提供充分的变异空间

### 1.2 参考方案：SQLancer 的三阶段模型

本项目参考了 SQLancer（ESEC/FSE 2020）和 SQLsmith 等成熟工具的初始化策略：

```
阶段1: SchemaInitializer (DDL)
   → 在两种数据库中创建结构等价的测试表

阶段2: DataPopulator (DML)
   → 插入覆盖边界条件的测试数据

阶段3: SeedGenerator (种子SQL文件)
   → 生成覆盖 SQL 各特性的种子查询
```

与 SQLancer 的区别在于：SQLancer 动态随机生成 schema 和数据，而本项目采用**静态预定义**策略。原因是：
- 本项目的核心创新点在 **AST 变异 + 跨方言转译**，而非 schema 随机化
- 静态数据使实验**可复现**（Reproducibility），更适合学术研究
- 低行数策略（每表 15–20 行）参考了 SQLancer 的经验——行数过多会导致 JOIN 笛卡尔积超时

### 1.3 不使用 SQLGlot 转译 DDL 的原因

虽然本项目使用 SQLGlot 做 DQL（查询语句）的跨方言转译，但 DDL 的生成选择了**手动类型映射**方案。原因：
- SQLGlot 对 DDL 转译的支持不够稳定（已知问题：DEFAULT 子句、CHECK 约束等在部分方言间转译有 Bug）
- DDL 数量有限（仅 5 张表），手动映射的开发成本可控
- 类型映射表（`_TYPE_MAP`）使差异点清晰可见，有利于论文中的方言差异分析

---

## 2. 测试表结构设计

### 2.1 设计目标

5 张表的设计需覆盖以下 SQL 测试维度：

| 测试维度 | 对应表 | 说明 |
|---------|--------|------|
| 基础 CRUD | t_users, t_products | 字符串、数值、时间戳的基本操作 |
| JOIN 操作 | t_orders (FK→t_users, t_products) | INNER/LEFT JOIN、多表连接 |
| 聚合与分组 | t_products (category), t_orders (status) | GROUP BY、HAVING、聚合函数 |
| 窗口函数 | t_metrics (user_id 分区) | ROW_NUMBER、RANK、SUM OVER 等 |
| 集合操作 | t_tags (多态关联) | UNION、INTERSECT、EXCEPT |
| NULL 处理 | 所有表的可空列 | IS NULL、COALESCE、聚合中的 NULL |
| 布尔语义 | t_users.active, t_products.discontinued | 0/1 映射 |
| 浮点精度 | t_users.score, t_metrics.metric_value | DECIMAL 精度对比 |

### 2.2 表结构详细定义

#### t_users — 核心用户实体表

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| username | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | 字符串操作、DISTINCT |
| email | VARCHAR(100) | VARCHAR2(100) | TEXT | YES | — | NULL 测试、空串测试 |
| age | INTEGER | NUMBER(10) | INTEGER | YES | — | 边界值(0, -1, 999, NULL) |
| score | DECIMAL(10,2) | NUMBER(10,2) | REAL | YES | — | 浮点精度对比 |
| active | INTEGER | NUMBER(10) | INTEGER | NO | 1 | 布尔语义(0/1) |
| created_at | TIMESTAMP | TIMESTAMP | TEXT | YES | — | 时间日期 |

#### t_products — 商品实体表

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| name | VARCHAR(100) | VARCHAR2(100) | TEXT | NO | — | 字符串函数(LIKE等) |
| category | VARCHAR(50) | VARCHAR2(50) | TEXT | YES | — | GROUP BY、DISTINCT |
| price | DECIMAL(10,2) | NUMBER(10,2) | REAL | NO | — | 算术运算、聚合 |
| stock | INTEGER | NUMBER(10) | INTEGER | YES | — | NULL、边界值 |
| discontinued | INTEGER | NUMBER(10) | INTEGER | NO | 0 | 布尔语义(0/1) |

#### t_orders — 订单关联表

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| user_id | INTEGER FK→t_users | NUMBER(10) | INTEGER | NO | — | JOIN 操作 |
| product_id | INTEGER FK→t_products | NUMBER(10) | INTEGER | NO | — | JOIN 操作 |
| quantity | INTEGER | NUMBER(10) | INTEGER | NO | — | 算术运算 |
| total_price | DECIMAL(10,2) | NUMBER(10,2) | REAL | NO | — | 聚合、精度 |
| order_date | TIMESTAMP | TIMESTAMP | TEXT | YES | — | 日期过滤 |
| status | VARCHAR(20) | VARCHAR2(20) | TEXT | YES | — | CASE WHEN、GROUP BY |

**索引**: idx_orders_user(user_id), idx_orders_product(product_id), idx_orders_status(status)

#### t_metrics — 指标表（窗口函数专用）

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| user_id | INTEGER FK→t_users | NUMBER(10) | INTEGER | NO | — | PARTITION BY |
| metric_name | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | 分组键 |
| metric_value | DECIMAL(15,5) | NUMBER(15,5) | REAL | YES | — | 高精度测试 |
| recorded_at | TIMESTAMP | TIMESTAMP | TEXT | YES | — | ORDER BY |

#### t_tags — 标签表（集合操作专用）

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| entity_type | VARCHAR(20) | VARCHAR2(20) | TEXT | NO | — | 'user'/'product' 区分 |
| entity_id | INTEGER | NUMBER(10) | INTEGER | NO | — | 多态外键 |
| tag | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | UNION/INTERSECT/EXCEPT |

**索引**: idx_tags_entity(entity_type, entity_id)

### 2.3 类型映射策略

由于 Oracle 和 SQLite 的类型系统差异显著，采用以下映射：

| 通用类型 | Oracle | SQLite | 差异说明 |
|---------|--------|--------|---------|
| INTEGER | NUMBER(10) | INTEGER | Oracle 无原生 INTEGER，用 NUMBER 模拟 |
| VARCHAR(n) | VARCHAR2(n) | TEXT | SQLite 不强制长度限制 |
| DECIMAL(p,s) | NUMBER(p,s) | REAL | SQLite REAL 为 IEEE 754 双精度浮点，精度损失是差分测试关键点 |
| TIMESTAMP | TIMESTAMP | TEXT | SQLite 无原生时间戳类型，以 ISO 8601 字符串存储 |

### 2.4 方言差异处理

#### DROP TABLE
- **Oracle**: 不支持 `IF EXISTS`，使用 PL/SQL 匿名块：
  ```sql
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE t_xxx CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
  END;
  ```
- **SQLite**: 直接使用 `DROP TABLE IF EXISTS t_xxx`

#### 外键约束
- **Oracle**: CREATE TABLE 中的 FOREIGN KEY 原生支持
- **SQLite**: 需要在连接后执行 `PRAGMA foreign_keys = ON` 才能启用外键检查

#### 删除顺序
反序删除（t_tags → t_metrics → t_orders → t_products → t_users），先删子表再删父表，避免外键约束冲突。

---

## 3. 测试数据设计

### 3.1 数据规模与策略

| 表 | 行数 | 策略 |
|----|------|------|
| t_users | 15 | 覆盖所有列的正常值、NULL、边界值 |
| t_products | 15 | 覆盖价格边界(0.01~9999.99)、NULL stock |
| t_orders | 18 | 覆盖所有 user/product 引用、状态覆盖 |
| t_metrics | 16 | 覆盖极小值(0.00001)、极大值(99999.99999)、NULL |
| t_tags | 18 | user/product 标签有交叉（INTERSECT 测试关键） |
| **合计** | **82** | — |

### 3.2 边界值覆盖矩阵

以下矩阵展示了每张表中故意植入的边界条件：

#### t_users 边界值

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| email = NULL | id=3 (charlie), id=7 (grace), id=12 (linda) | NULL 判断、COALESCE |
| email = '' (空串) | id=5 (eve), id=10 (judy), id=15 (oscar) | **Oracle 视空串为 NULL** — 差分测试核心点 |
| age = 0 | id=3, id=12 | 零值边界 |
| age = -1 | id=4, id=14 | 负数边界 |
| age = 999 | id=5 | 大数边界 |
| age = NULL | id=7, id=15 | NULL 在算术/排序中的行为 |
| score = NULL | id=4, id=11, id=15 | 聚合函数中 NULL 的处理 |
| score = 0.00 | id=6 | 零值浮点 |
| score = -10.50 | id=7 | 负数浮点 |
| score = 0.01 | id=14 | 极小正浮点 |
| active = 0 | id=4, id=7, id=10, id=13 | 布尔假值过滤 |
| created_at = NULL | id=5, id=12, id=15 | 时间戳缺失 |

#### t_products 边界值

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| price = 0.01 | id=4, id=13 | 极小价格 |
| price = 9999.99 | id=3 | 大数价格 |
| category = NULL | id=4, id=9, id=14 | NULL 分组 |
| stock = 0 | id=2, id=9, id=14 | 零库存 |
| stock = NULL | id=4, id=8, id=15 | NULL 库存 |
| stock = 999 | id=11 | 大数库存 |
| discontinued = 1 | id=4, id=8, id=11 | 布尔真值 |

#### t_orders 边界值

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| status = NULL | id=7, id=13 | NULL 状态 |
| status 全覆盖 | delivered/shipped/pending/cancelled | CASE WHEN、GROUP BY |
| order_date = NULL | id=6, id=11, id=17 | 时间戳缺失 |
| total_price = 0.02 | id=13 | 极小金额 |
| total_price = 9999.99 | id=3 | 大金额 |
| total_price = 0.10 | id=10 | 小数精度 |
| 同一用户多订单 | user_id=1(3笔), user_id=5(2笔) | 聚合、GROUP BY |

#### t_metrics 边界值

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| metric_value = 0.00001 | id=5 | 五位小数精度 |
| metric_value = 99999.99999 | id=6 | 大数+高精度 |
| metric_value = 0.00000 | id=9 | 零值高精度 |
| metric_value = -5.55555 | id=11 | 负数高精度 |
| metric_value = NULL | id=4, id=7, id=15 | NULL 在窗口函数中的行为 |
| recorded_at = NULL | id=7, id=13 | 窗口 ORDER BY 中的 NULL |

#### t_tags 数据交叉设计

| tag | user 端 | product 端 | 集合操作测试 |
|-----|---------|-----------|------------|
| vip | user 1, 5 | product 1, 12 | INTERSECT 可发现 |
| active | user 1, 2, 9 | product 7 | INTERSECT 可发现 |
| new | user 3, 13 | — | EXCEPT (user - product) |
| inactive | user 7, 10 | — | EXCEPT (user - product) |
| popular | — | product 1, 5 | EXCEPT (product - user) |
| premium | — | product 3, 10 | EXCEPT (product - user) |

### 3.3 Oracle 空串 = NULL 问题

这是差分测试中最重要的行为差异之一：

```
Oracle:  '' IS NULL → TRUE  (Oracle 将空字符串视为 NULL)
SQLite:  '' IS NULL → FALSE (SQLite 区分空串和 NULL)
```

t_users 中 email 列有 3 行为空字符串 `''`（id=5, 10, 15），专门用于触发这个差异。在后续差分分析器（Analyzer）中需要特殊处理。

### 3.4 参数绑定差异

| 方面 | Oracle (oracledb) | SQLite (sqlite3) |
|------|-------------------|------------------|
| 占位符格式 | `:1, :2, :3, ...` | `?, ?, ?, ...` |
| 时间戳绑定 | 需要 `datetime` 对象 | 接受 `datetime` 对象（自动转 str 存入 TEXT 列） |
| NULL 绑定 | Python `None` → Oracle `NULL` | Python `None` → SQLite `NULL` |

---

## 4. 种子 SQL 设计

### 4.1 设计原则

1. **确定性 ORDER BY** — 每条种子必须带 ORDER BY 子句，保证跨数据库结果集顺序一致，使差分对比有意义
2. **通用 SQL 方言** — 不使用任何数据库特有语法（如 Oracle 的 NVL、ROWNUM、FETCH FIRST；SQLite 的 typeof()），后续由 transpiler 模块负责方言转换
3. **仅引用 5 张测试表** — 与 SchemaInitializer 绑定
4. **故意查询含 NULL 列** — NULL 处理是 Oracle/SQLite 差异的重灾区
5. **不含 RIGHT JOIN** — SQLite 3.39.0 之前不支持 RIGHT JOIN，改用等价的 LEFT JOIN

### 4.2 种子分类与覆盖范围

#### 01_basic_select（10 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| select_all_users.sql | SELECT 全列 | 基线查询，验证全表读取一致性 |
| where_equality.sql | WHERE = | 等值过滤 |
| where_in.sql | WHERE IN (...) | 集合成员测试 |
| where_between.sql | WHERE BETWEEN | 范围过滤（含边界值行为） |
| where_like.sql | WHERE LIKE | 模式匹配 |
| distinct_category.sql | SELECT DISTINCT | 去重（含 NULL 过滤） |
| order_by_desc.sql | ORDER BY DESC | 降序排序（NULL 排序位置差异） |
| limit_offset.sql | LIMIT ... OFFSET | 分页（转译为 Oracle 的 FETCH FIRST/OFFSET） |
| multiple_conditions.sql | AND 多条件 | 复合条件 |
| or_condition.sql | OR 条件 | 析取条件 |

#### 02_aggregation（7 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| count_all.sql | COUNT(*) | 全行计数 |
| count_non_null.sql | COUNT(column) | 非 NULL 计数（验证 NULL 跳过行为） |
| sum_avg.sql | SUM, AVG | 数值聚合（精度对比） |
| min_max.sql | MIN, MAX | 极值 |
| group_by_category.sql | GROUP BY + AVG | 分组聚合 |
| group_by_status.sql | GROUP BY + SUM | 分组聚合（含 NULL 分组键） |
| having_filter.sql | HAVING COUNT(*) > n | 聚合后过滤 |

#### 03_join（6 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| inner_join.sql | 三表 INNER JOIN | 基础内连接 |
| left_join_users_orders.sql | LEFT JOIN | 左连接（含不匹配行 → NULL 填充） |
| left_join_null_check.sql | LEFT JOIN + IS NULL | 反连接模式（找无订单用户） |
| self_join_users.sql | 自连接 | 同表 JOIN（age 相等的用户对） |
| join_with_aggregation.sql | LEFT JOIN + GROUP BY | 连接后聚合 |
| multi_table_join.sql | 三表 JOIN + WHERE | 连接 + 过滤 |

#### 04_subquery（6 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| scalar_subquery.sql | SELECT 中的标量子查询 | 子查询返回单值 |
| in_subquery.sql | WHERE IN (子查询) | 非相关子查询 |
| not_in_subquery.sql | WHERE NOT IN (子查询) | NOT IN 的 NULL 陷阱 |
| exists_subquery.sql | WHERE EXISTS | 存在性子查询 |
| derived_table.sql | FROM (子查询) | 派生表/内联视图 |
| correlated_subquery.sql | 相关子查询 | WHERE 中引用外层表列 |

#### 05_set_operations（4 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| union.sql | UNION | 去重合并 |
| union_all.sql | UNION ALL | 保留重复合并 |
| intersect.sql | INTERSECT | 交集（user 和 product 共有 tag） |
| except.sql | EXCEPT | 差集（user 独有 tag） |

#### 06_window_functions（6 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| row_number.sql | ROW_NUMBER() OVER | 行号（PARTITION BY user_id） |
| rank.sql | RANK() OVER | 排名（含并列） |
| dense_rank.sql | DENSE_RANK() OVER | 密集排名 |
| sum_over.sql | SUM() OVER ... ORDER BY | 累计和 |
| avg_over.sql | AVG() OVER (PARTITION BY) | 分区平均 |
| count_over.sql | COUNT(*) OVER (PARTITION BY) | 分区计数 |

#### 07_null_handling（6 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| is_null.sql | IS NULL | NULL 判断 |
| is_not_null.sql | IS NOT NULL | 非 NULL 判断 |
| coalesce.sql | COALESCE(col, default) | NULL 替代 |
| null_in_aggregation.sql | COUNT(*) vs COUNT(col) | 聚合中 NULL 计数差异 |
| null_in_sort.sql | ORDER BY 含 NULL 列 | NULL 排序位置差异 |
| coalesce_in_comparison.sql | COALESCE 在 WHERE 中 | NULL 安全比较 |

#### 08_expressions（7 个种子）

| 文件 | SQL 特性 | 测试目的 |
|------|---------|---------|
| arithmetic.sql | +, -, *, / | 基础算术（除法精度） |
| case_expression.sql | CASE WHEN ... END | 条件表达式 |
| case_with_null.sql | CASE WHEN col IS NULL | NULL 分支 |
| cast_integer.sql | CAST(col AS INTEGER) | 类型转换 |
| nested_expression.sql | score * 2 + COALESCE(age, 0) | 嵌套表达式 |
| arithmetic_with_null.sql | age + score (含 NULL) | NULL 传播 |
| complex_case.sql | JOIN + CASE | 复合查询 |

### 4.3 种子文件格式

每个 `.sql` 文件包含**单条** SQL 语句（不含分号），便于后续 AST 解析器直接读取：

```
data/seeds/
├── 01_basic_select/       # 10 files
├── 02_aggregation/        # 7 files
├── 03_join/               # 6 files
├── 04_subquery/           # 6 files
├── 05_set_operations/     # 4 files
├── 06_window_functions/   # 6 files
├── 07_null_handling/      # 6 files
└── 08_expressions/        # 7 files
合计: 52 个种子文件
```

### 4.4 种子在模糊测试流水线中的角色

```
种子 SQL (.sql) → sqlglot.parse_one() → AST → 变异策略(Strategy) → 变异后 AST
     ↓                                                                    ↓
  SeedGenerator 生成                                            MutatorContext 处理
                                                                          ↓
                                              sqlglot.transpile() → Oracle SQL + SQLite SQL
                                                                          ↓
                                              OracleConnector.execute() ←→ SQLiteConnector.execute()
                                                                          ↓
                                                              DifferentialAnalyzer 对比结果
```

种子的质量直接决定变异的有效性：
- **多样的 SQL 结构** → 变异后能覆盖更多代码路径
- **含 NULL 的查询** → 变异后更容易触发 Oracle/SQLite 的行为差异
- **确定性 ORDER BY** → 保证变异后的查询结果仍可对比

---

## 5. 跨数据库差异总结

以下是测试基础设施设计中重点考虑的 Oracle vs SQLite 行为差异：

| 差异类别 | Oracle 行为 | SQLite 行为 | 影响的测试数据/种子 |
|---------|------------|------------|-------------------|
| 空串处理 | `'' IS NULL → TRUE` | `'' IS NULL → FALSE` | t_users.email 空串行 |
| 类型系统 | 强类型 NUMBER(p,s) | 弱类型亲和性 | DECIMAL 列的精度对比 |
| NULL 排序 | NULL 默认排最后(ASC) | NULL 默认排最前(ASC) | null_in_sort.sql |
| LIMIT 语法 | 需转译为 FETCH FIRST/OFFSET | 原生支持 LIMIT/OFFSET | limit_offset.sql |
| BOOLEAN | 无原生 BOOLEAN，用 NUMBER | 无原生 BOOLEAN，用 INTEGER | active/discontinued 列 |
| TIMESTAMP | 原生 TIMESTAMP 类型 | TEXT 存储 ISO 8601 字符串 | created_at/order_date/recorded_at |
| 外键约束 | 默认启用 | 需 PRAGMA foreign_keys=ON | SchemaInitializer 处理 |
| DDL IF EXISTS | 不支持 | 支持 | DROP TABLE 阶段 |
| EXCEPT | 使用 MINUS 关键字 | 使用 EXCEPT 关键字 | except.sql（需转译） |

---

## 6. 实现统计

| 指标 | 数值 |
|------|------|
| 测试表数量 | 5 |
| 总数据行数 | 82 |
| 种子 SQL 类别 | 8 |
| 种子 SQL 文件数 | 52 |
| 覆盖的 SQL 特性 | SELECT/WHERE/JOIN/子查询/集合操作/窗口函数/NULL处理/表达式 |
| 类型映射条目 | 4 (INTEGER/VARCHAR/DECIMAL/TIMESTAMP × 2 方言) |
| 索引数量 | 4 (3个单列 + 1个复合索引) |
| 外键关系 | 4 (t_orders→t_users, t_orders→t_products, t_metrics→t_users, t_tags 多态) |
