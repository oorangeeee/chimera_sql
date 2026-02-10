# 测试基础设施设计详解
> 本文档记录 ChimeraSQL 测试数据库初始化流水线的结构化设计，包含基础方案与扩展维度（递归/自关联、Unicode 字符串、JSON）的统一说明。

---

## 1. 设计目标与原则

### 1.1 设计目标

1. 统一表结构：Oracle 与 SQLite 具备语义等价的表结构。
2. 统一数据集：两端数据一致，避免数据差异导致噪声。
3. 高质量种子：覆盖多维 SQL 特性，为 AST 变异提供丰富空间。

### 1.2 设计原则

1. 可复现：采用静态 schema 与数据集，便于实验复现实验。
2. 低行数：每表 15–20 行，避免 JOIN 笛卡尔积超时。
3. 通用 SQL：种子使用通用 SQL，方言差异留给 transpiler 处理。
4. 差异驱动：针对 Oracle/SQLite 差异点做数据与种子设计。

---

## 2. 测试表结构设计

### 2.1 表结构覆盖维度

| 测试维度 | 对应表 | 说明 |
|---------|--------|------|
| 基础 CRUD | t_users, t_products | 字符串、数值、时间戳的基本操作 |
| JOIN 操作 | t_orders | INNER/LEFT JOIN、多表连接 |
| 聚合与分组 | t_products, t_orders | GROUP BY、HAVING、聚合函数 |
| 窗口函数 | t_metrics | ROW_NUMBER、RANK、SUM OVER |
| 集合操作 | t_tags | UNION、INTERSECT、EXCEPT |
| NULL 处理 | 全表 | IS NULL、COALESCE、聚合中的 NULL |
| 布尔语义 | t_users, t_products | 0/1 映射 |
| 浮点精度 | t_users, t_metrics | DECIMAL 精度对比 |
| 递归/自关联 | t_users | manager_id 自关联，递归 CTE |
| Unicode | t_users, t_products, t_tags | 多语言字符与排序 |
| JSON | t_users, t_products | JSON 存储与提取 |

### 2.2 表结构定义（基础）

#### t_users

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| username | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | 字符串操作、DISTINCT |
| email | VARCHAR(100) | VARCHAR2(100) | TEXT | YES | — | NULL 测试、空串测试 |
| age | INTEGER | NUMBER(10) | INTEGER | YES | — | 边界值 |
| score | DECIMAL(10,2) | NUMBER(10,2) | REAL | YES | — | 浮点精度 |
| active | INTEGER | NUMBER(10) | INTEGER | NO | 1 | 布尔语义 |
| created_at | TIMESTAMP | TIMESTAMP | TEXT | YES | — | 时间日期 |

#### t_products

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| name | VARCHAR(100) | VARCHAR2(100) | TEXT | NO | — | 字符串函数 |
| category | VARCHAR(50) | VARCHAR2(50) | TEXT | YES | — | GROUP BY、DISTINCT |
| price | DECIMAL(10,2) | NUMBER(10,2) | REAL | NO | — | 算术运算、聚合 |
| stock | INTEGER | NUMBER(10) | INTEGER | YES | — | NULL、边界值 |
| discontinued | INTEGER | NUMBER(10) | INTEGER | NO | 0 | 布尔语义 |

#### t_orders

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| user_id | INTEGER | NUMBER(10) | INTEGER | NO | — | JOIN 操作 |
| product_id | INTEGER | NUMBER(10) | INTEGER | NO | — | JOIN 操作 |
| quantity | INTEGER | NUMBER(10) | INTEGER | NO | — | 算术运算 |
| total_price | DECIMAL(10,2) | NUMBER(10,2) | REAL | NO | — | 聚合、精度 |
| order_date | TIMESTAMP | TIMESTAMP | TEXT | YES | — | 日期过滤 |
| status | VARCHAR(20) | VARCHAR2(20) | TEXT | YES | — | CASE WHEN、GROUP BY |

索引：idx_orders_user(user_id), idx_orders_product(product_id), idx_orders_status(status)

#### t_metrics

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| user_id | INTEGER | NUMBER(10) | INTEGER | NO | — | PARTITION BY |
| metric_name | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | 分组键 |
| metric_value | DECIMAL(15,5) | NUMBER(15,5) | REAL | YES | — | 高精度测试 |
| recorded_at | TIMESTAMP | TIMESTAMP | TEXT | YES | — | ORDER BY |

#### t_tags

| 列名 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 默认值 | 测试用途 |
|------|---------|------------|------------|------|--------|---------|
| id | INTEGER | NUMBER(10) | INTEGER | NO (PK) | — | 主键 |
| entity_type | VARCHAR(20) | VARCHAR2(20) | TEXT | NO | — | 'user'/'product' 区分 |
| entity_id | INTEGER | NUMBER(10) | INTEGER | NO | — | 多态外键 |
| tag | VARCHAR(50) | VARCHAR2(50) | TEXT | NO | — | UNION/INTERSECT/EXCEPT |

索引：idx_tags_entity(entity_type, entity_id)

### 2.3 表结构扩展（递归/Unicode/JSON）

#### t_users 新增列

| 新列 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 用途 |
|------|---------|------------|------------|------|------|
| manager_id | INTEGER | NUMBER(10) | INTEGER | YES | 自关联 FK→t_users(id)，递归 CTE/自连接 |
| profile | VARCHAR(500) | VARCHAR2(500) | TEXT | YES | JSON 格式用户配置 |

新增外键约束：FOREIGN KEY (manager_id) REFERENCES t_users(id)

#### t_products 新增列

| 新列 | 通用类型 | Oracle 类型 | SQLite 类型 | 可空 | 用途 |
|------|---------|------------|------------|------|------|
| metadata | VARCHAR(500) | VARCHAR2(500) | TEXT | YES | JSON 格式产品属性 |

---

## 3. 测试数据设计

### 3.1 数据规模与策略

| 表 | 行数 | 策略 |
|----|------|------|
| t_users | 15 | 覆盖正常值、NULL、边界值、空字符串 |
| t_products | 15 | 覆盖价格边界、NULL stock |
| t_orders | 18 | 覆盖所有引用与状态 |
| t_metrics | 16 | 覆盖极小值、极大值、NULL |
| t_tags | 18 | 交叉标签用于集合操作 |

### 3.2 边界值覆盖矩阵（摘要）

#### t_users

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| email = NULL | id=3, 7, 12 | NULL 判断、COALESCE |
| email = '' | id=5, 10, 15 | Oracle 空串差异 |
| age = 0 / -1 / 999 / NULL | 多行 | 边界值与 NULL |
| score = NULL / 0 / -10.50 / 0.01 | 多行 | 浮点精度 |
| active = 0 | id=4, 7, 10, 13 | 布尔假值 |

#### t_products

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| price = 0.01 / 9999.99 | 多行 | 极值与精度 |
| category = NULL | 多行 | NULL 分组 |
| stock = 0 / NULL / 999 | 多行 | 边界值 |
| discontinued = 1 | 多行 | 布尔真值 |

#### t_orders

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| status = NULL | id=7, 13 | NULL 状态 |
| 状态全覆盖 | delivered/shipped/pending/cancelled | CASE/GROUP BY |
| order_date = NULL | id=6, 11, 17 | 时间戳缺失 |
| total_price 极值 | id=3, 10, 13 | 金额精度 |

#### t_metrics

| 数据特征 | 示例行 | 测试目标 |
|---------|--------|---------|
| metric_value 极值 | id=5, 6, 9, 11 | 高精度 + 负数 |
| metric_value = NULL | id=4, 7, 15 | NULL 在窗口函数 |
| recorded_at = NULL | id=7, 13 | ORDER BY 中 NULL |

#### t_tags

| tag | user 端 | product 端 | 集合操作测试 |
|-----|---------|-----------|------------|
| vip | user 1, 5 | product 1, 12 | INTERSECT |
| active | user 1, 2, 9 | product 7 | INTERSECT |
| new | user 3, 13 | — | EXCEPT |
| inactive | user 7, 10 | — | EXCEPT |
| popular | — | product 1, 5 | EXCEPT |
| premium | — | product 3, 10 | EXCEPT |

### 3.3 关键差异点数据

1. Oracle 空串 = NULL：t_users.email 中 3 行空字符串用于触发差异。
2. NULL 与聚合：多个表包含 NULL 列，验证 COUNT/AVG 行为。

### 3.4 参数绑定差异

| 方面 | Oracle (oracledb) | SQLite (sqlite3) |
|------|-------------------|------------------|
| 占位符格式 | :1, :2, :3 | ?, ?, ? |
| 时间戳绑定 | 需要 datetime | datetime 自动转 TEXT |
| NULL 绑定 | None → NULL | None → NULL |

### 3.5 扩展数据设计（递归/Unicode/JSON）

#### manager_id 层级树

```
alice (1) ← root
├── bob (2)
│   ├── diana (4)
│   │   └── José (15)
│   └── Ève (5)
├── Charlie (3)
│   ├── frank (6)
│   └── kevin (11)
grace (7) ← root
├── heidi (8)
│   └── 小明 (10)
└── mike (13)
ivan (9) ← root (叶子)
linda (12) ← root (叶子)
O'Brien (14) ← root (叶子)
```

设计要点：manager_id 仅指向更小 id，保证插入顺序不违反外键。

#### Unicode 用户名与产品名

| 类型 | id | 新值 | Unicode 特征 |
|------|----|------|-------------|
| username | 5 | Ève | 拉丁重音字符 |
| username | 10 | 小明 | CJK 字符 |
| username | 14 | O'Brien | 含撇号 |
| username | 15 | José | 拉丁重音字符 |
| product name | 9 | 配件Z | CJK 字符 |
| product name | 14 | Paquet Économique | 法语重音字符 |

#### profile / metadata JSON

覆盖 NULL、空对象、不同键集、布尔值、嵌套数组等形态。

#### t_tags Unicode 标签

| id | entity_type | entity_id | tag | 用途 |
|----|------------|-----------|-----|------|
| 19 | user | 2 | 重要 | CJK 标签，与 id=20 重叠 → INTERSECT |
| 20 | product | 3 | 重要 | CJK 标签，与 id=19 重叠 → INTERSECT |
| 21 | user | 4 | café | 拉丁重音标签 |

---

## 4. 种子 SQL 设计

### 4.1 设计原则

1. 每条种子必须带 ORDER BY，保证结果顺序可比较。
2. 仅使用通用 SQL 语法，避免数据库特有特性。
3. 仅引用 5 张测试表，保证与 schema 一致。
4. 故意覆盖 NULL 场景。
5. 避免 RIGHT JOIN，确保 SQLite 兼容。

### 4.2 种子分类与覆盖范围

#### 01_basic_select（10）

覆盖 SELECT/WHERE/IN/BETWEEN/LIKE/DISTINCT/ORDER BY/LIMIT。

#### 02_aggregation（7）

覆盖 COUNT/SUM/AVG/MIN/MAX/GROUP BY/HAVING。

#### 03_join（6）

覆盖 INNER/LEFT/自连接/连接后聚合。

#### 04_subquery（6）

覆盖标量子查询/IN/NOT IN/EXISTS/派生表/相关子查询。

#### 05_set_operations（4）

覆盖 UNION/UNION ALL/INTERSECT/EXCEPT。

#### 06_window_functions（6）

覆盖 ROW_NUMBER/RANK/DENSE_RANK/SUM OVER/AVG OVER/COUNT OVER。

#### 07_null_handling（6）

覆盖 IS NULL/IS NOT NULL/COALESCE/NULL 与聚合/排序。

#### 08_expressions（7）

覆盖算术、CASE、CAST、嵌套表达式。

#### 09_recursive_self_join（6，扩展）

覆盖自连接、WITH RECURSIVE、层级深度、递归聚合。

#### 10_string_collation（6，扩展）

覆盖 UPPER/LOWER、LENGTH、LIKE、排序、TRIM、SUBSTR 的 Unicode 行为。

#### 11_json_handling（6，扩展）

覆盖 JSON 提取、NULL 处理、WHERE 过滤、聚合、嵌套路径。

### 4.3 种子文件格式

每个 `.sql` 文件包含单条 SQL 语句（不含分号）。目录结构：

```
data/seeds/
├── 01_basic_select/
├── 02_aggregation/
├── 03_join/
├── 04_subquery/
├── 05_set_operations/
├── 06_window_functions/
├── 07_null_handling/
├── 08_expressions/
├── 09_recursive_self_join/
├── 10_string_collation/
└── 11_json_handling/
```

### 4.4 种子在流水线中的角色

```
种子 SQL → sqlglot.parse_one() → AST → 变异策略 → 变异后 AST
     ↓                                               ↓
  SeedGenerator                                 MutatorContext
     ↓                                               ↓
sqlglot.transpile() → Oracle SQL + SQLite SQL → 双库执行 → 差分分析 → 报告
```

---

## 5. 跨数据库差异总结

| 差异类别 | Oracle 行为 | SQLite 行为 | 影响点 |
|---------|------------|------------|--------|
| 空串处理 | '' IS NULL → TRUE | '' IS NULL → FALSE | t_users.email |
| 类型系统 | 强类型 NUMBER(p,s) | 弱类型亲和性 | DECIMAL 精度 |
| NULL 排序 | NULL 默认排最后(ASC) | NULL 默认排最前(ASC) | NULL 排序种子 |
| LIMIT 语法 | 需转译为 FETCH FIRST/OFFSET | 原生支持 | limit_offset.sql |
| BOOLEAN | 无原生 BOOLEAN | 无原生 BOOLEAN | active/discontinued |
| TIMESTAMP | 原生类型 | TEXT 存储 | created_at/order_date |
| 外键约束 | 默认启用 | PRAGMA foreign_keys=ON | 初始化阶段 |
| DDL IF EXISTS | 不支持 | 支持 | DROP TABLE |
| EXCEPT/MINUS | Oracle 用 MINUS | SQLite 用 EXCEPT | set operations |
| 递归 CTE（扩展） | 仅 WITH | WITH RECURSIVE | 递归种子 |
| JSON 提取（扩展） | JSON_VALUE | json_extract | JSON 种子 |
| Unicode LENGTH/UPPER（扩展） | 依赖字符语义 | 依赖 ICU | Unicode 种子 |

---

## 6. 实现统计（设计基线）

| 指标 | 数值 |
|------|------|
| 测试表数量 | 5 |
| 基础种子类别 | 8 |
| 基础种子文件数 | 52 |
| 扩展种子类别 | 3 |
| 扩展种子文件数 | 18 |
| 覆盖的 SQL 特性 | SELECT/WHERE/JOIN/子查询/集合操作/窗口函数/NULL处理/表达式/递归CTE/Unicode/JSON |
| 类型映射条目 | 4 (INTEGER/VARCHAR/DECIMAL/TIMESTAMP × 2 方言) |
| 索引数量 | 4 |
| 外键关系 | 5（含自关联） |
