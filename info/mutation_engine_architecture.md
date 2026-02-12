# AST 变异引擎架构调研：面向泛用性的设计

> 核心问题：变异规则是否需要按数据库/版本定制？如何以最低代码成本支持新数据库？

## 1. 结论先行

**变异规则需要"能力感知"，但不应该需要"代码定制"。**

调研结论分三层：

1. **业界共识**：变异必须感知目标数据库能力，否则无效样例激增（SQLancer/SQLsmith/SQLRight 均如此）。
2. **业界痛点**：现有工具全部采用"硬编码类继承"实现能力感知，每新增一个数据库需 10–30 个文件，扩展成本极高。
3. **本项目方案**：利用 SQLGlot 已内置的 150+ 方言能力标志（覆盖 32 种数据库），以声明式 YAML 配置驱动变异规则门控，实现**新增数据库零代码（仅写 YAML）或极低代码（仅写 connector）**。

## 2. 业界工具变异策略对比

### 2.1 SQLancer — 硬编码类继承，每库 10–30+ 文件

SQLancer 是当前最活跃的数据库模糊测试工具（ESEC/FSE 2020），支持 22 种数据库。

**架构**：每个数据库是一个独立 Java 包，包含：

| 文件 | 职责 |
|------|------|
| `*Provider.java` | 中心入口，管理建库与测试预言执行 |
| `*Schema.java` | 数据库 schema 表示 |
| `*ExpressionGenerator.java` | SQL 表达式随机生成（核心变异逻辑所在） |
| `*ToStringVisitor.java` | AST → SQL 文本序列化 |
| `*NoRECOracle.java` / `*TLPOracle.java` | 测试预言实现 |
| `*Errors.java` | 该库的已知错误白名单 |
| 各类 `*Statement.java` | INSERT/CREATE/DROP 等语句生成器 |

**关键设计决策**：

- **Typed vs Untyped 生成器**：对"严格类型"数据库（PostgreSQL、CockroachDB）使用类型感知的表达式生成器，对"弱类型"数据库（SQLite、MySQL）使用无类型生成器。这一决策直接硬编码在 Java 类继承中。
- **无任何配置文件**：所有能力感知逻辑都在代码里，没有 YAML/JSON 配置。
- **扩展成本**：新增一个数据库通常需要编写 10–30 个 Java 文件，约 2000–5000 行代码。

**评价**：高质量 bug 发现率（已报 500+ bug），但扩展成本极高，且变异逻辑无法跨库复用。

来源：
- [SQLancer GitHub](https://github.com/sqlancer/sqlancer)
- [CONTRIBUTING.md](https://github.com/sqlancer/sqlancer/blob/main/CONTRIBUTING.md)
- Rigger & Su, "Testing Database Engines via Pivoted Query Synthesis", OSDI 2020
- Rigger & Su, "Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction", ESEC/FSE 2020

### 2.2 SQLsmith — 目录式内省，半自动能力发现

SQLsmith（Seltenreich, 2015）采用不同于 SQLancer 的策略：

- **运行时内省**：通过查询目标数据库的系统目录（`information_schema`、`pg_catalog`）获取可用的函数、运算符、类型、表、列。
- **半自动适配**：核心生成逻辑相对通用，但仍包含 PostgreSQL 特有的假设。
- CockroachDB 团队将其移植时需要修改代码，因为 CockroachDB 不完全兼容 PostgreSQL 的系统目录。
- DuckDB 团队将 SQLsmith 集成为扩展时，改为"从 catalog 动态加载函数列表"，不再硬编码。

**评价**：内省思路先进，但仅适用于 PostgreSQL 兼容库，跨方言泛用性有限。

来源：
- [SQLsmith GitHub](https://github.com/anse1/sqlsmith)
- [CockroachDB SQLsmith 实践](https://www.cockroachlabs.com/blog/sqlsmith-randomized-sql-testing/)
- [DuckDB SQLsmith 扩展 PR #3410](https://github.com/duckdb/duckdb/pull/3410)

### 2.3 SQLRight — 覆盖引导 + 有效性导向变异

SQLRight（USENIX Security 2023, Liang et al.）在变异层引入两个关键创新：

- **覆盖引导（Coverage-Guided）**：用代码覆盖反馈指导变异方向，优先保留触发新路径的变异体。
- **有效性导向（Validity-Oriented）**：变异后检查 AST 是否仍可解析，丢弃语法无效的变异体。

SQLRight 的变异策略本身不做方言定制，但通过有效性过滤隐式实现了能力门控——目标数据库无法执行的语句会在运行时被淘汰。

**评价**：思路优雅但依赖运行时反馈，冷启动阶段无效样例比例高。

来源：
- Liang et al., "Detecting Logic Bugs of DBMS with Coverage-based Guidance", USENIX Security 2023

### 2.4 Squirrel — IR 中间表示 + 类型感知变异

Squirrel（CCS 2020, Zhong et al.）设计了专用的 SQL 中间表示（IR）：

- SQL → IR（保留类型信息的树结构）
- 在 IR 上执行变异（插入/删除/替换子树）
- IR → SQL（根据目标方言生成）

**关键设计**：变异在 IR 层执行，类型约束嵌入 IR 节点元数据，避免生成类型不兼容的表达式。

**评价**：类型感知变异是正确方向，但 IR 定义仍是硬编码的。

来源：
- Zhong et al., "Squirrel: Testing Database Management Systems with Language Validity and Coverage Feedback", CCS 2020

### 2.5 DynSQL — 运行时状态感知生成

DynSQL（USENIX Security 2023, Jiang et al.）的核心创新：

- **动态状态收集**：每执行一条 SQL 后，收集数据库返回的状态信息（schema 变化、执行成功/失败、错误码等）。
- **状态驱动生成**：用收集到的状态信息指导后续 SQL 生成，形成闭环。
- 测试了 6 种数据库（SQLite、MySQL、MariaDB、PostgreSQL、MonetDB、ClickHouse），发现 40 个 bug。

**评价**：运行时反馈最强大但工程复杂度最高，适合长期运行的 fuzzing campaign。

来源：
- Jiang et al., "DynSQL: Stateful Fuzzing for Database Management Systems with Complex and Valid SQL Query Generation", USENIX Security 2023

### 2.6 工具对比汇总

| 工具 | 能力感知方式 | 新增 DB 成本 | 变异质量 | 跨库泛用性 |
|------|-------------|-------------|----------|-----------|
| SQLancer | 硬编码类继承 | 极高（10-30文件） | 高 | 低（每库重写） |
| SQLsmith | 运行时内省 | 中（需适配目录） | 中 | 低（PostgreSQL 中心） |
| SQLRight | 覆盖引导+运行时 | 中 | 高 | 中 |
| Squirrel | IR 类型元数据 | 中 | 中高 | 中 |
| DynSQL | 运行时状态反馈 | 中 | 最高 | 中高 |
| **ChimeraSQL（目标）** | **声明式配置+SQLGlot** | **极低（仅YAML）** | 中高 | **高** |

## 3. 关键发现：SQLGlot 已是现成的"能力画像数据库"

本次调研最重要的发现：**SQLGlot 已经在代码中维护了 150 个布尔能力标志，覆盖 32 种 SQL 方言**。

### 3.1 三层标志体系

| 层级 | 基类标志数 | 作用域 |
|------|-----------|--------|
| Dialect（方言层） | 53 个 | 语义行为（类型安全、NULL 语义、连接语法等） |
| Generator（生成层） | 71 个 | SQL 生成能力（窗口函数、CTE、DECODE、MEDIAN 等） |
| Parser（解析层） | 26 个 | 解析行为（JOIN 优先级、字符串别名等） |

### 3.2 Oracle vs SQLite 关键差异（实测提取）

**Dialect 层差异**：

| 标志 | Oracle | SQLite | 变异含义 |
|------|--------|--------|---------|
| `SUPPORTS_SEMI_ANTI_JOIN` | True | **False** | 不对 SQLite 生成 SEMI/ANTI JOIN |
| `TYPED_DIVISION` | False | **True** | SQLite 整数除法返回整数 |
| `SAFE_DIVISION` | False | **True** | SQLite 除零返回 NULL |
| `SUPPORTS_COLUMN_JOIN_MARKS` | **True** | False | Oracle 支持 `(+)` 语法 |

**Generator 层差异**：

| 标志 | Oracle | SQLite | 变异含义 |
|------|--------|--------|---------|
| `NVL2_SUPPORTED` | True | **False** | 不对 SQLite 注入 NVL2 |
| `SUPPORTS_DECODE_CASE` | True | **False** | 不对 SQLite 注入 DECODE |
| `SUPPORTS_MEDIAN` | True | **False** | 不对 SQLite 注入 MEDIAN |
| `SUPPORTS_TO_NUMBER` | True | **False** | 不对 SQLite 注入 TO_NUMBER |
| `SUPPORTS_CREATE_TABLE_LIKE` | True | **False** | DDL 变异时注意 |
| `SUPPORTS_TABLE_ALIAS_COLUMNS` | True | **False** | 别名列变异需跳过 |
| `LOCKING_READS_SUPPORTED` | **True** | False | FOR UPDATE 仅 Oracle |
| `SUPPORTS_SELECT_INTO` | **True** | False | SELECT INTO 仅 Oracle |
| `EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE` | True | **False** | SQLite 不支持 EXCEPT ALL |
| `TRY_SUPPORTED` | **False** | **False** | 两者均不支持 TRY_CAST |

### 3.3 自动提取方案

```python
import sqlglot

def extract_capability_profile(dialect_name: str) -> dict:
    """从 SQLGlot 方言类自动提取能力标志。"""
    dialect_cls = sqlglot.Dialect.get_or_raise(dialect_name)
    flags = {}
    for layer_name, layer_cls in [
        ("dialect", dialect_cls),
        ("generator", dialect_cls.Generator),
        ("parser", dialect_cls.Parser),
    ]:
        for attr in dir(layer_cls):
            if attr.startswith("_"):
                continue
            val = getattr(layer_cls, attr, None)
            if isinstance(val, bool):
                flags[f"{layer_name}.{attr}"] = val
    return flags
```

这意味着：**当 SQLGlot 新增方言支持时，ChimeraSQL 无需任何代码改动即可获得该方言的能力画像。**

### 3.4 对比已有方案

| 方案 | 数据来源 | 覆盖度 | 维护成本 | 准确性 |
|------|---------|--------|---------|--------|
| 手工 YAML | 人工编写 | 低（有遗漏风险） | 高 | 取决于编写者 |
| 运行时内省 | 数据库系统目录 | 高 | 低 | 高（但需连接数据库） |
| **SQLGlot 标志提取** | SQLGlot 源码 | 中高（150 标志） | **零**（随 SQLGlot 升级） | 中高（SQLGlot 社区维护） |
| 手工 + SQLGlot 混合 | 两者合并 | **最高** | 低 | **最高** |

**推荐方案**：以 SQLGlot 自动提取为基础层，上叠手工 YAML 覆盖层（补充 SQLGlot 未编码的能力，如版本门控、数据库特有函数清单等）。

## 4. 面向泛用性的变异引擎架构

### 4.1 核心设计原则

1. **变异策略与数据库解耦**：变异策略本身不 import 任何数据库特有代码。
2. **能力画像驱动门控**：每条变异策略声明所需能力（`requires`），调度器查询目标数据库画像决定是否启用。
3. **新增数据库零代码**：仅需编写一个 YAML 能力画像文件（或完全自动从 SQLGlot 提取），无需修改任何 Python 代码。
4. **渐进式精确**：从 SQLGlot 自动提取 → 手工覆盖补充 → 运行时反馈修正，精确度逐步提升。

### 4.2 能力画像分层

```
┌───────────────────────────────────────────────┐
│  Layer 3: 手工覆盖层 (YAML)                    │  ← 用户/开发者补充
│  version_range, expected_errors, custom flags  │
├───────────────────────────────────────────────┤
│  Layer 2: SQLGlot 自动提取层                    │  ← 零代码，150+ flags
│  dialect.*, generator.*, parser.*              │
├───────────────────────────────────────────────┤
│  Layer 1: SQL 标准基线层                        │  ← 所有数据库共享
│  SQL:2016 Core features (E011-E151)            │
└───────────────────────────────────────────────┘
```

合并规则：Layer 3 > Layer 2 > Layer 1（高层覆盖低层）。

### 4.3 变异策略元数据

每条变异策略除实现 `mutate(ast)` 外，还必须声明元数据：

```yaml
# 以 BoundaryInjectionStrategy 为例
id: boundary_injection
category: generic          # generic | dialect_specific
requires:                  # 所需能力（AND 关系）
  - "generator.IS_BOOL_ALLOWED"   # 如果注入布尔边界值
applicable_node_types:     # 可作用的 AST 节点类型
  - "exp.Literal"
  - "exp.Column"
risk_level: low            # low | medium | high
```

```yaml
# 以 WindowFrameVariation 为例
id: window_frame_variation
category: dialect_specific
requires:
  - "generator.SUPPORTS_WINDOW_EXCLUDE"  # 仅 Oracle/SQLite 3.28+ 支持
applicable_node_types:
  - "exp.WindowSpec"
risk_level: medium
```

### 4.4 门控调度流程

```
Seed SQL
  │
  ▼
sqlglot.parse_one(sql) → AST
  │
  ▼
枚举 AST 中的可变异节点
  │
  ▼
对每个节点，收集候选变异策略
  │
  ▼
┌─────────────────────────────────────┐
│  can_apply(strategy, profile, node) │  ← 门控函数
│                                     │
│  1. strategy.requires ⊆ profile?    │  ← 能力检查
│  2. node.type ∈ strategy.nodes?     │  ← 节点类型匹配
│  3. not in policy.disable_rules?    │  ← 用户黑名单
│  4. AST depth/size within budget?   │  ← 预算约束
│                                     │
│  → allowed + effective_weight       │
└─────────────────────────────────────┘
  │
  ▼
加权随机采样 → 选定策略集合
  │
  ▼
依次应用策略 → 变异后 AST
  │
  ▼
sanity check（重新解析验证语法正确性）
  │
  ▼
输出变异 SQL + 变异元数据
```

### 4.5 新增数据库的工作量对比

| 操作 | SQLancer | SQLsmith | ChimeraSQL（本方案） |
|------|----------|----------|---------------------|
| 能力定义 | 写 Java 类 | 改 C++ 代码 | **自动提取 + 可选 YAML 覆盖** |
| 变异逻辑 | 重写生成器 | 改生成函数 | **无需改动（共享策略池）** |
| 转译支持 | N/A | N/A | **SQLGlot 已内置 32 方言** |
| 连接器 | 写 JDBC 适配 | N/A | 写 DBConnector 子类 |
| **总计** | **10-30 文件** | **大量 C++** | **1 YAML + 1 connector（可选）** |

如果目标数据库已有 SQLGlot 方言支持 + Python DB-API 驱动，则新增数据库的工作量为：

- **零代码路径**：SQLGlot 自动提取画像 + 已有 connector → 仅需一行配置指定方言名。
- **低代码路径**：写一个 YAML 画像覆盖文件（~30 行）+ 一个 connector 类（~50 行）。

## 5. 变异策略分类体系

基于调研，建议将变异策略分为以下类别：

### 5.1 通用策略（Generic） — 所有数据库可用

| 策略 ID | 描述 | 作用节点 |
|---------|------|---------|
| `boundary_injection` | 数值边界值注入（0, -1, MAX_INT, MIN_INT） | `Literal.number` |
| `null_injection` | 将表达式/列替换为 NULL | `Column`, `Literal` |
| `predicate_negation` | 谓词取反（`=` → `<>`，`>` → `<=`） | `Binary`（比较运算） |
| `logic_tautology` | 注入恒真/恒假条件（`OR 1=1`，`AND 1=0`） | `Where`, `Having` |
| `operand_swap` | 交换二元运算符两侧操作数 | `Binary` |
| `aggregate_substitution` | 聚合函数互换（`SUM` ↔ `COUNT`） | `Anonymous`, `Func` |
| `sort_direction_flip` | 排序方向翻转（`ASC` ↔ `DESC`） | `Ordered` |
| `distinct_toggle` | 添加/移除 DISTINCT | `Select` |
| `limit_variation` | 修改 LIMIT/OFFSET 值 | `Limit`, `Offset` |
| `string_boundary` | 空字符串、超长字符串、Unicode 边界 | `Literal.string` |

### 5.2 结构策略（Structural） — 需节点类型匹配

| 策略 ID | 描述 | requires |
|---------|------|----------|
| `subquery_wrapping` | 将标量表达式包装为子查询 | — |
| `join_type_variation` | JOIN 类型切换（INNER ↔ LEFT） | — |
| `union_type_variation` | UNION ↔ UNION ALL 切换 | — |
| `cte_extraction` | 将子查询提取为 CTE | `parser.SUPPORTS_CTE`（推断） |
| `predicate_promotion` | WHERE 条件提升到 HAVING 或反之 | — |
| `expression_nesting` | 增加表达式嵌套层级 | — |

### 5.3 方言感知策略（Dialect-Aware） — 需能力画像门控

| 策略 ID | 描述 | requires |
|---------|------|----------|
| `window_frame_variation` | 窗口帧变化（ROWS ↔ RANGE） | `generator.SUPPORTS_WINDOW_EXCLUDE` |
| `except_all_injection` | EXCEPT ALL 注入 | `generator.EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE` |
| `decode_case_swap` | DECODE ↔ CASE 互换 | `generator.SUPPORTS_DECODE_CASE` |
| `nvl2_injection` | NVL2 函数注入 | `generator.NVL2_SUPPORTED` |
| `median_injection` | MEDIAN 聚合注入 | `generator.SUPPORTS_MEDIAN` |
| `locking_read_injection` | FOR UPDATE 注入 | `generator.LOCKING_READS_SUPPORTED` |
| `division_safety_test` | 除零行为差分测试 | （检查 `dialect.SAFE_DIVISION` 差异） |
| `semi_anti_join_injection` | SEMI/ANTI JOIN 变异 | `dialect.SUPPORTS_SEMI_ANTI_JOIN` |

### 5.4 差分专项策略（Differential） — 针对已知跨库差异

这类策略专门**瞄准**数据库之间已知的语义分歧点，是差分测试中发现 bug 概率最高的变异：

| 策略 ID | 瞄准的差异 | 说明 |
|---------|-----------|------|
| `empty_string_null_probe` | Oracle `'' = NULL` | 生成空字符串相关条件，检测差分 |
| `integer_division_probe` | SQLite 整数除法 vs Oracle 浮点除法 | 除法表达式差分 |
| `null_sort_order_probe` | NULLS FIRST/LAST 默认值差异 | 排序中 NULL 位置差分 |
| `boolean_representation_probe` | Oracle 0/1 vs SQLite True/False | 布尔类型输出差分 |
| `type_coercion_probe` | 隐式类型转换规则差异 | 混合类型表达式差分 |

## 6. YAML 能力画像格式设计

### 6.1 自动生成画像（基线）

```yaml
# config/profiles/auto/sqlite.yaml — 由 SQLGlot 自动提取
# 不要手动编辑此文件，运行 `python -m src.core.mutator.profile_extractor` 重新生成

dialect_name: sqlite
sqlglot_version: "26.x"
auto_generated: true

flags:
  dialect:
    SUPPORTS_SEMI_ANTI_JOIN: false
    TYPED_DIVISION: true
    SAFE_DIVISION: true
    # ... 53 个 dialect 层标志
  generator:
    NVL2_SUPPORTED: false
    SUPPORTS_MEDIAN: false
    SUPPORTS_DECODE_CASE: false
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: false
    # ... 71 个 generator 层标志
  parser:
    JOINS_HAVE_EQUAL_PRECEDENCE: true
    STRING_ALIASES: true
    # ... 26 个 parser 层标志
```

### 6.2 手工覆盖画像

```yaml
# config/profiles/override/sqlite_3_45.yaml — 手工补充/修正

extends: auto/sqlite            # 继承自动提取画像
version_range: ">=3.45"

overrides:
  # 补充 SQLGlot 未编码的能力
  custom:
    supports_json_extract: true
    supports_recursive_cte: true     # SQLite 3.8.3+
    supports_window_functions: true  # SQLite 3.25.0+
    supports_upsert: true            # SQLite 3.24.0+
    max_expr_depth: 1000
    max_compound_select: 500

  # 修正 SQLGlot 标志（如果有误）
  # generator:
  #   SOME_FLAG: true

# 跨库差分相关
known_differences:
  - id: empty_string_is_null
    description: "Oracle treats '' as NULL; SQLite does not"
    affects: [null_handling, string_comparison]
  - id: integer_division
    description: "SQLite integer division truncates; Oracle returns decimal"
    affects: [arithmetic]

expected_errors:
  - pattern: "datatype mismatch"
    category: type_error
  - pattern: "no such function"
    category: unsupported_function
```

### 6.3 合并逻辑

```
最终画像 = auto/{dialect}.yaml    ← SQLGlot 自动提取（基线）
         + override/{id}.yaml    ← 手工覆盖（可选）
         + policy（运行时策略）    ← 用户指定（可选）
```

## 7. 与现有 mutation_capability_policy_design.md 的关系

已有的 `mutation_capability_policy_design.md` 定义了 Profile + Policy + Campaign 三层配置模型和 `can_apply()` 门控算法。本文档在其基础上补充了：

1. **SQLGlot 自动提取**：Profile 不再需要完全手写，大部分能力标志可自动获得。
2. **分层画像合并**：auto → override → policy 三层递进。
3. **具体策略分类**：Generic / Structural / Dialect-Aware / Differential 四类。
4. **泛用性量化**：明确了新增数据库的最小工作量。

两份文档互补，建议实现时同时参照。

## 8. 参考文献与来源

### 学术论文

| 论文 | 会议 | 关键贡献 |
|------|------|---------|
| Rigger & Su, "Testing Database Engines via Pivoted Query Synthesis" | OSDI 2020 | PQS 测试预言 |
| Rigger & Su, "...via Non-Optimizing Reference Engine Construction" | ESEC/FSE 2020 | NoREC 测试预言 |
| Liang et al., "Detecting Logic Bugs of DBMS with Coverage-based Guidance" | USENIX Security 2023 | SQLRight，覆盖引导+有效性变异 |
| Zhong et al., "Squirrel: Testing DBMS with Language Validity and Coverage Feedback" | CCS 2020 | IR 中间表示 + 类型感知 |
| Jiang et al., "DynSQL: Stateful Fuzzing for DBMS..." | USENIX Security 2023 | 运行时状态驱动 |

### 工具与框架

| 工具 | 链接 | 参考价值 |
|------|------|---------|
| SQLancer | [github.com/sqlancer/sqlancer](https://github.com/sqlancer/sqlancer) | 多库架构、Typed/Untyped 生成器 |
| SQLsmith | [github.com/anse1/sqlsmith](https://github.com/anse1/sqlsmith) | 运行时内省 |
| SQLGlot | [github.com/tobymao/sqlglot](https://github.com/tobymao/sqlglot) | 150+ 方言能力标志、32 方言支持 |
| SQLAlchemy | [sqlalchemy.org](https://docs.sqlalchemy.org/en/21/dialects/) | `supports_*` 布尔标志设计模式 |
| sqllogictest | [sqlite.org/sqllogictest](https://www.sqlite.org/sqllogictest) | `skipif/onlyif` 跨库门控 |

### SQL 标准

| 标准 | 说明 |
|------|------|
| ISO/IEC 9075 (SQL:2016/2023) | 定义了 320+ 可选特性码（E/F/S/T/X 前缀） |
| PostgreSQL SQL Conformance | [postgresql.org/docs/18/features.html](https://www.postgresql.org/docs/18/features.html) |
