# ChimeraSQL Core 文档

本文档整合原 `info/` 目录中的核心思路、变异架构调研、测试基础设施设计。

## 1. 项目核心目标

ChimeraSQL 的目标是：以通用 SQL 种子为输入，通过 AST 级变异生成高质量测试用例，使用 SQLGlot + 自定义规则进行跨方言转译，在多数据库执行后输出结构化报告，用于发现崩溃、语法兼容问题与异常行为。

核心主流程：

`Seed SQL -> AST 变异 -> 方言转译 -> 多库执行 -> 报告`

研发阶段允许只跑 Seeds 回归，用于验证连接器和测试基建。

## 2. 解耦原则与 MVP

解耦原则：

- 变异模块与转译模块解耦
- 变异策略不依赖具体数据库实现
- 转译模块只处理语法/方言兼容，不承担执行职责

MVP：

- 可用 AST 变异策略
- SQLGlot 基础转译 + 关键规则修正
- 端到端流程可运行并产出报告

## 3. 方言转译器设计

### 3.1 为什么要规则引擎

SQLGlot 能覆盖大量方言转换，但在部分语法点需要补充规则链。经过 SQLite→Oracle 转译实践验证，SQLGlot 原生处理存在以下盲区：

1. **JSON 函数名映射**: `json_extract()` 不会被自动转换为 `JSON_VALUE()`
2. **WITH RECURSIVE 关键字**: SQLGlot 可移除 RECURSIVE，但不会补回 Oracle 要求的 CTE 列名列表
3. **标量子查询 FROM DUAL**: SQLite 允许 `(SELECT expr)` 无 FROM 子句，Oracle 不允许
4. **GROUP BY 中的标量子查询**: Oracle 禁止在 GROUP BY 中使用子查询（ORA-22818）
5. **聚合函数 `*` 参数**: `MAX(*)/MIN(*)/SUM(*)/AVG(*)` 在 Oracle 中不合法（ORA-00936）
6. **递归 CTE 列名列表**: SQLGlot 在 SQLite 方言序列化时丢弃 CTE 列名列表，导致 Oracle 报 ORA-32039
7. **递归 CTE UNION ALL**: 变异可能将 UNION ALL 改为 UNION，Oracle 要求递归 CTE 必须使用 UNION ALL（ORA-32040）

### 3.2 三阶段转译流程

1. `sqlglot.parse_one(sql, read=source)` 解析 AST
2. 规则链按顺序变换 AST（每条规则接收前一条的输出）
3. `tree.sql(dialect=target)` 生成目标方言 SQL

### 3.3 规则架构

- `TranspilationRule`：规则接口（`name`/`description`/`apply(tree)` 契约）
- `RuleRegistry`：按 `(source, target)` 管理有序规则链
- `SQLTranspiler`：编排器，提供单条/批量转译

**已实现规则:**

| 规则 | 方向 | 处理的方言差异 |
|------|------|---------------|
| `JsonExtractToJsonValueRule` | SQLite→Oracle | `json_extract()` → `JSON_VALUE()` |
| `JsonValueToJsonExtractRule` | Oracle→SQLite | `JSON_VALUE()` → `json_extract()` |
| `RemoveRecursiveKeywordRule` | SQLite→Oracle | 移除 RECURSIVE + 补回 CTE 列名列表 + 强制 UNION ALL |
| `AddRecursiveKeywordRule` | Oracle→SQLite | 启发式检测递归 CTE 并添加 RECURSIVE |
| `AddFromDualRule` | SQLite→Oracle | 标量子查询补 FROM DUAL + 展开 GROUP BY 中的子查询 |
| `FixAggregateStarRule` | SQLite→Oracle | `MAX(*)/MIN(*)/SUM(*)/AVG(*)` → `MAX(1)` 等 |
| `ExceptToMinusRule` | 可选 | EXCEPT → MINUS |
| `MinusToExceptRule` | 可选 | MINUS → EXCEPT |

**SQLite→Oracle 规则链执行顺序:**

```
JsonExtractToJsonValueRule      — JSON 函数名映射
RemoveRecursiveKeywordRule      — RECURSIVE 关键字 + CTE 列名 + UNION ALL 修复
AddFromDualRule                 — 标量子查询 FROM DUAL 补全 + GROUP BY 子查询展开
FixAggregateStarRule            — MAX(*)/MIN(*)/SUM(*)/AVG(*) → MAX(1) 等
```

### 3.4 SQLGlot 关键限制

在转译引擎开发过程中发现的 SQLGlot 限制：

| 限制 | 影响 | 应对 |
|------|------|------|
| CTE 列名列表在 SQLite 方言序列化时被丢弃 | Oracle 递归 CTE 缺列名（ORA-32039） | `RemoveRecursiveKeywordRule` 从 SELECT expressions 重建列名 |
| AST 属性 `from` 实际存储为 `from_`（带下划线） | 自定义规则访问 FROM 子句时取到 None | 使用 `node.args.get("from_")` 访问 |
| `MAX(*)` 中 `*` 存储在 `node.this`（Star 类型） | 检查 `node.expressions` 时遗漏 | 同时检查 `node.this` 和 `node.expressions` |

### 3.5 转译引擎质量

通过自定义规则链补充 SQLGlot 的盲区后，SQLite→Oracle 转译引擎（SQLGlot + 自定义规则组合）对变异后 SQL 的转译成功率可达 **95%+**。剩余失败主要来自变异引擎产生的语义错误（如 `null_injection` 破坏 GROUP BY/ORDER BY 引用、`subquery_wrap` 在递归 CTE 中破坏列引用），而非转译引擎本身的问题。

### 3.6 容错策略

- 单条规则失败不中断整条转译
- 批量转译单条失败不中断整体任务
- 同方言转换直接返回原 SQL

## 4. 变异引擎架构调研结论

核心结论：变异应当“能力感知”，但不应为每个数据库重复写一套策略代码。

### 4.1 业界工具结论摘要

- SQLancer：质量高，但按库硬编码，新增数据库成本高
- SQLsmith：依赖系统目录内省，跨方言泛化有限
- SQLRight：覆盖引导+有效性过滤，运行时反馈强
- Squirrel：IR + 类型约束，工程复杂
- DynSQL：状态驱动，效果强但实现复杂

### 4.2 本项目方案

采用“SQLGlot 自动能力提取 + YAML 覆盖”的声明式能力画像：

- 自动提取 `Dialect/Generator/Parser` 布尔标志
- YAML 补充版本特性与项目自定义能力
- 策略声明 `requires`，由门控决定是否启用

分层模型：

- Layer 1：SQL 标准基线
- Layer 2：SQLGlot 自动提取
- Layer 3：YAML 手工覆盖（优先级最高）

### 4.3 变异策略分类

- Generic：全库通用
- Structural：结构重写
- Dialect-Aware：按能力画像启用

### 4.4 门控与调度

典型调度流程：

1. 解析 SQL 为 AST
2. 枚举可变异节点
3. 根据 `can_apply(strategy, profile, node)` 过滤
4. 随机采样候选策略并执行
5. 语法回检并输出结果

## 5. 测试基础设施设计

### 5.1 设计目标

- Oracle/SQLite 语义等价表结构
- 一致的数据集与可复现样本
- 能覆盖 JOIN、聚合、窗口、集合、NULL、JSON、递归、Unicode

### 5.2 三阶段初始化流水线

`SchemaInitializer -> DataPopulator -> SeedGenerator`

- SchemaInitializer：构建统一 5 张核心表
- DataPopulator：填充边界值与多类型数据
- SeedGenerator：生成多类别种子 SQL

### 5.3 核心表与覆盖维度

核心表：

- `t_users`
- `t_products`
- `t_orders`
- `t_metrics`
- `t_tags`

覆盖维度：

- CRUD
- JOIN / 子查询
- 聚合 / 窗口函数
- 集合操作
- NULL 与边界值
- 递归自关联
- Unicode 字符串
- JSON 提取

### 5.4 数据设计要点

- 每表控制在 15-20 行，避免笛卡尔积爆炸
- 覆盖 `NULL`、空字符串、0/-1/极值、负数、高精度小数
- 同时包含 ASCII 与 Unicode 字符，覆盖排序/比较场景
- JSON 字段覆盖空对象、嵌套、布尔、数组

### 5.5 参数绑定与方言差异

- Oracle 占位符：`:1, :2, ...`
- SQLite 占位符：`?, ?, ...`
- 时间与 NULL 绑定行为按各驱动实现处理

## 6. 批量转译功能定位

`BatchTranspileRunner` 负责：

1. 校验输入参数
2. 递归收集 `.sql`
3. 调用 `SQLTranspiler.transpile()`
4. 按目录层级输出结果
5. 生成 `report.md` + `report.json`

输出目录格式：`result/{timestamp}_{source}_{target}/`

## 8. 分析模块设计

### 8.1 设计目标

对模糊测试执行结果进行多维度统计分析，帮助研究者：
- 快速定位最常见的错误模式
- 评估各变异策略的有效性
- 量化转译引擎的效果
- 识别性能瓶颈
- 评估种子覆盖率

### 8.2 架构

```
src/analyzer/
├── __init__.py      # 导出公开 API
├── analyzer.py      # FuzzAnalyzer 分析器
├── result.py        # 数据类定义（AnalysisResult, ErrorCategory 等）
└── report.py        # AnalysisReport 报告生成器
```

**FuzzAnalyzer**: 核心分析器，接收 `List[SQLExecutionResult]`，通过六个独立分析维度产出 `AnalysisResult`。

**AnalysisResult**: 数据类容器，包含所有统计字段，提供 `to_dict()` 方法用于 JSON 序列化。

**AnalysisReport**: 报告生成器，接收 `AnalysisResult` 生成 Markdown + JSON 双格式报告。

### 8.3 分析维度

1. **执行成功率**: 总执行数、成功数、失败数、成功率、总耗时、平均耗时
2. **错误分类**: 使用正则模式匹配将错误信息分为 10 类（表不存在、列不存在、语法错误、类型不匹配等），按出现次数排序并保留示例
3. **变异策略效果**: 统计各策略的触发次数、成功数、失败数、成功率
4. **转译效果**: 统计应用转译规则的 SQL 数量、各规则触发次数、转译后成功/失败率
5. **性能分析**: 找出耗时最慢的 Top-5 SQL
6. **种子覆盖**: 统计每条种子产出的变异数、成功/失败比例

### 8.4 错误分类模式

分析器使用 10 个预定义正则模式按优先级匹配错误信息：

| 分类 | Oracle 错误码 | 匹配关键字 |
|------|:---:|------|
| 表不存在 | ORA-00942 | `table.*does not exist`, `no such table` |
| 列不存在 | ORA-00904 | `column.*does not exist`, `no such column` |
| 语法错误 | ORA-00900/00933 | `syntax error` |
| 类型不匹配 | ORA-00932 | `type mismatch`, `datatype mismatch` |
| 唯一约束冲突 | ORA-00001 | `unique constraint`, `duplicate key` |
| 连接错误 | ORA-12154/12514 | `connection` |
| 权限不足 | ORA-01031 | `permission`, `insufficient privileges` |
| 非空约束冲突 | ORA-01400 | `cannot be null`, `NOT NULL constraint` |
| 函数不存在 | — | `no such function`, `undefined function` |
| 其他错误 | — | `.*`（兜底） |

### 8.5 报告输出

每次流水线运行后在输出目录生成两份报告：

- `analysis.md`: Markdown 格式，包含概览表格、错误分析表格、策略效果表格、转译效果统计、性能 Top-5、种子覆盖表格
- `analysis.json`: JSON 格式，包含完整的结构化数据，可供程序化消费

## 9. 工程化建议

- 将调研结论沉淀为配置而非分支代码
- 新数据库优先尝试”仅 YAML + 连接器”路径
- 转译规则采用最小可维护增量，避免过度定制
- 端到端报告优先结构化，便于回归与自动消费
- 方言兼容性校验在 `mutate`、`transpile`、`run` 三个入口自动执行，发现不兼容的种子 SQL 时拒绝执行并报错。扩展新数据库时，需在 `src/utils/dialect_detector.py` 的 `_INCOMPATIBLE` 字典中添加该方言的不兼容签名映射（详见 `tech.md` 2.5 节）
- `run` 子命令采用单目标模式（`-t/--target` 指定方言），同方言时自动跳过转译阶段，用于变异引擎本身的回归测试
- 转译引擎质量目标：SQLGlot + 自定义规则组合对变异后 SQL 的转译成功率应达到 95%+，剩余失败主要来自变异引擎的语义错误
