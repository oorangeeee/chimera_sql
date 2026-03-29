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

SQLGlot 能覆盖大量方言转换，但在部分语法点（例如 JSON 函数名、递归 CTE 关键字）需要补充规则链。

### 3.2 三阶段转译流程

1. `sqlglot.parse_one(sql, read=source)` 解析 AST
2. 规则链按顺序变换 AST
3. `tree.sql(dialect=target)` 生成目标方言 SQL

### 3.3 规则架构

- `TranspilationRule`：规则接口
- `RuleRegistry`：按 `(source, target)` 管理有序规则链
- `SQLTranspiler`：编排器，提供单条/批量转译

典型规则：

- `json_extract` <-> `JSON_VALUE`
- `WITH RECURSIVE` 的添加/移除
- `EXCEPT` <-> `MINUS`（可选）

### 3.4 容错策略

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

## 7. 工程化建议

- 将调研结论沉淀为配置而非分支代码
- 新数据库优先尝试”仅 YAML + 连接器”路径
- 转译规则采用最小可维护增量，避免过度定制
- 端到端报告优先结构化，便于回归与自动消费
- 方言兼容性校验在 `mutate`、`transpile`、`run` 三个入口自动执行，发现不兼容的种子 SQL 时拒绝执行并报错。扩展新数据库时，需在 `src/utils/dialect_detector.py` 的 `_INCOMPATIBLE` 字典中添加该方言的不兼容签名映射（详见 `tech.md` 2.5 节）
