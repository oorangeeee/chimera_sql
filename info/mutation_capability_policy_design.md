# 变异配置详细方案（Capability Profile + Mutation Policy）

## 1. 目标与范围

目标：在不牺牲跨库可比性的前提下，让用户可控地生成高质量 AST 变异 SQL。

范围：

- 配置模型：`profile`（DBMS+Version 能力）+ `policy`（用户变异策略）
- 调度算法：规则门控、规则采样、执行反馈分类
- 报告模型：记录生效规则、被门控规则、期望错误分类
- 不包含：具体变异规则实现细节（另行在 mutator 子模块实现）

## 2. 核心设计原则

1. 能力优先：数据库能力约束是硬边界，不能被用户策略突破。
2. 用户可控：在硬边界内允许用户调节规则开关、权重和预算。
3. 可复现：同一 `profile + policy + seed + random_seed` 必须可复现。
4. 可解释：每条变异 SQL 都可追溯“为何被生成/为何被过滤”。

## 3. 配置模型

### 3.1 Profile（内置/可扩展）

`profile` 描述目标数据库能力，按 `dbms + version_range` 定义。

关键字段：

- `id`: profile 唯一标识（如 `oracle_21c`, `sqlite_3_45`）
- `dbms`: `oracle` / `sqlite` / ...
- `version_range`: semver 或表达式
- `features`: 能力开关（如 `json_value`, `recursive_cte`, `window_function`）
- `limits`: 语法/资源限制（如 `max_join_tables`, `max_expr_depth`）
- `expected_errors`: 预期错误模式（正则或错误码）
- `known_differences`: 已知差异标签（用于差分降噪）

### 3.2 Policy（用户输入）

`policy` 描述本次变异活动策略。

关键字段：

- `id`: policy 唯一标识
- `mode`: `balanced` / `aggressive` / `stability_first`
- `enable_rules`: 显式启用规则列表
- `disable_rules`: 显式禁用规则列表
- `weights`: 规则权重映射
- `budget`: 预算（每条 seed 生成条数、最大 AST 深度、最大重试数）
- `constraints`: 结果约束（必须保留 ORDER BY 等）

### 3.3 Campaign（一次执行）

`campaign` 绑定一次运行参数：

- `profile_id`
- `policy_id`
- `random_seed`
- `seed_source`
- `target_count`

## 4. 合并与优先级

最终生效配置由 `profile` 与 `policy` 合并得到，规则如下：

1. 先应用 `profile`（硬约束）。
2. 再应用 `policy`（软约束）。
3. 若 `policy` 启用了超出 profile 能力的规则：
- 规则不生效
- 写 warning（含原因）
- 计入 `gated_rules`

优先级顺序：

1. `disable_rules`
2. `profile capability gate`
3. `enable_rules`
4. `weights` 与 `budget`

## 5. 规则门控算法

每条规则附带元数据：

- `rule_id`
- `requires_features`
- `incompatible_features`
- `supported_dbms`
- `version_constraints`
- `risk_level`

判定函数：`can_apply(rule, context)`

输入：

- `rule`
- `profile`
- `policy`
- `sql_shape`（seed 的 AST 特征）

输出：

- `allowed: bool`
- `reason: str`
- `effective_weight: float`

判定步骤：

1. `rule_id in disable_rules` => 拒绝
2. `dbms/version` 不匹配 => 拒绝
3. 缺少 `requires_features` => 拒绝
4. 命中 `incompatible_features` => 拒绝
5. AST shape 不满足 => 拒绝
6. 允许并返回权重

## 6. 生成流程

```
seed SQL -> parse AST -> enumerate candidate rules
        -> gate by can_apply
        -> weighted sampling
        -> mutate AST
        -> sanity check (parse back / depth / size)
        -> transpile + execute
        -> classify result (bug | expected | invalid)
        -> emit artifact + telemetry
```

关键点：

- 失败重试有上限，避免死循环
- 对 `expected_errors` 分类后不计入 bug
- 保留原始 SQL、变异 SQL、规则链与随机种子

## 7. 配置示例（YAML）

```yaml
mutation:
  profiles:
    oracle_21c:
      dbms: oracle
      version_range: ">=21.0,<22.0"
      features:
        recursive_cte: true
        json_value: true
        except_op: true
      limits:
        max_expr_depth: 12
        max_join_tables: 6
      expected_errors:
        - "ORA-00932"
      known_differences:
        - "empty_string_is_null"

    sqlite_3_45:
      dbms: sqlite
      version_range: ">=3.45,<3.46"
      features:
        recursive_cte: true
        json_extract: true
        except_op: true
      limits:
        max_expr_depth: 14
        max_join_tables: 8
      expected_errors:
        - "datatype mismatch"

  policies:
    balanced_default:
      mode: balanced
      enable_rules: [boundary_injection, predicate_rewrite, null_rewrite]
      disable_rules: [deep_subquery_nesting]
      weights:
        boundary_injection: 1.0
        predicate_rewrite: 1.2
        null_rewrite: 0.8
      budget:
        max_mutations_per_seed: 20
        max_retry_per_mutation: 5
      constraints:
        require_deterministic_output: true

  campaign:
    profile_id: oracle_21c
    policy_id: balanced_default
    random_seed: 20260211
    seed_source: data/seeds
    target_count: 2000
```

## 8. 工程接口草案

建议新增对象：

- `CapabilityProfile`
- `MutationPolicy`
- `MutationCampaign`
- `RuleGate`（`can_apply`）
- `MutationPlanner`（采样与调度）
- `MutationTelemetry`（统计与报告）

建议模块路径：

- `src/core/mutator/profile.py`
- `src/core/mutator/policy.py`
- `src/core/mutator/gate.py`
- `src/core/mutator/planner.py`
- `src/core/mutator/telemetry.py`

## 9. 报告与可观测性

每次运行至少输出：

- `profile_id`, `policy_id`, `random_seed`
- `total_candidates`, `gated_count`, `generated_count`, `valid_count`
- `rule_hit_count`（每条规则命中数）
- `gated_rules`（规则 + 原因）
- `expected_error_count`, `bug_suspect_count`

建议在现有报告中新增 JSON 字段：

- `mutation_context`
- `gate_decisions`
- `classification`

## 10. 实施计划（分阶段）

Phase 1（低风险，先可跑）：

1. 落地 `profile/policy` 配置读取与校验
2. 给现有规则补元数据
3. 实现 `RuleGate.can_apply()` 并接入调度

Phase 2（质量提升）：

1. 接入 weighted sampling
2. 增加 telemetry 与报告字段
3. 引入 expected-error 分类

Phase 3（高级优化）：

1. 反馈回灌（coverage/diff novelty）
2. profile 自动探测（可选）
3. 多 profile 并行 campaign

## 11. 验收标准

1. 可配置性：同一代码可通过切换 profile 适配至少 2 个 DBMS。 
2. 稳定性：无效 SQL 比例较当前基线明显下降。 
3. 可解释性：任意一条变异 SQL 可追溯规则链与门控决策。 
4. 可复现性：固定随机种子时产物一致。 

## 12. 风险与对策

- 风险：配置复杂度上升。
- 对策：内置 profile + policy 模板，用户只需覆盖少量字段。

- 风险：规则元数据维护成本增加。
- 对策：规则注册时强制校验元数据完整性。

- 风险：门控过严导致多样性下降。
- 对策：提供 `aggressive` policy 并跟踪 valid_ratio 与 diff_yield 做动态调参。
