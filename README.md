# 云原生运维控制器测试工具

本工具面向运行在 Kubernetes 集群中的云原生运维控制器（Operator），实现了两套互补的测试方法：**程序分支覆盖导向的端到端测试**与**基于反向代理的故障注入测试**。端到端测试阶段生成的测试用例池可直接作为故障注入测试的输入，形成完整的韧性测试流水线。本仓库同时提供基于 FastAPI 的可视化调试界面，用于测试运行时的监控与调试。

---

## 目录

- [方法概述](#方法概述)
- [系统架构](#系统架构)
- [快速开始](#快速开始)
  - [环境依赖](#环境依赖)
  - [项目数据准备](#项目数据准备)
  - [启动可视化界面](#启动可视化界面)
- [测试流水线](#测试流水线)
  - [阶段一：关联分析（explore-all）](#阶段一关联分析explore-all)
  - [阶段二：测试计划生成（testplan）](#阶段二测试计划生成testplan)
  - [阶段三：端到端验证（run）](#阶段三端到端验证run)
  - [阶段四：故障注入测试（fault）](#阶段四故障注入测试fault)
- [可视化调试界面](#可视化调试界面)
- [CLI 使用方法](#cli-使用方法)
- [配置文件格式](#配置文件格式)
- [目录结构](#目录结构)
- [环境变量](#环境变量)
- [同步更新](#同步更新)

---

## 方法概述

云原生运维控制器负责将用户声明的期望状态（Custom Resource，CR）持续调谐为集群的实际状态，其正确性高度依赖内部决策逻辑在各条件分支下的行为，其韧性则取决于在控制器崩溃、API 服务器异常、网络延迟等故障场景下的自我恢复能力。

本工具的核心设计有三：

1. **以关联分析结果为先验知识**：在测试用例生成之前，通过对各 CR 属性字段进行差分变异，采集插桩执行轨迹，建立 *CR 属性字段 → 程序分支条件变量* 的映射关系，为后续有针对性的变异提供导向。
2. **以大语言模型为变异引擎**：利用大语言模型对 CRD 定义及字段间语义约束的理解能力，生成能够覆盖特定分支取值组合的测试输入，克服纯规则变异在复杂语义约束场景下的局限。
3. **以反向代理为故障载体**：通过边车容器拦截控制器与 API 服务器之间的通信层，对任意控制器实施通信类故障注入；借助控制器内部插桩记录函数在调谐生命周期关键节点的轮询，保证故障触发与调谐阶段精准同步。

### 故障注入类型

| 故障类型 | 注入机制 | 验证目标 |
|---|---|---|
| **崩溃故障**（crash） | 强制删除控制器 Pod，由 Kubernetes 自动重建 | 控制器重启后能否正确恢复对集群资源的管理 |
| **重连故障**（reconnect） | 反向代理将响应中的资源对象替换为预先缓存的历史状态快照 | 控制器在陈旧状态误导下调谐决策的幂等性 |
| **延迟故障**（delay） | 反向代理丢弃中间状态事件，使控制器直接面对最终状态 | 控制器跳过中间状态变化后调谐的最终一致性 |

故障触发通过**故障管理服务**协调：测试编排器提交配置时同步注册故障任务；控制器内部跟踪逻辑在调谐起始节点轮询故障管理服务，获取待执行任务后通知反向代理进入对应的故障注入模式，确保故障与调谐阶段严格同步。

---

## 系统架构

```
┌──────────────────────────────────────────────────────────────┐
│               可视化调试界面（gsod_ui.py）                    │
│          FastAPI 后端  +  gsod_ui_template.html 前端          │
│  ┌──────────────┐  ┌────────────────┐  ┌───────────────────┐  │
│  │  项目管理     │  │  测试任务控制   │  │  检查点 / 报告     │  │
│  │  (侧边栏)     │  │  启动/中断/日志 │  │  实时预览 & 统计   │  │
│  └──────────────┘  └────────────────┘  └───────────────────┘  │
└────────────────────────────┬─────────────────────────────────┘
                             │ subprocess 调用
┌────────────────────────────▼─────────────────────────────────┐
│                      main.py  CLI                            │
│   explore-all │ testplan │ run │ fault │ report │ ...         │
└──────┬──────────────┬───────────────┬──────────┬─────────────┘
       │              │               │          │
  runner/        runner/         runner/    runner/
  explore_all    testplan        e2e        fault
       │              │               │          │
       └──────────────┴───────────────┴──────────┘
                             │
                     phases/（核心执行逻辑）
                             │
          ┌──────────────────┼──────────────────┐
       cluster/      instrumentation/          llm/
     （集群生命周期）  （插桩轨迹采集）      （大语言模型调用）
```

### 主要功能模块

| 模块 | 对应路径 | 职责 |
|---|---|---|
| **关联分析模块** | `phases/explore_all.py` | 对 CRD 全量字段差分变异，采集运行时插桩轨迹，建立属性字段与程序分支条件变量的映射关系 |
| **测试用例变异验证模块** | `phases/testplan.py`、`llm/` | 调用大语言模型生成满足 CRD 约束的目标导向 CR 变异；提交集群前完成合法性校验；采集覆盖数据反馈 |
| **测试计划探索生成模块** | `phases/testplan.py` | 维护分支覆盖导向的测试用例池；优先选取未覆盖目标；检测停滞时发起定向变异以突破局部瓶颈 |
| **端到端验证模块** | `phases/e2e.py` | 将测试用例池逐个应用到集群，采集容器状态与运行指标，检测控制器缺陷 |
| **故障注入测试模块** | `phases/fault.py` | 以测试用例池为输入，每轮随机注入崩溃/重连/延迟故障，评估控制器恢复能力 |
| **数据采集服务** | `instrumentation/collector.py` | 接收控制器插桩上报的调谐执行轨迹；接收反向代理上报的故障事件；维护调谐轮次日志 |
| **可视化调试界面** | `utils/gsod_ui.py` | 测试监控、检查点浏览、插桩信息与字段关联可视化、故障注入历史回溯 |

---

## 快速开始

### 环境依赖

- Python ≥ 3.11
- [Kind](https://kind.sigs.k8s.io/) ≥ 0.20（本地容器化 Kubernetes 集群管理）
- `kubectl`（需在 PATH 中可访问）
- Docker（Kind 运行依赖）

安装 Python 依赖：

```bash
pip install fastapi uvicorn pydantic pyyaml requests rich
```

### 项目数据准备

每个待测控制器需在 `data/<OperatorName>/` 目录下准备以下文件：

| 文件 | 说明 |
|---|---|
| `config.json` | 控制器部署配置（镜像、CRD 路径、部署步骤等） |
| `context.json` | CRD 字段结构与语义约束描述，由 `preflight` 子命令自动生成 |
| `runner.yaml` | 运行参数 Profile，可由可视化界面自动发现 |
| `base_cr.yaml` | （可选）完整 seed CR，用于替代工具默认生成的最小 CR |

`runner.yaml` 示例：

```yaml
common:
  reuse_cluster: my-kind-cluster
  operator_namespace: cass-operator
  base_cr: data/CassOp/base_cr.yaml
  config: data/CassOp/config.json
  context: data/CassOp/context.json
  instrument_info: /mnt/d/instrument/CassOp/instrument_info.json
  wait_sec: 30
  collect_max_wait: 120

explore-all:
  instrument_dir: /mnt/d/instrument/CassOp
  project_path: /mnt/d/cass-operator

testplan:
  instrument_dir: /mnt/d/instrument/CassOp
  project_path: /mnt/d/cass-operator
  max_rounds: 0

run:
  max_rounds: 0

fault:
  fault_types: crash,reconnect
  max_rounds: 50
```

### 启动可视化界面

```bash
# 从项目根目录启动，默认监听 0.0.0.0:7860
python utils/gsod_ui.py

# 自定义端口与插桩根目录
python utils/gsod_ui.py --port 8080 --instrument-base /mnt/d/instrument
```

在浏览器中打开 `http://localhost:7860` 即可进入可视化调试界面。路径配置也可通过环境变量指定（详见[环境变量](#环境变量)）。

---

## 测试流水线

四个阶段可独立运行，也可通过可视化界面按序推进。前序阶段的输出作为后续阶段的输入：关联分析结果（`field_relations.json`）指导测试计划生成，测试计划 checkpoint（含 `testplan.testcases`）驱动端到端验证与故障注入测试。所有阶段均支持断点续传，中断后以相同参数重新运行即可从上次停止处继续。

### 阶段一：关联分析（explore-all）

**目标**：建立 CR 属性字段与程序分支条件变量的映射关系，为后续变异提供分支覆盖导向。

从基础测试输入出发，对 CRD 中各属性字段逐一进行差分变异，将不同取值依次应用到集群，采集控制器调谐时的插桩执行轨迹，提取各程序分支条件变量的运行时取值，比较前后差异，将发生变化的条件变量与当前变异字段建立映射关系。通过黑名单机制排除与大量字段均相关的高频通用变量，以降低误判。

```bash
python main.py --profile data/CassOp/runner.yaml explore-all
```

输出：`field_relations.json`（字段-分支关联映射）、插桩元数据，以及 HTML 格式的分析报告。

### 阶段二：测试计划生成（testplan）

**目标**：以最大化分支覆盖为目标，生成能够覆盖不同程序执行路径的测试用例池。

工具维护一个分支覆盖导向的循环，每轮从当前覆盖状态出发执行以下步骤：
1. **目标选取**：优先选取尚未覆盖的分支取值组合，结合测试用例池覆盖情况与历史触发频次综合决策；
2. **输入变异**：将目标分支的条件表达式、关联分析信息及当前基准输入构造为提示词，调用大语言模型生成字段级差分补丁；补丁经 CRD 约束校验后应用于基准输入，生成可提交的测试输入；
3. **覆盖信息采集与反馈**：应用测试输入后，插桩记录逻辑采集本次调谐的决策路径，通过集群内数据采集服务上报；测试框架拉取覆盖数据，更新覆盖状态，驱动下一轮目标选取。

当用例池停滞时，主动对具体未覆盖目标发起定向变异，生成多样性用例以突破局部瓶颈。

```bash
python main.py --profile data/CassOp/runner.yaml testplan
```

输出：`checkpoint.json`（含 `testplan.testcases` 测试用例池），以及覆盖率 HTML 报告。

### 阶段三：端到端验证（run）

**目标**：将测试用例池内的各测试用例应用到集群，采集集群状态与运行指标，检测控制器缺陷。

每轮测试按固定顺序执行：清理上一轮测试输入 → 重启控制器 → 等待准入检查就绪 → 提交新的 CR → 等待控制器及其管理的资源达到稳定态 → 采集覆盖数据与集群状态。遇到不可恢复的集群故障时自动重建测试环境，保障长时间测试的连续性。

```bash
python main.py --profile data/CassOp/runner.yaml run \
  --testplan-checkpoint gsod_output_v5/tp-CassOp-YYYYMMDD-HHMMSS/checkpoint.json
```

### 阶段四：故障注入测试（fault）

**目标**：在真实调谐场景下注入可控异常，验证控制器的容错能力与自我恢复能力。

以测试用例池为输入，每轮随机选取一个测试用例并确定本轮故障类型：
- **崩溃故障**：直接删除控制器 Pod，等待 Kubernetes 自动重建后检验资源管理是否正确恢复；
- **重连/延迟故障**：向故障管理服务提交故障任务并缓存当前资源状态快照，再将 CR 提交集群，由反向代理在后续事件流中执行具体注入动作。

每轮结束后检查集群健康状态，按故障类型统计触发次数、成功恢复次数与恢复失败次数。

```bash
python main.py --profile data/CassOp/runner.yaml fault \
  --testplan-checkpoint gsod_output_v5/tp-CassOp-YYYYMMDD-HHMMSS/checkpoint.json \
  --fault-types crash,reconnect \
  --max-rounds 100
```

---

## 可视化调试界面

### 侧边栏

- **控制器项目选择**：扫描 `data/` 目录，自动列出包含 `runner.yaml` 的控制器项目
- **流水线阶段指示器**：直观展示各阶段运行状态（空闲 / 运行中 / 完成 / 错误）

### 各阶段控制面板

每个测试阶段对应独立的控制 Tab，提供：

- **参数配置区**：选择运行 Profile、指定断点续传文件（Checkpoint）、设置故障类型等
- **实时日志流**：通过 SSE（Server-Sent Events）推送子进程标准输出，无需手动刷新
- **进度仪表盘**：从 checkpoint 实时读取已完成字段数、分支覆盖率、测试用例数、当前轮次等指标
- **启动 / 中断控制**：支持随时安全中断，重启后自动从断点续传

### 检查点浏览器

Checkpoint 选择器提供历史检查点列表，每条目展示：
- 生成时间戳与阶段类型（ea / tp / run / fault）
- 关键统计预览（测试用例数、分支覆盖数、已完成轮次等）

### 可视化查看器

| 查看器 | 内容 |
|---|---|
| **插桩信息查看器** | 按源文件分组展示全部程序分支点（BranchPoint），含行号、判断条件表达式与条件变量列表 |
| **字段关联查看器** | 分支 → 关联字段的反向索引，含条件变量运行时取值映射 |
| **测试用例查看器** | 测试用例池列表，含所覆盖分支、历史触发频次与完整 CR 内容 |
| **Pod 状态监控** | 实时通过 kubectl 查询 Pod 运行状态、容器就绪情况与重启次数 |
| **故障轮次日志** | 通过数据采集服务获取每轮调谐事件序列，展示故障注入前后的资源状态差异对比 |

---

## CLI 使用方法

所有子命令均支持 `--profile <runner.yaml>` 参数预设通用配置项。

```
python main.py [--profile PATH] <subcommand> [options]

子命令:
  explore-all         CR 属性字段关联分析，建立字段-分支映射
  testplan            分支覆盖导向的测试用例池生成
  run                 端到端验证测试
  fault               故障注入测试
  report              从 checkpoint 生成 HTML 汇总报告
  testplan-report     生成测试计划覆盖率报告
  explore-all-report  生成关联分析 HTML 报告
  coverage-test       针对指定分支目标生成并验证单个 CR
  validate            重放字段变异序列，验证关联分析结果
  preflight           从 CRD YAML 生成或更新字段约束描述文件

通用选项（大多数子命令支持）:
  --checkpoint PATH      指定断点续传文件路径
  --reuse-cluster NAME   复用已有 Kind 集群（跳过创建流程）
  --keep-cluster         测试结束后保留集群，不执行清理
  --workdir-base DIR     输出目录的父目录（默认 gsod_output_v5）
  --max-rounds N         最大测试轮数，0 表示不限制
  --wait-sec N           提交 CR 后等待控制器调谐的秒数（默认 15）
  --debug                启用 DEBUG 级别日志
```

---

## 配置文件格式

### config.json（控制器部署配置）

```json
{
  "deploy": {
    "steps": [
      {"apply": {"file": "data/CassOp/cass-operator.yaml", "operator": true}},
      {"apply": {"file": "data/CassOp/crd.yaml"}}
    ]
  },
  "seed_custom_resource": "data/CassOp/cr.yaml",
  "custom_resource_definition": "data/CassOp/crd.yaml",
  "operator_image": "k8ssandra/cass-operator:v1.22.1-inst"
}
```

### context.json（CRD 字段结构与约束）

由 `preflight` 子命令从 CRD YAML 自动生成，记录各字段路径、类型、必填约束、字段间语义依赖关系等信息，供大语言模型变异时参考。

```bash
python main.py preflight --context data/CassOp/context.json
```

---

## 目录结构

```
publish/
├── utils/
│   ├── gsod_ui.py              # 可视化调试界面后端（FastAPI，主入口）
│   └── gsod_ui_template.html   # 前端单页面应用
├── main.py                     # CLI 入口，由可视化界面以子进程方式调用
├── runner/                     # 各阶段 Runner 入口函数
│   ├── common.py               # 公共辅助函数（集群、checkpoint、配置加载）
│   ├── explore_all.py          # 关联分析入口
│   ├── testplan.py             # 测试计划生成入口
│   ├── e2e.py                  # 端到端验证入口
│   ├── fault.py                # 故障注入测试入口
│   └── workflow.py             # 完整流水线串联入口
├── phases/                     # 各阶段核心执行逻辑
│   ├── explore_all.py          # 字段差分变异与关联轨迹采集主循环
│   ├── testplan.py             # 分支覆盖导向测试用例生成主循环
│   ├── e2e.py                  # 端到端验证执行主循环
│   └── fault.py                # 故障注入测试执行主循环
├── checkpoint/
│   └── store.py                # Checkpoint 读写（中断安全写入）
├── cluster/
│   ├── env.py                  # Kind 集群生命周期管理（创建/复用/销毁）
│   └── apply.py                # CR 提交、稳态等待与插桩数据采集
├── instrumentation/
│   ├── collector.py            # 数据采集服务客户端与 port-forward 管理
│   ├── diff.py                 # 分支 index 构建与执行轨迹差分对比
│   ├── loader.py               # instrument_info.json 解析加载
│   └── source.py               # 程序分支源码上下文定位
├── llm/
│   ├── client.py               # 大语言模型 API 客户端
│   ├── prompts.py              # Prompt 模板库
│   ├── constraints.py          # CRD 字段约束提取与生成
│   └── runtime_constraints.py  # 运行时语义约束推断
├── crd/
│   ├── schema.py               # CRD 字段结构解析与提取
│   └── validation.py           # CR 合法性校验
├── relations/
│   ├── tracker.py              # 字段-分支关联关系追踪与维护
│   └── html.py                 # 关联关系 HTML 报告生成
├── report/
│   ├── explore_all.py          # 关联分析报告生成
│   ├── testplan.py             # 测试计划覆盖率报告生成
│   └── style.py                # 报告公共样式
└── core/
    ├── rich_logger.py          # 基于 Rich Live 的动态日志显示
    ├── cr_utils.py             # CR YAML 处理工具函数
    └── patch.py                # CR 差分补丁生成与应用
```

---

## 技术选型

| 技术 / 组件 | 用途 |
|---|---|
| **Python** | 测试编排、大语言模型交互、数据分析与可视化调试界面 |
| **Go** | 反向网络代理与控制器运行时跟踪模板，利用高并发特性与主流控制器同语言优势 |
| **FastAPI** | 可视化调试界面与集群内数据采集服务的 HTTP 框架 |
| **Kind** | 本地容器化 Kubernetes 集群管理，支持多版本集群快速创建与销毁 |
| **kubectl** | 集群操作接口，用于 CR 提交、Pod 管理、端口转发等 |

---

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `GSOD_INSTRUMENT_BASE` | `/mnt/d/instrument` | 插桩元数据根目录（含各控制器子目录） |
| `GSOD_DATA_BASE` | `<项目根>/data` | 控制器数据目录根路径 |
| `GSOD_WORKDIR_BASE` | `<项目根>/gsod_output_v5` | 测试输出工作目录根路径 |
| `GSOD_COLLECTOR_URL` | *(自动)* | 集群内数据采集服务 URL；未设置时自动通过 kubectl port-forward 建立连接 |
| `OPENAI_API_KEY` | *(必填)* | OpenAI 兼容 API 密钥，用于大语言模型调用 |
| `OPENAI_BASE_URL` | *(可选)* | 自定义 LLM API Base URL，支持本地部署或第三方兼容端点 |