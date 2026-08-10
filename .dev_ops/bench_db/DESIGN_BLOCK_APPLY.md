# 固定区块导出与离线回放设计

本文记录 `feature/block_apply` 的问题定义、设计方案和方案权衡。实现与验收证据见
[`IMPLEMENTATION_BLOCK_APPLY.md`](IMPLEMENTATION_BLOCK_APPLY.md)，完整数据库
性能实验流程见 [`README.md`](README.md)。

## 1. 问题描述

### 1.1 背景

真实 P2P 同步能够反映完整业务表现，但数据库 A/B 初筛容易受到以下因素干扰：

- peer 响应速度和网络抖动；
- block fetch 批次、队列和网络线程调度；
- 数据源继续增长或不同轮次取得不同区块；
- 同步连接建立、断开及其他 peer 流量；
- API 请求和后台任务与区块处理争用资源。

为了快速判断数据库方案是否值得进入复杂测试，需要从一个固定 D1 中提取真实
区块，并在同一 D0 的不同副本上离线回放同一高度区间：

```text
停止的 D1
  block-index: height -> block ID
  block:       block ID -> protobuf
                │
                ▼
       versioned block file
                │
                ▼
D0 fresh copy -> sync processing path -> D0 + N blocks
```

该方式排除区块获取网络，但保留真实交易组合、状态依赖、共识校验、VM、
maintenance、revoking DB 和持久化行为。

### 1.2 要解决的问题

#### 可重复的数据输入

A、B 必须处理完全相同的区块。如果每轮重新向 peer 获取数据，即使高度区间相同，
数据源状态、请求节奏和连接行为也可能变化。固定文件需要成为每轮只读输入。

#### 不能只复制数据库原始 KV

直接把 `block`、`block-index` 记录写入 D0 只证明数据库可以写入原始数据，不会
执行交易、VM、maintenance 和状态变更，无法代表数据库在复杂业务中的表现。

回放必须进入节点真实的同步区块处理路径。

#### 跨数据库导出的完整性

区块需要先从 `block-index` 取得 block ID，再到 `block` 读取 protobuf。两个库
之间没有供外部工具使用的原子快照，因此导出应要求 D1 正常停止，并对高度、
父块和文件结构做二次校验。

#### 防止误修改数据库

离线回放会真实修改 D0。如果文件接错快照或用户误把生产目录当作测试副本，
可能产生不可逆的状态变化。因此命令默认只校验文件，应用必须显式授权，并在
写入前校验 D0 head。

#### 大区间内存和故障恢复

测试窗口可能包含数万乃至更多区块。实现不能一次性把全部区块加载到内存；导出
过程中发生错误也不能留下一个看起来完整的目标文件。

### 1.3 目标

- 从停止节点导出闭区间 `[start, end]` 的主链真实区块；
- 使用版本化、可流式处理的文件格式；
- 检测非法范围、缺块、protobuf 损坏、CRC 错误、区块不连续和尾随数据；
- 默认拒绝覆盖已有文件，并通过临时文件完成原子发布；
- 提供不修改 D0 的只读 verify 模式；
- 只有显式 `--apply` 才打开 D0 并执行回放；
- 首块必须为 `D0 head + 1`，parent ID 必须匹配 D0 head ID；
- 逐块通过真实同步入口处理，并在每块后校验新 head；
- 支持预热区间和最大处理块数，服务快速 preflight；
- 输出固定窗口的处理块数、耗时和 blocks/s。

### 1.4 非目标

- 不替代固定 peer 的真实 P2P 同步验收；
- 不支持在线节点的一致性导出；
- 不从任意 fork 或 raw block 记录推断主链，区块来源以 `block-index` 为准；
- 不绕过共识、交易、VM 或状态执行来制造更高的基准数字；
- 不自动复制、恢复或清理 D0；
- 不允许在同一已经应用过的 D0 上重复一轮 A/B；
- 不因工具链测试通过就声称数据库性能提升。

## 2. 设计方案

### 2.1 模块边界

```text
common
  BlockFile
    ├─ versioned header
    ├─ streaming writer
    ├─ streaming reader
    └─ structural validation

plugins / Toolkit.jar
  db block export
    └─ read block-index + block

framework / FullNode.jar
  BlockReplay
    ├─ verify-only
    └─ apply via TronNetDelegate.processBlock(block, true)
```

文件格式放在 `common`，使 Toolkit 写入端和 FullNode 读取端共享同一份协议实现，
避免两个模块分别维护编码规则。

### 2.2 区块文件格式

文件头：

| 字段 | 类型 | 说明 |
|---|---|---|
| magic | 8 bytes | `TRONBLK1` |
| version | int32 | 当前为 1 |
| start | int64 | 首块高度 |
| end | int64 | 末块高度 |
| count | int64 | 必须等于 `end - start + 1` |

每条记录：

| 字段 | 类型 | 说明 |
|---|---|---|
| height | int64 | 当前高度 |
| block ID | 32 bytes | 来源 `block-index` 的主链 ID |
| protobuf length | int32 | 最大允许 64 MiB |
| protobuf | bytes | 原始 `Protocol.Block` |
| CRC32 | int32 | protobuf 内容校验 |

选择固定头加 length-prefixed record，可以逐块读写，不需要为整个窗口分配内存。
block ID 独立保存，用于在 FullNode 使用目标链配置重新计算 ID 后进行核对。

### 2.3 导出流程

Toolkit 命令：

```text
db block export
  --database-directory <D1/database>
  --start <height>
  --end <height>
  --output <file>
  [--overwrite]
```

流程：

1. 检查 `block` 和 `block-index` 目录已经存在，避免因路径错误创建空库；
2. 按高度从 `block-index` 读取 block ID；
3. 使用 block ID 从 `block` 读取 protobuf；
4. 校验 protobuf 高度和前后父块连续性；
5. 写入与目标文件同目录的临时文件；
6. 全部成功后原子移动为目标文件；
7. 任一步失败时删除临时文件并保留原目标文件。

默认禁止覆盖，只有显式 `--overwrite` 才允许替换。

### 2.4 只读校验

`BlockReplay` 不带 `--apply` 时：

- 加载指定 config，以使用正确的链和加密引擎配置；
- 校验 magic、version、范围和 count；
- 校验每条记录的长度和 CRC；
- 解析 protobuf 并校验记录高度；
- 校验文件内部父块连续性；
- 重新计算 block ID，并与文件保存的源 block ID 比较；
- 读完整个文件并拒绝尾随数据；
- 不创建 Spring context，不打开或修改 D0。

### 2.5 离线应用

`--apply` 模式要求指定一个已存在的 D0 output directory，并强制：

```text
--p2p-disable true
```

Spring context 完成真实 Manager、Consensus、VM 和 Store 初始化，但不启动普通节点
服务。区块处理入口为：

```java
tronNetDelegate.processBlock(block, true);
```

它保留同步区块的锁、fresh-block cache 和业务指标，然后进入
`Manager.pushBlock`。不使用 `pushVerifiedBlock`，因为该入口会设置
`generatedByMyself=true` 并绕过部分外部区块校验。

应用门禁：

1. 文件 start 必须等于 `D0 head + 1`；
2. 首块 parent ID 必须等于 D0 head ID；
3. 文件 block ID 必须等于当前 config 下重新计算的 block ID；
4. 每块处理后 D0 head 高度和 ID 必须等于该块；
5. 任一校验或业务执行失败，命令立即停止。

### 2.6 计时边界

计时只包围：

```text
TronNetDelegate.processBlock(block, true)
```

不包含：

- JVM 和 Spring 启动；
- config 解析和数据库打开；
- 文件读取、CRC、protobuf 解析和 block ID 校验；
- 最终 context 关闭。

`--warmup-blocks N` 表示前 N 块仍然完整应用，但不计入 elapsed 和吞吐。
`--max-blocks N` 用于只处理文件开头的 N 块，适合 100～1,000 块 preflight。

输出使用固定 Locale，便于脚本解析：

```text
mode=apply range=[start,end] processed=N warmup=W measured=M
elapsed_ms=... blocks_per_second=...
```

### 2.7 实验使用边界

固定文件只控制区块输入；以下变量仍需实验脚本控制：

- A、B 每轮必须从同一 D0 的全新副本开始；
- 使用同一 config、JDK、JVM、CPU 和磁盘；
- 指标代码和开关保持一致；
- 每轮使用新 JVM；
- 记录是否覆盖 maintenance、flush 和 compaction；
- 离线回放用于筛选和归因，最终仍需固定 peer 同步确认端到端结果。

## 3. 备选方案与权衡

| 议题 | 备选方案 | 结论与原因 |
|---|---|---|
| 区块来源 | 每轮从 P2P 获取 | 不作为初筛；网络和 peer 行为成为干扰变量 |
| 区块来源 | 固定真实区块文件 | 采用；每轮输入完全一致且可复用 |
| 数据格式 | JSON | 放弃；体积大、转换慢且可能改变 protobuf 未知字段 |
| 数据格式 | Java serialization | 放弃；版本耦合且不适合作为稳定数据协议 |
| 数据格式 | 原始 protobuf 顺序拼接 | 不足；缺少版本、范围、ID 和损坏检测 |
| 数据格式 | 版本头 + length-prefixed protobuf + ID + CRC | 采用；流式、可校验、可演进 |
| 导出读取 | 只遍历 `block` | 放弃；raw block 可能包含非当前主链记录 |
| 导出读取 | 以 `block-index` 按高度定位 `block` | 采用；明确选择当前主链区块 |
| 在线一致性 | 允许运行节点直接导出 | 放弃；两个数据库没有外部原子快照 |
| 在线一致性 | 要求 D1 正常停止 | 采用；简单、低风险且适合固定数据源 |
| 文件写入 | 直接写目标文件 | 放弃；失败会留下看似可用的半文件 |
| 文件写入 | 同目录临时文件后原子移动 | 采用；失败时不发布不完整结果 |
| 默认行为 | 直接应用 D0 | 放弃；误操作风险高 |
| 默认行为 | verify-only，显式 `--apply` | 采用；先验证再修改 |
| 回放入口 | 直接写 block/block-index KV | 放弃；不执行复杂业务状态 |
| 回放入口 | `Manager.pushBlock` | 可执行核心业务，但少同步入口锁、cache 和 process metric |
| 回放入口 | `TronNetDelegate.processBlock(block, true)` | 采用；最贴近真实同步且不需要网络获取 |
| 回放入口 | `pushVerifiedBlock` | 放弃；会标记本地产生并绕过部分校验 |
| 工具承载 | 普通短生命周期 JUnit | 不作为用户入口；不适合真实 D0 和长窗口 |
| 工具承载 | 独立 CLI + 聚焦单测 | 采用；CLI 执行真实实验，单测验证格式和门禁 |
| 重复测试 | 在同一 D0 连续 replay | 放弃；后轮状态已变化，不能形成 A/B |
| 重复测试 | 每轮恢复 D0 新副本 | 采用；保持起始状态一致 |

## 设计边界

该设计建立的是“固定真实输入、排除网络获取”的低噪声集成测试能力。它比构造
少量假交易的单元测试更接近真实业务，但仍不包含 P2P、peer 队列和网络反压。

因此离线回放出现明显差异后，仍需在固定单 peer 的真实同步中确认方向；只有
正确性门禁、重复性和端到端结果同时成立，才能形成数据库性能结论。
