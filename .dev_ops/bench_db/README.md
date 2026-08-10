# java-tron 数据库性能快速校验方案

## 目标

通过处理一段固定的真实区块，快速判断数据库实现或数据库配置变化是否带来
可重复的性能收益，同时区分：

- 数据库内部行为变化；
- java-tron 区块及交易处理性能变化；
- 网络、缓存、JIT、GC、SSD 和后台任务造成的实验噪声。

本方案采用两层验证：先做低噪声的离线固定区块回放，再做固定数据源的真实
P2P 同步。离线回放用于筛选和归因，真实同步用于确认端到端收益；两者不能
互相替代。

```text
离线固定区块回放
  └─ 排除网络，快速定位 DB 差异
             │
             ▼
固定数据源真实同步
  └─ 验证网络、队列、线程调度和业务执行叠加后的收益
```

## 实验对象和控制变量

定义：

- `A`：Baseline，例如当前数据库配置；
- `B`：Candidate，例如修改后的数据库配置；
- `D0`：正常关闭、最新高度为 `H` 的同一份基础数据库；
- `D1`：固定且停止增长的数据源，至少包含测试结束高度的全部区块；
- `N_warmup`：预热区块数；
- `N_measure`：正式测量区块数。

A、B 必须满足：

- 从同一个代码基线构建，只保留待验证的数据库变量；
- 包含完全相同的指标代码，并使用相同的指标开关和抓取周期；
- 每轮都从 D0 的全新副本开始，不能复用上一轮已经同步过的数据库；
- 使用相同 JDK、JVM 参数、节点配置、磁盘、CPU 配额和数据源；
- 处理相同的固定高度区间；
- 测量期间关闭非必要 API 流量、事件插件和其他后台任务，或确保各轮负载一致。

先用 A 同步约 1,000 个区块标定速度，然后按固定时间预算换算区块数：

```text
N_warmup  = ceil(A 的标定 blocks/s × 300s)
N_measure = ceil(A 的标定 blocks/s × 1200s)
N_total   = N_warmup + N_measure
```

正式比较区间固定为：

```text
[H + N_warmup + 1, H + N_total]
```

不能让 A、B 各自运行固定时长后比较处理块数，因为两组可能处理了不同的区块
内容，交易数量、合约类型和 maintenance block 都会成为混杂变量。

测试区间应尽量同时包含：

- 普通转账和高交易数区块；
- 智能合约读写密集区块；
- 至少一个 maintenance block；
- 足以触发热点数据库 flush/compaction 的写入量。

如果测试期间没有产生新的 SST 或 compaction，只能评价立即生效的 cache/index
策略，不能据此判断 Bloom、block size、target file size 等新 SST 属性的收益。

## A/B、ABBA 和运行顺序

### 复杂测试前的低成本自检

不要一开始就进入 ABBA、随机交叉或火焰图分析。先对测试链路做一次短窗口
preflight，目的只是尽早发现实验环境和操作错误，防止在复杂测试中继续衍生
问题或把测试系统问题误诊为数据库问题。

建议 A、B 各从 D0 新副本回放或同步相同的 100～1,000 个区块，并检查：

- 构建 commit、配置 diff 和数据库变量符合预期；
- D0 的高度和 block ID 一致，D1 覆盖目标区间且保持停止增长；
- 每轮能够从 D0 干净启动、到达预定高度并正常关闭；
- 最终高度、block ID 和错误数符合预期；
- Prometheus/Grafana 能采到所需标签，采样时间和区块窗口可以对齐；
- DB 副本、JVM、日志和指标目录在轮次之间确实完成重置；
- 没有端口冲突、额外 peer、后台 API 流量或磁盘空间不足。

preflight 通过只表示“测试链路可以继续”，不证明数据库正确性完备，也不产生
正式性能结论。发现问题时应先修正测试链路并重新自检，不要带着已知异常扩大
到 ABBA 或 profile 阶段。

### 单次 A/B 是什么

最小实验只运行：

```text
A1 → B1
```

它可以快速筛选明显差异，但版本变量和运行顺序完全绑定：A 永远先跑，B 永远
后跑。因此以下现象都可能被误判为 B 的效果：

- JIT、操作系统 Page Cache 或磁盘缓存逐渐变热，导致后跑的 B 更快；
- SSD 垃圾回收、温度、compaction backlog 累积，导致后跑的 B 更慢；
- 同机其他任务或硬件频率随时间变化；
- 第一次启动特有的依赖加载、类加载和文件系统元数据开销。

所以单次 A/B 适合快速淘汰明显无效方案，不适合直接形成最终结论。

### ABBA 是什么

ABBA 仍然是 A/B 测试，只是把同一个 A/B 重复四轮，并采用对称顺序：

```text
A1 → B1 → B2 → A2
```

每一轮都必须重置为 D0 的新副本并启动新 JVM。最终分别聚合 A1/A2 和 B1/B2，
不能把 B2 接着 B1 的数据库继续运行。

它主要抵消随时间近似线性的漂移。假设四轮位于时间点 1、2、3、4：

```text
A 的平均时间位置 = (1 + 4) / 2 = 2.5
B 的平均时间位置 = (2 + 3) / 2 = 2.5
```

因此 A、B 不再分别绑定“早跑”和“晚跑”。ABBA 不能消除所有噪声，但比一次
`A→B` 更容易识别顺序效应，成本约为后者的两倍。

ABBA 不作为默认起步动作。应先完成 preflight 和一次简单的 `A1→B1`；只有
出现值得解释的明显差异、Candidate 方案基本确定并准备形成可信结论时，才补
`B2→A2` 形成 ABBA，用来排除顺序和环境漂移干扰。初筛没有差异或 Candidate
在测量前失败时，没有必要直接增加 ABBA 的复杂度。

也可以使用方向相反但同样对称的：

```text
B1 → A1 → A2 → B2    # BAAB
```

在多天或多台机器重复实验时，可以让一半使用 ABBA，另一半使用 BAAB。

### 其他可选设计

| 设计 | 顺序示例 | 用途 | 局限 |
|---|---|---|---|
| A/A | `A1 → A2` | 测量实验自身噪声，校验脚本和重置是否可靠 | 不比较 Candidate |
| 单次 A/B | `A1 → B1` | 最快初筛 | 无法分离版本和顺序效应 |
| 反向复测 | `A1 → B1 → B2 → A2` | 即 ABBA，适合单机快速确认 | 仍可能受非线性漂移影响 |
| 成对交叉 | 第一天 `A→B`，第二天 `B→A` | 跨天抵消顺序影响 | 环境跨天变化可能较大 |
| 随机交叉 | 随机排列多个 A/B 新副本 | 轮次较多时更稳健，可做置信区间 | 成本更高，必须提前冻结随机顺序 |
| 并行 A/B | 两台相同机器同时运行 A、B | 抵消共同时间变化 | 机器差异会替代顺序成为混杂变量；同机并行会争抢资源 |
| Latin square | A/B/C 使用 `ABC`、`BCA`、`CAB` | 比较三个以上候选 | 设计和分析更复杂 |

建议执行顺序：

1. 先做 100～1,000 块的 preflight，确认测试链路没有制造二次问题；
2. 运行一次简单的 A1→B1，快速筛选 Candidate；
3. 出现明显差异并基本确定方案后，再补 B2→A2 形成 ABBA；
4. 需要量化自然误差时补 A/A；收益接近噪声时再增加随机交叉轮次；
5. 只有差异来源仍不清楚时才进入 Arthas/async-profiler 分析。

候选改善幅度至少应大于：

```text
max(5%, 2 × A/A 或重复轮次的变异系数)
```

同时要求吞吐、CPU/block、磁盘写入/block 和关键 p95/p99 中没有不可接受的
反向退化。5% 是快速实验的初始门槛，不是固定的发布标准；最终应根据 A/A
测得的噪声调整。

## 第一层：离线固定区块回放

### 定位

不要实现成普通的短生命周期 JUnit 单元测试。更合适的是可重复启动的集成性能
基准：使用真实 D0、真实历史区块和生产处理链路，只移除 P2P 获取过程。

```text
领先数据库 D1
  block-index → block bytes
          │
          ▼ 导出 length-prefixed protobuf 文件
  blocks-H+1-to-H+N.dat
          │
          ▼
D0 新副本 → 启动真实 Manager/Consensus/VM/DB
          │
          ▼
TronNetDelegate.processBlock(block, true)
          │
          ▼
校验高度、block ID、错误数并输出性能指标
```

java-tron 已有的可复用入口包括：

- `BlockStore#getLimitNumber`：读取连续区块；
- `BlockCapsule(byte[])`：反序列化真实区块；
- `TronNetDelegate#processBlock(block, true)`：走同步区块处理路径；
- `Manager#pushBlock`：进入共识、签名、交易、VM、maintenance、revoking DB
  和持久化处理。

不要使用 `pushVerifiedBlock` 作为回放入口，因为它会设置
`generatedByMyself=true`，从而绕过部分针对外部区块的 Merkle、共识和交易签名
校验。

离线回放保留真实交易组合、状态依赖、VM、共识、maintenance、数据库读写及
compaction，能够回答：

> 相同状态、相同区块和相同 JVM 条件下，数据库方案本身是否更快？

它不能完整反映 P2P 请求、网络抖动、peer 队列、网络反压以及同步线程和网络
线程之间的竞争，因此不能单独证明真实节点同步会获得相同比例的收益。

### 固定区块文件命令

构建导出和回放工具：

```bash
./gradlew :plugins:buildToolkitJar :framework:buildFullNodeJar
```

从已经停止的 D1 节点导出闭区间 `[H+1, H+N]`。`-d` 指向实际数据库目录，
即其中直接包含 `block` 和 `block-index` 的目录：

```bash
java -jar plugins/build/libs/Toolkit.jar db block export \
  -d /data/D1/database \
  --start 68000001 \
  --end 68010000 \
  -o /data/block-files/68000001-68010000.dat
```

导出文件包含版本头、起止高度、记录数、每块的源数据库 block ID、原始
protobuf 和 CRC32。导出时检查高度及父块连续性，默认拒绝覆盖已有文件；确实
需要替换时显式使用 `--overwrite`。不要对正在运行的节点执行导出，跨库读取
无法为在线 `block-index` 和 `block` 提供一致快照。

修改 D0 前先做只读文件校验：

```bash
java -cp framework/build/libs/FullNode.jar org.tron.program.BlockReplay \
  --input /data/block-files/68000001-68010000.dat \
  --config /data/config.conf
```

从 D0 的一次性副本执行离线回放。这里 `-d` 指向节点 output directory，而不是
其内部的 `database` 子目录：

```bash
java -cp framework/build/libs/FullNode.jar org.tron.program.BlockReplay \
  --input /data/block-files/68000001-68010000.dat \
  --config /data/config.conf \
  --output-directory /data/A1 \
  --apply \
  --warmup-blocks 2000
```

安全和结果边界：

- 不带 `--apply` 时只验证文件，不打开和修改 D0；
- `--apply` 要求 output directory 已存在，并强制关闭 P2P；
- 文件首块必须是 `D0 head + 1`，其 parent ID 必须等于 D0 head ID；
- 每块通过 `TronNetDelegate.processBlock(block, true)` 进入真实同步处理路径；
- 每次应用后校验 D0 head 高度和 block ID，失败立即停止；
- `--max-blocks` 可用于 100～1,000 块 preflight；`--warmup-blocks` 只从时间统计
  中排除前段区块，不跳过实际应用；
- 输出的 `elapsed_ms` 和 `blocks_per_second` 只计逐块处理窗口，不包含 Spring、
  数据库打开及文件预检查时间；正式 A/B 的每轮必须使用 D0 的全新副本。

## 第二层：固定数据源真实同步

使用停止增长的固定 D1 作为单一 peer，让 A、B 从各自的 D0 副本同步相同区间。
数据源和被测节点之间应使用稳定的本机或局域网连接，并避免连接其他 peer。

真实同步保留网络、消息处理、队列、锁竞争和反压，是最终业务判断依据。必须
同时观察 block fetch/receive 指标：如果网络已经成为瓶颈，blocks/s 相同不能
证明两个数据库性能相同。

## 指标与证据层级

### L0：正确性和配置准入

- A、B 起始高度和 block ID 相同；
- 最终高度和 block ID 相同；
- 无 block validation、fork、DB error 或异常退出；
- `OPTIONS-*`/LOG 证明 RocksDB 参数实际生效；
- 记录测试窗口内是否发生 flush、compaction 和 write stall。

`OPTIONS-*` 只能证明配置生效，不能证明配置带来性能收益。

### L1：数据库行为

`feat/db_metric` 当前提供：

- `tron:db_operate_latency_seconds{type,db,op}`；
- `tron:db_operate_bytes{type,db,op}`；
- `tron:db_event{type,db,event}`，当前主要是 LevelDB 事件；
- `tron:db_memory_bytes{type,db,property}`；
- `tron:db_rocksdb_property{type,db,property}`：compaction/flush/write stall
  即时状态；
- `tron:db_rocksdb_ticker_total{type,db,ticker}`：cache、Bloom、level hit、
  bytes、flush/compaction 和 stall 累计量；
- 原有 `tron:db_size_bytes`、`tron:db_sst_level`。

重点计算：

- 每个 DB、每种操作的 ops/s、平均值、p95 和 p99；
- DB payload bytes/s 和每块 payload bytes；
- 每块 DB 操作次数及 DB latency sum；
- SST/L0 文件数、memtable、block cache、index/filter 内存变化；
- RocksDB cache hit、Bloom useful、compaction bytes/time 和 write stall；
- 系统磁盘 read/write bytes、IOPS、await 和 util。

注意：

- `db_operate_bytes` 是 value payload，不等于实际 WAL/SST/compaction 写盘量；
- 共享 block cache 下，不应在未确认语义前直接累加不同 DB 标签的 cache usage；
- `DbStatService` 按 `statIntervalSeconds` 采集，快速实验建议 10～30 秒；
- RocksDB Statistics 的 StatsLevel 必须在 A、B 中保持一致。

### L2：业务性能

主要指标：

- `tron:header_height`：计算 blocks/s；
- `tron:block_process_latency_seconds{sync="true"}`；
- `tron:block_push_latency_seconds`；
- `tron:process_transaction_latency_seconds{type="block"}`；
- `tron:block_transaction_count`；
- CPU、RSS、GC pause、磁盘吞吐和 await。

建议统一换算为：

- elapsed/block；
- CPU seconds/block；
- disk read/write bytes/block；
- DB operations/block；
- DB latency sum/block。

数据库内部指标用于解释原因，最终收益应由固定区块窗口内的业务吞吐、延迟和
资源消耗共同判断。

## `feat/db_metric` 合并和观测开销

`feat/db_metric` 已合并本次测试采用的最新 `upstream/develop`。开始正式实验前
仍需记录实际 commit，并确认 A、B 从同一个 commit 构建，只改变目标 DB 变量。

DB 指标独立于全局 Prometheus 默认关闭。基准节点使用：

```hocon
node.metrics.prometheus {
  enable = true
  database {
    enable = true
    statIntervalSeconds = 30
  }
}
```

每次 DB `get/put/delete/batch` 上更新 Histogram 可能影响微秒级热点路径。因此
在正式数据库 A/B 前增加一个指标开销对照：

```text
相同代码 + 相同 DB 配置 + 指标关闭
相同代码 + 相同 DB 配置 + 指标开启
```

当前实现已预绑定固定的 engine/db/op label，避免每次 DB 操作重复查找标签，
但计时和 Histogram observe 的成本仍然存在。如果开启指标后的变化已接近数据库
Candidate 的收益，只能把指标开启结果用于归因，不能外推为无监控时的绝对性能。

## Arthas / async-profiler

Arthas 或 async-profiler 用于差异出现后的定位，不建议作为每轮默认采集项。

- 优先使用低开销采样生成 CPU 或 wall-clock 火焰图；
- 重点观察 `Manager.processBlock → transaction/VM → Store → RocksDB`；
- blocks/s 下降但 DB 指标无明显变化时，检查锁等待、GC、签名验证和
  maintenance；
- 避免对每次 DB 调用做大范围 `trace/watch`，探针开销会污染热点路径。

## 推荐的快速执行阶段

| 阶段 | 内容 | 预计时间 | 结论边界 |
|---|---|---:|---|
| 0 | A/B 各 100～1,000 块 preflight + 标定 | 10～30 分钟 | 只确认构建、数据、重置、指标和执行链路可用 |
| 1 | 离线回放 A1→B1 | 30～60 分钟 | 快速筛选 DB 自身差异 |
| 2 | 固定 peer 同步 A1→B1 | 约 1～1.5 小时 | 初步端到端判断 |
| 3 | 明显差异且方案确定后补 B2→A2 | 总计约 2～3 小时 | 形成 ABBA，排除顺序漂移并确认重复性 |
| 4 | 对异常轮次采样 profile | 按需 | 定位差异来源 |

preflight 是进入复杂测试前的低成本自检，不是全数据库证明或性能结论。对于
准备采用的 Candidate，只有离线回放和真实同步方向一致、ABBA 结果可重复、
正确性门禁通过，才能认定其具有可信收益。若 Candidate 在预热阶段失败或未
进入正式测量窗口，结论只能记录为“测量前失败”，不能计算成性能退化百分比。
