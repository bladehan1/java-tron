# DB Metrics 快速性能校验设计

本文归纳 `feat/db_metric` 优化工作的三个前置部分：问题描述、设计方案，以及
设计过程中评估过的替代方案和权衡。具体代码修改与验收记录见
[`IMPLEMENTATION.md`](IMPLEMENTATION.md)，固定区块实验操作方案见
[`README.md`](README.md)。

## 1. 问题描述

### 1.1 背景

目标是通过同步或回放一段固定的真实区块，快速判断数据库实现、数据库参数或
RocksDB 表配置变化是否带来可重复的性能收益。

最终判断不能只看数据库内部数据，也不能只看节点 blocks/s，需要建立三层证据：

```text
L0 配置与正确性
  └─ 参数是否生效、区块范围是否一致、执行是否成功
              │
              ▼
L1 数据库内部行为
  └─ get/put/batch、cache、Bloom、flush、compaction、stall
              │
              ▼
L2 节点业务表现
  └─ blocks/s、block latency、CPU/block、disk bytes/block、GC
```

L0 是实验准入条件，L1 用来解释原因，L2 才是数据库方案是否值得采用的最终判断。

### 1.2 原 `feat/db_metric` 的主要问题

#### 指标和普通 Prometheus 强绑定

原实现只要启用全局 Prometheus，就会在每次 DB `get/put/delete/batch` 上启动
Histogram timer。这样存在两个问题：

- 普通节点为了使用系统、P2P 或业务指标，也被迫承担 DB 热点路径观测成本；
- 无法做“相同代码、相同 DB、DB 指标关闭/开启”的观测开销对照。

#### 快速测试采样周期不可用

`DbStatService` 原来每 6 小时采集一次 SST 和容量。一次快速测试通常只有
20～60 分钟，除启动值外基本得不到中间状态，也无法观察 compaction backlog、
write stall 或内存随区块推进的变化。

#### 数据库内部证据不足

原分支已经提供操作耗时、payload 大小和 RocksDB memory gauge，但仍缺少：

- block cache 的 data/index/filter 命中与未命中；
- Bloom filter 是否真正过滤了无效读取；
- 读取落在 memtable、L0、L1 或 L2+ 的分布；
- flush/compaction 的实际读写量；
- pending compaction、running flush/compaction 和 write stall；
- RocksDB 逻辑 bytes 与操作系统实际磁盘 IO 的交叉证据。

#### 指标本身可能成为性能变量

原实现每次操作都通过 metric key 和字符串 labels 查找 Histogram child。DB 命中
可能处于微秒级，此类重复数组分配、map 查找和 label 解析可能成为可见开销。

#### 直方图无法区分长尾 stall

DB latency Histogram 的最大显式 bucket 是 10ms，所有超过 10ms 的操作都进入
`+Inf`。这会丢失 20ms、100ms、500ms 等长尾差异，而这些长尾正是 compaction
和 IO stall 调查最关心的部分。

### 1.3 目标

- DB 指标必须独立 opt-in，默认不改变普通 Prometheus 节点的热点路径；
- 支持 10～30 分钟快速实验中的周期性状态采集；
- A、B 使用完全相同的指标实现和采集周期；
- 指标开启后尽量减少与业务无关的额外 allocation 和 label lookup；
- 同时提供操作层和 RocksDB 内部层证据；
- amd64 RocksDB JNI 5.15.10 与 aarch64 9.7.4 的属性差异不能导致采集线程退出；
- 指标能够解释结果，但不能被描述成真实业务收益本身。

### 1.4 非目标

- 不在本次修改中实现完整的固定区块导出和离线回放工具；
- 不把单元测试耗时当作生产数据库性能数据；
- 不以 value payload bytes 替代 WAL/SST/compaction 或操作系统磁盘 bytes；
- 不在尚未完成 D0/D1 固定区块实验前声称数据库性能已经提升；
- 不用指标自动决定数据库配置或执行数据库修复。

## 2. 设计方案

### 2.1 独立开关

在全局 Prometheus 下增加数据库子配置：

```hocon
node.metrics.prometheus {
  enable = true
  database {
    enable = true
    statIntervalSeconds = 30
  }
}
```

语义如下：

| 配置 | 含义 |
|---|---|
| `prometheus.enable=false` | 所有 Prometheus 指标关闭 |
| `prometheus.enable=true, database.enable=false` | 保留原有业务/系统指标，不注入 DB 热点路径指标 |
| `prometheus.enable=true, database.enable=true` | 开启 DB 操作 Histogram、周期 property 和 RocksDB Statistics |
| `statIntervalSeconds` | DB 状态采集周期，允许 5～3600 秒，默认 30 秒 |

独立开关在节点初始化、数据库打开前确定，不支持运行期动态切换。性能实验中的
metrics-off/metrics-on 对照应通过重启新 JVM 和 D0 新副本完成。

### 2.2 热点操作指标

每个 DB 实例在打开时一次性绑定固定 label 组合：

```text
engine × db × operation
```

operation 的有界集合为 `get/put/delete/batch`。预绑定对象持有对应的
`Histogram.Child`：

- 开关关闭时，child 为 `null`，热点路径只执行一次空值判断；
- 开关开启时，操作开始只调用已绑定 child 的 `startTimer()`；
- 不在每次操作中重新构造 labels 或执行 `histogram.labels(...)`。

操作指标：

| Metric | 说明 |
|---|---|
| `tron:db_operate_latency_seconds` | DB 操作耗时分布 |
| `tron:db_operate_bytes` | get 返回 value、put value、batch 非空 values 的 payload 分布 |

latency buckets 保留 1～100µs 的密集区间，并从原最大 10ms 扩展到 1s，以区分
长尾 IO/compaction stall。

### 2.3 周期状态指标

`DbStatService` 只在 database metrics 开启时注册周期任务，按
`statIntervalSeconds` 运行。

继续保留：

- `tron:db_sst_level`；
- `tron:db_size_bytes`；
- `tron:db_memory_bytes`。

新增即时 RocksDB property gauge：

```text
tron:db_rocksdb_property{type,db,property}
```

覆盖：

- pending compaction bytes；
- running compactions / flushes；
- actual delayed write rate；
- write stopped；
- immutable memtable；
- pending flush / compaction；
- background errors。

不同 RocksDB JNI 版本不支持某个 property 时，只记录 DEBUG 并跳过该 property，
不能让定时任务因异常永久停止。

### 2.4 RocksDB Statistics

database metrics 开启时，为每个 RocksDB Options 启用 Statistics，StatsLevel 固定为：

```text
EXCEPT_DETAILED_TIMERS
```

选择它是为了获得 ticker，同时避免 `ALL` 中详细 timer 带来的额外观测成本。
Prometheus 主动轮询 ticker，因此关闭 RocksDB 自带的周期 stats log dump，避免形成
第二套重复采集和日志噪声。

导出：

```text
tron:db_rocksdb_ticker_total{type,db,ticker}
```

ticker 包括：

- block cache 总体及 data/index/filter hit/miss；
- Bloom useful；
- memtable、L0、L1、L2+ hit；
- keys read/written；
- logical bytes read/written；
- flush write bytes；
- compaction read/write bytes；
- stall microseconds。

RocksDB ticker 是数据库打开以来的累计值。实现保存上次采样快照，只向 Prometheus
Counter 增加 delta；DB reset/reopen 时清空快照。

### 2.5 Native 资源生命周期

`Options.statistics()` 会返回新的 Java/native wrapper，不能在每次周期采集时反复
调用而不关闭。设计为：

1. DB 打开时获取一次 Statistics wrapper；
2. 周期任务始终复用该 wrapper；
3. DB 关闭时按 `RocksDB → Statistics wrapper → Options` 的顺序释放；
4. 设置 Options 时使用的临时 Statistics wrapper 通过 try-with-resources 立即释放，
   Options 保留 native shared reference。

### 2.6 测试和结果解释

指标开启后的正确比较方式：

```text
A(metrics on)  vs B(metrics on)   # 比较数据库候选
A(metrics off) vs A(metrics on)   # 测量观测成本
```

DB 指标只用于 L1 归因：

- `db_operate_bytes` 是 value payload，不是物理写盘量；
- shared block cache 下，各 DB 的 `block-cache-usage` 不应未经验证直接求和；
- RocksDB Statistics 与 OS `iostat`/exporter 必须同时观察；
- 最终仍以固定区块窗口的 blocks/s、latency、CPU/block、disk bytes/block 为准。

## 3. 备选方案与权衡

| 议题 | 备选方案 | 结论与原因 |
|---|---|---|
| DB 指标开关 | 跟随全局 Prometheus | 放弃；无法隔离热点路径观测成本 |
| DB 指标开关 | 独立 opt-in | 采用；普通指标和 DB 压测指标解耦 |
| 状态采集周期 | 固定 6 小时 | 放弃；快速实验拿不到中间状态 |
| 状态采集周期 | 固定 10 秒 | 放弃；普通诊断过密，缺乏环境适配 |
| 状态采集周期 | 5～3600 秒可配置、默认 30 秒 | 采用；兼顾短测和运行成本 |
| 状态触发 | HTTP 按需采集接口 | 暂缓；增加 API、安全和并发语义，不是最小修改 |
| 操作 labels | 每次调用动态查找 | 放弃；热点路径有重复 allocation/map lookup |
| 操作 labels | DB 打开时预绑定 child | 采用；label 集合固定且基数有界 |
| 操作采样 | 每 N 次记录一次 | 暂缓；会使 Histogram count/ops 解释复杂，需要额外 sample ratio |
| 内部统计 | 只使用 Java 层 timer | 不足；无法解释 cache/Bloom/compaction/stall |
| 内部统计 | 只使用 RocksDB Statistics | 不足；LevelDB 无对应能力，也缺少 java-tron 调用边界 |
| 内部统计 | Java 操作指标 + RocksDB Statistics | 采用；兼顾调用层和引擎层 |
| StatsLevel | `ALL` | 放弃；详细 timer 观测成本更高 |
| StatsLevel | `EXCEPT_DETAILED_TIMERS` | 采用；目标 ticker 可用，成本更可控 |
| RocksDB stats 输出 | 保留每 60 秒 LOG dump | 放弃；与 Prometheus 重复并产生大量日志 |
| RocksDB stats 输出 | Prometheus 主动轮询 | 采用；统一采集窗口和标签 |
| property 兼容性 | 任一不支持就失败 | 放弃；amd64/aarch64 JNI 属性集合可能不同 |
| property 兼容性 | 单项跳过并 DEBUG | 采用；保留其余指标，避免周期任务终止 |
| 初始性能实验 | 一开始直接 ABBA | 放弃；复杂度和成本过高，容易二次衍生问题 |
| 初始性能实验 | preflight → A/B → 明显差异后 ABBA | 采用；先筛选，再用 ABBA 排除顺序漂移 |
| 区块来源 | 只做真实 P2P 同步 | 不足；网络和 peer 队列会干扰 DB 归因 |
| 区块来源 | 只做 JUnit 单元测试 | 放弃；不能反映真实状态、VM 和 maintenance |
| 区块来源 | 离线真实区块回放 + 固定 peer 同步 | 目标方案；分别回答 DB 自身和端到端表现 |

## 设计边界

本设计完成的是“可用于受控实验的指标能力”。它不直接证明任何数据库参数有
性能收益。性能结论必须在同一 D0、同一区块窗口、同一代码基线和相同指标配置下
另行执行，并按 [`README.md`](README.md) 的阶段门禁记录结果。
