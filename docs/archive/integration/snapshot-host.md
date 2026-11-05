# Chainbase snapshot host

`SnapshotManager`, `Chainbase` and the snapshot classes own the revocable layers, lock boundaries,
physical writes and lifecycle. Shared checkpoint orchestration lives in
[checkpoint/common](../architecture/common-checkpoint.md).

`ChainbaseCheckpointMaterializer` remains here to call the package-private `SnapshotRoot`
checkpoint write methods. `P66CoupledMutationMaterializer` and `P66LaneStartup` apply the physical
Account/AccountAsset contract inside the active snapshot layer. These are host adapters;
checkpoint WAL, payload, coordination and runtime implementations belong to the dedicated package.


## 专属编排与观测

P66启动和配对物化归`chainbase/.../db2/core/p66`，执行归因归`db2/core/metrics`。
核心生产根12→9；SnapshotManager、SnapshotRoot、SnapshotImpl仍持有事务、锁和直接写入。
没有开放SnapshotImpl构造器或更改P66分支；依赖该构造器的归因集成测试保留在核心测试包。
P66 upstream rules测试随职责迁移。collector三实现归`common/prometheus/exporters`，
通用指标根13→10；Archive引擎策略归`config/archive`，args根14→13，配置优先级和默认值不变。
