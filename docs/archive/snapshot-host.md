# Chainbase snapshot host

`SnapshotManager`, `Chainbase` and the snapshot classes own the revocable layers, lock boundaries,
physical writes and lifecycle. Shared checkpoint orchestration lives in
[checkpoint/common](common-checkpoint.md).

`ChainbaseCheckpointMaterializer` remains here to call the package-private `SnapshotRoot`
checkpoint write methods. `P66CoupledMutationMaterializer` and `P66LaneStartup` apply the physical
Account/AccountAsset contract inside the active snapshot layer. These are host adapters;
checkpoint WAL, payload, coordination and runtime implementations belong to the dedicated package.
