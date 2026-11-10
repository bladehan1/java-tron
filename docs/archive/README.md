# State Archive source guide

Archive remains in the existing Gradle modules. Read the guides below for source ownership,
integration points and package boundaries; implementation comments stay beside the code.

| Area | Guide |
| --- | --- |
| History storage, serving and queries | [Storage](architecture/storage.md) |
| Root implementation groups and remaining boundaries | [Root boundaries](integration/root-boundaries.md) |
| Shared read contracts | [Read contracts](architecture/read-contracts.md) |
| PathStateRoot packages | [PathStateRoot layout](architecture/stateroot-layout.md) |
| Directory split backlog and candidates | [All 32 directory tasks](maintenance/directory-split-plan-20261027.md) |
| Archive branch directories over 10 files | [Directory inventory](maintenance/directory-inventory-20261027.md) |
| Common checkpoint | [Checkpoint coordination](architecture/common-checkpoint.md) |
| Snapshot host | [Snapshot ownership](integration/snapshot-host.md) |
| Historical execution | [Execution](architecture/execution.md) |
| Repository adapters | [Repository sources](architecture/repository.md) |
| VM Repository host | [VM host](integration/vm-repository-host.md) |
| Serving/physical P1 extraction | [P1 scope and validation](maintenance/p1-layout-20261027.md) |
| Node storage and comparison cleanup | [Retention rule](operations/node-storage-retention.md) |
| FullNode startup and request integration | [Node integration](integration/node-integration.md) |

These guides were moved out of source directories. The [relocation manifest](maintenance/source-document-moves.json)
records their former paths and original hashes so historical references can be traced without
rewriting evidence. General source guides live in [advanced configuration](../advanced-configuration.md)
and [protocol sources](../protocol-source.md).
