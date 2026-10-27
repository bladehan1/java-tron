# State Archive source guide

Archive remains in the existing Gradle modules. Read the guides below for source ownership,
integration points and package boundaries; implementation comments stay beside the code.

| Area | Guide |
| --- | --- |
| History storage, serving and queries | [Storage](storage.md) |
| Root implementation groups and remaining boundaries | [Root boundaries](root-boundaries.md) |
| Shared read contracts | [Read contracts](read-contracts.md) |
| PathStateRoot packages | [PathStateRoot layout](stateroot-layout.md) |
| Directory split backlog and candidates | [All 32 directory tasks](directory-split-plan-20261027.md) |
| Archive branch directories over 10 files | [Directory inventory](directory-inventory-20261027.md) |
| Common checkpoint | [Checkpoint coordination](common-checkpoint.md) |
| Snapshot host | [Snapshot ownership](snapshot-host.md) |
| Historical execution | [Execution](execution.md) |
| Repository adapters | [Repository sources](repository.md) |
| VM Repository host | [VM host](vm-repository-host.md) |
| FullNode startup and request integration | [Node integration](node-integration.md) |

These guides were moved out of source directories. The [relocation manifest](source-document-moves.json)
records their former paths and original hashes so historical references can be traced without
rewriting evidence. General source guides live in [advanced configuration](../advanced-configuration.md)
and [protocol sources](../protocol-source.md).
