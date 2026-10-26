# Common checkpoint coordination

This package owns the shared Chainbase / PathState / Archive checkpoint protocol inside the
existing `chainbase` module. Java packages follow the directory tree.

| Responsibility | Classes |
| --- | --- |
| Format and admitted baseline | `CommonCheckpointFormat`, `CommonCheckpointBaseline`, `CommonCheckpointBaselineFile` |
| Immutable capture and identity | `CommonCheckpointPayload`, `CommonCheckpointCapture`, `CommonCheckpointPayloadCodec`, `CommonCheckpointPayloadFactory`, `CommonCheckpointTarget` |
| Redo and durable materialization records | `CommonCheckpointFile`, `CommonCheckpointMaterializedStore` |
| Participant protocol and coordination | `CommonCheckpointMaterializer`, `CommonCheckpointRedoCoordinator` |
| Runtime and recovery | `CommonCheckpointRuntime`, `CommonCheckpointRuntimeOwner`, `CommonCheckpointRuntimeAttachment`, `CommonCheckpointHotRecovery`, `CommonCheckpointRecoveryStateAdapter` |
| In-memory completion after durable publication | `CommonCheckpointMemoryRebaser`, `CommonCheckpointSnapshotRebaser` |

`SnapshotManager` retains the host monitor, flush boundary and attachment lifecycle.
`org.tron.core.db2.core.ChainbaseCheckpointMaterializer` remains beside `SnapshotRoot`: it is the
physical write adapter and uses package-private checkpoint mutation methods. Moving that adapter
would require exposing the write authority. Its protocol dependency points to this directory.
`CommonCheckpointFile.fileName()` supplies the stable redo filename to host admission checks;
the filename, format bytes and durability sequence are unchanged.

Package-private constructors, fault hooks, timing records and coordination methods stay package
private. Tests for these details live in the matching package under `framework/src/test/java`.
A test-only `ChainbaseCheckpointTestSupport` in the host package supplies physical fault injection
and block-artifact attachment without expanding production access.
