# Common checkpoint coordination

This package owns the shared Chainbase / PathState / Archive checkpoint protocol inside the
existing `chainbase` module. Java packages follow the directory tree.

| Directory | Responsibility | Classes |
| --- | --- | --- |
| `format` | Format and admitted baseline | `CommonCheckpointFormat`, `CommonCheckpointBaseline`, `CommonCheckpointBaselineFile` |
| `payload` | Immutable capture, codec and capture factory | `CommonCheckpointPayload`, `CommonCheckpointCapture`, `CommonCheckpointPayloadCodec`, `CommonCheckpointPayloadFactory` |
| `materialize` | Admitted materialization target | `CommonCheckpointTarget` |
| root | Redo file | `CommonCheckpointFile` |
| `materialize` | Durable materialization records | `CommonCheckpointMaterializedStore` |
| `materialize` | Participant protocol | `CommonCheckpointMaterializer` |
| root | Redo coordination | `CommonCheckpointRedoCoordinator` |
| root | Runtime and recovery | `CommonCheckpointRuntime`, `CommonCheckpointRuntimeOwner`, `CommonCheckpointRuntimeAttachment`, `CommonCheckpointHotRecovery`, `CommonCheckpointRecoveryStateAdapter` |
| root | In-memory completion after durable publication | `CommonCheckpointMemoryRebaser`, `CommonCheckpointSnapshotRebaser` |

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


The root production directory now has 9 files, down from 19 through the earlier 12-file stage; all 19 production classes remain.
The matching root test directory has 9 files, with payload, baseline and materialized-store tests in their matching subpackages.
Constructors, codec restore helpers and fault hooks retain their original visibility.

`CommonCheckpointPayloadFactory` depends on the single-method
`archive.contract.StateArchiveCheckpointBindingPlanner` to compute a transient binding.
The existing `archive.checkpoint.StateArchiveCheckpointPlanner` also implements that contract and
keeps its durable participant / prepare protocol. This keeps the payload package independent of
checkpoint implementations; the object, null-check timing and publication order remain the same.

The remaining runtime/redo/rebase and file responsibilities still need directory review.
See the [full directory backlog](../maintenance/directory-split-plan-20261027.md); reaching 10 root test files
alone does not close the corresponding directory task.


`materialize/` groups the existing participant protocol, immutable target and durable materialization
record Store. Its fault-injection constructor, stages and directory constant remain package-private;
the record Store test follows that package. The runtime still owns when the participants begin,
materialize, publish, retire and close. No format, durability ordering or failure policy changed.
The parent runtime/redo/recovery and redo-file responsibilities still need review before closing
the directory task or claiming aggregate validation.
