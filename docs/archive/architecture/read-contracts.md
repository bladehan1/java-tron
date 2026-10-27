# Shared Archive contracts

`PinnedHistory` is the closeable, immutable-prefix history view shared by history readers and
request snapshots. Its authority metadata and point-read semantics are unchanged from the former
`ArchiveReadSnapshot.PinnedHistory` contract.

`ArchiveQueryLimitExceededException` retains the existing budget failure hierarchy and messages.
Serving models and range readers share it without depending on request control packages.

`AccountAssetStorage` defines the physical Account, AccountAsset and properties Store names and
returns a defensive copy of the proposal-66 property key. Capture and historical queries share
these identities without introducing a dependency from writers to query implementations.

This package owns no files, native handles, publication state or runtime lifecycle.
