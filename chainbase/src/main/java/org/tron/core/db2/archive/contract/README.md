# Shared read contracts

`PinnedHistory` is the closeable, immutable-prefix history view shared by history readers and
request snapshots. Its authority metadata and point-read semantics are unchanged from the former
`ArchiveReadSnapshot.PinnedHistory` contract.

`ArchiveQueryLimitExceededException` retains the existing budget failure hierarchy and messages.
Serving models and range readers share it without depending on request control packages.

This package owns no files, native handles, publication state or runtime lifecycle.
