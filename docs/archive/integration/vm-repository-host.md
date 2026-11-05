# VM Repository host

`Repository`, `RepositoryImpl`, `Key`, `Value` and `Type` retain the VM overlay, cache, mutation and
parent/child frame contracts. Historical/current source adapters and provider selection live in
[archive/repository](../architecture/repository.md).

`RepositoryImpl.createHistoricalRoot` remains the construction hook. Reads delegate through
`RepositoryStateSources`; overlay writes and frame ownership stay in this package. The Archive
source implementations are not exposed as public classes.
