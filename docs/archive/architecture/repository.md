# Repository execution sources

This directory contains the Repository extension introduced for historical Archive execution.
The existing `actuator` module and VM execution entry points remain in use.

| Boundary | Responsibility |
| --- | --- |
| `RepositoryProvider`, `CurrentRepositoryProvider`, `HistoricalRepositoryProvider` | Select a request-owned root for current or historical execution |
| `RepositoryStateSources` | Small public read facade used by `RepositoryImpl`; constructs one current or historical source |
| `RepositoryStateSource` | Package-private read contract |
| `CurrentStoreStateSource` | Package-private adapter preserving current Chainbase reads |
| `HistoricalArchiveStateSource`, `HistoricalPropertyLog` | Package-private historical reads, request-local asset/property caches, terminal failure and absent-property logging |

`RepositoryImpl` remains in `vm/repository` as the VM overlay host. It retains root/child creation,
cache and write behavior and shares the same source facade across child frames. Its historical
root uses the same read-only mode and the same pinned session admission. Source implementations
and their constructors remain package private. Pinned sessions remain owned by their original
request lifecycle.
