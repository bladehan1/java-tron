# RocksDB block-cache trace bridge

This optional JNI library exposes RocksDB's native per-level block-cache trace to java-tron.
It is loaded only when `blockCacheTraceDbAllowList` is non-empty.

The bridge is ABI-bound to RocksDB `v9.7.4` (`3c27a3dde0993210c5cc30d99717093f7537916f`).
The Java runtime also rejects versions other than `9.7.x` before loading it.

Build on the target Linux architecture. The bridge uses Linux dynamic-loader APIs and CMake
rejects non-Linux hosts explicitly:

```bash
git clone --depth 1 --branch v9.7.4 https://github.com/facebook/rocksdb.git \
  /tmp/rocksdb-v9.7.4
JAVA_HOME=/path/to/jdk cmake -S . -B build \
  -DROCKSDB_SOURCE_DIR=/tmp/rocksdb-v9.7.4 -DCMAKE_BUILD_TYPE=Release
JAVA_HOME=/path/to/jdk cmake --build build
ldd -r build/libjava_tron_rocksdb_trace.so
```

`ldd -r` must not report unresolved RocksDB symbols. The bridge resolves the version-specific
`DBImpl::StartBlockCacheTrace` and `DBImpl::EndBlockCacheTrace` symbols from the already loaded
rocksdbjni library; it does not link a second RocksDB engine into the JVM.

The Maven ARM rocksdbjni artifact uses the legacy libstdc++ string ABI. CMake therefore fixes
`_GLIBCXX_USE_CXX11_ABI=0`; changing it corrupts `BlockCacheTraceRecord` field offsets even when
the RocksDB source tag matches.

Example configuration:

```hocon
storage.dbSettings {
  blockCacheTraceSampleOneIn = 100
  blockCacheTraceDbAllowList = [account, account-asset, storage-row]
  # Use ["*"] or ["all"] to trace every RocksDB store with the same implementation.
  blockCacheTraceOutputDirectory = "/data/traces/run-001"
  blockCacheTraceMaxBytesPerDb = 536870912
  blockCacheTraceNativeLibrary = "/data/tools/libjava_tron_rocksdb_trace.so"
}
```

Trace output must be outside the database directory. Each database has an independent bounded CSV
file. Get requests are sampled by `get_id`, so all level events belonging to a selected Get are
retained together.
