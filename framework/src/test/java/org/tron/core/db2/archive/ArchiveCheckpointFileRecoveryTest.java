package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.context.ApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.config.args.Storage;
import org.tron.core.config.args.StorageConfig;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.CheckPointV2Store;
import org.tron.core.store.CheckTmpStore;

public class ArchiveCheckpointFileRecoveryTest {

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void v1ReopensTheSameBindingAcrossSecondRestart() throws Exception {
    Path output = temporaryFolder.newFolder("checkpoint-v1-restart").toPath();
    ArchiveWalBinding binding = binding(10);
    withStorage(output, 1, () -> {
      writeV1(bindingBytes(binding));
      assertRecoveredV1(binding);
      assertRecoveredV1(binding);
    });
  }

  @Test
  public void v1CorruptBindingFailsDuringPreflight() throws Exception {
    Path output = temporaryFolder.newFolder("checkpoint-v1-corrupt").toPath();
    withStorage(output, 1, () -> {
      byte[] corrupt = bindingBytes(binding(10));
      corrupt[corrupt.length - 1] ^= 1;
      writeV1(corrupt);
      try (CheckTmpStore checkpoint = checkpointV1()) {
        SnapshotManager snapshots = snapshots(1, checkpoint);
        try {
          assertThrows(ArchivePersistenceException.class, snapshots::check);
        } finally {
          snapshots.shutdown();
        }
      }
    });
  }

  @Test
  public void v2SelectsLatestBindingAndIgnoresEmptyBoundariesAcrossRestart() throws Exception {
    Path output = temporaryFolder.newFolder("checkpoint-v2-selection").toPath();
    ArchiveWalBinding older = binding(10);
    ArchiveWalBinding latest = binding(11);
    withStorage(output, 2, () -> {
      writeV2("1000", null); // pre-enable/before-force boundary
      writeV2("2000", bindingBytes(older));
      writeV2("3000", null); // empty boundary must not erase older authority
      writeV2("4000", bindingBytes(latest));
      writeV2("5000", null); // after-force empty boundary must not erase latest authority
      assertRecoveredV2(latest);
      assertRecoveredV2(latest);
    });
  }

  @Test
  public void v2CorruptLatestBindingFailsInsteadOfFallingBackToOlderAuthority()
      throws Exception {
    Path output = temporaryFolder.newFolder("checkpoint-v2-corrupt").toPath();
    ArchiveWalBinding older = binding(10);
    withStorage(output, 2, () -> {
      writeV2("1000", bindingBytes(older));
      byte[] corrupt = bindingBytes(binding(11));
      corrupt[corrupt.length - 1] ^= 1;
      writeV2("2000", corrupt);
      try (CheckTmpStore checkpoint = checkpointV1()) {
        SnapshotManager snapshots = snapshots(2, checkpoint);
        try {
          assertThrows(ArchivePersistenceException.class, snapshots::check);
          assertArrayEquals(older.getBatchDigest(),
              snapshots.getRecoveredArchiveWalBinding().getBatchDigest());
        } finally {
          snapshots.shutdown();
        }
      }
    });
  }

  private static void assertRecoveredV1(ArchiveWalBinding expected) throws Exception {
    try (CheckTmpStore checkpoint = checkpointV1()) {
      SnapshotManager snapshots = snapshots(1, checkpoint);
      try {
        snapshots.check();
        assertArrayEquals(expected.getBatchDigest(),
            snapshots.getRecoveredArchiveWalBinding().getBatchDigest());
      } finally {
        snapshots.shutdown();
      }
    }
  }

  private static void assertRecoveredV2(ArchiveWalBinding expected) throws Exception {
    try (CheckTmpStore checkpoint = checkpointV1()) {
      SnapshotManager snapshots = snapshots(2, checkpoint);
      try {
        snapshots.check();
        assertArrayEquals(expected.getBatchDigest(),
            snapshots.getRecoveredArchiveWalBinding().getBatchDigest());
      } finally {
        snapshots.shutdown();
      }
    }
  }

  private static SnapshotManager snapshots(int version, CheckTmpStore checkpoint) {
    CommonParameter.getInstance().getStorage().setCheckpointVersion(version);
    SnapshotManager snapshots = new SnapshotManager("");
    snapshots.setCheckTmpStore(checkpoint);
    snapshots.init();
    return snapshots;
  }

  private static void writeV1(byte[] encoded) throws Exception {
    try (CheckTmpStore checkpoint = checkpointV1()) {
      checkpoint.updateByBatch(Collections.singletonMap(
          ArchiveWalBinding.getCheckpointKey(), encoded));
    }
  }

  private static CheckTmpStore checkpointV1() {
    return new CheckTmpStore(mock(ApplicationContext.class));
  }

  private static void writeV2(String name, byte[] encoded) {
    try (CheckPointV2Store checkpoint = new CheckPointV2Store("checkpoint/" + name)) {
      Map<byte[], byte[]> batch = new HashMap<>();
      if (encoded != null) {
        batch.put(ArchiveWalBinding.getCheckpointKey(), encoded);
      }
      checkpoint.updateByBatch(batch);
    }
  }

  private static byte[] bindingBytes(ArchiveWalBinding binding) {
    return new ArchiveWalBindingCodec().encode(binding);
  }

  private static ArchiveWalBinding binding(int epoch) {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(epoch, epoch, hash(epoch),
        hash(epoch - 1), epoch * 1_000L);
    return new ArchiveWalBinding(meta, meta, epoch - 1L, hash(epoch - 1),
        digest(epoch), digest(epoch + 10), digest(epoch + 20), digest(epoch + 30));
  }

  private static byte[] hash(int suffix) {
    byte[] value = new byte[32];
    value[31] = (byte) suffix;
    return value;
  }

  private static byte[] digest(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static void withStorage(Path output, int version, ThrowingRunnable action)
      throws Exception {
    CommonParameter parameters = CommonParameter.getInstance();
    Storage oldStorage = parameters.getStorage();
    Storage storage = new Storage();
    storage.setDefaultDbOptions(new StorageConfig());
    String oldOutput = parameters.outputDirectory;
    try {
      parameters.storage = storage;
      parameters.outputDirectory = output.toString();
      storage.setDbDirectory("database");
      storage.setDbEngine("LEVELDB");
      storage.setDbSync(true);
      storage.setCheckpointVersion(version);
      storage.setCheckpointSync(true);
      action.run();
    } finally {
      parameters.outputDirectory = oldOutput;
      parameters.storage = oldStorage;
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
