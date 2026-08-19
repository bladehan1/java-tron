package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;

/** One immutable physical-key archive read context pinned at {@code S(P)}. */
public final class ArchiveReadSnapshot implements Closeable {

  private final long targetBlock;
  private final long pinnedBlock;
  private final byte[] pinnedHash;
  private final ServingKeyIndex serving;
  private final PinnedLatestState latest;
  private final PinnedHistory history;
  private boolean closed;

  private ArchiveReadSnapshot(long targetBlock, long pinnedBlock, byte[] pinnedHash,
      ServingKeyIndex serving, PinnedLatestState latest, PinnedHistory history) {
    if (targetBlock > pinnedBlock) {
      throw new IllegalArgumentException("Target block must not exceed pinned block");
    }
    this.pinnedHash = copyHash(pinnedHash, "pinnedHash");
    this.serving = Objects.requireNonNull(serving, "serving");
    this.latest = Objects.requireNonNull(latest, "latest");
    this.history = Objects.requireNonNull(history, "history");
    validateIdentity(targetBlock, pinnedBlock);
    this.targetBlock = targetBlock;
    this.pinnedBlock = pinnedBlock;
  }

  /** Takes ownership of already pinned resources, including on identity-validation failure. */
  public static ArchiveReadSnapshot pin(long targetBlock, long pinnedBlock, byte[] pinnedHash,
      ServingKeyIndex serving, PinnedLatestState latest, PinnedHistory history)
      throws IOException {
    try {
      return new ArchiveReadSnapshot(targetBlock, pinnedBlock, pinnedHash, serving, latest,
          history);
    } catch (RuntimeException failure) {
      closeAfterFailedPin(serving, history, latest, failure);
      throw failure;
    }
  }

  /** Pins catalog, authoritative history, and latest-engine resources as one request unit. */
  public static ArchiveReadSnapshot pin(long targetBlock,
      PersistentServingKeyIndexCatalog catalog, ArchiveProgressEnvelope readerVisible,
      PinnedHistoryFactory historyFactory, PinnedLatestStateFactory latestFactory)
      throws IOException {
    Objects.requireNonNull(catalog, "catalog");
    Objects.requireNonNull(historyFactory, "historyFactory");
    Objects.requireNonNull(latestFactory, "latestFactory");
    PersistentServingKeyIndexGeneration serving = catalog.pin(readerVisible);
    PinnedHistory history;
    try {
      history = historyFactory.pin(serving);
    } catch (IOException | RuntimeException failure) {
      serving.close();
      throw failure;
    }
    PinnedLatestState latest;
    try {
      latest = latestFactory.pin(serving);
    } catch (IOException | RuntimeException failure) {
      try {
        history.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      serving.close();
      throw failure;
    }
    return pin(targetBlock, serving.getIndexedThrough(), serving.getHeadHash(), serving, latest,
        history);
  }

  /** Pins persistent commit/index/segment handles plus one caller-owned latest engine snapshot. */
  public static ArchiveReadSnapshot pin(long targetBlock,
      PersistentServingKeyIndexCatalog catalog, ArchiveProgressEnvelope readerVisible,
      java.nio.file.Path archiveDirectory, long maxSegmentSize,
      PinnedLatestStateFactory latestFactory) throws IOException {
    return pin(targetBlock, catalog, readerVisible,
        serving -> PersistentCommittedHistoryReader.open(
            archiveDirectory, maxSegmentSize, serving), latestFactory);
  }

  public synchronized OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
    ensureOpen();
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(physicalRawKey, "physicalRawKey");
    OptionalLong first = serving.firstChangeAfter(
        dbName, physicalRawKey, targetBlock, pinnedBlock);
    OldValue value = first.isPresent()
        ? history.read(dbName, physicalRawKey, first.getAsLong())
        : latest.get(dbName, physicalRawKey);
    if (value == null) {
      throw new IllegalStateException("Pinned latest state returned null");
    }
    return value;
  }

  public synchronized List<Entry> range(String dbName, KeyRange range, Limits limits)
      throws IOException {
    ensureOpen();
    List<Entry> pinnedLatest = latest.range(dbName, range.getLowerInclusive(),
        range.getUpperExclusive());
    if (pinnedLatest == null) {
      throw new IllegalStateException("Pinned latest range returned null");
    }
    return HistoricalRangeOverlay.materialize(dbName, targetBlock, pinnedBlock, range,
        pinnedLatest, serving, history::read, limits);
  }

  public long getTargetBlock() {
    return targetBlock;
  }

  public long getPinnedBlock() {
    return pinnedBlock;
  }

  public byte[] getPinnedHash() {
    return Arrays.copyOf(pinnedHash, pinnedHash.length);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      history.close();
    } catch (IOException e) {
      failure = e;
    }
    try {
      latest.close();
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    try {
      serving.close();
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void validateIdentity(long target, long pinned) {
    if (target < serving.getIndexedFrom() || pinned != serving.getIndexedThrough()) {
      throw new IllegalArgumentException(
          "Prototype read snapshot requires complete serving coverage through P");
    }
    if (latest.getBlockNumber() != pinned || history.getIndexedFrom() != serving.getIndexedFrom()
        || history.getIndexedThrough() != pinned
        || !Arrays.equals(pinnedHash, serving.getHeadHash())
        || !Arrays.equals(pinnedHash, latest.getBlockHash())
        || !Arrays.equals(pinnedHash, history.getHeadHash())
        || !Arrays.equals(serving.getAuthoritativePrefixDigest(),
        history.getAuthoritativePrefixDigest())) {
      throw new IllegalArgumentException("Archive read snapshot identity mismatch");
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Archive read snapshot is closed");
    }
  }

  private static byte[] copyHash(byte[] hash, String name) {
    if (hash == null || hash.length != 32) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
    return Arrays.copyOf(hash, hash.length);
  }

  private static void closeAfterFailedPin(ServingKeyIndex serving, PinnedHistory history,
      PinnedLatestState latest, RuntimeException failure) throws IOException {
    IOException closeFailure = null;
    if (history != null) {
      try {
        history.close();
      } catch (IOException e) {
        closeFailure = e;
      }
    }
    if (latest != null) {
      try {
        latest.close();
      } catch (IOException e) {
        if (closeFailure == null) {
          closeFailure = e;
        } else {
          closeFailure.addSuppressed(e);
        }
      }
    }
    if (serving != null) {
      try {
        serving.close();
      } catch (IOException e) {
        if (closeFailure == null) {
          closeFailure = e;
        } else {
          closeFailure.addSuppressed(e);
        }
      }
    }
    if (closeFailure != null) {
      failure.addSuppressed(closeFailure);
    }
  }

  public interface PinnedLatestState extends Closeable {
    long getBlockNumber();

    byte[] getBlockHash();

    /** Empty means the legacy/in-memory prototype is not bound to engine source identities. */
    default byte[] getSourceIdentityDigest() {
      return new byte[0];
    }

    OldValue get(String dbName, byte[] physicalRawKey) throws IOException;

    List<Entry> range(String dbName, byte[] lowerInclusive, byte[] upperExclusive)
        throws IOException;
  }

  public interface PinnedHistory extends Closeable {
    long getIndexedFrom();

    long getIndexedThrough();

    byte[] getHeadHash();

    byte[] getAuthoritativePrefixDigest();

    OldValue read(String dbName, byte[] physicalRawKey, long firstChangeBlock)
        throws IOException;
  }

  @FunctionalInterface
  public interface PinnedHistoryFactory {
    PinnedHistory pin(PersistentServingKeyIndexGeneration serving) throws IOException;
  }

  @FunctionalInterface
  public interface PinnedLatestStateFactory {
    PinnedLatestState pin(PersistentServingKeyIndexGeneration serving) throws IOException;
  }
}
