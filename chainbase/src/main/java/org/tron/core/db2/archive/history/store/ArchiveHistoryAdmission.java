package org.tron.core.db2.archive.history.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.recovery.wal.ArchiveWalBinding;

/** Read-only base identity and startup validation, without Store or manifest write access. */
public final class ArchiveHistoryAdmission {

  private ArchiveHistoryAdmission() {
  }

  public static BaseIdentity inspectBase(Path directory, List<String> participants)
      throws IOException {
    ArchiveBaseManifest.ExistingBase base =
        ArchiveBaseManifest.validateExisting(directory, participants);
    return new BaseIdentity(base.getEpoch(), base.getHash());
  }

  public static void requireFixedPoint(ArchiveHistoryWriter history, BlockSnapshotMeta head,
      ArchiveWalBinding binding, List<String> stores) throws IOException {
    ArchiveWalStartupValidator.requireFixedPoint(history, head, binding, stores);
  }

  /** A detached immutable identity; it carries no file handle or mutation capability. */
  public static final class BaseIdentity {
    private final long epoch;
    private final byte[] hash;

    private BaseIdentity(long epoch, byte[] hash) {
      this.epoch = epoch;
      this.hash = Arrays.copyOf(hash, hash.length);
    }

    public long getEpoch() {
      return epoch;
    }

    public byte[] getHash() {
      return Arrays.copyOf(hash, hash.length);
    }
  }
}
