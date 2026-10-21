package org.tron.core.archive.startup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.config.args.Storage;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Result;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Status;
import org.tron.core.db2.archive.ArchiveHistoryWriter;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.SnapshotOldValueCollector;
import org.tron.core.db2.archive.StateArchiveRuntimeOwner;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.DynamicPropertiesStore;

/** Archive startup work owned by the FullNode, outside Manager's general lifecycle. */
@Slf4j
public final class StateArchiveStartup {

  private StateArchiveStartup() {
  }

  public static Result requireAdmitted(boolean enabled, Path archiveDirectory) {
    return StateArchiveBasePreflight.requireAdmitted(enabled, archiveDirectory);
  }

  /**
   * Attach a normal Archive runtime after the caller has fixed the canonical Chainbase head.
   * The caller still owns startup order and the returned runtime's lifetime.
   */
  public static Attachment attach(SnapshotManager snapshots, Path archiveDirectory, Storage storage,
      DynamicPropertiesStore dynamicProperties, BlockSnapshotMeta canonicalHead, Result admission,
      StateArchiveRuntimeOwner.ServingIndexFaultHook servingIndexFaultHook,
      StateArchiveRuntimeOwner.ReadableStateFaultHook readableStateFaultHook) throws IOException {
    StateArchiveRuntimeOwner recovered = null;
    try {
      if (admission.getStatus() == Status.EMPTY_NEW) {
        recovered = StateArchiveRuntimeOwner.bootstrapAndRecover(snapshots, archiveDirectory,
            storage.getStateArchiveMaxSegmentSize(), canonicalHead,
            servingIndexFaultHook, readableStateFaultHook);
        logger.info("State archive fresh baseline published: directory={}, head={}",
            archiveDirectory, canonicalHead.getBlockNumber());
      } else {
        recovered = StateArchiveRuntimeOwner.recover(snapshots, archiveDirectory,
            storage.getStateArchiveMaxSegmentSize(), servingIndexFaultHook,
            readableStateFaultHook);
      }
      BlockSnapshotMeta archiveHead = recovered.getRecoveredHead();
      if (archiveHead != null
          && (archiveHead.getBlockNumber()
          != dynamicProperties.getLatestBlockHeaderNumber()
          || !Arrays.equals(archiveHead.getBlockHash(),
          dynamicProperties.getLatestBlockHeaderHash().getBytes()))) {
        throw new IllegalStateException(
            "State archive committed head differs from the persisted state root");
      }
      boolean accountAssetRegistered = snapshots.getDbs().stream()
          .anyMatch(database -> "account-asset".equals(database.getDbName()));
      if (!accountAssetRegistered) {
        throw new IllegalStateException(
            "State archive requires the AccountAsset Snapshot participant");
      }
      ArchiveHistoryWriter writer = recovered.attachNormalWriter(new SnapshotOldValueCollector(),
          storage.getStateArchiveQueueCapacity(), canonicalHead);
      Attachment attachment = new Attachment(recovered, writer, archiveHead);
      recovered = null;
      return attachment;
    } catch (IOException | RuntimeException failure) {
      if (recovered != null) {
        try {
          recovered.close();
        } catch (IOException | RuntimeException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  /** The Manager accepts both fields together after startup succeeds. */
  public static final class Attachment {

    private final StateArchiveRuntimeOwner runtime;
    private final ArchiveHistoryWriter writer;
    private final BlockSnapshotMeta recoveredHead;

    private Attachment(StateArchiveRuntimeOwner runtime, ArchiveHistoryWriter writer,
        BlockSnapshotMeta recoveredHead) {
      this.runtime = runtime;
      this.writer = writer;
      this.recoveredHead = recoveredHead;
    }

    public StateArchiveRuntimeOwner getRuntime() {
      return runtime;
    }

    public ArchiveHistoryWriter getWriter() {
      return writer;
    }

    public BlockSnapshotMeta getRecoveredHead() {
      return recoveredHead;
    }
  }
}
