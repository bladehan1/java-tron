package org.tron.core.archive.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.StorageConfig.StateArchiveAppendFileConfig;
import org.tron.core.db2.archive.checkpoint.StateArchiveAppendFileRuntime;
import org.tron.core.db2.archive.history.v5.StateArchiveAppendCheckpointMaterializerV5;
import org.tron.core.db2.archive.history.write.SnapshotOldValueCollector;
import org.tron.core.db2.checkpoint.common.CommonCheckpointBaseline;
import org.tron.core.db2.checkpoint.common.CommonCheckpointPayload;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRecoveryStateAdapter;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;
import org.tron.core.store.DynamicPropertiesStore;

/** Archive-owned setup for the FullNode common-checkpoint runtime. */
public final class StateArchiveCheckpointStartup {

  private StateArchiveCheckpointStartup() {
  }

  public static SnapshotOldValueCollector collector(SnapshotManager snapshots) {
    if (snapshots.getDbs().stream().noneMatch(database ->
        "account-asset".equals(database.getDbName()))) {
      throw new IllegalStateException(
          "Common checkpoint requires the AccountAsset Snapshot participant");
    }
    return new SnapshotOldValueCollector();
  }

  public static StateArchiveAppendFileRuntime createAppendMaterializer(Path appendDirectory,
      byte[] formatIdentity, Engine archiveEngine, CommonCheckpointBaseline baseline,
      StateArchiveAppendFileConfig appendConfig, CommonCheckpointPayload recoveryPayload,
      DynamicPropertiesStore dynamicProperties, ChainBaseManager chainBaseManager)
      throws IOException {
    final long firstBlockNumber;
    try {
      firstBlockNumber = StrictMathWrapper.addExact(baseline.getHead().getBlockNumber(), 1L);
    } catch (ArithmeticException failure) {
      throw new IOException("State Archive V5 first block overflows", failure);
    }
    CommonCheckpointRecoveryStateAdapter canonical =
        new CommonCheckpointRecoveryStateAdapter(dynamicProperties, chainBaseManager);
    byte[] baselineHistoryDigest = StateArchiveAppendCheckpointMaterializerV5
        .baselineHistoryDigest(firstBlockNumber, baseline.getHead().getBlockHash());
    return StateArchiveAppendCheckpointMaterializerV5.openForCommon(appendDirectory, formatIdentity,
        archiveEngine, firstBlockNumber, baselineHistoryDigest,
        appendConfig.getSegmentTargetBytes(), canonical::loadIfPresent, recoveryPayload == null
            ? null : CommonCheckpointTarget.from(recoveryPayload));
  }
}
