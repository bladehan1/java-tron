package org.tron.core.db2.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;

/** Test-only access to physical write fault points; production access stays package private. */
public final class ChainbaseCheckpointTestSupport {

  private ChainbaseCheckpointTestSupport() {
  }

  public static void attachBlockArtifacts(SnapshotImpl layer, BlockSnapshotMeta meta,
      BlockReverseDiff archive, PathStateSnapshotDelta path) {
    layer.attachBlockArtifacts(meta, archive, path);
  }

  public static String currentFileName() {
    return ChainbaseCheckpointMaterializer.CURRENT_FILE;
  }

  public static String[] stages() {
    return Arrays.stream(ChainbaseCheckpointMaterializer.Stage.values())
        .map(Enum::name).toArray(String[]::new);
  }

  public static ChainbaseCheckpointMaterializer withFailure(Path root, byte[] format,
      List<Chainbase> databases, String failedStage) {
    return new ChainbaseCheckpointMaterializer(root, format, databases, (stage, dbName) -> {
      if (stage.name().equals(failedStage)) {
        throw new IOException("injected " + stage + " at " + dbName);
      }
    });
  }
}
