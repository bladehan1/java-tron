package org.tron.core.db2.core;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;

/** Two-barrier, idempotent redo coordinator for one durable common checkpoint. */
public final class CommonCheckpointRedoCoordinator {

  private static final Authority[] ORDER = {
      Authority.CHAINBASE, Authority.PATH_STATE, Authority.STATE_ARCHIVE};

  private final CommonCheckpointFile checkpointFile;
  private final Map<Authority, CommonCheckpointMaterializer> materializers;
  private final FaultHook faultHook;

  public CommonCheckpointRedoCoordinator(CommonCheckpointFile checkpointFile,
      CommonCheckpointMaterializer chainbase, CommonCheckpointMaterializer pathState,
      CommonCheckpointMaterializer stateArchive) {
    this(checkpointFile, chainbase, pathState, stateArchive, stage -> { });
  }

  CommonCheckpointRedoCoordinator(CommonCheckpointFile checkpointFile,
      CommonCheckpointMaterializer chainbase, CommonCheckpointMaterializer pathState,
      CommonCheckpointMaterializer stateArchive, FaultHook faultHook) {
    this.checkpointFile = Objects.requireNonNull(checkpointFile, "checkpointFile");
    this.materializers = new EnumMap<>(Authority.class);
    admit(Authority.CHAINBASE, chainbase);
    admit(Authority.PATH_STATE, pathState);
    admit(Authority.STATE_ARCHIVE, stateArchive);
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  /** Durably publishes the redo payload before applying it to any authority. */
  public synchronized RecoveryAction apply(CommonCheckpointPayload payload) throws IOException {
    checkpointFile.publish(Objects.requireNonNull(payload, "payload"));
    return redo(checkpointFile.loadRequired());
  }

  /** Resumes the only durable checkpoint, or performs no work when none exists. */
  public synchronized RecoveryAction recover() throws IOException {
    CommonCheckpointPayload payload = checkpointFile.loadIfPresent();
    return payload == null ? RecoveryAction.NO_CHECKPOINT : redo(payload);
  }

  private RecoveryAction redo(CommonCheckpointPayload payload) throws IOException {
    CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
    Map<Authority, Status> initial = inspectAll(target);
    if (initial.containsValue(Status.PUBLISHED)
        && initial.containsValue(Status.NEEDS_MATERIALIZATION)) {
      throw new IOException("common checkpoint has published authority before materialization "
          + "barrier");
    }

    for (Authority authority : ORDER) {
      CommonCheckpointMaterializer materializer = materializers.get(authority);
      if (initial.get(authority) == Status.NEEDS_MATERIALIZATION) {
        materializer.materialize(payload, target);
        requireStatus(authority, Status.MATERIALIZED, materializer.inspect(target),
            "materialization");
        faultHook.after(materializeStage(authority));
      }
    }

    Map<Authority, Status> materialized = inspectAll(target);
    if (materialized.containsValue(Status.NEEDS_MATERIALIZATION)) {
      throw new IOException("common checkpoint materialization barrier is incomplete");
    }
    for (Authority authority : ORDER) {
      CommonCheckpointMaterializer materializer = materializers.get(authority);
      if (materialized.get(authority) == Status.MATERIALIZED) {
        materializer.publish(target);
        requireStatus(authority, Status.PUBLISHED, materializer.inspect(target), "publication");
        faultHook.after(publishStage(authority));
      }
    }

    Map<Authority, Status> published = inspectAll(target);
    for (Authority authority : ORDER) {
      requireStatus(authority, Status.PUBLISHED, published.get(authority), "retirement");
    }
    faultHook.after(Stage.BEFORE_CHECKPOINT_RETIRE);
    checkpointFile.retire();
    faultHook.after(Stage.AFTER_CHECKPOINT_RETIRE);
    return RecoveryAction.COMPLETED_REDO;
  }

  private Map<Authority, Status> inspectAll(CommonCheckpointTarget target) throws IOException {
    Map<Authority, Status> statuses = new EnumMap<>(Authority.class);
    for (Authority authority : ORDER) {
      Status status = materializers.get(authority).inspect(target);
      if (status == null) {
        throw new IOException("common checkpoint " + authority + " returned null status");
      }
      statuses.put(authority, status);
    }
    return statuses;
  }

  private void admit(Authority expected, CommonCheckpointMaterializer materializer) {
    CommonCheckpointMaterializer admitted = Objects.requireNonNull(materializer,
        expected + " materializer");
    if (admitted.authority() != expected || materializers.put(expected, admitted) != null) {
      throw new IllegalArgumentException("common checkpoint materializer authority differs: "
          + expected);
    }
  }

  private static void requireStatus(Authority authority, Status expected, Status actual,
      String operation) throws IOException {
    if (actual != expected) {
      throw new IOException("common checkpoint " + authority + " " + operation
          + " returned " + actual + " instead of " + expected);
    }
  }

  private static Stage materializeStage(Authority authority) {
    switch (authority) {
      case CHAINBASE:
        return Stage.AFTER_CHAINBASE_MATERIALIZE;
      case PATH_STATE:
        return Stage.AFTER_PATH_STATE_MATERIALIZE;
      case STATE_ARCHIVE:
        return Stage.AFTER_ARCHIVE_MATERIALIZE;
      default:
        throw new IllegalArgumentException("unsupported checkpoint authority " + authority);
    }
  }

  private static Stage publishStage(Authority authority) {
    switch (authority) {
      case CHAINBASE:
        return Stage.AFTER_CHAINBASE_PUBLISH;
      case PATH_STATE:
        return Stage.AFTER_PATH_STATE_PUBLISH;
      case STATE_ARCHIVE:
        return Stage.AFTER_ARCHIVE_PUBLISH;
      default:
        throw new IllegalArgumentException("unsupported checkpoint authority " + authority);
    }
  }

  public enum RecoveryAction {
    NO_CHECKPOINT,
    COMPLETED_REDO
  }

  enum Stage {
    AFTER_CHAINBASE_MATERIALIZE,
    AFTER_PATH_STATE_MATERIALIZE,
    AFTER_ARCHIVE_MATERIALIZE,
    AFTER_CHAINBASE_PUBLISH,
    AFTER_PATH_STATE_PUBLISH,
    AFTER_ARCHIVE_PUBLISH,
    BEFORE_CHECKPOINT_RETIRE,
    AFTER_CHECKPOINT_RETIRE
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage) throws IOException;
  }
}
