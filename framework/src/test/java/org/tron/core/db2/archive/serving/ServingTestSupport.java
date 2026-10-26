package org.tron.core.db2.archive.serving;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.tron.core.db2.archive.history.format.V5HistoryTail;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public final class ServingTestSupport {
  private ServingTestSupport() {}

  public static final long MAX_SOURCE_BYTES = StateArchiveServingWorkerV3.MAX_SOURCE_BYTES;

  public static byte[] progressKey() {
    return PersistentServingKeyIndexGeneration.PROGRESS_KEY.clone();
  }

  public static Engine engine(PersistentServingKeyIndexGeneration generation) {
    return generation.getEngine();
  }

  public static void publishArchiveTail(
      StateArchiveServingIndexBuildCoordinatorV3 coordinator, V5HistoryTail tail)
      throws IOException {
    coordinator.publishArchiveTailV5(tail);
  }

  public static final class MutableIndex implements Closeable {
    private final PersistentServingKeyIndexGeneration.MutableIndex index;

    public MutableIndex(Path root, Engine engine) throws IOException {
      index = new PersistentServingKeyIndexGeneration.MutableIndex(root, engine);
    }

    public void publishArchiveTailV5(V5HistoryTail tail) throws IOException {
      index.publishArchiveTailV5(tail);
    }

    public V5HistoryTail archiveTailV5(CommonCheckpointTarget target) throws IOException {
      return index.archiveTailV5(target);
    }

    @Override
    public void close() throws IOException {
      index.close();
    }
  }

  @FunctionalInterface
  public interface CoordinatorFactory {
    StateArchiveServingIndexBuildCoordinatorV3 open() throws IOException;
  }

  public static final class Worker implements AutoCloseable {
    private final StateArchiveServingWorkerV3 worker;

    public Worker(
        CoordinatorFactory factory, StateArchiveServingSource source, Runnable hook, long delay) {
      worker = new StateArchiveServingWorkerV3(factory::open, source, hook, delay);
    }

    public void offer(CommonCheckpointTarget target) throws IOException {
      worker.offer(target);
    }

    public void completeInitialSync(CommonCheckpointTarget target) throws IOException {
      worker.completeInitialSync(target);
    }

    public StateArchiveServingIndexBuildCoordinatorV3.BuildProgress status() {
      return worker.status();
    }

    @Override
    public void close() throws IOException {
      worker.close();
    }
  }
}
