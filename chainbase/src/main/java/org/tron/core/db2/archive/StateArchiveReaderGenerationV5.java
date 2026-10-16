package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ChannelOpener;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ProcessFdAdmission;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.ReadObserver;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** One immutable committed reader generation and all handles owned by it. */
final class StateArchiveReaderGenerationV5 implements StateArchivePointSource, Closeable {

  private final StateArchiveCommittedViewV5 view;
  private final StateArchiveDataHandlePoolV5 handlePool;
  private final StateArchivePointReaderV5 reader;
  private boolean closed;

  private StateArchiveReaderGenerationV5(StateArchiveCommittedViewV5 view,
      StateArchiveDataHandlePoolV5 handlePool) {
    this.view = view;
    this.handlePool = handlePool;
    this.reader = new StateArchivePointReaderV5(view, handlePool);
  }

  static StateArchiveReaderGenerationV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission) throws IOException {
    return open(root, tail, target, admission, null);
  }

  static StateArchiveReaderGenerationV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission, ChannelOpener opener)
      throws IOException {
    return open(root, tail, target, admission, opener, (path, position, length) -> { });
  }

  static StateArchiveReaderGenerationV5 open(Path root, StateArchiveTailV5 tail,
      CommonCheckpointTarget target, ProcessFdAdmission admission, ChannelOpener opener,
      ReadObserver observer) throws IOException {
    StateArchiveCommittedViewV5 openedView = StateArchiveCommittedViewV5.openForPreopen(
        root, tail, target, Objects.requireNonNull(admission, "admission"),
        Objects.requireNonNull(observer, "observer"));
    try {
      StateArchiveDataHandlePoolV5 pool = opener == null
          ? StateArchiveDataHandlePoolV5.open(openedView)
          : StateArchiveDataHandlePoolV5.open(openedView, opener);
      return new StateArchiveReaderGenerationV5(openedView, pool);
    } catch (IOException | RuntimeException failure) {
      try {
        openedView.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  long getCommittedBlockNumber() {
    return view.getCommittedBlockNumber();
  }

  @Override
  public OldValue readCommittedOldValue(String dbName, byte[] rawKey, long blockNumber)
      throws IOException {
    return reader.readCommittedOldValue(dbName, rawKey, blockNumber);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      handlePool.close();
    } catch (IOException closeFailure) {
      failure = closeFailure;
    }
    try {
      view.close();
    } catch (IOException closeFailure) {
      if (failure == null) {
        failure = closeFailure;
      } else {
        failure.addSuppressed(closeFailure);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
