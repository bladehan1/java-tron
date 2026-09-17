package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.DataFile;
import org.tron.core.db2.archive.StateArchiveCommittedViewV5.PointLocation;

/** Budget-gated preopened data handles with positioned-read leases. */
final class StateArchiveDataHandlePoolV5 implements Closeable {

  static final long MAX_ARCHIVE_DATA_HANDLES = 50_000;
  static final long PROCESS_FD_RESERVE = 8_192;
  private static final ChannelOpener DEFAULT_OPENER = path ->
      FileChannel.open(path, StandardOpenOption.READ);

  private final Map<FileKey, FileChannel> channels;
  private long activeLeases;
  private boolean closing;
  private boolean closed;

  private StateArchiveDataHandlePoolV5(Map<FileKey, FileChannel> channels) {
    this.channels = channels;
  }

  static StateArchiveDataHandlePoolV5 open(StateArchiveCommittedViewV5 view)
      throws IOException {
    return open(view, DEFAULT_OPENER);
  }

  static StateArchiveDataHandlePoolV5 open(StateArchiveCommittedViewV5 view,
      ChannelOpener opener) throws IOException {
    StateArchiveCommittedViewV5 admittedView = Objects.requireNonNull(view, "view");
    ChannelOpener admittedOpener = Objects.requireNonNull(opener, "opener");
    List<DataFile> files = admittedView.committedDataFiles();
    requireApplicationCap(files.size());

    Map<FileKey, FileChannel> opened = new LinkedHashMap<>();
    try {
      for (DataFile file : files) {
        FileKey key = new FileKey(file.getLaneId(), file.getFileId());
        FileChannel channel = Objects.requireNonNull(admittedOpener.open(file.getPath()),
            "opened channel");
        if (opened.put(key, channel) != null) {
          channel.close();
          throw new IOException("State Archive V5 duplicate committed data handle");
        }
      }
      return new StateArchiveDataHandlePoolV5(opened);
    } catch (IOException | RuntimeException failure) {
      closeAll(opened.values(), failure);
      throw failure;
    }
  }

  static void requireApplicationCap(long requiredDataHandles) throws IOException {
    if (requiredDataHandles < 0 || requiredDataHandles > MAX_ARCHIVE_DATA_HANDLES) {
      throw new IOException("State Archive V5 data handle requirement exceeds 50,000");
    }
  }

  synchronized Lease acquire(PointLocation location) throws IOException {
    Objects.requireNonNull(location, "location");
    if (closing || closed) {
      throw new IOException("State Archive V5 data handle pool is closing");
    }
    FileChannel channel = channels.get(new FileKey(location.getLaneId(),
        location.getFileId()));
    if (channel == null || !channel.isOpen()) {
      throw new IOException("State Archive V5 committed data handle is unavailable");
    }
    activeLeases++;
    return new Lease(this, channel);
  }

  synchronized int getHandleCount() {
    return channels.size();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closing = true;
    boolean interrupted = false;
    while (activeLeases > 0) {
      try {
        wait();
      } catch (InterruptedException failure) {
        interrupted = true;
      }
    }
    IOException failure = null;
    for (FileChannel channel : channels.values()) {
      try {
        channel.close();
      } catch (IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    closed = true;
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (failure != null) {
      throw failure;
    }
  }

  private synchronized void release() {
    if (activeLeases <= 0) {
      throw new IllegalStateException("State Archive V5 data handle lease underflow");
    }
    activeLeases--;
    if (activeLeases == 0) {
      notifyAll();
    }
  }

  private static void closeAll(Iterable<FileChannel> channels, Throwable failure) {
    for (FileChannel channel : channels) {
      try {
        channel.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  interface ProcessFdAdmission {
    void require(long requiredNewDataHandles, long reserveHandles) throws IOException;
  }

  interface ChannelOpener {
    FileChannel open(Path path) throws IOException;
  }

  static final class Lease implements Closeable {
    private final StateArchiveDataHandlePoolV5 owner;
    private final FileChannel channel;
    private boolean released;

    private Lease(StateArchiveDataHandlePoolV5 owner, FileChannel channel) {
      this.owner = owner;
      this.channel = channel;
    }

    FileChannel getChannel() {
      return channel;
    }

    @Override
    public void close() {
      synchronized (this) {
        if (released) {
          return;
        }
        released = true;
      }
      owner.release();
    }
  }

  private static final class FileKey {
    private final int laneId;
    private final long fileId;

    private FileKey(int laneId, long fileId) {
      this.laneId = laneId;
      this.fileId = fileId;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof FileKey)) {
        return false;
      }
      FileKey that = (FileKey) object;
      return laneId == that.laneId && fileId == that.fileId;
    }

    @Override
    public int hashCode() {
      return 31 * laneId + Long.hashCode(fileId);
    }
  }
}
