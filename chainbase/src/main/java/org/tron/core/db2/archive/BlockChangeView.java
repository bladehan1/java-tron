package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotImpl;

/**
 * Immutable block boundary handed to both block-final preparation branches.
 *
 * <p>Changed keys and post values are copied. Each database group deliberately retains a strong
 * reference to the block layer's previous snapshot until collection finishes.  The view is the
 * handoff that makes Archive old-value lookup and PathState transition construction observe the
 * same P66-materialized post values; it is not a durable record and must not escape the Snapshot
 * lifetime.
 */
public final class BlockChangeView {

  private final BlockSnapshotMeta meta;
  private final List<DatabaseChanges> databases;

  private BlockChangeView(BlockSnapshotMeta meta, List<DatabaseChanges> databases) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.databases = Collections.unmodifiableList(new ArrayList<>(databases));
  }
  // TODO BlockChangeView 不必要存在，这里似乎不需要额外捕获，直接复用snapshot 就行，因为可以根据db 不同区分，后续pathState 区别
  public static BlockChangeView capture(BlockSnapshotMeta meta, List<Chainbase> databases) {
    // Capture changed keys/post values once before the two consumers run.  Reading each Store
    // again in the Archive and PathState branches would allow different Snapshot heads or P66
    // interpretations and would defeat the common block identity contract.
    List<DatabaseChanges> changes = new ArrayList<>();
    for (Chainbase database : databases) {
      if (!ArchiveStoreScope.isStateDatabase(database.getDbName())) {
        continue;
      }
      Snapshot head = database.getHead();
      if (!Snapshot.isImpl(head)) {
        throw new IllegalStateException(
            "Block snapshot head is not SnapshotImpl for dbName=" + database.getDbName());
      }
      SnapshotImpl layer = (SnapshotImpl) head;
      List<Change> entries = new ArrayList<>();
      // TODO 这里似乎尝试区分DELETE和为空，为什么要区分这个呢？理论上不需要，而且哪来的delete ,可能会分叉？
      layer.getDb().forEach(entry -> entries.add(new Change(entry.getKey().getBytes(),
          entry.getValue().getOperator() == Value.Operator.DELETE
              ? PostValue.absent() : PostValue.present(entry.getValue().getBytes()))));
      entries.sort((left, right) -> BlockReverseDiff.compareUnsigned(left.key, right.key));
      changes.add(new DatabaseChanges(database.getDbName(), layer.getPrevious(), entries));
    }
    changes.sort((left, right) -> left.dbName.compareTo(right.dbName));
    return new BlockChangeView(meta, changes);
  }

  public BlockSnapshotMeta getMeta() {
    return meta;
  }

  public List<DatabaseChanges> getDatabases() {
    return databases;
  }

  public static final class DatabaseChanges {
    private final String dbName;
    private final Snapshot previous;
    private final ConcurrentMap<WrappedByteArray,
        PostValue> previousValues = new ConcurrentHashMap<>();
    private final List<Change> changes;

    private DatabaseChanges(String dbName, Snapshot previous, List<Change> changes) {
      this.dbName = dbName;
      this.previous = Objects.requireNonNull(previous, "previous");
      this.changes = Collections.unmodifiableList(new ArrayList<>(changes));
    }

    public String getDbName() {
      return dbName;
    }

    public byte[] getPrevious(byte[] key) {
      PostValue value = previousValues.computeIfAbsent(
          WrappedByteArray.copyOf(key), ignored -> {
            byte[] bytes = previous.get(key);
            return bytes == null ? PostValue.absent() : PostValue.present(bytes);
          });
      return value.isPresent() ? value.getValue() : null;
    }

    public List<Change> getChanges() {
      return changes;
    }
  }

  public static final class Change {
    private final byte[] key;
    private final PostValue postValue;

    private Change(byte[] key, PostValue postValue) {
      Objects.requireNonNull(key, "key");
      this.key = Arrays.copyOf(key, key.length);
      this.postValue = postValue;
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public PostValue getPostValue() {
      return postValue;
    }
  }

  public static final class PostValue {
    private static final PostValue ABSENT = new PostValue(false, null);

    private final boolean present;
    private final byte[] value;

    private PostValue(boolean present, byte[] value) {
      this.present = present;
      this.value = value;
    }

    public static PostValue absent() {
      return ABSENT;
    }

    public static PostValue present(byte[] value) {
      Objects.requireNonNull(value, "value");
      return new PostValue(true, Arrays.copyOf(value, value.length));
    }

    public boolean isPresent() {
      return present;
    }

    public byte[] getValue() {
      if (!present) {
        throw new IllegalStateException("absent post value has no bytes");
      }
      return Arrays.copyOf(value, value.length);
    }
  }
}
