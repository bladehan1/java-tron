package org.tron.core.db2.archive;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotImpl;

/**
 * Immutable block boundary handed to an old-value collector.
 *
 * <p>Changed keys and post values are copied. Each database group deliberately retains a strong
 * reference to the block layer's previous snapshot until collection finishes.
 */
public final class BlockChangeView {

  private static final byte[] DIGEST_DOMAIN =
      "java-tron/block-change-view".getBytes(StandardCharsets.US_ASCII);
  private static final int DIGEST_VERSION = 1;

  private final BlockSnapshotMeta meta;
  private final List<DatabaseChanges> databases;
  private final byte[] mutationViewDigest;

  private BlockChangeView(BlockSnapshotMeta meta, List<DatabaseChanges> databases) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.databases = Collections.unmodifiableList(new ArrayList<>(databases));
    this.mutationViewDigest = digest(meta, this.databases);
  }

  public static BlockChangeView capture(BlockSnapshotMeta meta, List<Chainbase> databases) {
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

  /** Canonical identity of the exact block-final database/key/post-value view. */
  public byte[] getMutationViewDigest() {
    return Arrays.copyOf(mutationViewDigest, mutationViewDigest.length);
  }

  private static byte[] digest(BlockSnapshotMeta meta, List<DatabaseChanges> databases) {
    Hasher digest = Hashing.sha256().newHasher();
    digest.putInt(DIGEST_DOMAIN.length).putBytes(DIGEST_DOMAIN).putInt(DIGEST_VERSION)
        .putLong(meta.getEpoch()).putLong(meta.getBlockNumber()).putBytes(meta.getBlockHash())
        .putBytes(meta.getParentHash()).putLong(meta.getTimestamp()).putInt(databases.size());
    for (DatabaseChanges database : databases) {
      byte[] dbName = database.dbName.getBytes(StandardCharsets.UTF_8);
      digest.putInt(dbName.length).putBytes(dbName).putInt(database.changes.size());
      for (Change change : database.changes) {
        digest.putInt(change.key.length).putBytes(change.key)
            .putBoolean(change.postValue.present);
        if (change.postValue.present) {
          digest.putInt(change.postValue.value.length).putBytes(change.postValue.value);
        }
      }
    }
    return digest.hash().asBytes();
  }

  public static final class DatabaseChanges {
    private final String dbName;
    private final Snapshot previous;
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
      return previous.get(key);
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
