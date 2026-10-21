package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable reverse delta for one committed physical 27+1 child. */
final class PathStatePhysicalReverseJournal {

  static final int MAX_ENCODED_LENGTH = 256 * 1024 * 1024;
  private static final int MAGIC = 0x5053524a; // PSRJ
  private static final short VERSION = 1;
  private static final int CHECKSUM_LENGTH = 32;
  private static final int MAX_STORES = 27;
  private static final int MAX_MUTATIONS = 1_000_000;
  private static final int MAX_VALUE_LENGTH = 16 * 1024 * 1024;
  private static final int MAX_PATH_LENGTH = 64;

  private final byte[] childTarget;
  private final byte[] parentTarget;
  private final List<StoreReverse> stores;
  private final List<Entry> superNodes;

  PathStatePhysicalReverseJournal(byte[] childTarget, byte[] parentTarget,
      List<StoreReverse> stores, List<Entry> superNodes) {
    this.childTarget = target(childTarget, "childTarget");
    this.parentTarget = target(parentTarget, "parentTarget");
    PathStatePhysicalGlobalIntent child = PathStatePhysicalGlobalIntent.decode(this.childTarget);
    PathStatePhysicalGlobalIntent parent = PathStatePhysicalGlobalIntent.decode(this.parentTarget);
    if (child.getMetadata().getBlockNumber() != parent.getMetadata().getBlockNumber() + 1
        || !Arrays.equals(child.getMetadata().getParentHash(),
        parent.getMetadata().getBlockHash())) {
      throw new IllegalArgumentException("physical reverse journal is not a direct child");
    }
    List<StoreReverse> supplied = new ArrayList<>(Objects.requireNonNull(stores, "stores"));
    if (supplied.size() > MAX_STORES) {
      throw new IllegalArgumentException("physical reverse journal has too many Stores");
    }
    int previousStoreId = 0;
    for (StoreReverse store : supplied) {
      if (store.storeId <= previousStoreId) {
        throw new IllegalArgumentException("physical reverse Store IDs are not ascending");
      }
      previousStoreId = store.storeId;
    }
    this.stores = Collections.unmodifiableList(supplied);
    this.superNodes = immutableEntries(superNodes, false);
  }

  byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      writeBytes(output, childTarget);
      writeBytes(output, parentTarget);
      output.writeInt(stores.size());
      for (StoreReverse store : stores) {
        output.writeInt(store.storeId);
        writeEntries(output, store.flatEntries);
        writeEntries(output, store.nodeEntries);
      }
      writeEntries(output, superNodes);
      output.flush();
      byte[] body = bytes.toByteArray();
      if (body.length > MAX_ENCODED_LENGTH - CHECKSUM_LENGTH) {
        throw new IllegalArgumentException("physical reverse journal exceeds byte limit");
      }
      return ByteBuffer.allocate(body.length + CHECKSUM_LENGTH).put(body)
          .put(Hashing.sha256().hashBytes(body).asBytes()).array();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory reverse journal encoding failed", impossible);
    }
  }

  static PathStatePhysicalReverseJournal decode(byte[] encoded) {
    byte[] supplied = Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length);
    if (supplied.length <= CHECKSUM_LENGTH || supplied.length > MAX_ENCODED_LENGTH) {
      throw new IllegalArgumentException("physical reverse journal length is invalid");
    }
    byte[] body = Arrays.copyOf(supplied, supplied.length - CHECKSUM_LENGTH);
    byte[] checksum = Arrays.copyOfRange(supplied, body.length, supplied.length);
    if (!Arrays.equals(checksum, Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IllegalArgumentException("physical reverse journal checksum differs");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION) {
        throw new IllegalArgumentException("physical reverse journal format is unsupported");
      }
      byte[] child = readBytes(input, PathStatePhysicalGlobalIntent.MAX_ENCODED_LENGTH);
      byte[] parent = readBytes(input, PathStatePhysicalGlobalIntent.MAX_ENCODED_LENGTH);
      int storeCount = input.readInt();
      if (storeCount < 0 || storeCount > MAX_STORES) {
        throw new IllegalArgumentException("physical reverse journal Store count is invalid");
      }
      List<StoreReverse> stores = new ArrayList<>();
      for (int index = 0; index < storeCount; index++) {
        stores.add(new StoreReverse(input.readInt(), readEntries(input, true),
            readEntries(input, false)));
      }
      List<Entry> superNodes = readEntries(input, false);
      if (input.available() != 0) {
        throw new IllegalArgumentException("physical reverse journal has trailing bytes");
      }
      return new PathStatePhysicalReverseJournal(child, parent, stores, superNodes);
    } catch (IOException truncated) {
      throw new IllegalArgumentException("physical reverse journal is truncated", truncated);
    }
  }

  byte[] getChildTarget() {
    return Arrays.copyOf(childTarget, childTarget.length);
  }

  byte[] getParentTarget() {
    return Arrays.copyOf(parentTarget, parentTarget.length);
  }

  List<StoreReverse> getStores() {
    return stores;
  }

  List<Entry> getSuperNodes() {
    return superNodes;
  }

  static final class StoreReverse {

    private final int storeId;
    private final List<Entry> flatEntries;
    private final List<Entry> nodeEntries;

    StoreReverse(int storeId, List<Entry> flatEntries, List<Entry> nodeEntries) {
      if (storeId <= 0) {
        throw new IllegalArgumentException("physical reverse Store ID must be positive");
      }
      this.storeId = storeId;
      this.flatEntries = immutableEntries(flatEntries, true);
      this.nodeEntries = immutableEntries(nodeEntries, false);
    }

    int getStoreId() {
      return storeId;
    }

    List<Entry> getFlatEntries() {
      return flatEntries;
    }

    List<Entry> getNodeEntries() {
      return nodeEntries;
    }
  }

  static final class Entry {

    private final byte[] key;
    private final byte[] oldValue;

    Entry(byte[] key, byte[] oldValue) {
      this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
      this.oldValue = oldValue == null ? null : Arrays.copyOf(oldValue, oldValue.length);
    }

    byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    byte[] getOldValue() {
      return oldValue == null ? null : Arrays.copyOf(oldValue, oldValue.length);
    }
  }

  private static List<Entry> immutableEntries(List<Entry> entries, boolean flat) {
    List<Entry> supplied = new ArrayList<>(Objects.requireNonNull(entries, "entries"));
    if (supplied.size() > MAX_MUTATIONS) {
      throw new IllegalArgumentException("physical reverse mutation count exceeds limit");
    }
    List<Entry> copies = new ArrayList<>(supplied.size());
    for (Entry entry : supplied) {
      Entry present = Objects.requireNonNull(entry, "entry");
      int expected = flat ? PathStateCommitmentCodec.ROOT_LENGTH : -1;
      if (flat && present.key.length != expected) {
        throw new IllegalArgumentException("physical reverse flat key length is invalid");
      }
      if (!flat && present.key.length > MAX_PATH_LENGTH) {
        throw new IllegalArgumentException("physical reverse node path length is invalid");
      }
      if (present.oldValue != null
          && (present.oldValue.length == 0 || present.oldValue.length > MAX_VALUE_LENGTH)) {
        throw new IllegalArgumentException("physical reverse value length is invalid");
      }
      copies.add(new Entry(present.key, present.oldValue));
    }
    return Collections.unmodifiableList(copies);
  }

  private static void writeEntries(DataOutputStream output, List<Entry> entries)
      throws IOException {
    output.writeInt(entries.size());
    for (Entry entry : entries) {
      writeBytes(output, entry.key);
      if (entry.oldValue == null) {
        output.writeInt(-1);
      } else {
        writeBytes(output, entry.oldValue);
      }
    }
  }

  private static List<Entry> readEntries(DataInputStream input, boolean flat)
      throws IOException {
    int count = input.readInt();
    if (count < 0 || count > MAX_MUTATIONS) {
      throw new IllegalArgumentException("physical reverse mutation count is invalid");
    }
    List<Entry> entries = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      byte[] key = readBytes(input, flat ? PathStateCommitmentCodec.ROOT_LENGTH
          : MAX_PATH_LENGTH);
      int valueLength = input.readInt();
      byte[] value = null;
      if (valueLength != -1) {
        if (valueLength <= 0 || valueLength > MAX_VALUE_LENGTH) {
          throw new IllegalArgumentException("physical reverse value length is invalid");
        }
        value = new byte[valueLength];
        input.readFully(value);
      }
      entries.add(new Entry(key, value));
    }
    return entries;
  }

  private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }

  private static byte[] readBytes(DataInputStream input, int maxLength) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > maxLength) {
      throw new IllegalArgumentException("physical reverse field length is invalid");
    }
    byte[] value = new byte[length];
    input.readFully(value);
    return value;
  }

  private static byte[] target(byte[] encoded, String name) {
    byte[] value = Arrays.copyOf(Objects.requireNonNull(encoded, name), encoded.length);
    if (value.length == 0 || value.length > PathStatePhysicalGlobalIntent.MAX_ENCODED_LENGTH) {
      throw new IllegalArgumentException(name + " length is invalid");
    }
    return value;
  }
}
