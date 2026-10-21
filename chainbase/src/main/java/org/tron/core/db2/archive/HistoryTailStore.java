package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.tron.core.db2.checkpoint.common.CommonCheckpointTarget;

/** History-owned two-slot record: a prepared suffix preserves the Common boundary. */
final class HistoryTailStore implements Closeable {
  static final String FILE_NAME = "ARCHIVE_TAIL";
  static final int SLOT_BYTES = 4096;
  private static final int MAGIC = 0x48545331;
  private final Path root;
  private final byte[] format;
  private final long firstBlock;
  private final byte[] parentHash;
  private final FileChannel channel;
  private final Record[] records = new Record[2];

  HistoryTailStore(Path root, byte[] format, long firstBlock, byte[] parentHash)
      throws IOException {
    this.root = root;
    this.format = Arrays.copyOf(format, format.length);
    this.firstBlock = firstBlock;
    this.parentHash = Arrays.copyOf(parentHash, parentHash.length);
    Path path = root.resolve(FILE_NAME);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      Path temporary = root.resolve(".ARCHIVE_TAIL.tmp");
      Files.createDirectories(root);
      try (FileChannel file = FileChannel.open(temporary, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        ByteBuffer bytes = ByteBuffer.allocate(2 * SLOT_BYTES);
        bytes.put(encode(0, 1, null));
        bytes.position(0);
        while (bytes.hasRemaining()) {
          file.write(bytes);
        }
        file.force(true);
      }
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
      HistorySegmentStore.syncDirectory(root);
    }
    if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("History Tail is not a regular file");
    }
    channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    try {
      if (channel.size() != 2L * SLOT_BYTES) {
        throw new IOException("History Tail slot file length differs");
      }
      for (int slot = 0; slot < 2; slot++) {
        records[slot] = decode(slot, read(slot));
      }
      if (records[0] == null && records[1] == null) {
        throw new IOException("both History Tail slots are invalid");
      }
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  synchronized StateArchiveTailV5 require(CommonCheckpointTarget target) throws IOException {
    Record selected = matching(target);
    if (selected == null) {
      throw new IOException("History Tail has no slot matching the Common commit");
    }
    return selected.tail;
  }

  synchronized void publish(StateArchiveTailV5 tail, CommonCheckpointTarget protectedTarget,
      PersistentServingKeyIndexGeneration.ExactWriteFaultHook beforeForce) throws IOException {
    Record existing = matchingTail(tail);
    if (existing != null) {
      return;
    }
    Record protectedRecord = protectedTarget == null ? baseline() : matching(protectedTarget);
    if (protectedRecord == null) {
      throw new IOException("History Tail cannot find its protected Common boundary");
    }
    long sequence = 1;
    for (Record record : records) {
      if (record != null) {
        sequence = org.tron.common.math.StrictMathWrapper.max(sequence, record.sequence + 1);
      }
    }
    int slot = 1 - protectedRecord.slot;
    byte[] encoded = encode(slot, sequence, tail);
    beforeForce.beforeWrite();
    ByteBuffer bytes = ByteBuffer.wrap(encoded);
    long offset = (long) slot * SLOT_BYTES;
    while (bytes.hasRemaining()) {
      int written = channel.write(bytes, offset);
      if (written <= 0) {
        throw new IOException("History Tail slot write made no progress");
      }
      offset += written;
    }
    channel.force(false);
    records[slot] = decode(slot, encoded);
  }

  private Record matching(CommonCheckpointTarget target) throws IOException {
    Record selected = null;
    for (Record record : records) {
      if (record == null || record.tail == null) {
        continue;
      }
      if (record.tail.getCommonBlockNumber() == target.getLastBlock().getBlockNumber()
          && Arrays.equals(record.tail.getCommonBlockHash(), target.getLastBlock().getBlockHash())
          && Arrays.equals(record.tail.getCommonTargetDigest(), target.getPayloadDigest())) {
        if (selected != null && !Arrays.equals(selected.tail.encode(), record.tail.encode())) {
          throw new IOException("History Tail slots conflict for the same Common target");
        }
        if (selected == null || selected.sequence < record.sequence) {
          selected = record;
        }
      }
    }
    return selected;
  }

  private Record matchingTail(StateArchiveTailV5 tail) throws IOException {
    for (Record record : records) {
      if (record != null && record.tail != null
          && Arrays.equals(record.tail.getCommonTargetDigest(), tail.getCommonTargetDigest())) {
        if (!Arrays.equals(record.tail.encode(), tail.encode())) {
          throw new IOException("History Tail retry identity differs");
        }
        return record;
      }
    }
    return null;
  }

  private Record baseline() {
    for (Record record : records) {
      if (record != null && record.tail == null) {
        return record;
      }
    }
    return null;
  }

  synchronized boolean hasBaseline() {
    return baseline() != null;
  }

  synchronized boolean hasOnlyBaseline() {
    return baseline() != null && Arrays.stream(records)
        .noneMatch(record -> record != null && record.tail != null);
  }

  private byte[] encode(int slot, long sequence, StateArchiveTailV5 tail) {
    ByteBuffer bytes = ByteBuffer.allocate(SLOT_BYTES);
    byte[] payload = tail == null ? new byte[0] : tail.encode();
    bytes.putInt(MAGIC).putInt(slot).putLong(sequence).put(format).putLong(firstBlock)
        .put(parentHash).putInt(payload.length).put(payload);
    bytes.putInt(SLOT_BYTES - Integer.BYTES,
        Hashing.crc32c().hashBytes(bytes.array(), 0, SLOT_BYTES - Integer.BYTES).asInt());
    return bytes.array();
  }

  private Record decode(int slot, byte[] encoded) {
    try {
      ByteBuffer bytes = ByteBuffer.wrap(encoded);
      if (bytes.getInt(SLOT_BYTES - Integer.BYTES)
          != Hashing.crc32c().hashBytes(encoded, 0, SLOT_BYTES - Integer.BYTES).asInt()
          || bytes.getInt() != MAGIC || bytes.getInt() != slot) {
        return null;
      }
      long sequence = bytes.getLong();
      byte[] boundFormat = new byte[32];
      bytes.get(boundFormat);
      long boundFirst = bytes.getLong();
      byte[] boundParent = new byte[32];
      bytes.get(boundParent);
      if (sequence <= 0 || boundFirst != firstBlock || !Arrays.equals(format, boundFormat)
          || !Arrays.equals(parentHash, boundParent)) {
        return null;
      }
      int length = bytes.getInt();
      if (length == 0) {
        return new Record(slot, sequence, null);
      }
      if (length != StateArchiveGethFormatV5.TAIL_TOTAL_LENGTH) {
        return null;
      }
      byte[] payload = new byte[length];
      bytes.get(payload);
      return new Record(slot, sequence, StateArchiveTailV5.decode(payload));
    } catch (IllegalArgumentException failure) {
      return null;
    }
  }

  private byte[] read(int slot) throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(SLOT_BYTES);
    long offset = (long) slot * SLOT_BYTES;
    while (bytes.hasRemaining()) {
      int length = channel.read(bytes, offset);
      if (length <= 0) {
        throw new IOException("History Tail is truncated");
      }
      offset += length;
    }
    return bytes.array();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private static final class Record {
    private final int slot;
    private final long sequence;
    private final StateArchiveTailV5 tail;

    private Record(int slot, long sequence, StateArchiveTailV5 tail) {
      this.slot = slot;
      this.sequence = sequence;
      this.tail = tail;
    }
  }
}
