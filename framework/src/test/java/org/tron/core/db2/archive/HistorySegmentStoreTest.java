package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class HistorySegmentStoreTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void appendsRotatesScansAndReadsRecords() throws Exception {
    Path archive = temporaryFolder.newFolder("archive").toPath();
    BlockHistoryCodec codec = new BlockHistoryCodec();
    HistoryLocation first;
    HistoryLocation second;
    try (HistorySegmentStore store = new HistorySegmentStore(archive, codec, 250)) {
      first = store.append(diff(1, randomBytes(100, 1)));
      second = store.append(diff(2, randomBytes(100, 2)));
      store.sync();
      assertEquals(0, first.getSegmentId());
      assertEquals(1, second.getSegmentId());
      assertEquals(2, store.getScanResult().getRecords().size());
      assertEquals(2, store.read(second).getMeta().getBlockNumber());
    }

    try (HistorySegmentStore reopened = new HistorySegmentStore(archive, codec, 250)) {
      assertNull(reopened.getScanResult().getInvalidTail());
      assertEquals(2, reopened.getScanResult().getRecords().size());
      assertArrayEquals(second.getBodyDigest(), reopened.getScanResult().getRecords().get(1)
          .getLocation().getBodyDigest());
    }
  }

  @Test
  public void findsAndTruncatesPartialTailBeforeAppending() throws Exception {
    Path archive = temporaryFolder.newFolder("partial").toPath();
    BlockHistoryCodec codec = new BlockHistoryCodec();
    HistoryLocation first;
    try (HistorySegmentStore store = new HistorySegmentStore(archive, codec, 4096)) {
      first = store.append(diff(1, bytes("one")));
      store.sync();
    }

    Path segment = archive.resolve("history").resolve("history.000000.dat");
    Files.write(segment, new byte[]{0x54, 0x41, 0x52}, StandardOpenOption.APPEND);
    try (HistorySegmentStore store = new HistorySegmentStore(archive, codec, 4096)) {
      assertNotNull(store.getScanResult().getInvalidTail());
      assertEquals(first.endOffset(), store.getScanResult().getInvalidTail().getOffset());
      assertThrows(IllegalStateException.class, () -> store.append(diff(2, bytes("two"))));
      store.truncateInvalidTail();
      assertNull(store.getScanResult().getInvalidTail());
      store.append(diff(2, bytes("two")));
      store.sync();
      assertEquals(2, store.getScanResult().getRecords().size());
    }
  }

  @Test
  public void rejectsHashOrEpochGaps() throws Exception {
    Path archive = temporaryFolder.newFolder("gap").toPath();
    try (HistorySegmentStore store = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096)) {
      store.append(diff(1, bytes("one")));
      BlockReverseDiff wrongParent = new BlockReverseDiff(
          new BlockSnapshotMeta(2, 2, hash(2), hash(99), 2L), Collections.emptyList());
      assertThrows(IllegalArgumentException.class, () -> store.append(wrongParent));
    }
  }

  private static BlockReverseDiff diff(int number, byte[] value) {
    return new BlockReverseDiff(new BlockSnapshotMeta(
        number, number, hash(number), hash(number - 1), number),
        Collections.singletonList(new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key-" + number), OldValue.present(value))))));
  }

  private static byte[] randomBytes(int length, int seed) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}
