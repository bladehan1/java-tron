package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class HistoryIndexStoreTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void locatesAndBackReferencesHistoryBody() throws Exception {
    Path archive = temporaryFolder.newFolder("index").toPath();
    BlockReverseDiff diff = diff(1);
    HistoryLocation bodyLocation;
    HistoryIndexLocation indexLocation;
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec())) {
      bodyLocation = bodies.append(diff);
      indexLocation = index.append(HistoryIndexRecord.from(diff, bodyLocation));
      bodies.sync();
      index.sync();

      HistoryIndexRecord decodedIndex = index.read(indexLocation);
      assertEquals(diff.getMeta(), decodedIndex.getMeta());
      assertEquals(bodyLocation.getSegmentId(),
          decodedIndex.getHistoryLocation().getSegmentId());
      assertArrayEquals(bodyLocation.getBodyDigest(),
          decodedIndex.getHistoryLocation().getBodyDigest());
      assertEquals(diff.getMeta(), bodies.read(decodedIndex.getHistoryLocation()).getMeta());
      assertArrayEquals(bytes("key-1"), decodedIndex.getGroups().get(0).getKeys().get(0));
    }

    try (HistoryIndexStore reopened = new HistoryIndexStore(archive, new HistoryIndexCodec())) {
      assertNull(reopened.getScanResult().getInvalidTailOffset());
      assertEquals(1, reopened.getScanResult().getRecordCount());
      assertArrayEquals(indexLocation.getDigest(), reopened.getScanResult().getHead()
          .getLocation().getDigest());
    }
  }

  @Test
  public void detectsAndTruncatesCorruptIndexTail() throws Exception {
    Path archive = temporaryFolder.newFolder("corrupt-index").toPath();
    BlockReverseDiff diff = diff(1);
    HistoryIndexLocation location;
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec())) {
      HistoryLocation body = bodies.append(diff);
      location = index.append(HistoryIndexRecord.from(diff, body));
      bodies.sync();
      index.sync();
    }

    Path indexPath = archive.resolve("state_history.idx");
    try (FileChannel channel = FileChannel.open(indexPath, StandardOpenOption.WRITE)) {
      channel.position(location.getOffset() + HistoryIndexCodec.FIXED_HEADER_LENGTH);
      channel.write(ByteBuffer.wrap(new byte[]{0x7f}));
      channel.force(true);
    }
    try (HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec())) {
      assertNotNull(index.getScanResult().getInvalidTailOffset());
      assertEquals(Long.valueOf(0), index.getScanResult().getInvalidTailOffset());
      assertThrows(IllegalStateException.class,
          () -> index.append(HistoryIndexRecord.from(diff,
              new HistoryLocation(0, 0, 1, 0, new byte[32]))));
      index.truncateInvalidTail();
      assertNull(index.getScanResult().getInvalidTailOffset());
      assertEquals(0, index.getScanResult().getRecordCount());
    }
  }

  @Test
  public void indexEncodingIsDeterministic() {
    BlockReverseDiff diff = diff(1);
    HistoryLocation location = new HistoryLocation(2, 7, 99, 17, new byte[32]);
    HistoryIndexCodec codec = new HistoryIndexCodec();
    byte[] first = codec.encode(HistoryIndexRecord.from(diff, location));
    byte[] second = codec.encode(HistoryIndexRecord.from(diff, location));
    assertArrayEquals(first, second);
    assertEquals(first.length, codec.recordLength(Arrays.copyOf(first, 12)));
  }

  private static BlockReverseDiff diff(int number) {
    return new BlockReverseDiff(new BlockSnapshotMeta(number, number, hash(number),
        hash(number - 1), number * 3_000L), Collections.singletonList(
        new DbGroup("account", Collections.singletonList(
            new Entry(bytes("key-" + number), OldValue.present(bytes("value-" + number)))))));
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
