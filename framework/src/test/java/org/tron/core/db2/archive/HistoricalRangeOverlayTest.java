package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Entry;
import org.tron.core.db2.archive.HistoricalRangeOverlay.KeyRange;
import org.tron.core.db2.archive.HistoricalRangeOverlay.Limits;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;

public class HistoricalRangeOverlayTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void overlaysOneStoreWithoutLosingDeletedOrEmptyValues() throws Exception {
    ServingKeyIndexGeneration index = buildIndex("overlay");
    List<Entry> latest = Arrays.asList(
        entry("p/a", "a3"),
        entry("p/c", "c1"),
        entry("p/d", "d1"),
        entry("p/e", "e2"));
    Map<String, OldValue> oldValues = new HashMap<>();
    oldValues.put("p/a", OldValue.present(bytes("a1")));
    oldValues.put("p/b", OldValue.present(bytes("b1")));
    oldValues.put("p/e", OldValue.absent());
    oldValues.put("p/f", OldValue.present(new byte[0]));
    Map<String, Long> readAt = new HashMap<>();

    List<Entry> result = HistoricalRangeOverlay.materialize("account", 1, 3,
        KeyRange.prefix(bytes("p/")), latest, index, (dbName, key, firstChange) -> {
          assertEquals("account", dbName);
          readAt.put(text(key), firstChange);
          return oldValues.get(text(key));
        }, new Limits(10, 10, 10));

    assertEntries(result, "p/a", "a1", "p/b", "b1", "p/c", "c1", "p/d", "d1");
    assertEquals(5, result.size());
    assertArrayEquals(bytes("p/f"), result.get(4).getKey());
    assertArrayEquals(new byte[0], result.get(4).getValue());
    assertEquals(Long.valueOf(2), readAt.get("p/a"));
    assertEquals(Long.valueOf(2), readAt.get("p/b"));
    assertEquals(Long.valueOf(2), readAt.get("p/e"));
    assertEquals(Long.valueOf(2), readAt.get("p/f"));
  }

  @Test
  public void rejectsEveryBudgetOverflowInsteadOfReturningPartialResults() throws Exception {
    ServingKeyIndexGeneration index = buildIndex("limits");
    List<Entry> latest = Arrays.asList(entry("p/a", "a3"), entry("p/c", "c1"));
    HistoricalRangeOverlay.HistoricalValueReader history =
        (dbName, key, block) -> OldValue.present(bytes("old"));

    assertThrows(ArchiveQueryLimitExceededException.class,
        () -> materialize(index, latest, history, new Limits(3, 10, 10)));
    assertThrows(ArchiveQueryLimitExceededException.class,
        () -> materialize(index, latest, history, new Limits(10, 4, 10)));
    assertThrows(ArchiveQueryLimitExceededException.class,
        () -> materialize(index, latest, history, new Limits(10, 10, 3)));
  }

  @Test
  public void rejectsUnsortedOrOutOfRangeLatestInput() throws Exception {
    ServingKeyIndexGeneration index = buildIndex("validation");
    HistoricalRangeOverlay.HistoricalValueReader history =
        (dbName, key, block) -> OldValue.absent();

    assertThrows(IllegalArgumentException.class,
        () -> materialize(index, Arrays.asList(entry("p/b", "b"), entry("p/a", "a")),
            history, new Limits(10, 10, 10)));
    assertThrows(IllegalArgumentException.class,
        () -> materialize(index, Collections.singletonList(entry("q/a", "a")), history,
            new Limits(10, 10, 10)));
  }

  private ServingKeyIndexGeneration buildIndex(String name) throws Exception {
    Path archive = temporaryFolder.newFolder(name).toPath();
    List<HistoryCommitMarker> markers = new ArrayList<>();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      markers.add(append(authoritative, 1, group("account", bytes("p/c"))));
      markers.add(append(authoritative, 2,
          group("account", bytes("p/a"), bytes("p/b"), bytes("p/e"), bytes("p/f")),
          group("other", bytes("p/a"))));
      markers.add(append(authoritative, 3,
          group("account", bytes("p/a"), bytes("q/z"))));
      authoritative.sync();
      return ServingKeyIndexGeneration.rebuild(
          "generation-" + name, 0, hash(0), markers, authoritative::read);
    }
  }

  private static List<Entry> materialize(ServingKeyIndexGeneration index, List<Entry> latest,
      HistoricalRangeOverlay.HistoricalValueReader history, Limits limits) throws Exception {
    return HistoricalRangeOverlay.materialize("account", 1, 3,
        KeyRange.prefix(bytes("p/")), latest, index, history, limits);
  }

  private static HistoryCommitMarker append(HistoryIndexStore authoritative, int block,
      KeyGroup... groups) throws Exception {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(block, block, hash(block), hash(block - 1),
        block * 3_000L);
    HistoryLocation body = new HistoryLocation(0, block * 100L, 80, block, hash(block));
    HistoryIndexLocation location = authoritative.append(
        new HistoryIndexRecord(meta, body, Arrays.asList(groups)));
    return new HistoryCommitMarker(meta, block - 1L, body, location, new byte[16],
        Arrays.asList("account", "other"));
  }

  private static KeyGroup group(String dbName, byte[]... keys) {
    return new KeyGroup(dbName, Arrays.asList(keys));
  }

  private static Entry entry(String key, String value) {
    return new Entry(bytes(key), bytes(value));
  }

  private static void assertEntries(List<Entry> entries, String... keyValues) {
    for (int i = 0; i < keyValues.length; i += 2) {
      assertArrayEquals(bytes(keyValues[i]), entries.get(i / 2).getKey());
      assertArrayEquals(bytes(keyValues[i + 1]), entries.get(i / 2).getValue());
    }
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String text(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }
}
