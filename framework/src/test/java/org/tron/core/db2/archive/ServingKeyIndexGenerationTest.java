package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;
import org.tron.core.db2.archive.ServingKeyIndexGeneration.IndexLayout;
import org.tron.core.db2.archive.ServingKeyIndexGeneration.StoreCoverage;

public class ServingKeyIndexGenerationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void rebuildsExactKeysAndSeeksOnlyInsideCompleteCoverage() throws Exception {
    Path archive = temporaryFolder.newFolder("serving-index").toPath();
    List<HistoryCommitMarker> markers = new ArrayList<>();
    byte[] accountKey = bytes("same-key");
    byte[] mutableInput = Arrays.copyOf(accountKey, accountKey.length);
    byte[] collisionLeft = new byte[]{0, 31};
    byte[] collisionRight = new byte[]{1, 0};
    assertEquals(Arrays.hashCode(collisionLeft), Arrays.hashCode(collisionRight));

    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      markers.add(append(authoritative, 1,
          groups(group("account", mutableInput), group("account-asset", accountKey))));
      markers.add(append(authoritative, 2,
          groups(group("account", collisionLeft, collisionRight, bytes("other-key")))));
      markers.add(append(authoritative, 3,
          groups(group("account", accountKey))));
      append(authoritative, 4, groups(group("account", accountKey))); // not committed
      authoritative.sync();

      ServingKeyIndexGeneration generation = ServingKeyIndexGeneration.rebuild(
          "generation-1", 0, hash(0), markers, authoritative::read);
      mutableInput[0] ^= 0x7f;

      assertEquals(0, generation.getIndexedFrom());
      assertEquals(3, generation.getIndexedThrough());
      assertArrayEquals(hash(3), generation.getHeadHash());
      assertEquals(5, generation.getKeyMetadataCount());
      assertEquals(5, generation.getInlineKeyCount());
      assertEquals(0, generation.getPagedKeyCount());
      assertEquals(0, generation.getEpochPageCount());
      StoreCoverage accountCoverage = generation.getStoreCoverage("account").get();
      assertEquals(0, accountCoverage.getIndexedFrom());
      assertEquals(3, accountCoverage.getIndexedThrough());
      assertArrayEquals(hash(3), accountCoverage.getHeadHash());
      assertArrayEquals(generation.getAuthoritativePrefixDigest(),
          accountCoverage.getAuthoritativePrefixDigest());
      assertTrue(generation.getStoreCoverage("account-asset").isPresent());
      assertEquals(1, change(generation, "account", accountKey, 0, 3));
      assertEquals(3, change(generation, "account", accountKey, 1, 3));
      assertEquals(1, change(generation, "account-asset", accountKey, 0, 3));
      assertEquals(2, change(generation, "account", collisionLeft, 0, 3));
      assertEquals(2, change(generation, "account", collisionRight, 0, 3));
      assertFalse(generation.firstChangeAfter("account", accountKey, 1, 2).isPresent());
      assertFalse(generation.firstChangeAfter("account", bytes("missing"), 0, 3)
          .isPresent());
      assertThrows(IllegalArgumentException.class,
          () -> generation.firstChangeAfter("account", accountKey, -1, 3));
      assertThrows(IllegalArgumentException.class,
          () -> generation.firstChangeAfter("account", accountKey, 3, 4));
      assertThrows(IllegalArgumentException.class,
          () -> generation.firstChangeAfter("properties", accountKey, 0, 3));
    }
  }

  @Test
  public void usesInlineMetadataAndBaseEpochPagesWithoutChangingSeekSemantics()
      throws Exception {
    Path archive = temporaryFolder.newFolder("hybrid-pages").toPath();
    List<HistoryCommitMarker> markers = new ArrayList<>();
    byte[] hot = bytes("hot");
    byte[] cold = bytes("cold");
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      for (int block = 1; block <= 6; block++) {
        markers.add(append(authoritative, block, groups(block == 1
            ? group("account", cold, hot) : group("account", hot))));
      }
      authoritative.sync();

      ServingKeyIndexGeneration generation = ServingKeyIndexGeneration.rebuild(
          "generation-hybrid", 0, hash(0), markers, authoritative::read,
          new IndexLayout(2, 2));

      assertEquals(2, generation.getKeyMetadataCount());
      assertEquals(1, generation.getInlineKeyCount());
      assertEquals(1, generation.getPagedKeyCount());
      assertEquals(3, generation.getEpochPageCount());
      assertEquals(1, change(generation, "account", cold, 0, 6));
      assertFalse(generation.firstChangeAfter("account", cold, 1, 6).isPresent());
      assertEquals(1, change(generation, "account", hot, 0, 6));
      assertEquals(3, change(generation, "account", hot, 2, 6));
      assertEquals(5, change(generation, "account", hot, 4, 6));
      assertEquals(6, change(generation, "account", hot, 5, 6));
      assertFalse(generation.firstChangeAfter("account", hot, 6, 6).isPresent());
    }
  }

  @Test
  public void rejectsParticipantSetChangesInsideOneGeneration() throws Exception {
    Path archive = temporaryFolder.newFolder("participant-change").toPath();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      HistoryCommitMarker first = append(authoritative, 1,
          groups(group("account", bytes("key-1"))));
      HistoryCommitMarker second = append(authoritative, 2,
          groups(group("account", bytes("key-2"))));
      second = new HistoryCommitMarker(second.getMeta(), second.getPreviousEpoch(),
          second.getHistoryLocation(), second.getIndexLocation(), second.getBatchId(),
          Collections.singletonList("account"));
      authoritative.sync();

      HistoryCommitMarker changedParticipants = second;
      assertThrows(IllegalArgumentException.class, () -> ServingKeyIndexGeneration.rebuild(
          "generation-bad-participants", 0, hash(0),
          Arrays.asList(first, changedParticipants), authoritative::read));
    }
  }

  @Test
  public void rebuildIsDeterministicAndCatalogRejectsStalePublication() throws Exception {
    Path archive = temporaryFolder.newFolder("generation-cas").toPath();
    List<HistoryCommitMarker> markers = new ArrayList<>();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      markers.add(append(authoritative, 1, groups(group("account", bytes("key-1")))));
      markers.add(append(authoritative, 2, groups(group("account", bytes("key-2")))));
      authoritative.sync();

      ServingKeyIndexGeneration first = ServingKeyIndexGeneration.rebuild(
          "generation-1", 0, hash(0), markers, authoritative::read);
      ServingKeyIndexGeneration replacement = ServingKeyIndexGeneration.rebuild(
          "generation-2", 0, hash(0), markers, authoritative::read);
      ServingKeyIndexGeneration stale = ServingKeyIndexGeneration.rebuild(
          "generation-stale", 0, hash(0), markers, authoritative::read);
      ServingKeyIndexGeneration regressed = ServingKeyIndexGeneration.rebuild(
          "generation-regressed", 0, hash(0), Collections.emptyList(), authoritative::read);
      assertArrayEquals(first.getAuthoritativePrefixDigest(),
          replacement.getAuthoritativePrefixDigest());

      ServingKeyIndexCatalog catalog = new ServingKeyIndexCatalog(first);
      ServingKeyIndexGeneration pinned = catalog.pin();
      assertThrows(IllegalArgumentException.class, () -> catalog.publish(first, regressed));
      assertTrue(catalog.publish(first, replacement));
      assertFalse(catalog.publish(first, stale));
      assertSame(replacement, catalog.pin());
      assertEquals(1, change(pinned, "account", bytes("key-1"), 0, 2));
    }
  }

  @Test
  public void rejectsMarkerMismatchWithoutReplacingCurrentGeneration() throws Exception {
    Path archive = temporaryFolder.newFolder("corrupt-source").toPath();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      HistoryCommitMarker valid = append(authoritative, 1,
          groups(group("account", bytes("key"))));
      authoritative.sync();
      ServingKeyIndexGeneration current = ServingKeyIndexGeneration.rebuild(
          "generation-1", 0, hash(0), Collections.singletonList(valid), authoritative::read);
      ServingKeyIndexCatalog catalog = new ServingKeyIndexCatalog(current);

      HistoryLocation wrongBody = bodyLocation(99);
      HistoryCommitMarker mismatched = new HistoryCommitMarker(valid.getMeta(), 0, wrongBody,
          valid.getIndexLocation(), new byte[16], Collections.singletonList("account"));
      assertThrows(IllegalArgumentException.class, () -> ServingKeyIndexGeneration.rebuild(
          "generation-bad", 0, hash(0), Collections.singletonList(mismatched),
          authoritative::read));
      assertSame(current, catalog.pin());
    }
  }

  private static HistoryCommitMarker append(HistoryIndexStore authoritative, int block,
      List<KeyGroup> groups) throws Exception {
    BlockSnapshotMeta meta = new BlockSnapshotMeta(block, block, hash(block), hash(block - 1),
        block * 3_000L);
    HistoryLocation body = bodyLocation(block);
    HistoryIndexLocation index = authoritative.append(new HistoryIndexRecord(meta, body, groups));
    return new HistoryCommitMarker(meta, block - 1L, body, index, new byte[16],
        Arrays.asList("account", "account-asset"));
  }

  private static List<KeyGroup> groups(KeyGroup... groups) {
    return Arrays.asList(groups);
  }

  private static KeyGroup group(String dbName, byte[]... keys) {
    return new KeyGroup(dbName, Arrays.asList(keys));
  }

  private static HistoryLocation bodyLocation(int block) {
    return new HistoryLocation(0, block * 100L, 80, block, hash(block));
  }

  private static long change(ServingKeyIndexGeneration generation, String dbName, byte[] key,
      long target, long upperBound) {
    OptionalLong changed = generation.firstChangeAfter(dbName, key, target, upperBound);
    assertTrue(changed.isPresent());
    return changed.getAsLong();
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
