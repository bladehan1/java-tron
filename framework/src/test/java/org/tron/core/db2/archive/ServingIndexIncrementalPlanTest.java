package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;

public class ServingIndexIncrementalPlanTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void plansOnlyCommittedSuffixAndAdvancesNoChangeStores() throws Exception {
    Path archive = temporaryFolder.newFolder("incremental-plan").toPath();
    List<HistoryCommitMarker> suffix = new ArrayList<>();
    AtomicInteger reads = new AtomicInteger();
    byte[] mutableKey = bytes("account-a");
    try (HistoryIndexStore authoritative = new HistoryIndexStore(
        archive, new HistoryIndexCodec())) {
      suffix.add(append(authoritative, 11,
          groups(group("account", mutableKey, bytes("account-b")))));
      suffix.add(append(authoritative, 12,
          groups(group("account", bytes("account-a")), group("witness", bytes("witness-a")))));
      authoritative.sync();

      ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.plan(10, hash(10),
          participants(), suffix, location -> {
            reads.incrementAndGet();
            return authoritative.read(location);
          });
      mutableKey[0] ^= 0x7f;

      assertEquals(2, reads.get());
      assertEquals(10, plan.getIndexedFrom());
      assertEquals(12, plan.getIndexedThrough());
      assertArrayEquals(hash(12), plan.getHeadHash());
      assertEquals(27, plan.getParticipatingDatabases().size());
      assertEquals(27, plan.getChangesByDatabase().size());
      assertEquals(3, plan.getChanges("account").size());
      assertEquals(11, plan.getChanges("account").get(0).getEpoch());
      assertArrayEquals(bytes("account-a"), plan.getChanges("account").get(0).getRawKey());
      assertEquals(1, plan.getChanges("witness").size());
      assertTrue(plan.getChanges("abi").isEmpty());
      assertEquals(32, plan.getDeltaSourceDigest().length);
      assertEquals(32, plan.getSourceSeedDigest().length);
      assertEquals(2, plan.getSourceStepDigests().size());
      assertThrows(UnsupportedOperationException.class,
          () -> plan.getChanges("abi").add(plan.getChanges("account").get(0)));
      assertThrows(IllegalArgumentException.class,
          () -> plan.getChanges("accountTrie"));
    }
  }

  @Test
  public void emptySuffixIsAZeroActionExact27Plan() throws Exception {
    ServingIndexIncrementalPlan plan = ServingIndexIncrementalPlan.plan(10, hash(10),
        participants(), Collections.emptyList(), ignored -> {
          throw new AssertionError("empty suffix must not read authoritative history");
        });

    assertEquals(10, plan.getIndexedFrom());
    assertEquals(10, plan.getIndexedThrough());
    assertArrayEquals(hash(10), plan.getHeadHash());
    assertTrue(plan.getChangesByDatabase().values().stream().allMatch(List::isEmpty));
    assertTrue(plan.getSourceStepDigests().isEmpty());
  }

  @Test
  public void rejectsGapBeforeReadingAuthoritativeHistory() throws Exception {
    HistoryCommitMarker gap = marker(12, 10, bodyLocation(12), indexLocation(12));
    AtomicInteger reads = new AtomicInteger();

    assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
        10, hash(10), participants(), Collections.singletonList(gap), location -> {
          reads.incrementAndGet();
          return null;
        }));
    assertEquals(0, reads.get());
  }

  @Test
  public void rejectsUnknownDuplicateAndMismatchedAuthoritativeChanges() throws Exception {
    Path unknownArchive = temporaryFolder.newFolder("unknown-incremental-plan").toPath();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(unknownArchive,
        new HistoryIndexCodec())) {
      HistoryCommitMarker unknown = append(authoritative, 11,
          groups(group("unknown", bytes("key"))));
      authoritative.sync();
      assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
          10, hash(10), participants(), Collections.singletonList(unknown), authoritative::read));
    }

    HistoryLocation duplicateBody = bodyLocation(11);
    HistoryCommitMarker duplicate = marker(11, 10, duplicateBody, indexLocation(11));
    HistoryIndexRecord duplicateRecord = new HistoryIndexRecord(meta(11), duplicateBody,
        groups(group("account", bytes("same"), bytes("same"))));
    assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
        10, hash(10), participants(), Collections.singletonList(duplicate),
        ignored -> duplicateRecord));

    Path mismatchArchive = temporaryFolder.newFolder("mismatch-incremental-plan").toPath();
    try (HistoryIndexStore authoritative = new HistoryIndexStore(mismatchArchive,
        new HistoryIndexCodec())) {
      HistoryCommitMarker valid = append(authoritative, 11,
          groups(group("account", bytes("valid"))));
      authoritative.sync();
      HistoryCommitMarker mismatched = new HistoryCommitMarker(valid.getMeta(), 10,
          bodyLocation(99), valid.getIndexLocation(), new byte[16], participants());
      assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
          10, hash(10), participants(), Collections.singletonList(mismatched),
          authoritative::read));
    }
  }

  @Test
  public void rejectsMissingCaseMismatchedOrDuplicateParticipants() {
    List<String> missing = participants();
    missing.remove("abi");
    assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
        10, hash(10), missing, Collections.emptyList(), ignored -> null));

    List<String> caseMismatch = participants();
    caseMismatch.set(caseMismatch.indexOf("DelegatedResource"), "delegatedresource");
    assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
        10, hash(10), caseMismatch, Collections.emptyList(), ignored -> null));

    List<String> duplicate = participants();
    duplicate.set(duplicate.indexOf("abi"), "account");
    assertThrows(IllegalArgumentException.class, () -> ServingIndexIncrementalPlan.plan(
        10, hash(10), duplicate, Collections.emptyList(), ignored -> null));
  }

  private static HistoryCommitMarker append(HistoryIndexStore authoritative, int block,
      List<KeyGroup> groups) throws Exception {
    HistoryLocation body = bodyLocation(block);
    HistoryIndexLocation index = authoritative.append(
        new HistoryIndexRecord(meta(block), body, groups));
    return marker(block, block - 1L, body, index);
  }

  private static HistoryCommitMarker marker(int block, long previousEpoch,
      HistoryLocation body, HistoryIndexLocation index) {
    return new HistoryCommitMarker(meta(block), previousEpoch, body, index, new byte[16],
        participants());
  }

  private static BlockSnapshotMeta meta(int block) {
    return new BlockSnapshotMeta(block, block, hash(block), hash(block - 1), block * 3_000L);
  }

  private static HistoryLocation bodyLocation(int block) {
    return new HistoryLocation(0, block * 100L, 80, block, hash(block));
  }

  private static HistoryIndexLocation indexLocation(int block) {
    return new HistoryIndexLocation(block * 120L, 100, hash(block));
  }

  private static List<String> participants() {
    return new ArrayList<>(ArchiveStoreScope.getStateDatabases());
  }

  private static List<KeyGroup> groups(KeyGroup... groups) {
    return Arrays.asList(groups);
  }

  private static KeyGroup group(String dbName, byte[]... keys) {
    return new KeyGroup(dbName, Arrays.asList(keys));
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
