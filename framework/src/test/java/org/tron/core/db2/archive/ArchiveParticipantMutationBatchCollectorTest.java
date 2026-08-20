package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.tron.common.BaseMethodTest;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.AccountAssetForwardMutationManifest.Entry;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.AccountAssetForwardProjector.Projection;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;

public class ArchiveParticipantMutationBatchCollectorTest extends BaseMethodTest {

  @Test
  public void collectsExactPostPutDeleteAndEmptyDeterministically() {
    BlockSnapshotMeta meta = meta(1);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] deleted = bytes(2, 2);
    try (Fixture first = new Fixture(participants());
        Fixture second = new Fixture(participants())) {
      first.rootPut("storage-row", deleted, bytes(1, 8));
      second.rootPut("storage-row", deleted, bytes(1, 8));
      BlockChangeView firstView = first.capture(meta, databases -> {
        databases.get("proposal").put(bytes(2, 3), bytes(1, 3));
        databases.get("code").put(bytes(2, 1), new byte[0]);
        databases.get("storage-row").delete(deleted);
      });
      BlockChangeView secondView = second.capture(meta, databases -> {
        databases.get("storage-row").delete(deleted);
        databases.get("code").put(bytes(2, 1), new byte[0]);
        databases.get("proposal").put(bytes(2, 3), bytes(1, 3));
      });
      ArchiveParticipantMutationBatchCollector collector =
          new ArchiveParticipantMutationBatchCollector();
      ArchiveTargetMutationPlan firstPlan = new ArchiveTargetMutationPlanBuilder().build(marker,
          collector.collect(marker, firstView));
      ArchiveTargetMutationPlan secondPlan = new ArchiveTargetMutationPlanBuilder().build(marker,
          collector.collect(marker, secondView));

      assertArrayEquals(new byte[0], firstPlan.getMutations("code").get(0).getValue());
      assertNull(firstPlan.getMutations("storage-row").get(0).getValue());
      assertArrayEquals(bytes(1, 3),
          firstPlan.getMutations("proposal").get(0).getValue());
      assertArrayEquals(firstPlan.digest(), secondPlan.digest());
    }
  }

  @Test
  public void accountMutationRequiresExplicitNoScanForwardProjection() {
    BlockSnapshotMeta meta = meta(1);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(3, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] canonicalAccount = bytes(3, 3);
    byte[] assetPut = bytes(3, 4);
    byte[] assetDelete = bytes(3, 5);
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector().collect(marker, view));
      AccountAssetForwardProjector projector = (key, post) -> {
        assertArrayEquals(accountKey, key);
        assertArrayEquals(rawAccount, post.getValue());
        return new Projection(BlockChangeView.PostValue.present(canonicalAccount), Arrays.asList(
            new AssetMutation(assetDelete, BlockChangeView.PostValue.absent()),
            new AssetMutation(assetPut, BlockChangeView.PostValue.present(new byte[0]))));
      };
      ArchiveParticipantMutationBatch batch =
          new ArchiveParticipantMutationBatchCollector(projector).collect(marker, view);
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker, batch);

      assertArrayEquals(canonicalAccount,
          plan.getMutations("account").get(0).getValue());
      assertArrayEquals(assetPut,
          plan.getMutations("account-asset").get(0).getKey());
      assertEquals(0, plan.getMutations("account-asset").get(0).getValue().length);
      assertArrayEquals(assetDelete,
          plan.getMutations("account-asset").get(1).getKey());
      assertNull(plan.getMutations("account-asset").get(1).getValue());
    }
  }

  @Test
  public void rejectsViewIdentityCoverageAndMissingProjectionResult() {
    BlockSnapshotMeta meta = meta(1);
    HistoryCommitMarker marker = marker(meta, participants());
    try (Fixture exact = new Fixture(participants())) {
      BlockChangeView view = exact.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector().collect(
              marker(meta(2), participants()), view));
    }

    try (Fixture incomplete = new Fixture(Collections.singletonList("code"))) {
      BlockChangeView view = incomplete.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector().collect(marker, view));
    }

    try (Fixture account = new Fixture(participants())) {
      BlockChangeView view = account.capture(meta,
          databases -> databases.get("account").put(bytes(1, 1), bytes(1, 2)));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector((key, post) -> null)
              .collect(marker, view));
    }
  }

  @Test
  public void manifestCollectsAccountCreateUpdateDeleteAndExactAssetStates() {
    BlockSnapshotMeta meta = meta(3);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] createKey = bytes(2, 1);
    byte[] updateKey = bytes(2, 2);
    byte[] deleteKey = bytes(2, 3);
    byte[] rawCreate = bytes(3, 11);
    byte[] rawUpdate = bytes(3, 12);
    byte[] canonicalCreate = bytes(3, 21);
    byte[] canonicalUpdate = bytes(3, 22);
    byte[] createAsset = assetKey(createKey, 1);
    byte[] updateAsset = assetKey(updateKey, 1);
    byte[] updateDeletedAsset = assetKey(updateKey, 2);
    byte[] deleteAsset = assetKey(deleteKey, 1);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", updateKey, bytes(3, 31));
      fixture.rootPut("account", deleteKey, bytes(3, 32));
      BlockChangeView view = fixture.capture(meta, databases -> {
        databases.get("account").put(createKey, rawCreate);
        databases.get("account").put(updateKey, rawUpdate);
        databases.get("account").delete(deleteKey);
      });
      AccountAssetForwardMutationManifest manifest =
          new AccountAssetForwardMutationManifest(marker, Arrays.asList(
              entry(createKey, rawCreate, canonicalCreate,
                  new AssetMutation(createAsset,
                      BlockChangeView.PostValue.present(new byte[0]))),
              entry(updateKey, rawUpdate, canonicalUpdate,
                  new AssetMutation(updateAsset,
                      BlockChangeView.PostValue.present(bytes(2, 41))),
                  new AssetMutation(updateDeletedAsset, BlockChangeView.PostValue.absent())),
              new Entry(deleteKey, BlockChangeView.PostValue.absent(),
                  BlockChangeView.PostValue.absent(), Collections.singletonList(
                      new AssetMutation(deleteAsset, BlockChangeView.PostValue.absent())))));

      ArchiveParticipantMutationBatch batch =
          new ArchiveParticipantMutationBatchCollector(manifest).collect(marker, view);
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker, batch);

      assertArrayEquals(canonicalCreate, plan.getMutations("account").get(0).getValue());
      assertArrayEquals(canonicalUpdate, plan.getMutations("account").get(1).getValue());
      assertNull(plan.getMutations("account").get(2).getValue());
      assertEquals(0, plan.getMutations("account-asset").get(0).getValue().length);
      assertArrayEquals(bytes(2, 41),
          plan.getMutations("account-asset").get(1).getValue());
      assertNull(plan.getMutations("account-asset").get(2).getValue());
      assertNull(plan.getMutations("account-asset").get(3).getValue());
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector(manifest).collect(marker, view));
    }
  }

  @Test
  public void manifestRejectsMissingExtraTargetAndRawValueMismatch() {
    BlockSnapshotMeta meta = meta(4);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      AccountAssetForwardMutationManifest missing =
          new AccountAssetForwardMutationManifest(marker, Collections.emptyList());
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector(missing).collect(marker, view));

      AccountAssetForwardMutationManifest extra = new AccountAssetForwardMutationManifest(marker,
          Arrays.asList(entry(accountKey, rawAccount, rawAccount),
              entry(bytes(2, 9), bytes(3, 9), bytes(3, 9))));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector(extra).collect(marker, view));

      AccountAssetForwardMutationManifest wrongRaw =
          new AccountAssetForwardMutationManifest(marker,
              Collections.singletonList(entry(accountKey, bytes(3, 8), rawAccount)));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector(wrongRaw).collect(marker, view));
    }

    BlockSnapshotMeta otherMeta = meta(5);
    HistoryCommitMarker otherMarker = marker(otherMeta, participants());
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView otherView = fixture.capture(otherMeta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      AccountAssetForwardMutationManifest wrongTarget =
          new AccountAssetForwardMutationManifest(marker,
              Collections.singletonList(entry(accountKey, rawAccount, rawAccount)));
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveParticipantMutationBatchCollector(wrongTarget)
              .collect(otherMarker, otherView));
    }
  }

  @Test
  public void manifestRejectsDuplicateCrossAccountAndUnusedEntries() {
    HistoryCommitMarker marker = marker(meta(6), participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    Entry entry = entry(accountKey, rawAccount, rawAccount);
    assertThrows(IllegalArgumentException.class,
        () -> new AccountAssetForwardMutationManifest(marker, Arrays.asList(entry, entry)));
    assertThrows(IllegalArgumentException.class,
        () -> new AccountAssetForwardMutationManifest(marker,
            Collections.singletonList(null)));
    assertThrows(IllegalArgumentException.class,
        () -> entry(accountKey, rawAccount, rawAccount,
            new AssetMutation(assetKey(accountKey, 1), BlockChangeView.PostValue.absent()),
            new AssetMutation(assetKey(accountKey, 1), BlockChangeView.PostValue.absent())));
    assertThrows(IllegalArgumentException.class,
        () -> entry(accountKey, rawAccount, rawAccount,
            new AssetMutation(bytes(3, 7), BlockChangeView.PostValue.absent())));

    AccountAssetForwardMutationManifest singleUse =
        new AccountAssetForwardMutationManifest(marker, Collections.singletonList(entry));
    singleUse.begin(marker, Collections.singletonList(accountKey));
    singleUse.project(accountKey, BlockChangeView.PostValue.present(rawAccount));
    assertThrows(ArchivePersistenceException.class,
        () -> singleUse.project(accountKey, BlockChangeView.PostValue.present(rawAccount)));
    singleUse.complete();

    AccountAssetForwardMutationManifest unused =
        new AccountAssetForwardMutationManifest(marker, Collections.singletonList(entry));
    unused.begin(marker, Collections.singletonList(accountKey));
    assertThrows(ArchivePersistenceException.class, unused::complete);
  }

  @Test
  public void recorderSealsUnorderedEventsIntoExactAccountAndAssetMutations() {
    BlockSnapshotMeta meta = meta(7);
    byte[] updateKey = bytes(2, 1);
    byte[] deleteKey = bytes(2, 2);
    byte[] rawUpdate = bytes(3, 3);
    byte[] canonicalUpdate = bytes(3, 4);
    byte[] emptyAsset = assetKey(updateKey, 1);
    byte[] deletedAsset = assetKey(updateKey, 2);
    byte[] removedAccountAsset = assetKey(deleteKey, 1);
    AccountAssetForwardMutationRecorder recorder =
        new AccountAssetForwardMutationRecorder(meta, limits());

    recorder.recordAssetDelete(meta, deleteKey, removedAccountAsset);
    recorder.recordAssetPut(meta, updateKey, emptyAsset, new byte[0]);
    recorder.recordAccount(meta, deleteKey, BlockChangeView.PostValue.absent(),
        BlockChangeView.PostValue.absent());
    recorder.recordAssetDelete(meta, updateKey, deletedAsset);
    recorder.recordAccount(meta, updateKey, BlockChangeView.PostValue.present(rawUpdate),
        BlockChangeView.PostValue.present(canonicalUpdate));
    HistoryCommitMarker marker = marker(meta, participants());
    AccountAssetForwardMutationManifest manifest = recorder.seal(marker);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", updateKey, bytes(3, 8));
      fixture.rootPut("account", deleteKey, bytes(3, 9));
      BlockChangeView view = fixture.capture(meta, databases -> {
        databases.get("account").put(updateKey, rawUpdate);
        databases.get("account").delete(deleteKey);
      });
      ArchiveParticipantMutationBatch batch =
          new ArchiveParticipantMutationBatchCollector(manifest).collect(marker, view);
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker, batch);

      assertArrayEquals(canonicalUpdate, plan.getMutations("account").get(0).getValue());
      assertNull(plan.getMutations("account").get(1).getValue());
      assertEquals(0, plan.getMutations("account-asset").get(0).getValue().length);
      assertNull(plan.getMutations("account-asset").get(1).getValue());
      assertNull(plan.getMutations("account-asset").get(2).getValue());
    }
  }

  @Test
  public void recorderCanonicalizesDifferentEventOrders() {
    BlockSnapshotMeta meta = meta(8);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] canonicalAccount = bytes(3, 3);
    byte[] firstAsset = assetKey(accountKey, 1);
    byte[] secondAsset = assetKey(accountKey, 2);
    AccountAssetForwardMutationRecorder first =
        new AccountAssetForwardMutationRecorder(meta, limits());
    AccountAssetForwardMutationRecorder second =
        new AccountAssetForwardMutationRecorder(meta, limits());

    first.recordAssetDelete(meta, accountKey, secondAsset);
    first.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount));
    first.recordAssetPut(meta, accountKey, firstAsset, bytes(2, 4));
    second.recordAssetPut(meta, accountKey, firstAsset, bytes(2, 4));
    second.recordAssetDelete(meta, accountKey, secondAsset);
    second.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount));

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      ArchiveTargetMutationPlan firstPlan = new ArchiveTargetMutationPlanBuilder().build(marker,
          new ArchiveParticipantMutationBatchCollector(first.seal(marker)).collect(marker, view));
      ArchiveTargetMutationPlan secondPlan = new ArchiveTargetMutationPlanBuilder().build(marker,
          new ArchiveParticipantMutationBatchCollector(second.seal(marker)).collect(marker, view));
      assertArrayEquals(firstPlan.digest(), secondPlan.digest());
    }
  }

  @Test
  public void recorderRejectsTargetDuplicatesIncompleteAndPostSealWrites() {
    BlockSnapshotMeta meta = meta(9);
    BlockSnapshotMeta otherMeta = meta(10);
    HistoryCommitMarker marker = marker(meta, participants());
    HistoryCommitMarker otherMarker = marker(otherMeta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] assetKey = assetKey(accountKey, 1);

    AccountAssetForwardMutationRecorder wrongTarget =
        new AccountAssetForwardMutationRecorder(meta, limits());
    assertThrows(ArchivePersistenceException.class,
        () -> wrongTarget.recordAccount(otherMeta, accountKey,
            BlockChangeView.PostValue.present(rawAccount),
            BlockChangeView.PostValue.present(rawAccount)));
    assertThrows(ArchivePersistenceException.class, () -> wrongTarget.seal(otherMarker));

    AccountAssetForwardMutationRecorder duplicates =
        new AccountAssetForwardMutationRecorder(meta, limits());
    duplicates.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(rawAccount));
    assertThrows(ArchivePersistenceException.class,
        () -> duplicates.recordAccount(meta, accountKey,
            BlockChangeView.PostValue.present(rawAccount),
            BlockChangeView.PostValue.present(rawAccount)));
    duplicates.recordAssetPut(meta, accountKey, assetKey, bytes(1, 3));
    assertThrows(ArchivePersistenceException.class,
        () -> duplicates.recordAssetDelete(meta, accountKey, assetKey));
    assertThrows(ArchivePersistenceException.class,
        () -> duplicates.recordAssetPut(meta, accountKey, bytes(3, 7), bytes(1, 3)));

    AccountAssetForwardMutationRecorder incomplete =
        new AccountAssetForwardMutationRecorder(meta, limits());
    incomplete.recordAssetDelete(meta, accountKey, assetKey);
    assertThrows(ArchivePersistenceException.class, () -> incomplete.seal(marker));

    AccountAssetForwardMutationRecorder sealed =
        new AccountAssetForwardMutationRecorder(meta, limits());
    sealed.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(rawAccount));
    sealed.seal(marker);
    assertThrows(ArchivePersistenceException.class, () -> sealed.seal(marker));
    assertThrows(ArchivePersistenceException.class,
        () -> sealed.recordAssetDelete(meta, accountKey, assetKey));
  }

  @Test
  public void recorderDefensivelyTransfersPayloadBeforeCommittedMarkerExists() {
    BlockSnapshotMeta meta = meta(11);
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] canonicalAccount = bytes(3, 3);
    byte[] assetKey = assetKey(accountKey, 1);
    byte[] assetValue = bytes(2, 4);
    byte[] expectedAccountKey = Arrays.copyOf(accountKey, accountKey.length);
    byte[] expectedRaw = Arrays.copyOf(rawAccount, rawAccount.length);
    byte[] expectedCanonical = Arrays.copyOf(canonicalAccount, canonicalAccount.length);
    byte[] expectedAssetKey = Arrays.copyOf(assetKey, assetKey.length);
    byte[] expectedAssetValue = Arrays.copyOf(assetValue, assetValue.length);
    AccountAssetForwardMutationRecorder recorder =
        new AccountAssetForwardMutationRecorder(meta, limits());

    recorder.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount));
    recorder.recordAssetPut(meta, accountKey, assetKey, assetValue);
    Arrays.fill(accountKey, (byte) 9);
    Arrays.fill(rawAccount, (byte) 9);
    Arrays.fill(canonicalAccount, (byte) 9);
    Arrays.fill(assetKey, (byte) 9);
    Arrays.fill(assetValue, (byte) 9);

    HistoryCommitMarker marker = marker(meta, participants());
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(expectedAccountKey, expectedRaw));
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker,
          new ArchiveParticipantMutationBatchCollector(recorder.seal(marker))
              .collect(marker, view));
      assertArrayEquals(expectedCanonical, plan.getMutations("account").get(0).getValue());
      assertArrayEquals(expectedAssetKey,
          plan.getMutations("account-asset").get(0).getKey());
      assertArrayEquals(expectedAssetValue,
          plan.getMutations("account-asset").get(0).getValue());
    }
  }

  @Test
  public void blockCaptureOwnsViewRecorderAndBatchAsOneShot() {
    BlockSnapshotMeta meta = meta(12);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    byte[] canonicalAccount = bytes(3, 3);
    byte[] assetKey = assetKey(accountKey, 1);
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, limits());
    capture.recordAssetPut(meta, accountKey, assetKey, new byte[0]);
    capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount));

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      capture.attach(view);
      assertTrue(capture.hasAttachedView());
      assertFalse(capture.isPayloadReleased());
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker,
          capture.seal(marker));
      assertArrayEquals(canonicalAccount, plan.getMutations("account").get(0).getValue());
      assertEquals(0, plan.getMutations("account-asset").get(0).getValue().length);
      assertFalse(capture.hasAttachedView());
      assertTrue(capture.isPayloadReleased());
      assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(view));
      assertThrows(ArchivePersistenceException.class,
          () -> capture.recordAssetDelete(meta, accountKey, assetKey));
      assertThrows(ArchivePersistenceException.class, capture::abort);
    }
  }

  @Test
  public void blockCapturePreconditionFailuresRemainRetryableBeforeManifestConsumption() {
    BlockSnapshotMeta meta = meta(13);
    BlockSnapshotMeta otherMeta = meta(14);
    HistoryCommitMarker marker = marker(meta, participants());
    HistoryCommitMarker otherMarker = marker(otherMeta, participants());
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, limits());
    assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));

    try (Fixture exact = new Fixture(participants());
        Fixture other = new Fixture(participants())) {
      BlockChangeView wrongView = other.capture(otherMeta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(wrongView));
      BlockChangeView view = exact.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      capture.attach(view);
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(view));
      assertThrows(ArchivePersistenceException.class, () -> capture.seal(otherMarker));
      ArchiveParticipantMutationBatch batch = capture.seal(marker);
      assertEquals(meta.getEpoch(), batch.getTargetEpoch());
    }
  }

  @Test
  public void blockCaptureCoverageFailureConsumesOwnershipAndBecomesTerminal() {
    BlockSnapshotMeta meta = meta(15);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(3, 2);
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, limits());

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      capture.attach(view);
      assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));
      assertFalse(capture.hasAttachedView());
      assertTrue(capture.isPayloadReleased());
      assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(view));
      assertThrows(ArchivePersistenceException.class,
          () -> capture.recordAccount(meta, accountKey,
              BlockChangeView.PostValue.present(rawAccount),
              BlockChangeView.PostValue.present(rawAccount)));
      assertThrows(ArchivePersistenceException.class, capture::abort);
    }
  }

  @Test
  public void blockCaptureAbortBeforeAttachReleasesPayloadAndRejectsEveryTerminalAction() {
    BlockSnapshotMeta meta = meta(20);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] assetKey = assetKey(accountKey, 1);
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, limits());
    capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(bytes(3, 2)),
        BlockChangeView.PostValue.present(bytes(3, 3)));
    capture.recordAssetPut(meta, accountKey, assetKey, bytes(3, 4));
    assertFalse(capture.hasAttachedView());
    assertFalse(capture.isPayloadReleased());

    capture.abort();

    assertFalse(capture.hasAttachedView());
    assertTrue(capture.isPayloadReleased());
    assertThrows(ArchivePersistenceException.class, capture::abort);
    assertThrows(ArchivePersistenceException.class,
        () -> capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
            BlockChangeView.PostValue.absent()));
    assertThrows(ArchivePersistenceException.class,
        () -> capture.recordAssetDelete(meta, accountKey, assetKey));
    assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(view));
    }
  }

  @Test
  public void blockCaptureAbortAfterAttachReleasesViewAndPayload() {
    BlockSnapshotMeta meta = meta(21);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, limits());
    capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
        BlockChangeView.PostValue.absent());

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", accountKey, bytes(1, 9));
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      capture.attach(view);
      assertTrue(capture.hasAttachedView());
      assertFalse(capture.isPayloadReleased());

      capture.abort();

      assertFalse(capture.hasAttachedView());
      assertTrue(capture.isPayloadReleased());
      assertThrows(ArchivePersistenceException.class, capture::abort);
      assertThrows(ArchivePersistenceException.class, () -> capture.attach(view));
      assertThrows(ArchivePersistenceException.class, () -> capture.seal(marker));
    }
  }

  @Test
  public void captureLimitsAcceptExactBoundaryWithDeleteAndPresentEmpty() {
    BlockSnapshotMeta meta = meta(16);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] rawAccount = bytes(1, 2);
    byte[] canonicalAccount = bytes(1, 3);
    byte[] emptyAsset = assetKey(accountKey, 1);
    byte[] deletedAsset = assetKey(accountKey, 2);
    ArchiveBlockForwardMutationLimits exact =
        new ArchiveBlockForwardMutationLimits(1, 2, 3, 1, 13);
    ArchiveBlockForwardMutationCapture capture =
        new ArchiveBlockForwardMutationCapture(meta, exact);
    capture.recordAssetPut(meta, accountKey, emptyAsset, new byte[0]);
    capture.recordAssetDelete(meta, accountKey, deletedAsset);
    capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount));

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, rawAccount));
      capture.attach(view);
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker,
          capture.seal(marker));
      assertEquals(0, plan.getMutations("account-asset").get(0).getValue().length);
      assertNull(plan.getMutations("account-asset").get(1).getValue());
    }
  }

  @Test
  public void captureLimitsRejectEveryDimensionAndNegativeConfiguration() {
    BlockSnapshotMeta meta = meta(17);
    byte[] accountKey = bytes(2, 1);
    byte[] assetKey = assetKey(accountKey, 1);
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveBlockForwardMutationLimits(-1, 1, 1, 1, 1));

    AccountAssetForwardMutationRecorder accounts = new AccountAssetForwardMutationRecorder(meta,
        new ArchiveBlockForwardMutationLimits(0, 1, 3, 1, 10));
    assertThrows(ArchivePersistenceException.class,
        () -> accounts.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
            BlockChangeView.PostValue.absent()));

    AccountAssetForwardMutationRecorder assets = new AccountAssetForwardMutationRecorder(meta,
        new ArchiveBlockForwardMutationLimits(1, 0, 3, 1, 10));
    assets.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
        BlockChangeView.PostValue.absent());
    assertThrows(ArchivePersistenceException.class,
        () -> assets.recordAssetDelete(meta, accountKey, assetKey));

    AccountAssetForwardMutationRecorder keys = new AccountAssetForwardMutationRecorder(meta,
        new ArchiveBlockForwardMutationLimits(1, 1, 1, 1, 10));
    assertThrows(ArchivePersistenceException.class,
        () -> keys.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
            BlockChangeView.PostValue.absent()));

    AccountAssetForwardMutationRecorder values = new AccountAssetForwardMutationRecorder(meta,
        new ArchiveBlockForwardMutationLimits(1, 1, 3, 0, 10));
    assertThrows(ArchivePersistenceException.class,
        () -> values.recordAccount(meta, accountKey,
            BlockChangeView.PostValue.present(bytes(1, 2)),
            BlockChangeView.PostValue.present(bytes(1, 3))));

    AccountAssetForwardMutationRecorder total = new AccountAssetForwardMutationRecorder(meta,
        new ArchiveBlockForwardMutationLimits(1, 1, 3, 1, 3));
    assertThrows(ArchivePersistenceException.class,
        () -> total.recordAccount(meta, accountKey,
            BlockChangeView.PostValue.present(bytes(1, 2)),
            BlockChangeView.PostValue.present(bytes(1, 3))));
  }

  @Test
  public void captureLimitRejectionAndDuplicatesDoNotConsumeReservation() {
    BlockSnapshotMeta meta = meta(18);
    HistoryCommitMarker marker = marker(meta, participants());
    byte[] accountKey = bytes(2, 1);
    byte[] firstAsset = assetKey(accountKey, 1);
    byte[] secondAsset = assetKey(accountKey, 2);
    ArchiveBlockForwardMutationCapture capture = new ArchiveBlockForwardMutationCapture(meta,
        new ArchiveBlockForwardMutationLimits(1, 2, 3, 1, 10));

    assertThrows(ArchivePersistenceException.class,
        () -> capture.recordAccount(meta, accountKey,
            BlockChangeView.PostValue.present(bytes(2, 2)),
            BlockChangeView.PostValue.present(bytes(2, 3))));
    capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
        BlockChangeView.PostValue.absent());
    assertThrows(ArchivePersistenceException.class,
        () -> capture.recordAccount(meta, accountKey, BlockChangeView.PostValue.absent(),
            BlockChangeView.PostValue.absent()));
    capture.recordAssetDelete(meta, accountKey, firstAsset);
    assertThrows(ArchivePersistenceException.class,
        () -> capture.recordAssetDelete(meta, accountKey, firstAsset));
    capture.recordAssetDelete(meta, accountKey, secondAsset);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", accountKey, bytes(1, 9));
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      capture.attach(view);
      ArchiveTargetMutationPlan plan = new ArchiveTargetMutationPlanBuilder().build(marker,
          capture.seal(marker));
      assertNull(plan.getMutations("account").get(0).getValue());
      assertEquals(2, plan.getMutations("account-asset").size());
    }
  }

  @Test
  public void captureLimitsReserveAttachedViewAtomicallyAndAllowRetry() {
    BlockSnapshotMeta meta = meta(19);
    HistoryCommitMarker marker = marker(meta, participants());
    ArchiveBlockForwardMutationCapture total = new ArchiveBlockForwardMutationCapture(meta,
        new ArchiveBlockForwardMutationLimits(0, 0, 3, 3, 3));
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView tooLarge = fixture.capture(meta,
          databases -> databases.get("code").put(bytes(2, 1), bytes(2, 2)));
      assertThrows(ArchivePersistenceException.class, () -> total.attach(tooLarge));
      BlockChangeView exact = fixture.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(1, 2)));
      total.attach(exact);
      assertEquals(meta.getEpoch(), total.seal(marker).getTargetEpoch());
    }

    ArchiveBlockForwardMutationCapture key = new ArchiveBlockForwardMutationCapture(meta,
        new ArchiveBlockForwardMutationLimits(0, 0, 1, 2, 10));
    ArchiveBlockForwardMutationCapture value = new ArchiveBlockForwardMutationCapture(meta,
        new ArchiveBlockForwardMutationLimits(0, 0, 2, 1, 10));
    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView keyTooLarge = fixture.capture(meta,
          databases -> databases.get("code").put(bytes(2, 1), new byte[0]));
      assertThrows(ArchivePersistenceException.class, () -> key.attach(keyTooLarge));
      BlockChangeView valueTooLarge = fixture.capture(meta,
          databases -> databases.get("code").put(bytes(1, 1), bytes(2, 2)));
      assertThrows(ArchivePersistenceException.class, () -> value.attach(valueTooLarge));
    }
  }

  private static Entry entry(byte[] accountKey, byte[] rawAccount, byte[] canonicalAccount,
      AssetMutation... mutations) {
    return new Entry(accountKey, BlockChangeView.PostValue.present(rawAccount),
        BlockChangeView.PostValue.present(canonicalAccount), Arrays.asList(mutations));
  }

  private static byte[] assetKey(byte[] accountKey, int suffix) {
    byte[] key = Arrays.copyOf(accountKey, accountKey.length + 1);
    key[key.length - 1] = (byte) suffix;
    return key;
  }

  private static ArchiveBlockForwardMutationLimits limits() {
    return new ArchiveBlockForwardMutationLimits(100, 1_000, 1_024, 1024 * 1024,
        10L * 1024 * 1024);
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return participants;
  }

  private static BlockSnapshotMeta meta(int epoch) {
    return BlockSnapshotMeta.forBlock(epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L);
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta, List<String> participants) {
    int epoch = (int) meta.getEpoch();
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100L, 100, epoch, bytes(32, epoch + 20)),
        new HistoryIndexLocation(epoch * 50L, 50, bytes(32, epoch + 30)),
        bytes(16, epoch + 40), participants);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  @FunctionalInterface
  private interface Mutator {
    void mutate(Map<String, Chainbase> databases);
  }

  private static final class Fixture implements AutoCloseable {
    private final SnapshotManager manager = new SnapshotManager("");
    private final Map<String, MemoryDb> roots = new LinkedHashMap<>();
    private final Map<String, Chainbase> databases = new LinkedHashMap<>();
    private final List<Chainbase> ordered = new ArrayList<>();

    private Fixture(List<String> participants) {
      for (String participant : participants) {
        MemoryDb root = new MemoryDb(participant);
        Chainbase database = new Chainbase(new SnapshotRoot(root));
        roots.put(participant, root);
        databases.put(participant, database);
        ordered.add(database);
        manager.add(database);
      }
      manager.enable();
    }

    private void rootPut(String dbName, byte[] key, byte[] value) {
      roots.get(dbName).put(key, value);
    }

    private BlockChangeView capture(BlockSnapshotMeta meta, Mutator mutator) {
      try (ISession session = manager.buildSession()) {
        mutator.mutate(databases);
        return BlockChangeView.capture(meta, ordered);
      }
    }

    @Override
    public void close() {
      manager.shutdown();
    }
  }

  private static final class MemoryDb implements DB<byte[], byte[]>, Flusher {
    private final String name;
    private final Map<WrappedByteArray, byte[]> values = new LinkedHashMap<>();

    private MemoryDb(String name) {
      this.name = name;
    }

    @Override
    public byte[] get(byte[] key) {
      byte[] value = values.get(WrappedByteArray.of(key));
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public void put(byte[] key, byte[] value) {
      values.put(WrappedByteArray.copyOf(key), Arrays.copyOf(value, value.length));
    }

    @Override
    public long size() {
      return values.size();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public void remove(byte[] key) {
      values.remove(WrappedByteArray.of(key));
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
      values.forEach((key, value) -> entries.add(new AbstractMap.SimpleImmutableEntry<>(
          key.getBytes(), Arrays.copyOf(value, value.length))));
      return entries.iterator();
    }

    @Override
    public void close() {
      values.clear();
    }

    @Override
    public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
      batch.forEach((key, value) -> {
        if (value == null || value.getBytes() == null) {
          values.remove(key);
        } else {
          values.put(WrappedByteArray.copyOf(key.getBytes()), value.getBytes());
        }
      });
    }

    @Override
    public void reset() {
      values.clear();
    }

    @Override
    public String getDbName() {
      return name;
    }

    @Override
    public void stat() {
    }

    @Override
    public DB<byte[], byte[]> newInstance() {
      return new MemoryDb(name);
    }
  }
}
