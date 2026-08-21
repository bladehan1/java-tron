package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
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
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.AccountAssetBlockProjectionBridge.PreparedBlockProjection;
import org.tron.core.db2.archive.AccountAssetBlockProjectionBridge.TargetAssetOptimization;
import org.tron.core.db2.archive.AccountAssetPreparedBlockPayloadOwner.FrozenBatch;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.core.store.AccountAssetStore;
import org.tron.protos.Protocol.Account;

public class AccountAssetBlockProjectionBridgeTest extends BaseMethodTest {

  @Test
  public void sharesExactAccountProjectionAcrossDeterministicReverseAndForwardBuilders() {
    BlockSnapshotMeta meta = meta(1);
    HistoryCommitMarker marker = marker(meta);
    byte[] updateKey = accountKey(1);
    byte[] deleteKey = accountKey(2);
    byte[] updateAsset = assetKey(updateKey, "1000001");
    byte[] deleteAsset = assetKey(deleteKey, "1000002");
    Account oldUpdate = optimizedAccount(updateKey);
    Account oldDelete = optimizedAccount(deleteKey);
    Account postUpdate = oldUpdate.toBuilder().putAssetV2("1000001", 80L).build();
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    when(assetStore.prefixQuery(any(byte[].class))).thenAnswer(invocation -> {
      byte[] accountKey = invocation.getArgument(0);
      Map<WrappedByteArray, byte[]> assets = new LinkedHashMap<>();
      if (Arrays.equals(accountKey, updateKey)) {
        assets.put(WrappedByteArray.copyOf(updateAsset), Longs.toByteArray(100L));
      } else if (Arrays.equals(accountKey, deleteKey)) {
        assets.put(WrappedByteArray.copyOf(deleteAsset), Longs.toByteArray(200L));
      }
      return assets;
    });
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", updateKey, oldUpdate.toByteArray());
      fixture.rootPut("account", deleteKey, oldDelete.toByteArray());
      BlockChangeView view = fixture.capture(meta, databases -> {
        databases.get("account").put(updateKey, postUpdate.toByteArray());
        databases.get("account").delete(deleteKey);
      });

      PreparedBlockProjection first = bridge.prepare(view,
          TargetAssetOptimization.forTarget(meta, true));
      verify(assetStore, times(2)).prefixQuery(any(byte[].class));
      BlockReverseDiff.DbGroup reverseAssets = group(first.getReverseDiff(), "account-asset");
      assertEquals(2, reverseAssets.getEntries().size());
      assertArrayEquals(Longs.toByteArray(100L),
          reverseAssets.getEntries().get(0).getOldValue().getValue());
      assertArrayEquals(Longs.toByteArray(200L),
          reverseAssets.getEntries().get(1).getOldValue().getValue());

      ArchiveTargetMutationPlan firstPlan = plan(marker, view, first.seal(marker));
      assertArrayEquals(Longs.toByteArray(80L),
          firstPlan.getMutations("account-asset").get(0).getValue());
      assertNull(firstPlan.getMutations("account-asset").get(1).getValue());

      PreparedBlockProjection retry = bridge.prepare(view,
          TargetAssetOptimization.forTarget(meta, true));
      verify(assetStore, times(4)).prefixQuery(any(byte[].class));
      ArchiveTargetMutationPlan retryPlan = plan(marker, view, retry.seal(marker));
      assertArrayEquals(new BlockHistoryCodec().encode(first.getReverseDiff()),
          new BlockHistoryCodec().encode(retry.getReverseDiff()));
      assertArrayEquals(firstPlan.digest(), retryPlan.digest());
    }
  }

  @Test
  public void rejectsActivationIdentityAndCoverageBeforeAnyPhysicalRead() {
    BlockSnapshotMeta meta = meta(2);
    HistoryCommitMarker marker = marker(meta);
    byte[] accountKey = accountKey(3);
    Account old = optimizedAccount(accountKey);
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);

    try (Fixture exact = new Fixture(participants())) {
      exact.rootPut("account", accountKey, old.toByteArray());
      BlockChangeView view = exact.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view,
              TargetAssetOptimization.forTarget(meta(3), true)));
    }
    try (Fixture incomplete = new Fixture(Collections.singletonList("account"))) {
      incomplete.rootPut("account", accountKey, old.toByteArray());
      BlockChangeView view = incomplete.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view,
              TargetAssetOptimization.forTarget(meta, true)));
    }
    try (Fixture duplicateSource = new Fixture(participants())) {
      duplicateSource.rootPut("account", accountKey, old.toByteArray());
      BlockChangeView view = duplicateSource.capture(meta, databases -> {
        databases.get("account").delete(accountKey);
        databases.get("account-asset").put(assetKey(accountKey, "1000004"),
            Longs.toByteArray(40L));
      });
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view,
              TargetAssetOptimization.forTarget(meta, true)));
    }
    verify(assetStore, never()).prefixQuery(any(byte[].class));
  }

  @Test
  public void projectionFailurePublishesNoPartialResultAndAllowsFreshRetry() {
    BlockSnapshotMeta meta = meta(4);
    HistoryCommitMarker marker = marker(meta);
    byte[] validKey = accountKey(4);
    byte[] invalidKey = accountKey(5);
    Account validOld = optimizedAccount(validKey);
    Account validPost = validOld.toBuilder().putAssetV2("1000003", 30L).build();
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(Collections.emptyMap());
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", validKey, validOld.toByteArray());
      BlockChangeView failing = fixture.capture(meta, databases -> {
        databases.get("account").put(validKey, validPost.toByteArray());
        databases.get("account").put(invalidKey, bytes(3, 99));
      });
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(failing,
              TargetAssetOptimization.forTarget(meta, true)));

      BlockChangeView retry = fixture.capture(meta,
          databases -> databases.get("account").put(validKey, validPost.toByteArray()));
      PreparedBlockProjection result = bridge.prepare(retry,
          TargetAssetOptimization.forTarget(meta, true));
      assertEquals(meta, result.getReverseDiff().getMeta());
      assertEquals(1, plan(marker, retry, result.seal(marker))
          .getMutations("account").size());
    }
    verify(assetStore, times(2)).prefixQuery(any(byte[].class));
  }

  @Test
  public void physicalInputFailurePublishesNothingAndFreshPrepareCanRetry() {
    BlockSnapshotMeta meta = meta(24);
    byte[] accountKey = accountKey(24);
    Account old = optimizedAccount(accountKey);
    int[] failureMode = {1};
    AccountAssetOldPhysicalAssetsSource source = key -> {
      if (failureMode[0] == 1) {
        throw new IllegalStateException("injected physical input failure");
      }
      if (failureMode[0] == 2) {
        return Collections.singletonMap(
            WrappedByteArray.copyOf(assetKey(key, "01000001")), Longs.toByteArray(1L));
      }
      if (failureMode[0] == 3) {
        return Collections.singletonMap(
            WrappedByteArray.copyOf(assetKey(key, "1000001")), new byte[7]);
      }
      if (failureMode[0] == 4) {
        return Collections.singletonMap(
            WrappedByteArray.copyOf(assetKey(key, "1000001")), Longs.toByteArray(0L));
      }
      return Collections.emptyMap();
    };
    AccountAssetBlockProjectionBridge bridge = new AccountAssetBlockProjectionBridge(
        new AccountAssetArchiveProjector(), source);

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", accountKey, old.toByteArray());
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      TargetAssetOptimization activation = TargetAssetOptimization.forTarget(meta, true);

      ArchivePersistenceException failure = assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, activation));
      assertTrue(failure.getMessage().contains("old physical AccountAsset input"));

      for (int mode = 2; mode <= 4; mode++) {
        failureMode[0] = mode;
        assertThrows(ArchivePersistenceException.class,
            () -> bridge.prepare(view, activation));
      }
      failureMode[0] = 0;
      PreparedBlockProjection prepared = bridge.prepare(view, activation);
      assertEquals(meta, prepared.getReverseDiff().getMeta());
      prepared.abort();
    }
  }

  @Test
  public void resolvesActivationBlockFromProposalSixtySixAndFeedsSharedBridge() {
    BlockSnapshotMeta meta = meta(5);
    HistoryCommitMarker marker = marker(meta);
    byte[] accountKey = accountKey(6);
    byte[] physicalAsset = assetKey(accountKey, "1000005");
    Account old = optimizedAccount(accountKey);
    Account post = old.toBuilder().putAssetV2("1000005", 50L).build();
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    Map<WrappedByteArray, byte[]> assets = new LinkedHashMap<>();
    assets.put(WrappedByteArray.copyOf(physicalAsset), Longs.toByteArray(40L));
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(assets);
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);
    AccountAssetTargetActivationResolver resolver =
        new AccountAssetTargetActivationResolver();

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("account", accountKey, old.toByteArray());
      fixture.rootPut("properties", proposal66Key(), ByteArray.fromLong(0L));
      BlockChangeView view = fixture.capture(meta, databases -> {
        databases.get("properties").put(proposal66Key(), ByteArray.fromLong(1L));
        databases.get("properties").put(proposal53Key(), ByteArray.fromLong(0L));
        databases.get("account").put(accountKey, post.toByteArray());
      });

      TargetAssetOptimization activation = resolver.resolve(meta, view);
      PreparedBlockProjection result = bridge.prepare(view, activation);
      assertTrue(activation.isEnabled());
      assertEquals(Phase.P66_ACTIVATION, activation.getPhase());
      ArchiveTargetMutationPlan plan = plan(marker, view, result.seal(marker));
      assertEquals(Phase.P66_ACTIVATION, plan.getTargetPhase());
      assertArrayEquals(Longs.toByteArray(50L),
          plan.getMutations("account-asset").get(0).getValue());
    }
    verify(assetStore, times(1)).prefixQuery(any(byte[].class));
  }

  @Test
  public void inheritsUnchangedProposalSixtySixWithoutUsingProposalFiftyThree() {
    BlockSnapshotMeta meta = meta(6);
    HistoryCommitMarker marker = marker(meta);
    byte[] accountKey = accountKey(7);
    Account raw = Account.newBuilder()
        .setAddress(ByteString.copyFrom(accountKey))
        .putAssetV2("1000006", 60L)
        .build();
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);
    AccountAssetTargetActivationResolver resolver =
        new AccountAssetTargetActivationResolver();

    try (Fixture fixture = new Fixture(participants())) {
      fixture.rootPut("properties", proposal66Key(), ByteArray.fromLong(0L));
      fixture.rootPut("properties", proposal53Key(), ByteArray.fromLong(1L));
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, raw.toByteArray()));

      TargetAssetOptimization activation = resolver.resolve(meta, view);
      PreparedBlockProjection result = bridge.prepare(view, activation);
      assertFalse(activation.isEnabled());
      assertEquals(Phase.P66_OFF, activation.getPhase());
      ArchiveTargetMutationPlan plan = plan(marker, view, result.seal(marker));
      assertEquals(Phase.P66_OFF, plan.getTargetPhase());
      assertArrayEquals(raw.toByteArray(), plan.getMutations("account").get(0).getValue());
      assertEquals(0, plan.getMutations("account-asset").size());
    }
    verify(assetStore, never()).prefixQuery(any(byte[].class));
  }

  @Test
  public void rejectsMissingCorruptSubstitutedAndReorgActivationBeforePrefix() {
    BlockSnapshotMeta meta = meta(7);
    HistoryCommitMarker marker = marker(meta);
    byte[] accountKey = accountKey(8);
    Account old = optimizedAccount(accountKey);
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);
    AccountAssetTargetActivationResolver resolver =
        new AccountAssetTargetActivationResolver();

    try (Fixture missing = new Fixture(participants())) {
      missing.rootPut("account", accountKey, old.toByteArray());
      missing.rootPut("properties", proposal53Key(), ByteArray.fromLong(1L));
      BlockChangeView view = missing.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, resolver.resolve(meta, view)));
    }
    try (Fixture corrupt = new Fixture(participants())) {
      corrupt.rootPut("account", accountKey, old.toByteArray());
      corrupt.rootPut("properties", proposal66Key(), new byte[] {1});
      BlockChangeView view = corrupt.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, resolver.resolve(meta, view)));
    }
    try (Fixture noncanonical = new Fixture(participants())) {
      noncanonical.rootPut("account", accountKey, old.toByteArray());
      noncanonical.rootPut("properties", proposal66Key(), ByteArray.fromLong(2L));
      BlockChangeView view = noncanonical.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, resolver.resolve(meta, view)));
    }
    try (Fixture deleted = new Fixture(participants())) {
      deleted.rootPut("account", accountKey, old.toByteArray());
      deleted.rootPut("properties", proposal66Key(), ByteArray.fromLong(1L));
      BlockChangeView view = deleted.capture(meta, databases -> {
        databases.get("properties").delete(proposal66Key());
        databases.get("account").delete(accountKey);
      });
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, resolver.resolve(meta, view)));
    }
    try (Fixture reorg = new Fixture(participants())) {
      reorg.rootPut("account", accountKey, old.toByteArray());
      reorg.rootPut("properties", proposal66Key(), ByteArray.fromLong(1L));
      BlockChangeView view = reorg.capture(meta,
          databases -> databases.get("account").delete(accountKey));
      assertThrows(ArchivePersistenceException.class,
          () -> bridge.prepare(view, resolver.resolve(meta(8), view)));
    }
    try (Fixture regressed = new Fixture(participants())) {
      regressed.rootPut("properties", proposal66Key(), ByteArray.fromLong(1L));
      BlockChangeView view = regressed.capture(meta,
          databases -> databases.get("properties").put(
              proposal66Key(), ByteArray.fromLong(0L)));
      assertThrows(ArchivePersistenceException.class,
          () -> resolver.resolve(meta, view));
    }
    verify(assetStore, never()).prefixQuery(any(byte[].class));
  }

  @Test
  public void rejectsWrongMarkerWithoutConsumingPreparedProjectionAndSealsExactlyOnce() {
    BlockSnapshotMeta meta = meta(9);
    HistoryCommitMarker marker = marker(meta);
    byte[] accountKey = accountKey(9);
    Account account = optimizedAccount(accountKey);
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    when(assetStore.prefixQuery(any(byte[].class))).thenReturn(Collections.emptyMap());
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta,
          databases -> databases.get("account").put(accountKey, account.toByteArray()));
      PreparedBlockProjection prepared = bridge.prepare(view,
          TargetAssetOptimization.forTarget(meta, true));

      assertThrows(ArchivePersistenceException.class,
          () -> prepared.seal(marker(meta(10))));
      assertThrows(ArchivePersistenceException.class,
          () -> prepared.seal(marker(meta, Collections.singletonList("account"))));
      assertEquals(meta, prepared.getReverseDiff().getMeta());
      assertTrue(prepared.retainsCapturedView());

      AccountAssetForwardMutationManifest manifest = prepared.seal(marker);
      assertEquals(1, plan(marker, view, manifest).getMutations("account").size());
      assertFalse(prepared.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class, () -> prepared.seal(marker));
      assertThrows(ArchivePersistenceException.class, prepared::abort);
    }
  }

  @Test
  public void sealsEmptyBlockAndAbortReleasesPreparedPayload() {
    BlockSnapshotMeta meta = meta(11);
    HistoryCommitMarker marker = marker(meta);
    AccountAssetStore assetStore = mock(AccountAssetStore.class);
    AccountAssetBlockProjectionBridge bridge = bridge(assetStore);

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta, databases -> { });
      TargetAssetOptimization activation = TargetAssetOptimization.forTarget(meta, false);
      PreparedBlockProjection sealed = bridge.prepare(view, activation);
      assertTrue(sealed.getReverseDiff().getGroups().isEmpty());
      assertTrue(plan(marker, view, sealed.seal(marker)).getMutations().values().stream()
          .allMatch(List::isEmpty));

      PreparedBlockProjection aborted = bridge.prepare(view, activation);
      assertTrue(aborted.retainsCapturedView());
      aborted.abort();
      assertFalse(aborted.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class, aborted::getReverseDiff);
      assertThrows(ArchivePersistenceException.class, () -> aborted.seal(marker));
      assertThrows(ArchivePersistenceException.class, aborted::abort);
    }
    verify(assetStore, never()).prefixQuery(any(byte[].class));
  }

  @Test
  public void layerOwnersTransferContiguousPayloadsAndBatchSealExactlyOnce() {
    BlockSnapshotMeta firstMeta = meta(12);
    BlockSnapshotMeta secondMeta = meta(13);
    HistoryCommitMarker firstMarker = marker(firstMeta);
    HistoryCommitMarker secondMarker = marker(secondMeta);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView firstView = fixture.capture(firstMeta, databases -> { });
      BlockChangeView secondView = fixture.capture(secondMeta, databases -> { });
      PreparedBlockProjection first = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));
      PreparedBlockProjection second = bridge.prepare(secondView,
          TargetAssetOptimization.forTarget(secondMeta, false));
      AccountAssetPreparedBlockPayloadOwner firstOwner =
          new AccountAssetPreparedBlockPayloadOwner(firstMeta);
      AccountAssetPreparedBlockPayloadOwner secondOwner =
          new AccountAssetPreparedBlockPayloadOwner(secondMeta);
      firstOwner.attach(first);
      secondOwner.attach(second);
      assertEquals(firstMeta, firstOwner.getReverseDiff().getMeta());

      FrozenBatch batch = AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
          Arrays.asList(firstOwner, secondOwner));
      assertThrows(ArchivePersistenceException.class, firstOwner::getReverseDiff);
      assertThrows(ArchivePersistenceException.class, firstOwner::discard);
      assertThrows(ArchivePersistenceException.class,
          () -> AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
              Collections.singletonList(secondOwner)));

      assertThrows(ArchivePersistenceException.class,
          () -> batch.seal(Arrays.asList(firstMarker, marker(meta(14)))));
      assertTrue(first.retainsCapturedView());
      assertTrue(second.retainsCapturedView());
      List<ArchiveBlockForwardPayload> payloads = batch.seal(
          Arrays.asList(firstMarker, secondMarker));
      assertEquals(2, payloads.size());
      assertEquals(firstMeta, payloads.get(0).getMeta());
      assertSame(firstView, payloads.get(0).getView());
      assertEquals(secondMeta, payloads.get(1).getMeta());
      assertSame(secondView, payloads.get(1).getView());
      assertTrue(plan(firstMarker, payloads.get(0).getView(),
          payloads.get(0).getAccountAssetManifest()).getMutations().values().stream()
          .allMatch(List::isEmpty));
      assertFalse(first.retainsCapturedView());
      assertFalse(second.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class,
          () -> batch.seal(Arrays.asList(firstMarker, secondMarker)));
      assertThrows(ArchivePersistenceException.class, batch::abort);
    }
  }

  @Test
  public void sealedForwardPayloadCarriesExactViewAndRejectsMixedIdentity() {
    BlockSnapshotMeta firstMeta = meta(14);
    BlockSnapshotMeta secondMeta = meta(15);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView firstView = fixture.capture(firstMeta, databases -> { });
      BlockChangeView secondView = fixture.capture(secondMeta, databases -> { });
      PreparedBlockProjection prepared = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));

      ArchiveBlockForwardPayload payload = prepared.sealPayload(marker(firstMeta));
      assertEquals(firstMeta, payload.getMeta());
      assertSame(firstView, payload.getView());
      assertFalse(prepared.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class,
          () -> prepared.sealPayload(marker(firstMeta)));

      AccountAssetForwardMutationManifest manifest =
          new AccountAssetForwardMutationManifest(marker(firstMeta), Phase.P66_OFF,
              Collections.emptyList());
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveBlockForwardPayload(marker(firstMeta), secondView, manifest));
    }
  }

  @Test
  public void layerOwnerRejectsWrongTargetAndDoubleAttachWithoutConsumingCallerPayload() {
    BlockSnapshotMeta firstMeta = meta(15);
    BlockSnapshotMeta secondMeta = meta(16);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView firstView = fixture.capture(firstMeta, databases -> { });
      BlockChangeView secondView = fixture.capture(secondMeta, databases -> { });
      PreparedBlockProjection first = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));
      PreparedBlockProjection second = bridge.prepare(secondView,
          TargetAssetOptimization.forTarget(secondMeta, false));
      PreparedBlockProjection sealed = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));
      sealed.seal(marker(firstMeta));
      PreparedBlockProjection aborted = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));
      aborted.abort();
      AccountAssetPreparedBlockPayloadOwner owner =
          new AccountAssetPreparedBlockPayloadOwner(firstMeta);

      assertThrows(ArchivePersistenceException.class, () -> owner.attach(second));
      assertEquals(secondMeta, second.getReverseDiff().getMeta());
      assertThrows(ArchivePersistenceException.class, () -> owner.attach(sealed));
      assertThrows(ArchivePersistenceException.class, () -> owner.attach(aborted));
      owner.attach(first);
      assertThrows(ArchivePersistenceException.class, () -> owner.attach(second));
      assertEquals(secondMeta, second.getReverseDiff().getMeta());
      owner.discard();
      second.abort();
    }
  }

  @Test
  public void fastPopDiscardAndFrozenShutdownAbortReleaseEveryPayload() {
    BlockSnapshotMeta firstMeta = meta(17);
    BlockSnapshotMeta secondMeta = meta(18);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView firstView = fixture.capture(firstMeta, databases -> { });
      BlockChangeView secondView = fixture.capture(secondMeta, databases -> { });
      PreparedBlockProjection discarded = bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false));
      AccountAssetPreparedBlockPayloadOwner discardedOwner =
          new AccountAssetPreparedBlockPayloadOwner(firstMeta);
      discardedOwner.attach(discarded);
      discardedOwner.discard();
      assertFalse(discarded.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class, discarded::getReverseDiff);
      assertThrows(ArchivePersistenceException.class, discardedOwner::discard);

      PreparedBlockProjection frozen = bridge.prepare(secondView,
          TargetAssetOptimization.forTarget(secondMeta, false));
      AccountAssetPreparedBlockPayloadOwner frozenOwner =
          new AccountAssetPreparedBlockPayloadOwner(secondMeta);
      frozenOwner.attach(frozen);
      FrozenBatch batch = AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
          Collections.singletonList(frozenOwner));
      batch.abort();
      assertFalse(frozen.retainsCapturedView());
      assertThrows(ArchivePersistenceException.class, frozen::getReverseDiff);
      assertThrows(ArchivePersistenceException.class, batch::abort);
      assertThrows(ArchivePersistenceException.class,
          () -> batch.seal(Collections.singletonList(marker(secondMeta))));
    }
  }

  @Test
  public void nonContiguousAndDuplicateFreezeFailureLeavesLayerOwnersAttached() {
    BlockSnapshotMeta firstMeta = meta(19);
    BlockSnapshotMeta thirdMeta = meta(21);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView firstView = fixture.capture(firstMeta, databases -> { });
      BlockChangeView thirdView = fixture.capture(thirdMeta, databases -> { });
      AccountAssetPreparedBlockPayloadOwner firstOwner =
          new AccountAssetPreparedBlockPayloadOwner(firstMeta);
      AccountAssetPreparedBlockPayloadOwner thirdOwner =
          new AccountAssetPreparedBlockPayloadOwner(thirdMeta);
      firstOwner.attach(bridge.prepare(firstView,
          TargetAssetOptimization.forTarget(firstMeta, false)));
      thirdOwner.attach(bridge.prepare(thirdView,
          TargetAssetOptimization.forTarget(thirdMeta, false)));

      assertThrows(ArchivePersistenceException.class,
          () -> AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
              Arrays.asList(firstOwner, thirdOwner)));
      assertThrows(ArchivePersistenceException.class,
          () -> AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
              Arrays.asList(firstOwner, firstOwner)));
      assertEquals(firstMeta, firstOwner.getReverseDiff().getMeta());
      assertEquals(thirdMeta, thirdOwner.getReverseDiff().getMeta());
      firstOwner.discard();
      thirdOwner.discard();
    }
  }

  @Test
  public void durableMarkerEvidenceFailureLeavesFrozenBatchRetryable() {
    BlockSnapshotMeta meta = meta(22);
    HistoryCommitMarker marker = marker(meta);
    AccountAssetBlockProjectionBridge bridge = emptyBridge();
    boolean[] substitute = {true};
    DurableHistoryMarkerRangeEvidence.Source source =
        new DurableHistoryMarkerRangeEvidence.Source() {
          @Override
          public HistoryCommitMarker marker(long epoch) {
            return substitute[0] ? AccountAssetBlockProjectionBridgeTest.marker(meta(23)) : marker;
          }

          @Override
          public BlockReverseDiff readCommitted(long epoch) {
            return new BlockReverseDiff(meta, Collections.emptyList());
          }
        };

    try (Fixture fixture = new Fixture(participants())) {
      BlockChangeView view = fixture.capture(meta, databases -> { });
      AccountAssetPreparedBlockPayloadOwner owner =
          new AccountAssetPreparedBlockPayloadOwner(meta);
      PreparedBlockProjection prepared = bridge.prepare(view,
          TargetAssetOptimization.forTarget(meta, false));
      owner.attach(prepared);
      FrozenBatch batch = AccountAssetPreparedBlockPayloadOwner.freezeContiguous(
          Collections.singletonList(owner));
      DurableHistoryMarkerRangeEvidence evidence =
          new DurableHistoryMarkerRangeEvidence(source, 1);

      assertThrows(ArchivePersistenceException.class, () -> evidence.seal(batch));
      assertEquals(meta, batch.getExpectedMetas().get(0));
      assertTrue(prepared.retainsCapturedView());
      substitute[0] = false;
      List<ArchiveBlockForwardPayload> payloads = evidence.seal(batch);
      assertEquals(1, payloads.size());
      assertSame(view, payloads.get(0).getView());
      assertFalse(prepared.retainsCapturedView());
      assertTrue(plan(payloads.get(0).getMarker(), payloads.get(0).getView(),
          payloads.get(0).getAccountAssetManifest()).getMutations().values().stream()
          .allMatch(List::isEmpty));
      assertThrows(ArchivePersistenceException.class, () -> evidence.seal(batch));
    }
  }

  private static AccountAssetBlockProjectionBridge emptyBridge() {
    return new AccountAssetBlockProjectionBridge(new AccountAssetArchiveProjector(),
        accountKey -> Collections.emptyMap());
  }

  private static AccountAssetBlockProjectionBridge bridge(AccountAssetStore assetStore) {
    return new AccountAssetBlockProjectionBridge(new AccountAssetArchiveProjector(),
        accountKey -> assetStore.prefixQuery(accountKey));
  }

  private static ArchiveTargetMutationPlan plan(HistoryCommitMarker marker, BlockChangeView view,
      AccountAssetForwardMutationManifest manifest) {
    ArchiveParticipantMutationBatch batch = new ArchiveParticipantMutationBatchCollector(
        manifest).collect(marker, view);
    return new ArchiveTargetMutationPlanBuilder().build(marker, batch);
  }

  private static BlockReverseDiff.DbGroup group(BlockReverseDiff diff, String dbName) {
    return diff.getGroups().stream()
        .filter(group -> dbName.equals(group.getDbName()))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static Account optimizedAccount(byte[] accountKey) {
    return Account.newBuilder()
        .setAddress(ByteString.copyFrom(accountKey))
        .setAssetOptimized(true)
        .build();
  }

  private static byte[] accountKey(int suffix) {
    byte[] key = new byte[21];
    key[0] = 0x41;
    key[20] = (byte) suffix;
    return key;
  }

  private static byte[] assetKey(byte[] accountKey, String token) {
    byte[] tokenBytes = token.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] key = Arrays.copyOf(accountKey, accountKey.length + tokenBytes.length);
    System.arraycopy(tokenBytes, 0, key, accountKey.length, tokenBytes.length);
    return key;
  }

  private static byte[] proposal66Key() {
    return AccountAssetTargetActivationResolver.proposal66PhysicalKey();
  }

  private static byte[] proposal53Key() {
    return AccountAssetTargetActivationResolver.PROPOSAL_53_KEY.getBytes(
        java.nio.charset.StandardCharsets.UTF_8);
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return participants;
  }

  private static BlockSnapshotMeta meta(int epoch) {
    return BlockSnapshotMeta.forBlock(epoch, hash(epoch), hash(epoch - 1), epoch * 1_000L);
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta) {
    return marker(meta, participants());
  }

  private static HistoryCommitMarker marker(BlockSnapshotMeta meta,
      List<String> markerParticipants) {
    int epoch = (int) meta.getEpoch();
    return new HistoryCommitMarker(meta, epoch - 1,
        new HistoryLocation(0, epoch * 100L, 100, epoch, bytes(32, epoch + 20)),
        new HistoryIndexLocation(epoch * 50L, 50, bytes(32, epoch + 30)),
        bytes(16, epoch + 40), markerParticipants);
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
      byte[] value = values.get(WrappedByteArray.copyOf(key));
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
      values.remove(WrappedByteArray.copyOf(key));
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
      List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
      values.forEach((key, value) -> entries.add(new AbstractMap.SimpleImmutableEntry<>(
          key.getBytes(), Arrays.copyOf(value, value.length))));
      return entries.iterator();
    }

    @Override
    public void flush(Map<WrappedByteArray, WrappedByteArray> rows) {
      rows.forEach((key, value) -> {
        if (value == null || value.getBytes() == null) {
          remove(key.getBytes());
        } else {
          put(key.getBytes(), value.getBytes());
        }
      });
    }

    @Override
    public void close() {
      values.clear();
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
