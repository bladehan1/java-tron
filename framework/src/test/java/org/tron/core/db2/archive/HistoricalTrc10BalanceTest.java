package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.tron.core.db2.archive.HistoricalTestState.address;
import static org.tron.core.db2.archive.HistoricalVm.RETURN_TOP;
import static org.tron.core.db2.archive.HistoricalVm.code;
import static org.tron.core.db2.archive.HistoricalVm.push20;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.runtime.vm.DataWord;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.utils.AssetUtil;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.vm.config.VMConfig;
import org.tron.protos.Protocol.Account;
import org.tron.protos.Protocol.AccountType;

/**
 * Chapter 05 WP-3 (D6, review R3-02): TRC10 balances in historical execution are read through the
 * session at S(T) with upstream importAsset semantics; the latest AssetUtil stores are never
 * touched. T-3a .. T-3d.
 */
public class HistoricalTrc10BalanceTest {

  private static final byte[] CONTRACT = address(30);
  private static final byte[] HOLDER = address(31);
  private static final long TOKEN = 1_000_001L;
  private static final String TOKEN_ID = String.valueOf(TOKEN);

  private AccountAssetStore latestAssets;
  private DynamicPropertiesStore latestProperties;
  private Object originalAssets;
  private Object originalProperties;

  @Before
  public void poisonLatest() {
    originalAssets = ReflectionTestUtils.getField(AssetUtil.class, "accountAssetStore");
    originalProperties = ReflectionTestUtils.getField(AssetUtil.class, "dynamicPropertiesStore");
    latestAssets = mock(AccountAssetStore.class);
    latestProperties = mock(DynamicPropertiesStore.class);
    AssetUtil.setAccountAssetStore(latestAssets);
    AssetUtil.setDynamicPropertiesStore(latestProperties);
  }

  @After
  public void restoreLatest() {
    ReflectionTestUtils.setField(AssetUtil.class, "accountAssetStore", originalAssets);
    ReflectionTestUtils.setField(AssetUtil.class, "dynamicPropertiesStore", originalProperties);
    VMConfig.clearLocalSnapshot();
  }

  private static byte[] assetKey(byte[] address) {
    return new P66AccountAssetCodec().assetPhysicalKey(address, TOKEN_ID);
  }

  private static Account holder(boolean optimized, Long inline) {
    Account.Builder builder = Account.newBuilder().setAddress(ByteString.copyFrom(HOLDER))
        .setType(AccountType.Normal).setAssetOptimized(optimized).setBalance(1L);
    if (inline != null) {
      builder.putAssetV2(TOKEN_ID, inline);
    }
    return builder.build();
  }

  private long tokenBalance(HistoricalTestState state) throws Exception {
    state.contract(CONTRACT, HistoricalVm.tokenBalanceOf(HOLDER, TOKEN));
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      return new DataWord(result.getHReturn()).longValue();
    }
  }

  /** T-3a: the latest AccountAssetStore and DPS differ from S(T) and are never accessed. */
  @Test
  public void tokenBalanceReadsStAndNeverTouchesLatestAssetStores() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .account(holder(true, null))
        .put("account-asset", assetKey(HOLDER), ByteArray.fromLong(77L));
    assertEquals(77L, tokenBalance(state));
    verifyNoInteractions(latestAssets, latestProperties);
    assertEquals("one import = properties + account + account-asset", 1,
        state.reads("account-asset", assetKey(HOLDER)));
    assertEquals(1, state.reads("properties",
        HistoricalTestState.bytes("ALLOW_ASSET_OPTIMIZATION")));
  }

  /** T-3b: P66_OFF and P66_ON at T, optimized and lazily migrated accounts. */
  @Test
  public void resolvesEveryP66PhaseAndAccountLayout() throws Exception {
    // P66_OFF: inline map only.
    assertEquals(5L, tokenBalance(new HistoricalTestState().vmDefaults()
        .property("ALLOW_ASSET_OPTIMIZATION", 0L).account(holder(false, 5L))));
    assertEquals(0L, tokenBalance(new HistoricalTestState().vmDefaults()
        .property("ALLOW_ASSET_OPTIMIZATION", 0L).account(holder(false, null))));
    // P66_ON, optimized account: direct row (ABSENT = 0).
    assertEquals(7L, tokenBalance(new HistoricalTestState().vmDefaults()
        .account(holder(true, null)).put("account-asset", assetKey(HOLDER),
            ByteArray.fromLong(7L))));
    assertEquals(0L, tokenBalance(new HistoricalTestState().vmDefaults()
        .account(holder(true, null))));
    // P66_ON, never re-written (unoptimized) account: inline value, or 0.
    assertEquals(3L, tokenBalance(new HistoricalTestState().vmDefaults()
        .account(holder(false, 3L))));
    assertEquals(0L, tokenBalance(new HistoricalTestState().vmDefaults()
        .account(holder(false, null))));
    // Orphan direct row under an unoptimized account: upstream answer, counted.
    long orphans = HistoricalAccountAssetBalanceResolver.getOrphanDirectRowCount();
    assertEquals(0L, tokenBalance(new HistoricalTestState().vmDefaults()
        .account(holder(false, null)).put("account-asset", assetKey(HOLDER),
            ByteArray.fromLong(9L))));
    assertEquals(orphans + 1, HistoricalAccountAssetBalanceResolver.getOrphanDirectRowCount());
    // Absent account: 0 without import.
    assertEquals(0L, tokenBalance(new HistoricalTestState().vmDefaults()));
    verifyNoInteractions(latestAssets, latestProperties);
  }

  @Test
  public void missingProposal66PropertyFailsClosed() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .removeProperty("ALLOW_ASSET_OPTIMIZATION").account(holder(true, null))
        .contract(CONTRACT, HistoricalVm.tokenBalanceOf(HOLDER, TOKEN));
    try (HistoricalQuerySession session = state.open()) {
      RuntimeException failure = assertThrows(RuntimeException.class,
          () -> HistoricalVm.call(session, CONTRACT));
      assertEquals(HistoricalFailureCategory.CAPABILITY, HistoricalFailureCategory.of(failure));
      assertEquals(HistoricalQueryCapabilityException.UNSUPPORTED_STATE,
          HistoricalFailureCategory.detail(failure));
    }
  }

  @Test
  public void repeatedImportsAreChargedOnce() throws Exception {
    byte[] twice = code(push20(HOLDER), String.format("62%06x", TOKEN), "d1", "50",
        push20(HOLDER), String.format("62%06x", TOKEN), "d1", RETURN_TOP);
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .account(holder(true, null)).put("account-asset", assetKey(HOLDER),
            ByteArray.fromLong(11L)).contract(CONTRACT, twice);
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(11L, new DataWord(result.getHReturn()).longValue());
    }
    assertEquals(1, state.reads("account-asset", assetKey(HOLDER)));
  }

  /** T-3c: CALLTOKEN preflight, rejected transfer, and the upstream insufficient-balance path. */
  @Test
  public void callTokenPreflightTransferAndInsufficientBalance() throws Exception {
    byte[] peer = address(32);
    // value 0: preflight only, the call proceeds (peer has no code).
    byte[] zeroValue = code(HistoricalVm.callToken(peer, TOKEN, 0).replaceAll("50$", ""),
        RETURN_TOP);
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .account(peer, 0L).contract(CONTRACT, zeroValue);
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(1L, new DataWord(result.getHReturn()).longValue());
    }

    // value > 0 with enough S(T) balance: TRC10 transfer is a capability failure.
    byte[] transfer = code(HistoricalVm.callToken(peer, TOKEN, 5).replaceAll("50$", ""),
        RETURN_TOP);
    state = new HistoricalTestState().vmDefaults().account(peer, 0L)
        .contract(CONTRACT, transfer)
        .put("account-asset", assetKey(CONTRACT), ByteArray.fromLong(100L));
    try (HistoricalQuerySession session = state.open()) {
      try {
        HistoricalVm.call(session, CONTRACT);
        fail("TRC10 transfer must fail closed");
      } catch (RuntimeException failure) {
        assertEquals(HistoricalFailureCategory.CAPABILITY, HistoricalFailureCategory.of(failure));
      }
    }

    // value > 0 with an insufficient S(T) balance: upstream pushes 0, no capability error.
    state = new HistoricalTestState().vmDefaults().account(peer, 0L)
        .contract(CONTRACT, transfer)
        .put("account-asset", assetKey(CONTRACT), ByteArray.fromLong(4L));
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(result.getException());
      assertEquals(0L, new DataWord(result.getHReturn()).longValue());
      assertFalse(session.isTerminal());
    }
    verifyNoInteractions(latestAssets, latestProperties);
  }

  /** T-3d: an overlay write to the account (TRX transfer) does not disturb its S(T) TRC10. */
  @Test
  public void tokenBalanceAfterAnOverlayWriteToTheSameAccount() throws Exception {
    // CALL HOLDER with 1 sun, then TOKENBALANCE(HOLDER).
    String payHolder = "6000600060006000" + "6001" + push20(HOLDER) + "620f4240" + "f1" + "50";
    byte[] contractCode = code(payHolder, push20(HOLDER), String.format("62%06x", TOKEN), "d1",
        RETURN_TOP);
    HistoricalTestState state = new HistoricalTestState().vmDefaults()
        .account(holder(true, null))
        .put("account-asset", assetKey(HOLDER), ByteArray.fromLong(13L))
        .contract(CONTRACT, contractCode);
    state.account(Account.newBuilder().setAddress(ByteString.copyFrom(CONTRACT))
        .setType(AccountType.Contract).setAssetOptimized(true).setBalance(10L).build());
    try (HistoricalQuerySession session = state.open()) {
      ProgramResult result = HistoricalVm.call(session, CONTRACT);
      assertNull(String.valueOf(result.getException()), result.getException());
      assertEquals(13L, new DataWord(result.getHReturn()).longValue());
      assertFalse(session.isTerminal());
    }
    verifyNoInteractions(latestAssets, latestProperties);
  }

  /** Trap: AssetUtil inside a historical scope fails closed and marks the session. */
  @Test
  public void assetUtilTrapsInsideHistoricalScope() throws Exception {
    HistoricalTestState state = new HistoricalTestState().vmDefaults();
    try (HistoricalQuerySession session = state.open()) {
      try (HistoricalExecutionScope ignored = HistoricalExecutionScope.enter(session)) {
        assertThrows(HistoricalQueryCapabilityException.class,
            () -> AssetUtil.importAsset(holder(true, null), TOKEN_ID.getBytes()));
      }
      assertTrue(session.isTerminal());
    }
    verifyNoInteractions(latestAssets, latestProperties);
  }
}
