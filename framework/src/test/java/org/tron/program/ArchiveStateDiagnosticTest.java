package org.tron.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Longs;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.db.Manager;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.HistoricalAccountAssetBalanceResolver;
import org.tron.core.db2.archive.HistoricalAccountAssetPrefixResolver;
import org.tron.core.db2.archive.OldValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.exception.TronError;
import org.tron.core.store.AccountAssetStore;
import org.tron.core.store.DynamicPropertiesStore;

public class ArchiveStateDiagnosticTest {

  private static final long BLOCK_NUMBER = 100L;

  @Test
  public void shouldConvertDiagnosticFailureToFatalStartupError() {
    ArchivePersistenceException failure = new ArchivePersistenceException("injected failure");

    TronError fatal = assertThrows(TronError.class,
        () -> FullNode.runArchiveStateDiagnostic(() -> {
          throw failure;
        }));

    assertEquals(TronError.ErrCode.STATE_ARCHIVE_INIT, fatal.getErrCode());
    assertEquals(failure, fatal.getCause());
  }

  @Test
  public void shouldRemainDisabledWithoutExplicitSystemProperty() {
    String previous = System.getProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
    System.clearProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
    try {
      TronApplicationContext context = mock(TronApplicationContext.class);

      ArchiveStateDiagnostic.runIfEnabled(context);

      verifyNoInteractions(context);
    } finally {
      if (previous == null) {
        System.clearProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
      } else {
        System.setProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY, previous);
      }
    }
  }

  @Test
  public void shouldCompareExact27AndP66ThroughManagerRequests() {
    Manager manager = mock(Manager.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    DynamicPropertiesStore properties = mock(DynamicPropertiesStore.class);
    AccountAssetStore accountAssetStore = mock(AccountAssetStore.class);
    @SuppressWarnings("unchecked")
    DbSourceInter<byte[]> accountAssetSource = mock(DbSourceInter.class);
    when(manager.getDynamicPropertiesStore()).thenReturn(properties);
    when(properties.getLatestBlockHeaderNumber()).thenReturn(BLOCK_NUMBER);
    when(manager.getAccountAssetStore()).thenReturn(accountAssetStore);
    when(accountAssetStore.getDbSource()).thenReturn(accountAssetSource);

    byte[] address = new byte[21];
    address[0] = 0x41;
    Arrays.fill(address, 1, address.length, (byte) 7);
    String tokenId = "1000001";
    byte[] assetKey = concat(address, tokenId.getBytes(StandardCharsets.US_ASCII));
    byte[] assetValue = Longs.toByteArray(9L);
    when(accountAssetSource.iterator()).thenAnswer(invocation -> Collections.singletonList(
        entry(assetKey, assetValue)).iterator());
    when(accountAssetSource.getData(any(byte[].class))).thenAnswer(invocation ->
        Arrays.equals(assetKey, invocation.getArgument(0)) ? assetValue : null);
    when(accountAssetStore.prefixQuery(any(byte[].class))).thenReturn(Collections.singletonMap(
        WrappedByteArray.of(assetKey), assetValue));

    List<Chainbase> databases = new ArrayList<>();
    for (String dbName : ArchiveStoreScope.getStateDatabases()) {
      if ("account-asset".equals(dbName)) {
        continue;
      }
      Chainbase database = mock(Chainbase.class);
      when(database.getDbName()).thenReturn(dbName);
      if ("nullifier".equals(dbName)) {
        when(database.iterator()).thenAnswer(invocation -> Collections.emptyIterator());
        when(database.getUnchecked(any(byte[].class))).thenReturn(null);
      } else {
        byte[] key = bytes("key-" + dbName);
        byte[] value = bytes("value-" + dbName);
        when(database.iterator()).thenAnswer(invocation -> Collections.singletonList(
            entry(key, value)).iterator());
        when(database.getUnchecked(any(byte[].class))).thenAnswer(invocation ->
            Arrays.equals(key, invocation.getArgument(0)) ? value : null);
      }
      databases.add(database);
    }
    when(snapshotManager.getDbs()).thenReturn(databases);
    when(manager.getArchiveStateValue(anyLong(), anyString(), any(byte[].class)))
        .thenAnswer(invocation -> {
          String dbName = invocation.getArgument(1);
          if ("nullifier".equals(dbName)) {
            return OldValue.absent();
          }
          if ("account-asset".equals(dbName)) {
            return OldValue.present(assetValue);
          }
          return OldValue.present(bytes("value-" + dbName));
        });

    HistoricalAccountAssetBalanceResolver.Result logical =
        mock(HistoricalAccountAssetBalanceResolver.Result.class);
    when(logical.isAccountPresent()).thenReturn(true);
    when(logical.getPhase()).thenReturn(Phase.P66_ON);
    when(logical.getBalance()).thenReturn(9L);
    when(manager.getArchiveAccountAssetBalance(eq(BLOCK_NUMBER), any(byte[].class), eq(tokenId)))
        .thenReturn(logical);
    HistoricalAccountAssetPrefixResolver.Balance prefixBalance =
        mock(HistoricalAccountAssetPrefixResolver.Balance.class);
    when(prefixBalance.getTokenId()).thenReturn(tokenId);
    when(prefixBalance.getBalance()).thenReturn(9L);
    HistoricalAccountAssetPrefixResolver.Result prefix =
        mock(HistoricalAccountAssetPrefixResolver.Result.class);
    when(prefix.isAccountPresent()).thenReturn(true);
    when(prefix.getPhase()).thenReturn(Phase.P66_ON);
    when(prefix.getBalances()).thenReturn(Collections.singletonList(prefixBalance));
    when(manager.getArchiveAccountAssets(eq(BLOCK_NUMBER), any(byte[].class),
        any(HistoricalAccountAssetPrefixResolver.Limits.class))).thenReturn(prefix);

    ArchiveStateDiagnostic.Report report = ArchiveStateDiagnostic.run(manager, snapshotManager);

    assertEquals(BLOCK_NUMBER, report.getBlockNumber());
    assertEquals(27, report.getStoreCount());
    assertEquals(26, report.getPresentCount());
    assertEquals(1, report.getAbsentCount());
    assertEquals(Phase.P66_ON, report.getP66Phase());
    assertEquals(9L, report.getP66Balance());
    assertEquals(1, report.getP66PrefixCount());
  }

  @Test
  public void shouldRejectIncompleteRuntimeStoreScope() {
    Manager manager = mock(Manager.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    DynamicPropertiesStore properties = mock(DynamicPropertiesStore.class);
    AccountAssetStore accountAssetStore = mock(AccountAssetStore.class);
    @SuppressWarnings("unchecked")
    DbSourceInter<byte[]> accountAssetSource = mock(DbSourceInter.class);
    when(manager.getDynamicPropertiesStore()).thenReturn(properties);
    when(properties.getLatestBlockHeaderNumber()).thenReturn(BLOCK_NUMBER);
    when(manager.getAccountAssetStore()).thenReturn(accountAssetStore);
    when(accountAssetStore.getDbSource()).thenReturn(accountAssetSource);
    when(snapshotManager.getDbs()).thenReturn(Collections.emptyList());

    assertThrows(ArchivePersistenceException.class,
        () -> ArchiveStateDiagnostic.run(manager, snapshotManager));
  }

  private static Map.Entry<byte[], byte[]> entry(byte[] key, byte[] value) {
    return new SimpleImmutableEntry<>(key, value);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }
}
