package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.AssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.DecodedAssetRow;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;
import org.tron.protos.Protocol.Account;

public class P66AccountAssetCodecTest {

  private final P66AccountAssetCodec codec = new P66AccountAssetCodec();

  @Test
  public void p66OffPreservesExactAccountBytesAndRejectsDirectRows() throws Exception {
    byte[] address = address(1);
    Account account = account(address).toBuilder()
        .putAsset("legacy-name", 7L)
        .putAssetV2("1000001", 11L)
        .build();
    byte[] raw = account.toByteArray();

    byte[] canonical = codec.canonicalizeAccount(Phase.P66_OFF, address, raw);
    assertArrayEquals(raw, canonical);
    assertFalse(Account.parseFrom(canonical).getAssetOptimized());
    codec.requireCanonicalLayout(Phase.P66_OFF, address, canonical,
        Collections.emptyList());
    assertThrows(ArchivePersistenceException.class,
        () -> codec.encodeAssetRow(Phase.P66_OFF, address, "1000001", 11L));

    byte[] optimized = account.toBuilder().setAssetOptimized(true).build().toByteArray();
    assertThrows(ArchivePersistenceException.class,
        () -> codec.canonicalizeAccount(Phase.P66_OFF, address, optimized));
  }

  @Test
  public void activationAndOnCanonicalizeAccountAndRowsDeterministically() throws Exception {
    byte[] address = address(2);
    Account raw = account(address).toBuilder()
        .setBalance(99L)
        .putAsset("legacy-name", 5L)
        .putAssetV2("1000001", 17L)
        .build();

    byte[] activation = codec.canonicalizeAccount(Phase.P66_ACTIVATION, address,
        raw.toByteArray());
    byte[] on = codec.canonicalizeAccount(Phase.P66_ON, address, raw.toByteArray());
    assertArrayEquals(activation, on);
    Account canonical = Account.parseFrom(activation);
    assertTrue(canonical.getAssetOptimized());
    assertTrue(canonical.getAssetMap().isEmpty());
    assertTrue(canonical.getAssetV2Map().isEmpty());
    assertEquals(99L, canonical.getBalance());
    assertEquals("1a154100000000000000000000000000000000000000022063e00301",
        ByteArray.toHexString(activation));

    AssetRow present = codec.encodeAssetRow(Phase.P66_ACTIVATION, address, "1000001", 17L);
    AssetRow absent = codec.encodeAssetRow(Phase.P66_ACTIVATION, address, "1000002", 0L);
    assertTrue(present.getPostValue().isPresent());
    assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(17L).array(),
        present.getPostValue().getValue());
    assertFalse(absent.getPostValue().isPresent());
    codec.requireCanonicalLayout(Phase.P66_ACTIVATION, address, activation,
        Arrays.asList(present, absent));
    codec.requireCanonicalLayout(Phase.P66_ON, address, on,
        Arrays.asList(present, absent));
  }

  @Test
  public void decodesSignedBalanceAndDefensivelyOwnsBytes() {
    byte[] address = address(3);
    AssetRow row = codec.encodeAssetRow(Phase.P66_ON, address, "0", -9L);
    byte[] key = row.getPhysicalRawKey();
    byte[] value = row.getPostValue().getValue();
    DecodedAssetRow decoded = codec.decodePresentAssetRow(key, value);
    assertArrayEquals(address, decoded.getAccountAddress());
    assertEquals("0", decoded.getTokenId());
    assertEquals(-9L, decoded.getBalance());

    key[0] ^= 1;
    value[0] ^= 1;
    assertArrayEquals(address, decoded.getAccountAddress());
    assertEquals(-9L, decoded.getBalance());
    assertArrayEquals(address, Arrays.copyOf(row.getPhysicalRawKey(), address.length));
  }

  @Test
  public void rejectsMalformedAccountKeyTokenValueAndStoredZero() {
    byte[] address = address(4);
    byte[] raw = account(address).toByteArray();
    assertThrows(ArchivePersistenceException.class,
        () -> codec.canonicalizeAccount(Phase.P66_OFF, new byte[20], raw));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.canonicalizeAccount(Phase.P66_OFF, address(5), raw));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.canonicalizeAccount(Phase.P66_OFF, address, new byte[]{-1, -1}));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.encodeAssetRow(Phase.P66_ON, address, "01", 1L));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.encodeAssetRow(Phase.P66_ON, address, "1a", 1L));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.decodePresentAssetRow(address, new byte[Long.BYTES]));
    byte[] key = concat(address, "1000001");
    assertThrows(ArchivePersistenceException.class,
        () -> codec.decodePresentAssetRow(key, new byte[7]));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.decodePresentAssetRow(key, new byte[Long.BYTES]));
  }

  @Test
  public void canonicalLayoutRejectsMixedUnsortedDuplicateAndForeignRows() {
    byte[] address = address(6);
    byte[] raw = account(address).toBuilder().putAssetV2("1000001", 1L).build().toByteArray();
    byte[] canonical = codec.canonicalizeAccount(Phase.P66_ON, address, raw);
    AssetRow first = codec.encodeAssetRow(Phase.P66_ON, address, "1000001", 1L);
    AssetRow second = codec.encodeAssetRow(Phase.P66_ON, address, "1000002", 2L);

    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_ON, address, raw,
            Collections.emptyList()));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_OFF, address, raw,
            Collections.singletonList(first)));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_ON, address, canonical,
            Arrays.asList(second, first)));
    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_ON, address, canonical,
            Arrays.asList(first, first)));
    AssetRow foreign = codec.encodeAssetRow(Phase.P66_ON, address(7), "1000003", 3L);
    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_ON, address, canonical,
            Collections.singletonList(foreign)));
    AssetRow malformedAbsent = new AssetRow(concat(address, "01"), PostValue.absent());
    assertThrows(ArchivePersistenceException.class,
        () -> codec.requireCanonicalLayout(Phase.P66_ON, address, canonical,
            Collections.singletonList(malformedAbsent)));
  }

  private static Account account(byte[] address) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address)).build();
  }

  private static byte[] address(int suffix) {
    byte[] address = new byte[21];
    address[0] = 0x41;
    address[20] = (byte) suffix;
    return address;
  }

  private static byte[] concat(byte[] address, String token) {
    byte[] tokenBytes = token.getBytes(StandardCharsets.US_ASCII);
    return ByteBuffer.allocate(address.length + tokenBytes.length)
        .put(address)
        .put(tokenBytes)
        .array();
  }
}
