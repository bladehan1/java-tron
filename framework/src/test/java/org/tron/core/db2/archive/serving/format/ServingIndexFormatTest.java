package org.tron.core.db2.archive.serving.format;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.serving.format.ServingIndexRecordCodec.CoverageView;
import org.tron.core.db2.archive.serving.format.ServingIndexRecordCodec.DescriptorView;
import org.tron.core.db2.archive.serving.format.ServingIndexRecordCodec.KeyMetadataView;

/** Golden bytes captured from the pre-extraction compiled implementation, not regenerated here. */
public class ServingIndexFormatTest {
  private static final byte[] RAW = new byte[] {1, 0, (byte) 255, 0, 2};

  @Test
  public void preservesLegacyAndExactDescriptorGoldenBytesAndDecoding() throws Exception {
    Map<String, byte[]> golden = golden();
    for (short version : new short[] {3, 4, 5}) {
      byte[] encoded = ServingIndexRecordCodec.encodeDescriptor(new Descriptor(version));
      assertArrayEquals(golden.get("descriptor-" + version), encoded);
      List<Object> decoded =
          ServingIndexRecordCodec.decodeDescriptor(
              encoded,
              (v, scope, generation, from, through, head, source, latest, participants, changes) ->
                  Arrays.<Object>asList(
                      v,
                      scope,
                      generation,
                      from,
                      through,
                      head,
                      source,
                      latest,
                      participants,
                      changes));
      assertEquals(version, decoded.get(0));
      assertEquals(
          ArchiveStoreScope.scopeIdentity(Arrays.asList("account", "code")), decoded.get(1));
      assertEquals("golden-gen", decoded.get(2));
      assertEquals(100L, decoded.get(3));
      assertEquals(600L, decoded.get(4));
      assertArrayEquals(hash(1), (byte[]) decoded.get(5));
      assertArrayEquals(hash(2), (byte[]) decoded.get(6));
      assertArrayEquals(hash(3), (byte[]) decoded.get(7));
      assertEquals(Arrays.asList("account", "code"), decoded.get(8));
      assertEquals(77L, decoded.get(9));
    }
  }

  @Test
  public void preservesCoverageInlinePagedAndEpochPageGoldenBytes() throws Exception {
    Map<String, byte[]> golden = golden();
    byte[] coverage = ServingIndexRecordCodec.encodeCoverage(new Coverage());
    assertArrayEquals(golden.get("coverage"), coverage);
    List<Object> decoded =
        ServingIndexRecordCodec.decodeCoverage(
            coverage,
            (name, from, through, head, source, generation, comparator) ->
                Arrays.<Object>asList(name, from, through, head, source, generation, comparator));
    assertEquals("code", decoded.get(0));
    assertEquals(100L, decoded.get(1));
    assertEquals(600L, decoded.get(2));
    assertArrayEquals(hash(1), (byte[]) decoded.get(3));
    assertArrayEquals(hash(2), (byte[]) decoded.get(4));
    assertEquals("golden-gen", decoded.get(5));
    assertEquals("UNSIGNED_RAW_V1", decoded.get(6));
    for (int count : new int[] {1, 4}) {
      byte[] encoded = ServingIndexRecordCodec.encodeKeyMeta(new Metadata(count, false));
      assertArrayEquals(golden.get("inline-" + count), encoded);
      List<Object> meta =
          ServingIndexRecordCodec.decodeKeyMeta(
              encoded,
              (mode, size, first, last, epochs) ->
                  Arrays.<Object>asList(mode, size, first, last, epochs));
      assertEquals((byte) 1, meta.get(0));
      assertEquals((long) count, meta.get(1));
      assertEquals(10L, meta.get(2));
      assertEquals(10L + 2 * (count - 1), meta.get(3));
      assertArrayEquals(epochs(count), (long[]) meta.get(4));
    }
    assertArrayEquals(
        golden.get("paged-513"), ServingIndexRecordCodec.encodeKeyMeta(new Metadata(513, true)));
    for (int count : new int[] {1, 4, 512}) {
      byte[] encoded = ServingIndexRecordCodec.encodeEpochPage(epochs(count));
      assertArrayEquals(golden.get("page-" + count), encoded);
      assertArrayEquals(epochs(count), ServingIndexRecordCodec.decodeEpochPage(encoded));
    }
  }

  @Test
  public void preservesExactAndEscapedRangeKeyGoldenBytes() throws Exception {
    Map<String, byte[]> golden = golden();
    assertArrayEquals(golden.get("key-meta"), ServingIndexKeyCodec.keyMetaKey("code", RAW));
    assertArrayEquals(golden.get("key-page"), ServingIndexKeyCodec.keyPageKey("code", RAW, 2));
    assertArrayEquals(golden.get("coverage-key"), ServingIndexKeyCodec.storeCoverageKey("code"));
    assertArrayEquals(golden.get("data-key"), ServingIndexKeyCodec.dataKey("code", RAW, 101));
    byte[] key = ServingIndexKeyCodec.rangeDataKey("code", RAW, 101);
    assertArrayEquals(golden.get("range-key"), key);
    byte[] prefix = ServingIndexKeyCodec.rangeDatabasePrefix("code");
    assertArrayEquals(golden.get("range-prefix"), prefix);
    assertArrayEquals(
        golden.get("range-after"), ServingIndexKeyCodec.rangeAfterRawKey(prefix, RAW));
    List<Object> decoded =
        ServingIndexKeyCodec.decodeRangeDataKey(
            key, prefix, (raw, epoch) -> Arrays.<Object>asList(raw, epoch));
    assertArrayEquals(RAW, (byte[]) decoded.get(0));
    assertEquals(101L, decoded.get(1));
  }

  @Test
  public void retainsCorruptionOrderingAndPageBoundaryRejection() throws Exception {
    Map<String, byte[]> golden = golden();
    byte[] descriptor =
        Arrays.copyOf(golden.get("descriptor-5"), golden.get("descriptor-5").length);
    descriptor[descriptor.length - 1] ^= 1;
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ServingIndexRecordCodec.decodeDescriptor(
                descriptor,
                (v,
                    scope,
                    generation,
                    from,
                    through,
                    head,
                    source,
                    latest,
                    participants,
                    changes) -> v));
    byte[] page = Arrays.copyOf(golden.get("page-4"), golden.get("page-4").length);
    page[page.length - 1] ^= 1;
    assertThrows(
        ArchivePersistenceException.class, () -> ServingIndexRecordCodec.decodeEpochPage(page));
    assertThrows(
        ArchivePersistenceException.class,
        () ->
            ServingIndexRecordCodec.decodeEpochPage(
                ServingIndexRecordCodec.encodeEpochPage(new long[] {12, 10})));
    assertThrows(
        ArchivePersistenceException.class,
        () ->
            ServingIndexRecordCodec.decodeEpochPage(
                ServingIndexRecordCodec.encodeEpochPage(epochs(513))));
    assertThrows(
        IllegalArgumentException.class, () -> ServingIndexKeyCodec.keyPageKey("code", RAW, -1));
  }

  private static Map<String, byte[]> golden() throws Exception {
    Map<String, byte[]> result = new LinkedHashMap<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                ServingIndexFormatTest.class.getResourceAsStream(
                    "/archive/serving/format-golden.tsv"),
                StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\t");
        result.put(parts[0], Hex.decode(parts[1]));
      }
    }
    assertEquals(17, result.size());
    return result;
  }

  private static byte[] hash(int seed) {
    byte[] result = new byte[32];
    for (int i = 0; i < result.length; i++) {
      result[i] = (byte) (seed + i);
    }
    return result;
  }

  private static long[] epochs(int count) {
    long[] result = new long[count];
    for (int i = 0; i < count; i++) {
      result[i] = 10L + 2 * i;
    }
    return result;
  }

  private static final class Descriptor implements DescriptorView {
    private final short version;

    private Descriptor(short version) {
      this.version = version;
    }

    @Override
    public short formatVersion() {
      return version;
    }

    @Override
    public String scopeIdentity() {
      return ArchiveStoreScope.scopeIdentity(Arrays.asList("account", "code"));
    }

    @Override
    public String generationId() {
      return "golden-gen";
    }

    @Override
    public long indexedFrom() {
      return 100;
    }

    @Override
    public long indexedThrough() {
      return 600;
    }

    @Override
    public byte[] headHash() {
      return hash(1);
    }

    @Override
    public byte[] sourceDigest() {
      return hash(2);
    }

    @Override
    public byte[] latestSourceIdentityDigest() {
      return hash(3);
    }

    @Override
    public List<String> participants() {
      return Arrays.asList("account", "code");
    }

    @Override
    public long keyChanges() {
      return 77;
    }
  }

  private static final class Coverage implements CoverageView {
    @Override
    public String getDbName() {
      return "code";
    }

    @Override
    public long getIndexedFrom() {
      return 100;
    }

    @Override
    public long getIndexedThrough() {
      return 600;
    }

    @Override
    public byte[] getHeadHash() {
      return hash(1);
    }

    @Override
    public byte[] getSourceDigest() {
      return hash(2);
    }

    @Override
    public String getGenerationId() {
      return "golden-gen";
    }

    @Override
    public String getComparatorId() {
      return "UNSIGNED_RAW_V1";
    }
  }

  private static final class Metadata implements KeyMetadataView {
    private final int count;
    private final boolean paged;

    private Metadata(int count, boolean paged) {
      this.count = count;
      this.paged = paged;
    }

    @Override
    public byte mode() {
      return (byte) (paged ? 2 : 1);
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public long firstEpoch() {
      return 10;
    }

    @Override
    public long lastEpoch() {
      return 10L + 2 * (count - 1);
    }

    @Override
    public int inlineEpochCount() {
      return count;
    }

    @Override
    public long inlineEpoch(int index) {
      return 10L + 2 * index;
    }
  }
}
