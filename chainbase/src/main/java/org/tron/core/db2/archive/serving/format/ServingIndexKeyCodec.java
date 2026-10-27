package org.tron.core.db2.archive.serving.format;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.tron.core.db2.archive.ArchiveStoreScope;

/** Persistent serving key layout; no Store, file handle or mutation ownership. */
public final class ServingIndexKeyCodec {
  private ServingIndexKeyCodec() {}

  public static final byte DATA_PREFIX = 1;
  public static final byte RANGE_DATA_PREFIX = 2;
  public static final byte KEY_META_PREFIX = 3;
  public static final byte KEY_PAGE_PREFIX = 4;
  public static final byte STORE_COVERAGE_PREFIX = 5;

  public static byte[] keyMetaKey(String dbName, byte[] rawKey) {
    return exactKey(KEY_META_PREFIX, dbName, rawKey, null);
  }

  public static byte[] keyPageKey(String dbName, byte[] rawKey, int pageIndex) {
    if (pageIndex < 0) {
      throw new IllegalArgumentException("Serving page index must not be negative");
    }
    return exactKey(KEY_PAGE_PREFIX, dbName, rawKey, pageIndex);
  }

  public static byte[] exactKey(byte prefix, String dbName, byte[] rawKey, Integer pageIndex) {
    int storeId = ArchiveStoreScope.getStoreId(dbName);
    byte[] encodedKey = encodeRangeRawKey(rawKey);
    int suffix = pageIndex == null ? 0 : Integer.BYTES;
    ByteBuffer key =
        ByteBuffer.allocate(1 + Short.BYTES + encodedKey.length + 2 + suffix)
            .put(prefix)
            .putShort((short) storeId)
            .put(encodedKey)
            .put((byte) 0)
            .put((byte) 0);
    if (pageIndex != null) {
      key.putInt(pageIndex);
    }
    return key.array();
  }

  public static byte[] storeCoverageKey(String dbName) {
    int storeId = ArchiveStoreScope.getStoreId(dbName);
    return ByteBuffer.allocate(1 + Short.BYTES)
        .put(STORE_COVERAGE_PREFIX)
        .putShort((short) storeId)
        .array();
  }

  public static byte[] exactPartitionPrefix(byte prefix, String dbName) {
    int storeId = ArchiveStoreScope.getStoreId(dbName);
    return ByteBuffer.allocate(1 + Short.BYTES).put(prefix).putShort((short) storeId).array();
  }

  public static byte[] dataKey(String dbName, byte[] rawKey, long epoch) {
    if (epoch < 0) {
      throw new IllegalArgumentException("Serving index epoch must not be negative");
    }
    byte[] prefix = dataPrefix(dbName, rawKey);
    return ByteBuffer.allocate(prefix.length + Long.BYTES).put(prefix).putLong(epoch).array();
  }

  public static byte[] dataPrefix(String dbName, byte[] rawKey) {
    byte[] database = dbName.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(1 + Integer.BYTES + database.length + Integer.BYTES + rawKey.length)
        .put(DATA_PREFIX)
        .putInt(database.length)
        .put(database)
        .putInt(rawKey.length)
        .put(rawKey)
        .array();
  }

  public static byte[] rangeDataKey(String dbName, byte[] rawKey, long epoch) {
    if (epoch < 0) {
      throw new IllegalArgumentException("Serving index epoch must not be negative");
    }
    byte[] databasePrefix = rangeDatabasePrefix(dbName);
    byte[] encodedKey = encodeRangeRawKey(rawKey);
    return ByteBuffer.allocate(databasePrefix.length + encodedKey.length + 2 + Long.BYTES)
        .put(databasePrefix)
        .put(encodedKey)
        .put((byte) 0)
        .put((byte) 0)
        .putLong(epoch)
        .array();
  }

  public static byte[] rangeDatabasePrefix(String dbName) {
    byte[] name = dbName.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(1 + Integer.BYTES + name.length)
        .put(RANGE_DATA_PREFIX)
        .putInt(name.length)
        .put(name)
        .array();
  }

  public static byte[] encodeRangeRawKey(byte[] rawKey) {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream(rawKey.length);
    for (byte value : rawKey) {
      encoded.write(value);
      if (value == 0) {
        encoded.write(0xff);
      }
    }
    return encoded.toByteArray();
  }

  public static byte[] rangeAfterRawKey(byte[] databasePrefix, byte[] rawKey) {
    byte[] encoded = encodeRangeRawKey(rawKey);
    return ByteBuffer.allocate(databasePrefix.length + encoded.length + 2)
        .put(databasePrefix)
        .put(encoded)
        .put((byte) 0)
        .put((byte) 1)
        .array();
  }

  public static <T> T decodeRangeDataKey(
      byte[] key, byte[] databasePrefix, RangeKeyFactory<T> factory) {
    if (!startsWith(key, databasePrefix) || key.length < databasePrefix.length + 2 + Long.BYTES) {
      return null;
    }
    ByteArrayOutputStream rawKey = new ByteArrayOutputStream();
    int cursor = databasePrefix.length;
    int epochOffset = key.length - Long.BYTES;
    while (cursor < epochOffset) {
      byte value = key[cursor++];
      if (value != 0) {
        rawKey.write(value);
      } else if (cursor < epochOffset && key[cursor] == (byte) 0xff) {
        rawKey.write(0);
        cursor++;
      } else if (cursor < epochOffset && key[cursor] == 0) {
        cursor++;
        if (cursor != epochOffset) {
          return null;
        }
        return factory.create(
            rawKey.toByteArray(), ByteBuffer.wrap(key, epochOffset, Long.BYTES).getLong());
      } else {
        return null;
      }
    }
    return null;
  }

  public static byte[] concat(byte[] left, byte[] right) {
    return ByteBuffer.allocate(left.length + right.length).put(left).put(right).array();
  }

  public static boolean startsWith(byte[] value, byte[] prefix) {
    if (value.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (value[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  @FunctionalInterface
  public interface RangeKeyFactory<T> {
    T create(byte[] rawKey, long epoch);
  }
}
