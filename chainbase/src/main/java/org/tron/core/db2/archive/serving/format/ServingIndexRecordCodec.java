package org.tron.core.db2.archive.serving.format;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveStoreScope;

/** Pure persistent serving records; callers retain their own value objects and Store ownership. */
public final class ServingIndexRecordCodec {
  private ServingIndexRecordCodec() {}

  private static final int MAGIC = 0x534b4947; // SKIG
  public static final short VERSION = 4;
  public static final short EXACT_VERSION = 5;
  private static final int MAX_MANIFEST_SIZE = 1024 * 1024;
  public static final int INLINE_EPOCH_LIMIT = 4;
  public static final int EPOCHS_PER_PAGE = 512;
  public static final byte INLINE = 1;
  public static final byte PAGED = 2;

  public static byte[] encodeKeyMeta(KeyMetadataView meta) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeByte(meta.mode());
      output.writeLong(meta.count());
      output.writeLong(meta.firstEpoch());
      output.writeLong(meta.lastEpoch());
      if (meta.mode() == INLINE) {
        output.writeInt(meta.inlineEpochCount());
        for (int index = 0; index < meta.inlineEpochCount(); index++) {
          output.writeLong(meta.inlineEpoch(index));
        }
      }
      output.flush();
      return withChecksum(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException(
          "Unexpected exact serving metadata encoding failure", impossible);
    }
  }

  public static <T> T decodeKeyMeta(byte[] encoded, KeyMetadataFactory<T> factory) {
    byte[] payload = checkedPayload(encoded, "exact serving key metadata");
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
      byte mode = input.readByte();
      long count = input.readLong();
      long first = input.readLong();
      long last = input.readLong();
      if (count <= 0 || first < 0 || last < first || mode != INLINE && mode != PAGED) {
        throw new ArchivePersistenceException("Invalid exact serving key metadata");
      }
      long[] inline = null;
      if (mode == INLINE) {
        int size = input.readInt();
        if (size != count || size <= 0 || size > INLINE_EPOCH_LIMIT) {
          throw new ArchivePersistenceException("Invalid inline serving metadata");
        }
        inline = new long[size];
        for (int i = 0; i < size; i++) {
          inline[i] = input.readLong();
        }
        requireStrictEpochs(asList(inline), Long.MIN_VALUE);
      }
      if (input.available() != 0) {
        throw new ArchivePersistenceException("Exact serving metadata payload mismatch");
      }
      return factory.create(mode, count, first, last, inline);
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Exact serving metadata is truncated", failure);
    }
  }

  public static byte[] encodeEpochPage(long[] epochs) {
    ByteBuffer payload =
        ByteBuffer.allocate(Integer.BYTES + epochs.length * Long.BYTES).putInt(epochs.length);
    for (long epoch : epochs) {
      payload.putLong(epoch);
    }
    return withChecksum(payload.array());
  }

  public static long[] decodeEpochPage(byte[] encoded) {
    byte[] payload = checkedPayload(encoded, "exact serving epoch page");
    ByteBuffer input = ByteBuffer.wrap(payload);
    if (input.remaining() < Integer.BYTES) {
      throw new ArchivePersistenceException("Exact serving epoch page is truncated");
    }
    int count = input.getInt();
    if (count <= 0 || count > EPOCHS_PER_PAGE || input.remaining() != count * Long.BYTES) {
      throw new ArchivePersistenceException("Invalid exact serving epoch page");
    }
    long[] epochs = new long[count];
    for (int i = 0; i < count; i++) {
      epochs[i] = input.getLong();
    }
    requireStrictEpochs(asList(epochs), Long.MIN_VALUE);
    return epochs;
  }

  public static byte[] encodeCoverage(CoverageView coverage) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeUTF(coverage.getDbName());
      output.writeLong(coverage.getIndexedFrom());
      output.writeLong(coverage.getIndexedThrough());
      output.write(coverage.getHeadHash());
      output.write(coverage.getSourceDigest());
      output.writeUTF(coverage.getGenerationId());
      output.writeUTF(coverage.getComparatorId());
      output.flush();
      return withChecksum(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected Store coverage encoding failure", impossible);
    }
  }

  public static <T> T decodeCoverage(byte[] encoded, CoverageFactory<T> factory) {
    byte[] payload = checkedPayload(encoded, "serving Store coverage");
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
      String database = input.readUTF();
      long from = input.readLong();
      long through = input.readLong();
      byte[] headHash = new byte[32];
      byte[] sourceDigest = new byte[32];
      input.readFully(headHash);
      input.readFully(sourceDigest);
      String generationId = input.readUTF();
      String comparatorId = input.readUTF();
      if (from < 0
          || through < from
          || generationId.isEmpty()
          || comparatorId.isEmpty()
          || input.available() != 0) {
        throw new ArchivePersistenceException("Invalid serving Store coverage");
      }
      return factory.create(
          database, from, through, headHash, sourceDigest, generationId, comparatorId);
    } catch (IOException failure) {
      throw new ArchivePersistenceException("Serving Store coverage is truncated", failure);
    }
  }

  public static byte[] encodeDescriptor(DescriptorView descriptor) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(descriptor.formatVersion());
      output.writeShort(0);
      output.writeUTF(descriptor.scopeIdentity());
      output.writeUTF(descriptor.generationId());
      output.writeLong(descriptor.indexedFrom());
      output.writeLong(descriptor.indexedThrough());
      output.write(descriptor.headHash());
      output.write(descriptor.sourceDigest());
      output.write(descriptor.latestSourceIdentityDigest());
      output.writeLong(descriptor.keyChanges());
      output.writeInt(descriptor.participants().size());
      for (String participant : descriptor.participants()) {
        output.writeUTF(participant);
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected serving manifest encoding failure", impossible);
    }
  }

  public static <T> T decodeDescriptor(byte[] encoded, DescriptorFactory<T> factory) {
    if (encoded == null || encoded.length < 96 || encoded.length > MAX_MANIFEST_SIZE) {
      throw new IllegalArgumentException("Serving index manifest length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Serving index manifest checksum mismatch");
    }
    try {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      if (input.readInt() != MAGIC) {
        throw new IllegalArgumentException("Unsupported serving index manifest");
      }
      short version = input.readShort();
      if ((version != EXACT_VERSION && version != VERSION && version != VERSION - 1)
          || input.readShort() != 0) {
        throw new IllegalArgumentException("Unsupported serving index manifest");
      }
      String scopeIdentity = input.readUTF();
      String generationId = input.readUTF();
      long from = input.readLong();
      long through = input.readLong();
      byte[] headHash = new byte[32];
      byte[] sourceDigest = new byte[32];
      input.readFully(headHash);
      input.readFully(sourceDigest);
      byte[] latestSourceIdentityDigest = new byte[32];
      input.readFully(latestSourceIdentityDigest);
      long keyChanges = input.readLong();
      int count = input.readInt();
      if (generationId.isEmpty()
          || from < 0
          || through < from
          || keyChanges < 0
          || count <= 0
          || count > ArchiveStoreScope.getStateDatabases().size() + 16) {
        throw new IllegalArgumentException("Invalid serving index manifest fields");
      }
      List<String> participants = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        participants.add(input.readUTF());
      }
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Serving index manifest payload mismatch");
      }
      List<String> sorted = sortedParticipants(participants);
      if (!scopeIdentity.equals(ArchiveStoreScope.scopeIdentity(sorted))) {
        throw new IllegalArgumentException("Serving index manifest scope identity mismatch");
      }
      return factory.create(
          version,
          scopeIdentity,
          generationId,
          from,
          through,
          headHash,
          sourceDigest,
          latestSourceIdentityDigest,
          sorted,
          keyChanges);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Serving index manifest is truncated", invalid);
    }
  }

  private static byte[] withChecksum(byte[] payload) {
    return ByteBuffer.allocate(payload.length + Integer.BYTES)
        .put(payload)
        .putInt(Hashing.crc32c().hashBytes(payload).asInt())
        .array();
  }

  private static byte[] checkedPayload(byte[] encoded, String name) {
    if (encoded == null || encoded.length <= Integer.BYTES) {
      throw new ArchivePersistenceException(name + " is missing or truncated");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new ArchivePersistenceException(name + " checksum mismatch");
    }
    return payload;
  }

  public static void requireStrictEpochs(List<Long> epochs, long previous) {
    for (long epoch : epochs) {
      if (epoch < 0 || epoch <= previous) {
        throw new ArchivePersistenceException("Serving epochs are not strictly increasing");
      }
      previous = epoch;
    }
  }

  public static List<String> sortedParticipants(List<String> databases) {
    List<String> result = new ArrayList<>(Objects.requireNonNull(databases, "databases"));
    Collections.sort(result);
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Serving index participant set must not be empty");
    }
    String previous = null;
    for (String database : result) {
      if (database == null || database.isEmpty() || database.equals(previous)) {
        throw new IllegalArgumentException("Serving index participant set is invalid");
      }
      previous = database;
    }
    return Collections.unmodifiableList(result);
  }

  private static List<Long> asList(long[] epochs) {
    List<Long> result = new ArrayList<>(epochs.length);
    for (long epoch : epochs) {
      result.add(epoch);
    }
    return result;
  }

  public interface DescriptorView {
    short formatVersion();

    String scopeIdentity();

    String generationId();

    long indexedFrom();

    long indexedThrough();

    byte[] headHash();

    byte[] sourceDigest();

    byte[] latestSourceIdentityDigest();

    List<String> participants();

    long keyChanges();
  }

  @FunctionalInterface
  public interface DescriptorFactory<T> {
    T create(
        short formatVersion,
        String scopeIdentity,
        String generationId,
        long indexedFrom,
        long indexedThrough,
        byte[] headHash,
        byte[] sourceDigest,
        byte[] latestSourceIdentityDigest,
        List<String> participants,
        long keyChanges);
  }

  public interface CoverageView {
    String getDbName();

    long getIndexedFrom();

    long getIndexedThrough();

    byte[] getHeadHash();

    byte[] getSourceDigest();

    String getGenerationId();

    String getComparatorId();
  }

  @FunctionalInterface
  public interface CoverageFactory<T> {
    T create(
        String dbName,
        long indexedFrom,
        long indexedThrough,
        byte[] headHash,
        byte[] sourceDigest,
        String generationId,
        String comparatorId);
  }

  public interface KeyMetadataView {
    byte mode();

    long count();

    long firstEpoch();

    long lastEpoch();

    int inlineEpochCount();

    long inlineEpoch(int index);
  }

  @FunctionalInterface
  public interface KeyMetadataFactory<T> {
    T create(byte mode, long count, long firstEpoch, long lastEpoch, long[] inlineEpochs);
  }
}
