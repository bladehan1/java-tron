package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;

/** Immutable identity shared by a durable base or one reversible current-root layer. */
public final class PathStateRootMetadata {

  public static final int DIGEST_LENGTH = 32;

  private static final int MAGIC = 0x50534d54; // PSMT
  private static final short VERSION = 1;
  private static final int MAX_LENGTH = 16 * 1024;

  private final Kind kind;
  private final long blockNumber;
  private final byte[] blockHash;
  private final byte[] parentHash;
  private final long timestamp;
  private final P66Phase phase;
  private final byte[] parentStateRoot;
  private final byte[] stateRoot;
  private final byte[] payloadDigest;

  private PathStateRootMetadata(Kind kind, long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] parentStateRoot, byte[] stateRoot,
      byte[] payloadDigest) {
    if (blockNumber < 0) {
      throw new IllegalArgumentException("blockNumber must not be negative");
    }
    this.kind = Objects.requireNonNull(kind, "kind");
    this.blockNumber = blockNumber;
    this.blockHash = copy32(blockHash, "blockHash");
    this.parentHash = copy32(parentHash, "parentHash");
    this.timestamp = timestamp;
    this.phase = Objects.requireNonNull(phase, "phase");
    this.parentStateRoot = parentStateRoot == null ? null
        : copy32(parentStateRoot, "parentStateRoot");
    this.stateRoot = copy32(stateRoot, "stateRoot");
    this.payloadDigest = copy32(payloadDigest, "payloadDigest");
    if (kind == Kind.BASE && this.parentStateRoot != null) {
      throw new IllegalArgumentException("base metadata must not contain a parent state root");
    }
    if (kind == Kind.LAYER && this.parentStateRoot == null) {
      throw new IllegalArgumentException("layer metadata requires a parent state root");
    }
  }

  /** Creates metadata for a rebuilt or compacted durable base. */
  public static PathStateRootMetadata base(long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] stateRoot, byte[] sourceDigest) {
    return new PathStateRootMetadata(Kind.BASE, blockNumber, blockHash, parentHash, timestamp,
        phase, null, stateRoot, sourceDigest);
  }

  /** Creates metadata for one immutable reversible transition above a parent root. */
  public static PathStateRootMetadata layer(long blockNumber, byte[] blockHash, byte[] parentHash,
      long timestamp, P66Phase phase, byte[] parentStateRoot, byte[] stateRoot,
      byte[] transitionDigest) {
    return new PathStateRootMetadata(Kind.LAYER, blockNumber, blockHash, parentHash, timestamp,
        phase, parentStateRoot, stateRoot, transitionDigest);
  }

  public Kind getKind() {
    return kind;
  }

  public long getBlockNumber() {
    return blockNumber;
  }

  public byte[] getBlockHash() {
    return Arrays.copyOf(blockHash, blockHash.length);
  }

  public byte[] getParentHash() {
    return Arrays.copyOf(parentHash, parentHash.length);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public P66Phase getPhase() {
    return phase;
  }

  public byte[] getParentStateRoot() {
    return parentStateRoot == null ? null : Arrays.copyOf(parentStateRoot, parentStateRoot.length);
  }

  public byte[] getStateRoot() {
    return Arrays.copyOf(stateRoot, stateRoot.length);
  }

  public byte[] getPayloadDigest() {
    return Arrays.copyOf(payloadDigest, payloadDigest.length);
  }

  /** Encodes metadata with an exact scope identity and CRC32C corruption check. */
  public byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      writeString(output, PathStateParticipantDescriptor.SCOPE_ID);
      output.writeByte(kind.tag);
      output.writeLong(blockNumber);
      output.write(blockHash);
      output.write(parentHash);
      output.writeLong(timestamp);
      output.writeByte(phaseTag(phase));
      if (parentStateRoot == null) {
        output.writeByte(0);
      } else {
        output.writeByte(parentStateRoot.length);
        output.write(parentStateRoot);
      }
      output.write(stateRoot);
      output.write(payloadDigest);
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = payload.length + Integer.BYTES;
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory path-state metadata encoding failed", impossible);
    }
  }

  public static PathStateRootMetadata decode(byte[] encoded) {
    byte[] bytes = Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length);
    if (bytes.length <= Integer.BYTES || bytes.length > MAX_LENGTH) {
      throw new IllegalArgumentException("path-state metadata length is invalid");
    }
    byte[] payload = Arrays.copyOf(bytes, bytes.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(bytes, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("path-state metadata checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != bytes.length) {
        throw new IllegalArgumentException("unsupported path-state metadata header");
      }
      if (!PathStateParticipantDescriptor.SCOPE_ID.equals(readString(input))) {
        throw new IllegalArgumentException("path-state metadata scope mismatch");
      }
      Kind kind = Kind.fromTag(input.readUnsignedByte());
      long blockNumber = input.readLong();
      byte[] blockHash = read32(input);
      byte[] parentHash = read32(input);
      long timestamp = input.readLong();
      P66Phase phase = phase(input.readUnsignedByte());
      int parentLength = input.readUnsignedByte();
      byte[] parentRoot = null;
      if (parentLength == DIGEST_LENGTH) {
        parentRoot = read32(input);
      } else if (parentLength != 0) {
        throw new IllegalArgumentException("path-state parent root length is invalid");
      }
      byte[] stateRoot = read32(input);
      byte[] digest = read32(input);
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("path-state metadata payload mismatch");
      }
      return new PathStateRootMetadata(kind, blockNumber, blockHash, parentHash, timestamp, phase,
          parentRoot, stateRoot, digest);
    } catch (IOException invalid) {
      throw new IllegalArgumentException("path-state metadata is truncated", invalid);
    }
  }

  private static byte[] copy32(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must be exactly " + DIGEST_LENGTH + " bytes");
    }
    return copy;
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeShort(encoded.length);
    output.write(encoded);
  }

  private static String readString(DataInputStream input) throws IOException {
    int length = input.readUnsignedShort();
    if (length == 0 || length > 1024 || length > input.available() - Integer.BYTES) {
      throw new IllegalArgumentException("path-state metadata string is invalid");
    }
    byte[] value = new byte[length];
    input.readFully(value);
    return new String(value, StandardCharsets.UTF_8);
  }

  private static byte[] read32(DataInputStream input) throws IOException {
    byte[] value = new byte[DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static int phaseTag(P66Phase phase) {
    switch (phase) {
      case P66_OFF:
        return 0;
      case P66_ACTIVATION:
        return 1;
      case P66_ON:
        return 2;
      default:
        throw new IllegalArgumentException("unknown P66 phase: " + phase);
    }
  }

  private static P66Phase phase(int tag) {
    switch (tag) {
      case 0:
        return P66Phase.P66_OFF;
      case 1:
        return P66Phase.P66_ACTIVATION;
      case 2:
        return P66Phase.P66_ON;
      default:
        throw new IllegalArgumentException("unknown path-state P66 phase tag: " + tag);
    }
  }

  public enum Kind {
    BASE(0),
    LAYER(1);

    private final int tag;

    Kind(int tag) {
      this.tag = tag;
    }

    private static Kind fromTag(int tag) {
      for (Kind value : values()) {
        if (value.tag == tag) {
          return value;
        }
      }
      throw new IllegalArgumentException("unknown path-state metadata kind: " + tag);
    }
  }
}
