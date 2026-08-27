package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.SnapshotIdentity;
import org.tron.core.db2.stateroot.PathStateRebuildCoordinator.StoreResult;

/** Durable, non-authoritative per-Store rebuild checkpoint stored inside the BASE native DB. */
final class PathStateRebuildCheckpoint {

  private static final int MAGIC = 0x50535243; // PSRC
  private static final short VERSION = 1;
  private static final int MAX_LENGTH = 64 * 1024;

  private final byte[] manifestDigest;
  private final byte[] sourceIdentityDigest;
  private final SnapshotIdentity identity;
  private final List<StoreResult> completedStores;
  private final byte[] partialRoot;

  PathStateRebuildCheckpoint(byte[] manifestDigest, byte[] sourceIdentityDigest,
      SnapshotIdentity identity, List<StoreResult> completedStores, byte[] partialRoot) {
    this.manifestDigest = copy32(manifestDigest, "manifestDigest");
    this.sourceIdentityDigest = copy32(sourceIdentityDigest, "sourceIdentityDigest");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.completedStores = validateStores(completedStores);
    this.partialRoot = copy32(partialRoot, "partialRoot");
  }

  byte[] getManifestDigest() {
    return Arrays.copyOf(manifestDigest, manifestDigest.length);
  }

  byte[] getSourceIdentityDigest() {
    return Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
  }

  SnapshotIdentity getIdentity() {
    return identity;
  }

  List<StoreResult> getCompletedStores() {
    return completedStores;
  }

  byte[] getPartialRoot() {
    return Arrays.copyOf(partialRoot, partialRoot.length);
  }

  byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      output.write(manifestDigest);
      output.write(sourceIdentityDigest);
      output.writeLong(identity.getBlockNumber());
      output.write(identity.getBlockHash());
      output.write(identity.getParentHash());
      output.writeLong(identity.getTimestamp());
      output.writeByte(identity.getPhase().ordinal());
      output.writeByte(completedStores.size());
      for (StoreResult store : completedStores) {
        output.writeInt(store.getStoreId());
        writeString(output, store.getDbName());
        output.writeLong(store.getEntryCount());
        output.write(store.getInputDigest());
        output.write(store.getStoreRoot());
      }
      output.write(partialRoot);
      output.flush();
      byte[] payload = bytes.toByteArray();
      ByteBuffer.wrap(payload).putInt(8, payload.length + Integer.BYTES);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory rebuild checkpoint encoding failed", impossible);
    }
  }

  static PathStateRebuildCheckpoint decode(byte[] encoded) throws IOException {
    byte[] bytes = Arrays.copyOf(Objects.requireNonNull(encoded, "encoded"), encoded.length);
    if (bytes.length <= Integer.BYTES || bytes.length > MAX_LENGTH) {
      throw new IOException("path-state rebuild checkpoint length is invalid");
    }
    byte[] payload = Arrays.copyOf(bytes, bytes.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(bytes, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IOException("path-state rebuild checkpoint checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != bytes.length) {
        throw new IOException("unsupported path-state rebuild checkpoint header");
      }
      byte[] manifestDigest = read32(input);
      byte[] sourceIdentityDigest = read32(input);
      long blockNumber = input.readLong();
      byte[] blockHash = read32(input);
      byte[] parentHash = read32(input);
      long timestamp = input.readLong();
      int phaseTag = input.readUnsignedByte();
      P66Phase[] phases = P66Phase.values();
      if (phaseTag >= phases.length) {
        throw new IOException("path-state rebuild checkpoint phase is invalid");
      }
      SnapshotIdentity identity = new SnapshotIdentity(blockNumber, blockHash, parentHash,
          timestamp, phases[phaseTag]);
      int completed = input.readUnsignedByte();
      List<StoreResult> stores = new ArrayList<>(completed);
      for (int index = 0; index < completed; index++) {
        stores.add(StoreResult.restore(input.readInt(), readString(input), input.readLong(),
            read32(input), read32(input)));
      }
      byte[] partialRoot = read32(input);
      if (input.available() != Integer.BYTES) {
        throw new IOException("path-state rebuild checkpoint payload mismatch");
      }
      return new PathStateRebuildCheckpoint(manifestDigest, sourceIdentityDigest, identity, stores,
          partialRoot);
    } catch (IllegalArgumentException invalid) {
      throw new IOException("path-state rebuild checkpoint is invalid", invalid);
    }
  }

  private static List<StoreResult> validateStores(List<StoreResult> stores) {
    List<StoreResult> supplied = new ArrayList<>(Objects.requireNonNull(stores, "stores"));
    List<PathStateParticipantDescriptor.StoreIdentity> expected =
        PathStateParticipantDescriptor.current().getStores();
    if (supplied.size() > expected.size() || supplied.contains(null)) {
      throw new IllegalArgumentException("rebuild checkpoint Store count is invalid");
    }
    for (int index = 0; index < supplied.size(); index++) {
      StoreResult actual = supplied.get(index);
      PathStateParticipantDescriptor.StoreIdentity participant = expected.get(index);
      if (actual.getStoreId() != participant.getStoreId()
          || !actual.getDbName().equals(participant.getDbName())) {
        throw new IllegalArgumentException("rebuild checkpoint Store order is invalid");
      }
    }
    return Collections.unmodifiableList(supplied);
  }

  private static byte[] copy32(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != PathStateRootMetadata.DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must be exactly 32 bytes");
    }
    return copy;
  }

  private static byte[] read32(DataInputStream input) throws IOException {
    byte[] value = new byte[PathStateRootMetadata.DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] encoded = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
    output.writeShort(encoded.length);
    output.write(encoded);
  }

  private static String readString(DataInputStream input) throws IOException {
    int length = input.readUnsignedShort();
    if (length == 0 || length > 1024 || length > input.available() - Integer.BYTES) {
      throw new IOException("path-state rebuild checkpoint string is invalid");
    }
    byte[] value = new byte[length];
    input.readFully(value);
    return new String(value, StandardCharsets.UTF_8);
  }
}
