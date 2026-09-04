package org.tron.core.db2.stateroot;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.core.CommonCheckpointMaterializer;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointTarget;

/** Next-format PathState participant for the common-checkpoint two-barrier protocol. */
public final class PathStateCheckpointMaterializer implements CommonCheckpointMaterializer {

  static final String CURRENT_FILE = "CURRENT";
  static final String MATERIALIZED_DIRECTORY = "checkpoint-materialized";
  private static final int MAGIC = 0x50534354; // PSCT
  private static final short VERSION = 1;
  private static final int DIGEST_LENGTH = 32;
  private static final int RECORD_LENGTH = Integer.BYTES + Short.BYTES + Short.BYTES
      + 4 * DIGEST_LENGTH + 2 * Long.BYTES + DIGEST_LENGTH;

  private final PathStatePhysicalStoreSet stores;
  private final PathStateParticipantScope scope;
  private final Path directory;
  private final byte[] formatIdentity;
  private final FaultHook faultHook;

  public PathStateCheckpointMaterializer(PathStatePhysicalStoreSet stores,
      PathStateParticipantScope scope, byte[] formatIdentity) {
    this(stores, scope, formatIdentity, (stage, storeId) -> { });
  }

  PathStateCheckpointMaterializer(PathStatePhysicalStoreSet stores,
      PathStateParticipantScope scope, byte[] formatIdentity, FaultHook faultHook) {
    this.stores = Objects.requireNonNull(stores, "stores");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.directory = stores.getDirectory();
    this.formatIdentity = digest(formatIdentity, "formatIdentity");
    this.faultHook = Objects.requireNonNull(faultHook, "faultHook");
  }

  @Override
  public Authority authority() {
    return Authority.PATH_STATE;
  }

  @Override
  public synchronized Status inspect(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    byte[] expected = encode(admitted);
    Path currentPath = directory.resolve(CURRENT_FILE);
    if (Files.exists(currentPath, LinkOption.NOFOLLOW_LINKS)) {
      Marker current = load(currentPath);
      if (Arrays.equals(current.encoded, expected)) {
        requireMaterialized(admitted, expected);
        return Status.PUBLISHED;
      }
      requireParent(current, admitted);
    }
    Path materialized = materializedPath(admitted);
    if (!Files.exists(materialized, LinkOption.NOFOLLOW_LINKS)) {
      return Status.NEEDS_MATERIALIZATION;
    }
    requireExact(materialized, expected);
    return Status.MATERIALIZED;
  }

  @Override
  public synchronized void materialize(CommonCheckpointPayload payload,
      CommonCheckpointTarget target) throws IOException {
    CommonCheckpointPayload admittedPayload = Objects.requireNonNull(payload, "payload");
    CommonCheckpointTarget admittedTarget = requireTarget(target);
    if (!admittedTarget.equals(CommonCheckpointTarget.from(admittedPayload))) {
      throw new IOException("PathState checkpoint payload and target differ");
    }
    Status status = inspect(admittedTarget);
    if (status == Status.PUBLISHED || status == Status.MATERIALIZED) {
      return;
    }
    byte[] marker = marker(admittedTarget);
    Set<Integer> seen = new HashSet<>();
    for (CommonCheckpointPayload.PathStoreTarget pathStore
        : admittedPayload.getPathStores()) {
      PathStateParticipant participant = scope.require(pathStore.getDbName());
      if (participant.getStoreId() != pathStore.getStoreId()
          || !seen.add(pathStore.getStoreId())) {
        throw new IOException("PathState checkpoint participant identity differs");
      }
      PathStatePhysicalStoreSet.PhysicalStore store = stores.participant(pathStore.getDbName());
      if (!Arrays.equals(marker, store.checkpointTargetMarker())) {
        store.applyCheckpointParticipant(pathStore, marker);
        faultHook.after(Stage.AFTER_PARTICIPANT_BATCH, pathStore.getStoreId());
      }
    }
    PathStatePhysicalStoreSet.PhysicalStore superStore = stores.superStore();
    if (!Arrays.equals(marker, superStore.checkpointTargetMarker())) {
      superStore.applyCheckpointSuper(admittedPayload.getSuperNodeMutations(), marker);
      faultHook.after(Stage.AFTER_SUPER_BATCH, 0);
    }
    byte[] encoded = encode(admittedTarget);
    PathStateMetadataFile.publishImmutableBytes(materializedPath(admittedTarget), encoded);
    faultHook.after(Stage.AFTER_MATERIALIZED_TARGET, 0);
  }

  @Override
  public synchronized void publish(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = requireTarget(target);
    Status status = inspect(admitted);
    if (status == Status.PUBLISHED) {
      return;
    }
    if (status != Status.MATERIALIZED) {
      throw new IOException("PathState checkpoint target is not fully materialized");
    }
    PathStateMetadataFile.replaceCurrentBytes(directory.resolve(CURRENT_FILE), encode(admitted));
    faultHook.after(Stage.AFTER_CURRENT, 0);
  }

  private CommonCheckpointTarget requireTarget(CommonCheckpointTarget target) throws IOException {
    CommonCheckpointTarget admitted = Objects.requireNonNull(target, "target");
    if (!Arrays.equals(formatIdentity, admitted.getFormatIdentity())) {
      throw new IOException("PathState checkpoint format identity differs");
    }
    return admitted;
  }

  private void requireMaterialized(CommonCheckpointTarget target, byte[] expected)
      throws IOException {
    requireExact(materializedPath(target), expected);
  }

  private void requireParent(Marker current, CommonCheckpointTarget target) throws IOException {
    BlockSnapshotMeta first = target.getFirstBlock();
    if (!Arrays.equals(current.formatIdentity, target.getFormatIdentity())
        || current.lastEpoch + 1 != first.getEpoch()
        || current.lastBlockNumber + 1 != first.getBlockNumber()
        || !Arrays.equals(current.lastBlockHash, first.getParentHash())
        || !Arrays.equals(current.stateRoot, target.getParentStateRoot())) {
      throw new IOException("PathState CURRENT is not the checkpoint parent target");
    }
  }

  private Path materializedPath(CommonCheckpointTarget target) {
    return directory.resolve(MATERIALIZED_DIRECTORY).resolve(hex(target.getPayloadDigest()));
  }

  private static byte[] marker(CommonCheckpointTarget target) {
    byte[] marker = new byte[2 * DIGEST_LENGTH];
    System.arraycopy(target.getPayloadDigest(), 0, marker, 0, DIGEST_LENGTH);
    System.arraycopy(target.getStateRoot(), 0, marker, DIGEST_LENGTH, DIGEST_LENGTH);
    return marker;
  }

  private static byte[] encode(CommonCheckpointTarget target) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(RECORD_LENGTH);
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.write(target.getFormatIdentity());
      output.write(target.getPayloadDigest());
      output.writeLong(target.getLastBlock().getEpoch());
      output.writeLong(target.getLastBlock().getBlockNumber());
      output.write(target.getLastBlock().getBlockHash());
      output.write(target.getStateRoot());
      output.flush();
      byte[] body = bytes.toByteArray();
      output.write(Hashing.sha256().hashBytes(body).asBytes());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory PathState target encoding failed", impossible);
    }
  }

  private static Marker load(Path path) throws IOException {
    byte[] encoded = PathStateMetadataFile.loadImmutableBytes(path, RECORD_LENGTH);
    if (encoded.length != RECORD_LENGTH) {
      throw new IOException("PathState checkpoint target length is invalid");
    }
    int bodyLength = encoded.length - DIGEST_LENGTH;
    byte[] body = Arrays.copyOf(encoded, bodyLength);
    byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
    if (!Arrays.equals(checksum, Hashing.sha256().hashBytes(body).asBytes())) {
      throw new IOException("PathState checkpoint target checksum differs");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(body))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0) {
        throw new IOException("PathState checkpoint target format is unsupported");
      }
      return new Marker(encoded, readDigest(input), readDigest(input), input.readLong(),
          input.readLong(), readDigest(input), readDigest(input));
    } catch (EOFException truncated) {
      throw new IOException("PathState checkpoint target is truncated", truncated);
    }
  }

  private static void requireExact(Path path, byte[] expected) throws IOException {
    Marker actual = load(path);
    if (!Arrays.equals(actual.encoded, expected)) {
      throw new IOException("PathState checkpoint target identity differs");
    }
  }

  private static byte[] readDigest(DataInputStream input) throws IOException {
    byte[] value = new byte[DIGEST_LENGTH];
    input.readFully(value);
    return value;
  }

  private static byte[] digest(byte[] value, String name) {
    byte[] copy = Arrays.copyOf(Objects.requireNonNull(value, name), value.length);
    if (copy.length != DIGEST_LENGTH) {
      throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
    }
    return copy;
  }

  private static String hex(byte[] value) {
    StringBuilder encoded = new StringBuilder(value.length * 2);
    for (byte current : value) {
      encoded.append(Character.forDigit(current >>> 4 & 0xf, 16));
      encoded.append(Character.forDigit(current & 0xf, 16));
    }
    return encoded.toString();
  }

  enum Stage {
    AFTER_PARTICIPANT_BATCH,
    AFTER_SUPER_BATCH,
    AFTER_MATERIALIZED_TARGET,
    AFTER_CURRENT
  }

  @FunctionalInterface
  interface FaultHook {
    void after(Stage stage, int storeId) throws IOException;
  }

  private static final class Marker {

    private final byte[] encoded;
    private final byte[] formatIdentity;
    private final byte[] payloadDigest;
    private final long lastEpoch;
    private final long lastBlockNumber;
    private final byte[] lastBlockHash;
    private final byte[] stateRoot;

    private Marker(byte[] encoded, byte[] formatIdentity, byte[] payloadDigest, long lastEpoch,
        long lastBlockNumber, byte[] lastBlockHash, byte[] stateRoot) {
      this.encoded = encoded;
      this.formatIdentity = formatIdentity;
      this.payloadDigest = payloadDigest;
      this.lastEpoch = lastEpoch;
      this.lastBlockNumber = lastBlockNumber;
      this.lastBlockHash = lastBlockHash;
      this.stateRoot = stateRoot;
    }
  }
}
