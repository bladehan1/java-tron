package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointPayloadCodec;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** One synchronously committed database retains both Chainbase and PathState redo generations. */
public final class SharedCheckpointDatabase implements Closeable {
  private static final byte[] HEAD = key("head/v1");
  private static final byte[] START = key("first-retained/v1");
  private static final byte[] VERSION = key("schema/v1");
  private static final byte[] SCHEMA = {83, 67, 68, 66, 1};
  private static final byte[] APPLIED = key("applied/v1");
  private static final byte[] BASE = key("durable-base/v1");
  private static final byte[] GENERATION = key("generation/v1/");
  private final Path directory;
  private final Engine engine;
  private final StateArchiveIndexDatabase.Writer writer;
  private final CommonCheckpointPayloadCodec codec = new CommonCheckpointPayloadCodec();

  public SharedCheckpointDatabase(Path directory, Engine engine) throws IOException {
    this.directory = directory;
    this.engine = engine;
    java.nio.file.Files.createDirectories(directory);
    writer = StateArchiveIndexDatabase.openWriter(directory, engine);
    try {
      byte[] version = writer.get(VERSION);
      if (version == null) {
        try (StateArchiveIndexDatabase.Reader reader =
            StateArchiveIndexDatabase.openReader(directory, engine);
            StateArchiveIndexDatabase.Cursor cursor = reader.cursor()) {
          cursor.seek(new byte[0]);
          if (cursor.next() != null) {
            throw new IOException("unknown non-empty checkpoint database");
          }
        }
        writer.write(Arrays.asList(StateArchiveIndexDatabase.put(VERSION, SCHEMA)), true);
      } else if (!Arrays.equals(version, SCHEMA)) {
        throw new IOException("shared checkpoint schema is not supported");
      }
      retained();
    } catch (IOException | RuntimeException failure) {
      writer.close();
      throw failure;
    }
  }

  /** The payload and commit pointer are one sync batch: no target write precedes this decision. */
  public synchronized void publish(CommonCheckpointPayload payload) throws IOException {
    byte[] encoded = codec.encode(payload);
    CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
    byte[] generation = generation(target.getLastBlock().getBlockNumber());
    byte[] existing = writer.get(generation);
    if (existing != null) {
      if (!Arrays.equals(existing, encoded) || !Arrays.equals(writer.get(HEAD), generation)) {
        throw new IOException("shared checkpoint immutable generation mismatch");
      }
      return;
    }
    CommonCheckpointPayload previous = latest();
    if (previous != null) {
      requireChild(CommonCheckpointTarget.from(previous), target);
      if (isPending()) {
        throw new IOException("previous shared checkpoint is still pending materialization");
      }
    }
    List<StateArchiveIndexDatabase.Mutation> commit = new ArrayList<>();
    commit.add(StateArchiveIndexDatabase.put(generation, encoded));
    commit.add(StateArchiveIndexDatabase.put(HEAD, generation));
    if (previous == null) {
      commit.add(StateArchiveIndexDatabase.put(START, generation));
    }
    writer.write(commit, true);
  }

  public synchronized boolean isPending() throws IOException {
    byte[] head = writer.get(HEAD);
    return head != null && !Arrays.equals(head, writer.get(APPLIED));
  }

  public synchronized CommonCheckpointPayload latest() throws IOException {
    byte[] head = writer.get(HEAD);
    if (head == null) {
      return null;
    }
    return decode(head, writer.get(head));
  }

  /** Mark completion, retaining every committed payload until an explicit durable-base barrier. */
  public synchronized void applied() throws IOException {
    byte[] head = writer.get(HEAD);
    if (head == null) {
      return;
    }
    latest();
    writer.write(Arrays.asList(StateArchiveIndexDatabase.put(APPLIED, head)), true);
  }

  public synchronized List<CommonCheckpointPayload> retained() throws IOException {
    List<CommonCheckpointPayload> result = new ArrayList<>();
    byte[] lastKey = null;
    byte[] firstKey = null;
    try (StateArchiveIndexDatabase.Reader reader =
        StateArchiveIndexDatabase.openReader(directory, engine);
        StateArchiveIndexDatabase.Cursor cursor = reader.cursor()) {
      cursor.seek(GENERATION);
      StateArchiveIndexDatabase.KeyValue row;
      while ((row = cursor.next()) != null) {
        byte[] rowKey = row.getKey();
        if (!startsWith(rowKey, GENERATION)) {
          break;
        }
        CommonCheckpointPayload payload = decode(rowKey, row.getValue());
        if (!result.isEmpty()) {
          requireChild(CommonCheckpointTarget.from(result.get(result.size() - 1)),
              CommonCheckpointTarget.from(payload));
        }
        if (firstKey == null) {
          firstKey = rowKey;
        }
        result.add(payload);
        lastKey = rowKey;
      }
    }
    if (!Arrays.equals(firstKey, writer.get(START))) {
      throw new IOException("first retained shared checkpoint generation is missing");
    }
    if (!Arrays.equals(lastKey, writer.get(HEAD))) {
      throw new IOException("shared checkpoint head differs from retained generations");
    }
    byte[] base = writer.get(BASE);
    if (base != null && (result.isEmpty() || !Arrays.equals(base,
        generation(CommonCheckpointTarget.from(result.get(0)).getLastBlock().getBlockNumber())))) {
      throw new IOException("shared checkpoint durable base generation is missing");
    }
    byte[] applied = writer.get(APPLIED);
    if (applied != null) {
      decode(applied, writer.get(applied));
    }
    if (applied != null && (lastKey == null || compareUnsigned(applied, lastKey) > 0)) {
      throw new IOException("shared checkpoint applied pointer exceeds committed head");
    }
    return result;
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    for (int index = 0; index < Math.min(left.length, right.length); index++) {
      int compared = Integer.compare(left[index] & 255, right[index] & 255);
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  /** Call only after all retained target mutations have been rewritten synchronously. */
  public synchronized void advanceDurableBase() throws IOException {
    if (isPending()) {
      throw new IOException("cannot prune an unmaterialized shared checkpoint");
    }
    byte[] head = writer.get(HEAD);
    if (head == null) {
      return;
    }
    List<StateArchiveIndexDatabase.Mutation> mutations = new ArrayList<>();
    for (byte[] generation : retainedGenerationKeys()) {
      if (!Arrays.equals(head, generation)) {
        mutations.add(StateArchiveIndexDatabase.delete(generation));
      }
    }
    mutations.add(StateArchiveIndexDatabase.put(BASE, head));
    mutations.add(StateArchiveIndexDatabase.put(START, head));
    writer.write(mutations, true);
  }

  /** Lists retained generation keys without decoding payloads; retained() validates the chain. */
  private List<byte[]> retainedGenerationKeys() throws IOException {
    List<byte[]> keys = new ArrayList<>();
    try (StateArchiveIndexDatabase.Reader reader =
        StateArchiveIndexDatabase.openReader(directory, engine);
        StateArchiveIndexDatabase.Cursor cursor = reader.cursor()) {
      cursor.seek(GENERATION);
      StateArchiveIndexDatabase.KeyValue row;
      while ((row = cursor.next()) != null) {
        byte[] rowKey = row.getKey();
        if (!startsWith(rowKey, GENERATION)) {
          break;
        }
        keys.add(rowKey);
      }
    }
    return keys;
  }

  private CommonCheckpointPayload decode(byte[] generation, byte[] encoded) throws IOException {
    if (encoded == null) {
      throw new IOException("shared checkpoint generation missing");
    }
    try {
      CommonCheckpointPayload payload = codec.decode(encoded);
      if (!Arrays.equals(generation, generation(CommonCheckpointTarget.from(payload)
          .getLastBlock().getBlockNumber()))) {
        throw new IOException("shared checkpoint generation identity mismatch");
      }
      return payload;
    } catch (IllegalArgumentException failure) {
      throw new IOException("shared checkpoint generation corrupt", failure);
    }
  }

  private static void requireChild(CommonCheckpointTarget parent, CommonCheckpointTarget child)
      throws IOException {
    if (parent.getLastBlock().getBlockNumber() + 1 != child.getFirstBlock().getBlockNumber()
        || parent.getLastBlock().getEpoch() + 1 != child.getFirstBlock().getEpoch()
        || !Arrays.equals(parent.getLastBlock().getBlockHash(),
            child.getFirstBlock().getParentHash())
        || !Arrays.equals(parent.getStateRoot(), child.getParentStateRoot())
        || !Arrays.equals(parent.getFormatIdentity(), child.getFormatIdentity())) {
      throw new IOException("shared checkpoint generation chain is discontinuous");
    }
  }

  private static byte[] generation(long block) {
    return ByteBuffer.allocate(GENERATION.length + Long.BYTES)
        .put(GENERATION).putLong(block).array();
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    return value.length >= prefix.length
        && Arrays.equals(Arrays.copyOf(value, prefix.length), prefix);
  }

  private static byte[] key(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
