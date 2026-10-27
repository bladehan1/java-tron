package org.tron.core.db2.stateroot.trie;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

/** Streaming trie construction over caller-owned node and leaf sinks. */
public final class PathStateTrieBuild {

  private PathStateTrieBuild() {}

  /** Consumes strictly ascending secure keys without retaining completed subtrees. */
  public static Ordered ordered(NodeSink nodeSink) {
    return new PathStateStackTrie(nodeSink);
  }

  /** Sorts input on disk before streaming it into the same ordered trie builder. */
  public static Spooled spooled(
      Path directory,
      Engine engine,
      byte[] generationPrefix,
      NodeSink nodeSink,
      LeafSink leafSink,
      BuildProgress progress)
      throws IOException {
    return new PathStateStoreTrieBuilder(
        directory, engine, generationPrefix, nodeSink, leafSink, progress);
  }

  public interface Ordered {

    void update(byte[] secureKey, byte[] encodedValue);

    byte[] rootHash();
  }

  public interface Spooled extends Closeable {

    void put(byte[] secureKey, byte[] encodedValue);

    byte[] build() throws IOException;
  }

  @FunctionalInterface
  public interface NodeSink {

    void put(byte[] path, byte[] encodedNode);
  }

  @FunctionalInterface
  public interface LeafSink {

    void put(byte[] secureKey, byte[] encodedValue);
  }

  @FunctionalInterface
  public interface BuildProgress {

    void report(long sortedRows, long elapsedMillis);
  }
}
