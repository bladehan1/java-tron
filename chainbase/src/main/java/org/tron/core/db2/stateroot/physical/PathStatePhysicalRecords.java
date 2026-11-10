package org.tron.core.db2.stateroot.physical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.stateroot.format.PathStateRootMetadata;

/** Immutable physical persistence records; codecs and mutable Store handles stay package-owned. */
public final class PathStatePhysicalRecords {
  private PathStatePhysicalRecords() {}

  public static GlobalIntent globalIntent(
      byte[] formatDigest,
      PathStateRootMetadata metadata,
      List<GlobalIntent.ParticipantTarget> participants,
      byte[] superGeneration,
      byte[] superRoot) {
    return new PathStatePhysicalGlobalIntent(
        formatDigest, metadata, participants, superGeneration, superRoot);
  }

  public static IngestCheckpoint ingestCheckpoint(
      byte[] identity, byte[] cursor, long rows, long bytes) {
    return new PathStatePhysicalIngestCheckpoint(identity, cursor, rows, bytes);
  }

  public static ReverseJournal reverseJournal(
      byte[] child,
      byte[] parent,
      List<ReverseJournal.StoreReverse> stores,
      List<ReverseJournal.Entry> superNodes) {
    return new PathStatePhysicalReverseJournal(child, parent, stores, superNodes);
  }

  public static OracleWindow oracleWindow(byte[] current, List<ReverseJournal> journals) {
    return new PathStatePhysicalOracleWindow(current, journals);
  }

  public interface GlobalIntent {
    int DIGEST_LENGTH = PathStatePhysicalGlobalIntent.DIGEST_LENGTH;
    int MAX_ENCODED_LENGTH = PathStatePhysicalGlobalIntent.MAX_ENCODED_LENGTH;

    static GlobalIntent decode(byte[] encoded) {
      return PathStatePhysicalGlobalIntent.decode(encoded);
    }

    byte[] encode();

    byte[] getFormatDigest();

    List<ParticipantTarget> getParticipants();

    PathStateRootMetadata getMetadata();

    byte[] getSuperGeneration();

    byte[] getSuperRoot();

    final class ParticipantTarget {

      final int storeId;
      final byte[] generation;
      final byte[] flatDigest;
      final byte[] storeRoot;

      public ParticipantTarget(
          int storeId, byte[] generation, byte[] flatDigest, byte[] storeRoot) {
        if (storeId <= 0) {
          throw new IllegalArgumentException("storeId must be positive");
        }
        this.storeId = storeId;
        this.generation = PathStatePhysicalGlobalIntent.digest(generation, "generation");
        this.flatDigest = PathStatePhysicalGlobalIntent.digest(flatDigest, "flatDigest");
        this.storeRoot = PathStatePhysicalGlobalIntent.digest(storeRoot, "storeRoot");
      }

      public int getStoreId() {
        return storeId;
      }

      public byte[] getGeneration() {
        return Arrays.copyOf(generation, generation.length);
      }

      public byte[] getFlatDigest() {
        return Arrays.copyOf(flatDigest, flatDigest.length);
      }

      public byte[] getStoreRoot() {
        return Arrays.copyOf(storeRoot, storeRoot.length);
      }
    }
  }

  public interface IngestCheckpoint {
    static IngestCheckpoint decode(byte[] encoded) {
      return PathStatePhysicalIngestCheckpoint.decode(encoded);
    }

    byte[] encode();

    byte[] getSourceIdentity();

    byte[] getCursor();

    long getRows();

    long getBytes();
  }

  public interface ReverseJournal {
    int MAX_ENCODED_LENGTH = PathStatePhysicalReverseJournal.MAX_ENCODED_LENGTH;

    static ReverseJournal decode(byte[] encoded) {
      return PathStatePhysicalReverseJournal.decode(encoded);
    }

    byte[] encode();

    byte[] getChildTarget();

    byte[] getParentTarget();

    List<StoreReverse> getStores();

    List<Entry> getSuperNodes();

    final class StoreReverse {

      final int storeId;
      final List<Entry> flatEntries;
      final List<Entry> nodeEntries;

      public StoreReverse(int storeId, List<Entry> flatEntries, List<Entry> nodeEntries) {
        if (storeId <= 0) {
          throw new IllegalArgumentException("physical reverse Store ID must be positive");
        }
        this.storeId = storeId;
        this.flatEntries = PathStatePhysicalReverseJournal.immutableEntries(flatEntries, true);
        this.nodeEntries = PathStatePhysicalReverseJournal.immutableEntries(nodeEntries, false);
      }

      public int getStoreId() {
        return storeId;
      }

      public List<Entry> getFlatEntries() {
        return flatEntries;
      }

      public List<Entry> getNodeEntries() {
        return nodeEntries;
      }
    }

    final class Entry {

      final byte[] key;
      final byte[] oldValue;

      public Entry(byte[] key, byte[] oldValue) {
        this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
        this.oldValue = oldValue == null ? null : Arrays.copyOf(oldValue, oldValue.length);
      }

      public byte[] getKey() {
        return Arrays.copyOf(key, key.length);
      }

      public byte[] getOldValue() {
        return oldValue == null ? null : Arrays.copyOf(oldValue, oldValue.length);
      }
    }
  }

  public interface OracleWindow {
    int getBlockCount();

    PathStateRootMetadata getCurrentMetadata();

    PathStateRootMetadata getOldestMetadata();

    List<ReverseJournal> journals();

    List<GlobalIntent> targets();
  }
}
