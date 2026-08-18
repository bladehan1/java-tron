package org.tron.core.db2.archive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.ServingKeyIndexGeneration.ChangedKey;

/**
 * Backend-neutral prototype for one-store historical range materialization.
 *
 * <p>The caller must supply latest entries from the same pinned {@code S(P)} generation as the
 * serving index. This class deliberately provides no cross-database merge API and no persistent
 * cursor encoding.
 */
public final class HistoricalRangeOverlay {

  private HistoricalRangeOverlay() {
  }

  public static List<Entry> materialize(String dbName, long targetBlock, long upperBound,
      KeyRange range, List<Entry> pinnedLatest, ServingKeyIndexGeneration index,
      HistoricalValueReader history, Limits limits) throws IOException {
    Objects.requireNonNull(dbName, "dbName");
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(pinnedLatest, "pinnedLatest");
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(history, "history");
    Objects.requireNonNull(limits, "limits");
    validateLatest(pinnedLatest, range);

    List<ChangedKey> changed = index.changesInRange(dbName, range.lowerInclusive,
        range.upperExclusive, targetBlock, upperBound, limits.maxChangedKeys);
    List<Entry> result = new ArrayList<>();
    int latestIndex = 0;
    int changedIndex = 0;
    int candidates = 0;

    while (latestIndex < pinnedLatest.size() || changedIndex < changed.size()) {
      if (++candidates > limits.maxCandidateKeys) {
        throw new ArchiveQueryLimitExceededException("candidate-key budget exceeded");
      }
      Entry latest = latestIndex < pinnedLatest.size() ? pinnedLatest.get(latestIndex) : null;
      ChangedKey delta = changedIndex < changed.size() ? changed.get(changedIndex) : null;
      int comparison = latest == null ? 1 : delta == null ? -1
          : BlockReverseDiff.compareUnsigned(latest.key, delta.getKey());

      if (comparison < 0) {
        addResult(result, latest, limits.maxResults);
        latestIndex++;
      } else {
        byte[] key = delta.getKey();
        OldValue value = history.read(dbName, key, delta.getFirstChangeBlock());
        if (value == null) {
          throw new IllegalStateException("historical value reader returned null");
        }
        if (value.isPresent()) {
          addResult(result, new Entry(key, value.getValue()), limits.maxResults);
        }
        changedIndex++;
        if (comparison == 0) {
          latestIndex++;
        }
      }
    }
    return Collections.unmodifiableList(result);
  }

  private static void validateLatest(List<Entry> latest, KeyRange range) {
    byte[] previous = null;
    for (Entry entry : latest) {
      Objects.requireNonNull(entry, "latest entry");
      if (!range.contains(entry.key)) {
        throw new IllegalArgumentException("latest entry is outside requested range");
      }
      if (previous != null && BlockReverseDiff.compareUnsigned(previous, entry.key) >= 0) {
        throw new IllegalArgumentException("latest entries must be strictly sorted");
      }
      previous = entry.key;
    }
  }

  private static void addResult(List<Entry> result, Entry entry, int maxResults) {
    if (result.size() == maxResults) {
      throw new ArchiveQueryLimitExceededException("result budget exceeded");
    }
    result.add(entry);
  }

  @FunctionalInterface
  public interface HistoricalValueReader {
    OldValue read(String dbName, byte[] rawKey, long firstChangeBlock) throws IOException;
  }

  public static final class Limits {
    private final int maxChangedKeys;
    private final int maxCandidateKeys;
    private final int maxResults;

    public Limits(int maxChangedKeys, int maxCandidateKeys, int maxResults) {
      if (maxChangedKeys <= 0 || maxCandidateKeys <= 0 || maxResults <= 0) {
        throw new IllegalArgumentException("historical range limits must be positive");
      }
      this.maxChangedKeys = maxChangedKeys;
      this.maxCandidateKeys = maxCandidateKeys;
      this.maxResults = maxResults;
    }
  }

  public static final class KeyRange {
    private final byte[] lowerInclusive;
    private final byte[] upperExclusive;

    private KeyRange(byte[] lowerInclusive, byte[] upperExclusive) {
      this.lowerInclusive = Arrays.copyOf(lowerInclusive, lowerInclusive.length);
      this.upperExclusive = upperExclusive == null ? null
          : Arrays.copyOf(upperExclusive, upperExclusive.length);
      if (this.upperExclusive != null
          && BlockReverseDiff.compareUnsigned(this.lowerInclusive, this.upperExclusive) > 0) {
        throw new IllegalArgumentException("lowerInclusive must not exceed upperExclusive");
      }
    }

    public static KeyRange range(byte[] lowerInclusive, byte[] upperExclusive) {
      Objects.requireNonNull(lowerInclusive, "lowerInclusive");
      return new KeyRange(lowerInclusive, upperExclusive);
    }

    public static KeyRange prefix(byte[] prefix) {
      Objects.requireNonNull(prefix, "prefix");
      byte[] upper = Arrays.copyOf(prefix, prefix.length);
      for (int i = upper.length - 1; i >= 0; i--) {
        if ((upper[i] & 0xff) != 0xff) {
          upper[i]++;
          upper = Arrays.copyOf(upper, i + 1);
          return new KeyRange(prefix, upper);
        }
      }
      return new KeyRange(prefix, null);
    }

    public byte[] getLowerInclusive() {
      return Arrays.copyOf(lowerInclusive, lowerInclusive.length);
    }

    public byte[] getUpperExclusive() {
      return upperExclusive == null ? null
          : Arrays.copyOf(upperExclusive, upperExclusive.length);
    }

    private boolean contains(byte[] key) {
      return BlockReverseDiff.compareUnsigned(key, lowerInclusive) >= 0
          && (upperExclusive == null
          || BlockReverseDiff.compareUnsigned(key, upperExclusive) < 0);
    }
  }

  public static final class Entry {
    private final byte[] key;
    private final byte[] value;

    public Entry(byte[] key, byte[] value) {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(value, "value");
      this.key = Arrays.copyOf(key, key.length);
      this.value = Arrays.copyOf(value, value.length);
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public byte[] getValue() {
      return Arrays.copyOf(value, value.length);
    }
  }
}
