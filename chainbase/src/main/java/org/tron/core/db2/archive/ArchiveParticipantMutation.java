package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Objects;

/** Immutable engine-neutral business mutation for one archive participant batch. */
public final class ArchiveParticipantMutation {

  private final byte[] key;
  private final byte[] value;

  private ArchiveParticipantMutation(byte[] key, byte[] value) {
    this.key = Arrays.copyOf(Objects.requireNonNull(key, "key"), key.length);
    this.value = value == null ? null : Arrays.copyOf(value, value.length);
  }

  public static ArchiveParticipantMutation put(byte[] key, byte[] value) {
    return new ArchiveParticipantMutation(key, Objects.requireNonNull(value, "value"));
  }

  public static ArchiveParticipantMutation delete(byte[] key) {
    return new ArchiveParticipantMutation(key, null);
  }

  byte[] getKey() {
    return Arrays.copyOf(key, key.length);
  }

  byte[] getValue() {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }
}
