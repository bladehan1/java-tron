package org.tron.core.db2.archive;

import java.util.Arrays;
import java.util.Objects;

/** An old value which preserves the distinction between absent and present-empty. */
public final class OldValue {

  private static final OldValue ABSENT = new OldValue(false, null);

  private final boolean present;
  private final byte[] value;

  private OldValue(boolean present, byte[] value) {
    this.present = present;
    this.value = value;
  }

  public static OldValue absent() {
    return ABSENT;
  }

  public static OldValue present(byte[] value) {
    Objects.requireNonNull(value, "value");
    return new OldValue(true, Arrays.copyOf(value, value.length));
  }

  public static OldValue fromNullable(byte[] value) {
    return value == null ? absent() : present(value);
  }

  public boolean isPresent() {
    return present;
  }

  public byte[] getValue() {
    if (!present) {
      throw new IllegalStateException("absent old value has no bytes");
    }
    return Arrays.copyOf(value, value.length);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof OldValue)) {
      return false;
    }
    OldValue that = (OldValue) object;
    return present == that.present && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return 31 * Boolean.hashCode(present) + Arrays.hashCode(value);
  }
}
