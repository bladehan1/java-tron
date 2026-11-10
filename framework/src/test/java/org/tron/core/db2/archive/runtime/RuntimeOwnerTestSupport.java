package org.tron.core.db2.archive.runtime;

public final class RuntimeOwnerTestSupport {
  private RuntimeOwnerTestSupport() {}

  public static String servingRuntimeDirectory() {
    return StateArchiveRuntimeOwner.SERVING_INDEX_RUNTIME_DIRECTORY;
  }
}
