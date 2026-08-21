package org.tron.core.db;

import java.nio.file.Path;
import java.util.Objects;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Result;
import org.tron.core.db2.archive.ArchiveFormatAdmissionValidator.Status;

/** Read-only base-format gate that must run before the archive writer opens. */
final class StateArchiveBasePreflight {

  private StateArchiveBasePreflight() {
  }

  static void requireAdmitted(boolean enabled, Path archiveDirectory) {
    if (!enabled) {
      return;
    }
    Result result = ArchiveFormatAdmissionValidator.inspect(
        Objects.requireNonNull(archiveDirectory, "archiveDirectory"));
    if (result.getStatus() == Status.EMPTY_NEW || result.getStatus() == Status.CURRENT_BASE) {
      return;
    }
    throw new IllegalStateException("State archive base requires quarantine: "
        + result.getReason() + ": " + result.getDetail());
  }

  /** S1 can recover an existing exact-27 base, but fresh-base bootstrap is not wired yet. */
  static void requireRecoverable(boolean enabled, Path archiveDirectory) {
    if (!enabled) {
      return;
    }
    Result result = ArchiveFormatAdmissionValidator.inspect(
        Objects.requireNonNull(archiveDirectory, "archiveDirectory"));
    if (result.getStatus() == Status.CURRENT_BASE) {
      return;
    }
    if (result.getStatus() == Status.EMPTY_NEW) {
      throw new IllegalStateException(
          "State archive fresh-base bootstrap is not available in S1 startup recovery");
    }
    throw new IllegalStateException("State archive base requires quarantine: "
        + result.getReason() + ": " + result.getDetail());
  }
}
