package org.tron.core.db2.archive;

import com.sun.management.UnixOperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import org.tron.common.math.StrictMathWrapper;
import org.tron.core.db2.archive.StateArchiveDataHandlePoolV5.ProcessFdAdmission;

/** Fail-closed process file-descriptor admission for the default-off V5 runtime. */
final class StateArchiveProcessFdAdmissionV5 implements ProcessFdAdmission {

  static final StateArchiveProcessFdAdmissionV5 INSTANCE =
      new StateArchiveProcessFdAdmissionV5();

  private StateArchiveProcessFdAdmissionV5() {
  }

  @Override
  public void require(long requiredNewDataHandles, long reserveHandles) throws IOException {
    if (requiredNewDataHandles < 0 || reserveHandles < 0) {
      throw new IllegalArgumentException("Archive V5 FD requirements must not be negative");
    }
    java.lang.management.OperatingSystemMXBean operatingSystem =
        ManagementFactory.getOperatingSystemMXBean();
    if (!(operatingSystem instanceof UnixOperatingSystemMXBean)) {
      throw new IOException("Archive V5 cannot inspect the process file-descriptor limit");
    }
    UnixOperatingSystemMXBean unix = (UnixOperatingSystemMXBean) operatingSystem;
    long open = unix.getOpenFileDescriptorCount();
    long limit = unix.getMaxFileDescriptorCount();
    final long required;
    try {
      required = StrictMathWrapper.addExact(StrictMathWrapper.addExact(open, requiredNewDataHandles), reserveHandles);
    } catch (ArithmeticException failure) {
      throw new IOException("Archive V5 process FD requirement overflow", failure);
    }
    if (open < 0 || limit < 0 || required > limit) {
      throw new IOException("Archive V5 process FD budget is unavailable: open=" + open
          + ", newData=" + requiredNewDataHandles + ", reserve=" + reserveHandles
          + ", limit=" + limit);
    }
  }
}
