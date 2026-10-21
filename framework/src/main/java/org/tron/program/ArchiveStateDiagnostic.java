package org.tron.program;

import org.tron.common.application.TronApplicationContext;

/** FullNode entry point for the opt-in Archive startup diagnostic. */
public final class ArchiveStateDiagnostic {

  public static final String ENABLE_PROPERTY =
      org.tron.core.archive.startup.diagnostic.ArchiveStateDiagnostic.ENABLE_PROPERTY;

  private ArchiveStateDiagnostic() {
  }

  public static void runIfEnabled(TronApplicationContext context) {
    org.tron.core.archive.startup.diagnostic.ArchiveStateDiagnostic.runIfEnabled(context);
  }
}
