package org.tron.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.Test;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.exception.TronError;

public class ArchiveStateDiagnosticTest {

  @Test
  public void shouldConvertDiagnosticFailureToFatalStartupError() {
    ArchivePersistenceException failure = new ArchivePersistenceException("injected failure");

    TronError fatal = assertThrows(TronError.class,
        () -> FullNode.runArchiveStateDiagnostic(() -> {
          throw failure;
        }));

    assertEquals(TronError.ErrCode.STATE_ARCHIVE_INIT, fatal.getErrCode());
    assertEquals(failure, fatal.getCause());
  }

  @Test
  public void shouldRemainDisabledWithoutExplicitSystemProperty() {
    String previous = System.getProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
    System.clearProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
    try {
      TronApplicationContext context = mock(TronApplicationContext.class);

      ArchiveStateDiagnostic.runIfEnabled(context);

      verifyNoInteractions(context);
    } finally {
      if (previous == null) {
        System.clearProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY);
      } else {
        System.setProperty(ArchiveStateDiagnostic.ENABLE_PROPERTY, previous);
      }
    }
  }
}
