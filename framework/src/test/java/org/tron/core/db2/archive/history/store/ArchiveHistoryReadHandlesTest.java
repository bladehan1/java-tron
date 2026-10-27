package org.tron.core.db2.archive.history.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.junit.Test;
import org.mockito.InOrder;
import org.tron.core.db2.archive.ArchivePersistenceException;

public class ArchiveHistoryReadHandlesTest {

  private final HistoryBodyStore bodies = mock(HistoryBodyStore.class);
  private final HistoryIndexStore index = mock(HistoryIndexStore.class);
  private final HistoryCommitStore commits = mock(HistoryCommitStore.class);

  @Test
  public void normalCloseKeepsIndexBodyCommitOrder() throws Exception {
    new ArchiveHistoryReadHandles(bodies, index, commits).close();

    InOrder order = inOrder(index, bodies, commits);
    order.verify(index).close();
    order.verify(bodies).close();
    order.verify(commits).close();
  }

  @Test
  public void normalCloseRetainsFirstIoFailureAndSuppressesTheRest() throws Exception {
    IOException indexFailure = new IOException("index");
    IOException bodyFailure = new IOException("body");
    IOException commitFailure = new IOException("commit");
    doThrow(indexFailure).when(index).close();
    doThrow(bodyFailure).when(bodies).close();
    doThrow(commitFailure).when(commits).close();

    IOException failure = assertThrows(IOException.class,
        () -> new ArchiveHistoryReadHandles(bodies, index, commits).close());

    assertSame(indexFailure, failure);
    assertArrayEquals(new Throwable[]{bodyFailure, commitFailure}, failure.getSuppressed());
    verify(index).close();
    verify(bodies).close();
    verify(commits).close();
  }

  @Test
  public void validationCleanupAddsEachCloseFailureToTheOriginalFailure() throws Exception {
    IOException indexFailure = new IOException("index");
    IOException bodyFailure = new IOException("body");
    IOException commitFailure = new IOException("commit");
    doThrow(indexFailure).when(index).close();
    doThrow(bodyFailure).when(bodies).close();
    doThrow(commitFailure).when(commits).close();
    ArchivePersistenceException original = new ArchivePersistenceException("authority");

    new ArchiveHistoryReadHandles(bodies, index, commits).closeAfterFailedConstruction(original);

    assertArrayEquals(new Throwable[]{indexFailure, bodyFailure, commitFailure},
        original.getSuppressed());
    InOrder order = inOrder(index, bodies, commits);
    order.verify(index).close();
    order.verify(bodies).close();
    order.verify(commits).close();
  }

  @Test
  public void constructionCleanupClosesOnlyTheOpenedHandles() throws Exception {
    IOException bodyFailure = new IOException("body");
    doThrow(bodyFailure).when(bodies).close();
    IOException original = new IOException("index open");

    new ArchiveHistoryReadHandles(bodies, null, null).closeAfterFailedConstruction(original);

    assertArrayEquals(new Throwable[]{bodyFailure}, original.getSuppressed());
    verify(bodies).close();
  }
}
