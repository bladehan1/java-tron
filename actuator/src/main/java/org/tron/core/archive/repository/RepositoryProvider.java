package org.tron.core.archive.repository;

import org.tron.core.db.TransactionContext;
import org.tron.core.vm.repository.Repository;

/** Creates the request-owned root Repository used by one VM execution. */
@FunctionalInterface
public interface RepositoryProvider {

  Repository createRoot(TransactionContext context);
}
