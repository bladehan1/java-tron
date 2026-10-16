package org.tron.core.vm.repository;

import org.tron.core.db.TransactionContext;

/** Creates the request-owned root Repository used by one VM execution. */
@FunctionalInterface
public interface RepositoryProvider {

  Repository createRoot(TransactionContext context);
}
