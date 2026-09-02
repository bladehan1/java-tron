package org.tron.core.vm.repository;

/** Default provider preserving the existing current Chainbase execution path. */
public enum CurrentRepositoryProvider implements RepositoryProvider {
  INSTANCE;

  @Override
  public Repository createRoot(org.tron.core.db.TransactionContext context) {
    return RepositoryImpl.createRoot(context.getStoreFactory());
  }
}
