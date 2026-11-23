package org.tron.core.db2.archive.serving.index;

import java.io.IOException;
import java.util.List;

/** Read-only serving metadata and the request-owned generation lease. */
public interface PinnedServingKeyIndex extends ServingKeyIndex {

  List<String> getParticipatingDatabases();

  byte[] getLatestSourceIdentityDigest();

  String getScopeIdentity();

  boolean isLatestSourceIdentityBound();

  @Override
  void close() throws IOException;
}
