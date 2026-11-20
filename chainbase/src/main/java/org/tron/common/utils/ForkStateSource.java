package org.tron.common.utils;

/**
 * The dynamic-property inputs of the fork-activation rule (chapter 05 section 3.9.3). Latest
 * execution backs it with the latest DynamicPropertiesStore; historical execution backs it with
 * the request's archive session at S(T).
 */
public interface ForkStateSource {

  long latestBlockHeaderTimestamp();

  long maintenanceTimeInterval();

  /** {@code FORK_VERSION_<version>} statistics, or {@code null} when the key is absent. */
  byte[] statsByVersion(int version);

  /** {@code CONSENSUS_LOGIC_OPTIMIZATION == 1}; selects the ceil implementation. */
  boolean disableJavaLangMath();

  long latestBlockHeaderNumber();
}
