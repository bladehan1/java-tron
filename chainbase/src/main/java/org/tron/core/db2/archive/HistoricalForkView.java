package org.tron.core.db2.archive;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.ForkController;
import org.tron.common.utils.ForkStateSource;
import org.tron.common.utils.ForkView;
import org.tron.core.config.Parameter.ForkBlockVersionEnum;
import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;

/**
 * {@code ForkView@S(T)} (chapter 05 section 3.9.3): the shared {@link ForkController#evaluate}
 * algorithm over dynamic properties read through one historical session. Results are cached per
 * version for the request. Required inputs missing from S(T) fail closed; an absent
 * {@code FORK_VERSION_<v>} is "not passed", exactly as for latest.
 */
final class HistoricalForkView implements ForkView {

  private final HistoricalQuerySession session;
  private final Map<Integer, Boolean> decisions = new HashMap<>();
  private final ForkStateSource source = new SessionForkStateSource();

  HistoricalForkView(HistoricalQuerySession session) {
    this.session = Objects.requireNonNull(session, "session");
  }

  @Override
  public boolean pass(ForkBlockVersionEnum version) {
    return pass(Objects.requireNonNull(version, "version").getValue());
  }

  boolean pass(int version) {
    Boolean cached = decisions.get(version);
    if (cached != null) {
      return cached;
    }
    boolean decision;
    try {
      decision = ForkController.evaluate(version, source);
    } catch (RuntimeException failure) {
      // Any failure while evaluating at S(T) fails the whole request (chapter 05 section 3.7).
      throw session.markTerminal(failure);
    }
    decisions.put(version, decision);
    return decision;
  }

  private long required(String key) {
    Optional<byte[]> value = session.dynamicProperty(key.getBytes(StandardCharsets.UTF_8));
    if (!value.isPresent()) {
      throw session.markTerminal(new HistoricalQueryCapabilityException(
          HistoricalQueryCapabilityException.UNSUPPORTED_STATE,
          "Fork evaluation input is missing at the historical block: " + key));
    }
    return ByteArray.toLong(value.get());
  }

  private final class SessionForkStateSource implements ForkStateSource {

    @Override
    public long latestBlockHeaderTimestamp() {
      return required("latest_block_header_timestamp");
    }

    @Override
    public long maintenanceTimeInterval() {
      return required("MAINTENANCE_TIME_INTERVAL");
    }

    @Override
    public byte[] statsByVersion(int version) {
      return session.dynamicProperty(("FORK_VERSION_" + version)
          .getBytes(StandardCharsets.UTF_8)).orElse(null);
    }

    @Override
    public boolean disableJavaLangMath() {
      return session.dynamicPropertyLong("CONSENSUS_LOGIC_OPTIMIZATION") == 1L;
    }

    @Override
    public long latestBlockHeaderNumber() {
      return required("latest_block_header_number");
    }
  }
}
