package org.tron.core.config.args;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Local witness configuration bean.
 * Reads top-level config keys: localwitness, localWitnessAccountAddress,
 * localPqWitnessAccountAddress, localwitnesskeystore, and
 * localwitness_pq.keys. These are not under a sub-section — they are at the
 * root of config.conf. ECDSA and PQ witness accounts use independent
 * `*AccountAddress` keys so the two consensus paths do not interfere.
 */
@Slf4j
@Getter
public class LocalWitnessConfig {

  /** Path of the PQ witness key list within config.conf. */
  public static final String PQ_KEYS_PATH = "localwitness_pq.keys";

  private List<String> privateKeys = new ArrayList<>();
  private String accountAddress = null;
  private String pqAccountAddress = null;
  private List<String> keystores = new ArrayList<>();
  private List<PqEntryConfig> pqEntries = Collections.emptyList();

  public static LocalWitnessConfig fromConfig(Config config) {
    LocalWitnessConfig lw = new LocalWitnessConfig();
    if (config.hasPath("localwitness")) {
      lw.privateKeys = config.getStringList("localwitness");
    }
    if (config.hasPath("localWitnessAccountAddress")) {
      lw.accountAddress = config.getString("localWitnessAccountAddress");
    }
    if (config.hasPath("localPqWitnessAccountAddress")) {
      lw.pqAccountAddress = config.getString("localPqWitnessAccountAddress");
    }
    if (config.hasPath("localwitnesskeystore")) {
      lw.keystores = config.getStringList("localwitnesskeystore");
    }
    if (config.hasPath(PQ_KEYS_PATH)) {
      List<? extends Config> raw = config.getConfigList(PQ_KEYS_PATH);
      List<PqEntryConfig> entries = new ArrayList<>(raw.size());
      for (int i = 0; i < raw.size(); i++) {
        Config entry = raw.get(i);
        String scheme = entry.hasPath("scheme") ? entry.getString("scheme") : null;
        String key = entry.hasPath("key") ? entry.getString("key") : null;
        String seed = entry.hasPath("seed") ? entry.getString("seed") : null;
        entries.add(new PqEntryConfig(i, scheme, key, seed));
      }
      lw.pqEntries = entries;
    }
    return lw;
  }
}
