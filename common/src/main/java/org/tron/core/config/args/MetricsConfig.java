package org.tron.core.config.args;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Metrics configuration bean. Field names match config.conf keys under "node.metrics".
 * Contains nested sub-bean for the prometheus section.
 */
@Slf4j
@Getter
@Setter
public class MetricsConfig {

  private PrometheusConfig prometheus = new PrometheusConfig();

  @Getter
  @Setter
  public static class PrometheusConfig {
    private boolean enable = false;
    private int port = 9527;
    private DatabaseConfig database = new DatabaseConfig();
  }

  @Getter
  @Setter
  public static class DatabaseConfig {
    private boolean enable = false;
    private int statIntervalSeconds = 30;
  }

  // Defaults come from reference.conf (loaded globally via Configuration.java)

  /**
   * Create MetricsConfig from the "node.metrics" section of the application config.
   */
  public static MetricsConfig fromConfig(Config config) {
    Config section = config.getConfig("node.metrics");
    MetricsConfig metricsConfig = ConfigBeanFactory.create(section, MetricsConfig.class);
    int interval = metricsConfig.getPrometheus().getDatabase().getStatIntervalSeconds();
    if (interval < 5 || interval > 3600) {
      throw new IllegalArgumentException(
          "node.metrics.prometheus.database.statIntervalSeconds must be between 5 and 3600, got "
              + interval);
    }
    return metricsConfig;
  }
}
