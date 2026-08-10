package org.tron.core.config.args;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class MetricsConfigTest {

  @Test
  public void defaultsKeepDatabaseMetricsDisabled() {
    MetricsConfig config = MetricsConfig.fromConfig(ConfigFactory.load());

    assertFalse(config.getPrometheus().isEnable());
    assertFalse(config.getPrometheus().getDatabase().isEnable());
    assertEquals(30,
        config.getPrometheus().getDatabase().getStatIntervalSeconds());
  }

  @Test
  public void databaseMetricsCanUseBenchmarkInterval() {
    Config config = ConfigFactory.parseString(
        "node.metrics.prometheus { enable = true, database { enable = true, "
            + "statIntervalSeconds = 10 } }")
        .withFallback(ConfigFactory.load());

    MetricsConfig metrics = MetricsConfig.fromConfig(config);

    assertTrue(metrics.getPrometheus().isEnable());
    assertTrue(metrics.getPrometheus().getDatabase().isEnable());
    assertEquals(10, metrics.getPrometheus().getDatabase().getStatIntervalSeconds());
  }

  @Test
  public void databaseStatIntervalRejectsUnsafeValues() {
    Config config = ConfigFactory.parseString(
        "node.metrics.prometheus.database.statIntervalSeconds = 1")
        .withFallback(ConfigFactory.load());

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> MetricsConfig.fromConfig(config));

    assertTrue(exception.getMessage().contains("must be between 5 and 3600"));
  }
}
